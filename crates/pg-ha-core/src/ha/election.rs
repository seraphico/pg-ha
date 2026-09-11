//! Election logic: unhealthy cluster processing, healthiest node determination,
//! stale primary detection, and failover key / designated candidate handling.

use tracing::info;

use crate::cluster::SyncState;

use super::{CycleResult, Ha};

/// Whether `name` may acquire the leader lock under current sync settings.
///
/// When sync mode is off, always true. When on, only names listed in
/// `/sync.sync_standby` are allowed; missing/empty/`*` means nobody.
pub(crate) fn sync_failover_allowed(
    name: &str,
    sync_mode_enabled: bool,
    sync_state: Option<&SyncState>,
) -> bool {
    if !sync_mode_enabled {
        return true;
    }
    let Some(state) = sync_state else {
        return false;
    };
    let Some(raw) = state.sync_standby.as_deref() else {
        return false;
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed == "*" {
        return false;
    }
    state.matches(name)
}

impl Ha {
    /// Cluster has no leader — run election
    pub(super) async fn process_unhealthy_cluster(&mut self) -> CycleResult {
        if self.is_paused {
            return CycleResult::Follower("no action in pause mode (cluster has no leader)".into());
        }

        // ─── Stale primary detection ───
        // If PG is running without standby.signal AND we're not the healthiest node,
        // this is a former primary that restarted after failover. It needs to rejoin
        // as a replica even though we can't see the new leader in DCS yet.
        if self.postgresql.is_running()
            && !self.has_standby_signal()
            && !self.is_healthiest_node()
            && !self.cluster.members.is_empty()
        {
            // Prefer the leader member, then any primary-role member, then any running member
            let source_member = self
                .cluster
                .leader
                .as_ref()
                .and_then(|l| self.cluster.get_member(&l.name))
                .filter(|m| m.state == crate::cluster::MemberState::Running)
                .or_else(|| {
                    self.cluster.members.iter().find(|m| {
                        m.name != self.config.name
                            && m.role == crate::cluster::MemberRole::Primary
                            && m.state == crate::cluster::MemberState::Running
                    })
                })
                .or_else(|| {
                    self.cluster.members.iter().find(|m| {
                        m.name != self.config.name
                            && m.state == crate::cluster::MemberState::Running
                    })
                });
            if let Some(source) = source_member {
                info!(
                    source = %source.name,
                    "Stale primary detected (no leader visible, not healthiest) — initiating rejoin"
                );
                let source_name = source.name.clone();
                return self.rejoin_as_replica(&source_name).await;
            }
        }

        // ─── Normal election logic ───
        // Replicas (with standby.signal) participate normally in elections.
        // They can become the new primary if they are the healthiest node.

        // Check if there's a pending switchover/failover request with a designated candidate.
        // If so, only the candidate should attempt to acquire the lock.
        let designated_candidate = self
            .cluster
            .failover
            .as_ref()
            .and_then(|f| f.candidate.as_deref());

        let should_attempt = if let Some(candidate) = designated_candidate {
            if candidate == self.config.name {
                let allowed = sync_failover_allowed(
                    &self.config.name,
                    self.dynamic_config_state
                        .last_config
                        .synchronous_mode
                        .unwrap_or(false),
                    self.cluster.sync_state.as_ref(),
                );
                if allowed {
                    info!(
                        candidate,
                        "This node is the designated switchover candidate — attempting lock acquisition"
                    );
                    true
                } else {
                    info!(
                        candidate,
                        "Designated candidate is not sync-eligible — not acquiring lock"
                    );
                    false
                }
            } else {
                false
            }
        } else {
            self.is_healthiest_node()
        };

        if should_attempt {
            match self.dcs.attempt_to_acquire_leader().await {
                Ok(true) => {
                    self.is_leader = true;
                    // Clear the failover key after successful acquisition
                    let _ = self.dcs.set_failover_value("").await;
                    self.enforce_primary_role().await
                }
                Ok(false) => {
                    self.is_leader = false;
                    CycleResult::Follower("failed to acquire lock, following new leader".into())
                }
                Err(e) => CycleResult::Error(format!("Leader acquisition error: {e}")),
            }
        } else {
            self.is_leader = false;
            match designated_candidate {
                Some(candidate) if candidate == self.config.name => CycleResult::Follower(
                    "designated candidate is not sync-eligible — not acquiring lock".into(),
                ),
                Some(candidate) => CycleResult::Follower(format!(
                    "deferring to designated candidate ({candidate})"
                )),
                None => CycleResult::Follower(
                    "not the healthiest node, waiting for election".into(),
                ),
            }
        }
    }

    /// Determine if this node is the healthiest and should attempt promotion.
    ///
    /// A node is "healthiest" if:
    /// - It is not tagged nofailover and failover_priority > 0
    /// - It has PostgreSQL running
    /// - It has the highest WAL position among all eligible candidates
    /// - In case of tie: higher failover_priority wins, then alphabetical name
    pub(super) fn is_healthiest_node(&self) -> bool {
        // Self-check first (cheap)
        if self.config.tags.nofailover || self.config.tags.failover_priority == 0 {
            return false;
        }
        if !self.postgresql.is_running() {
            return false;
        }

        let sync_mode = self
            .dynamic_config_state
            .last_config
            .synchronous_mode
            .unwrap_or(false);
        if !sync_failover_allowed(
            &self.config.name,
            sync_mode,
            self.cluster.sync_state.as_ref(),
        ) {
            return false;
        }

        // Get our info from cluster membership (published via touch_member)
        let my_name = &self.config.name;
        let my_member = self.cluster.get_member(my_name);
        let my_wal = my_member.and_then(|m| m.wal_position).unwrap_or(0);
        let my_priority = self.config.tags.failover_priority;

        // Compare against all other members
        for member in &self.cluster.members {
            if member.name == *my_name {
                continue;
            }
            // Skip ineligible members
            if member.is_nofailover() || member.failover_priority() == 0 {
                continue;
            }
            // Skip members not running
            if member.state != crate::cluster::MemberState::Running {
                continue;
            }
            if !sync_failover_allowed(&member.name, sync_mode, self.cluster.sync_state.as_ref()) {
                continue;
            }

            let their_wal = member.wal_position.unwrap_or(0);
            let their_priority = member.failover_priority();
            let _their_timeline = member.timeline;

            // Compare: higher WAL position wins
            if their_wal > my_wal {
                return false; // Someone else has more data
            }
            if their_wal == my_wal {
                // Tie-break: higher failover_priority wins
                if their_priority > my_priority {
                    return false;
                }
                if their_priority == my_priority {
                    // Tie-break: alphabetical name (lower wins for determinism)
                    if member.name < *my_name {
                        return false;
                    }
                }
            }
        }

        true
    }
}

#[cfg(test)]
mod sync_failover_tests {
    use super::sync_failover_allowed;
    use crate::cluster::SyncState;

    fn sync(leader: &str, standbys: Option<&str>) -> SyncState {
        SyncState {
            leader: leader.to_string(),
            sync_standby: standbys.map(|s| s.to_string()),
            quorum: 1,
        }
    }

    #[test]
    fn sync_off_allows_anyone() {
        assert!(sync_failover_allowed("node3", false, None));
        assert!(sync_failover_allowed(
            "node3",
            false,
            Some(&sync("node1", Some("node2")))
        ));
    }

    #[test]
    fn sync_on_missing_state_denies() {
        assert!(!sync_failover_allowed("node2", true, None));
    }

    #[test]
    fn sync_on_empty_standby_denies() {
        assert!(!sync_failover_allowed(
            "node2",
            true,
            Some(&sync("node1", None))
        ));
        assert!(!sync_failover_allowed(
            "node2",
            true,
            Some(&sync("node1", Some("")))
        ));
    }

    #[test]
    fn sync_on_star_placeholder_denies_all() {
        assert!(!sync_failover_allowed(
            "node2",
            true,
            Some(&sync("node1", Some("*")))
        ));
        assert!(!sync_failover_allowed("*", true, Some(&sync("node1", Some("*")))));
    }

    #[test]
    fn sync_on_listed_member_allowed() {
        let state = sync("node1", Some("node2,node3"));
        assert!(sync_failover_allowed("node2", true, Some(&state)));
        assert!(sync_failover_allowed("NODE3", true, Some(&state))); // case-insensitive via matches
        assert!(!sync_failover_allowed("node4", true, Some(&state)));
    }
}
