//! Election logic: unhealthy cluster processing, healthiest node determination,
//! stale primary detection, and failover key / designated candidate handling.

use tracing::{error, info, warn};

use crate::cluster::SyncState;

use super::identity::{
    self, ElectionStance, MarkedElectionGate, MarkedGateAction, PgIdentity, election_stance,
    marked_election_gate, marked_gate_action,
};
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

        // D5: a held node never campaigns (and is stopped if started by hand).
        if let Some(r) = self.enforce_rewind_hold().await {
            return r;
        }

        // ─── May this node take part at all? (see `identity`) ───
        let identity = self.pg_identity();
        match election_stance(identity, self.stop_pending) {
            ElectionStance::Participate => {}
            ElectionStance::StandAside => {
                error!(
                    "PostgreSQL is in recovery pg-ha did not create; not taking part in the \
                     election and not rewinding it. Operator action required"
                );
                let _ = self.touch_member().await;
                return CycleResult::Error(
                    "PostgreSQL in unmanaged recovery; not taking part in the election".into(),
                );
            }
            ElectionStance::Fence => {
                warn!(
                    "A previous stop after writing standby.signal failed and PostgreSQL may still \
                     be writable — stopping it again before any election"
                );
                return match self
                    .fence_primary("fast", "stopped again after a failed demotion stop", None)
                    .await
                {
                    Ok(()) => CycleResult::Follower(
                        "stopped PostgreSQL: a demotion stop had failed".into(),
                    ),
                    Err(e) => CycleResult::Error(format!("Fence stop failed: {e}")),
                };
            }
            ElectionStance::Wait => {
                let _ = self.touch_member().await;
                return CycleResult::Follower(format!(
                    "PostgreSQL is {}; not taking part in the election this cycle",
                    identity.describe()
                ));
            }
        }

        // ─── D6: a node with the rewind-check marker campaigns only if its
        // published timeline is >= the recorded cluster timeline ───
        let gate = marked_election_gate(
            self.has_rewind_check_marker(),
            self.own_published_timeline(),
            self.cluster.timeline,
            self.max_peer_timeline(),
        );
        if gate == MarkedElectionGate::AllowedIncompleteEvidence {
            warn!(
                "No /timeline key in DCS; comparing this node's timeline with the timelines members published. The evidence is incomplete"
            );
        }
        let stand_aside = match gate {
            MarkedElectionGate::Behind { own, cluster } => {
                info!(
                    "Not taking part in the election: rewind-check marker present and this node's timeline {own} is behind the cluster timeline {cluster}; waiting for a leader to rewind against"
                );
                format!(
                    "rewind-check marker present and timeline {own} is behind the cluster timeline {cluster}; not taking part in the election"
                )
            }
            MarkedElectionGate::OwnTimelineUnknown => {
                info!(
                    "Not taking part in the election this cycle: rewind-check marker present and this node's timeline is not published yet"
                );
                "rewind-check marker present and own timeline not published yet; not taking part in the election this cycle".to_string()
            }
            MarkedElectionGate::Allowed | MarkedElectionGate::AllowedIncompleteEvidence => {
                String::new()
            }
        };
        match marked_gate_action(gate, identity) {
            MarkedGateAction::Proceed => {}
            MarkedGateAction::StandAside => {
                let _ = self.touch_member().await;
                return CycleResult::Follower(stand_aside);
            }
            MarkedGateAction::StopThenStandAside => {
                warn!(
                    "Rewind-check marker present and not eligible for the election while PostgreSQL is a primary without the leader lock — stopping PostgreSQL"
                );
                // standby.signal + marker first; a failed stop sets
                // stop_pending, so the next cycle fences it again. A
                // successful stop is already published by fence_primary.
                if let Err(e) = self
                    .fence_primary(
                        "fast",
                        "stopped: marked node not eligible for the election while running as a primary",
                        None,
                    )
                    .await
                {
                    return CycleResult::Error(format!(
                        "stop failed for a marked, ineligible primary: {e}"
                    ));
                }
                return CycleResult::Follower(format!("{stand_aside}; stopped PostgreSQL"));
            }
        }

        // ─── Stale primary detection ───
        // If PG is observed as a primary AND we're not the healthiest node,
        // this is a former primary that restarted after failover. With no
        // lock holder there is no ready pg_rewind source (A12), so it is only
        // stopped (A22): it starts as a standby (with the rewind-check
        // marker) and is rewound once a leader runs as a primary.
        if identity == PgIdentity::Primary
            && !self.is_healthiest_node()
            && !self.cluster.members.is_empty()
        {
            info!(
                "Stale primary detected (no leader visible, not healthiest) — stopping PostgreSQL; it will start as a standby and be rewound once a leader runs as a primary"
            );
            // No lock holder, so no upstream (D3).
            if let Err(e) = self
                .fence_primary("fast", "stopped as a stale primary", None)
                .await
            {
                return CycleResult::Error(format!("stale primary: stop failed: {e}"));
            }
            let _ = self.touch_member().await;
            return CycleResult::Follower(
                "stale primary: stopped PostgreSQL; it will start as a standby and be rewound once a leader runs as a primary".into(),
            );
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
        // A25: a designated candidate that has the marker and may not take
        // part in the election would leave the cluster without a leader.
        let designated_candidate = designated_candidate.filter(|c| {
            let may = self.cluster.get_member(c).map(|m| {
                identity::peer_may_campaign(
                    m.rewind_check,
                    m.timeline,
                    self.cluster.timeline,
                    identity::max_timeline_excluding(&self.cluster.members, &m.name),
                )
            });
            let honour = identity::honour_designated_candidate(*c == self.config.name, may);
            if !honour {
                warn!(
                    candidate = %c,
                    "Designated candidate {c} has a rewind-check marker and may not take part in the election; proceeding as if no candidate were designated"
                );
            }
            honour
        });

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
            let anchor = super::lease::LeaseAnchor::now();
            match self.dcs.attempt_to_acquire_leader().await {
                Ok(true) => {
                    self.is_leader = true;
                    self.extend_leader_lease(anchor);
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
                None => {
                    CycleResult::Follower("not the healthiest node, waiting for election".into())
                }
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
            // A18: do not defer to a peer that will not campaign under the
            // marked-node rule (the same rule it applies to itself).
            if !identity::peer_may_campaign(
                member.rewind_check,
                member.timeline,
                self.cluster.timeline,
                identity::max_timeline_excluding(&self.cluster.members, &member.name),
            ) {
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
        assert!(!sync_failover_allowed(
            "*",
            true,
            Some(&sync("node1", Some("*")))
        ));
    }

    #[test]
    fn sync_on_listed_member_allowed() {
        let state = sync("node1", Some("node2,node3"));
        assert!(sync_failover_allowed("node2", true, Some(&state)));
        assert!(sync_failover_allowed("NODE3", true, Some(&state))); // case-insensitive via matches
        assert!(!sync_failover_allowed("node4", true, Some(&state)));
    }
}
