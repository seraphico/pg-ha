//! Election logic: unhealthy cluster processing, healthiest node determination,
//! stale primary detection, and failover key / designated candidate handling.

use tracing::{error, info, warn};

use crate::cluster::SyncState;

use super::helpers::OwnEligibility;
use super::identity::{self, ElectionStance, PgIdentity, election_stance};
use super::lineage::Ineligible;
use super::{CycleResult, Ha};
use crate::timeline_files::ACCEPT_TIMELINE_LOSS_FILE;

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

        // D6 / 2A first: data of an older history generation never
        // campaigns, and is never rebuilt by rewinding it (D5 Rebuild).
        if let Some(r) = self.enforce_generation().await {
            return r;
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

        // ─── D6 / I3: only a node on the latest timeline campaigns, marked
        // or not. A behind node would reuse a timeline number (I1) and drop
        // the newer history ───
        let stand_aside = match self.own_eligibility() {
            OwnEligibility::Eligible => None,
            OwnEligibility::Override(why) => {
                warn!(
                    "{ACCEPT_TIMELINE_LOSS_FILE} present and no other node may campaign: taking part in the election although {why}"
                );
                None
            }
            OwnEligibility::OverrideIgnored { why, eligible } => {
                warn!(
                    "{ACCEPT_TIMELINE_LOSS_FILE} present but {eligible} is on the latest timeline; ignoring the override"
                );
                Some(why)
            }
            OwnEligibility::Ineligible(why) => Some(why),
        };
        if let Some(why) = stand_aside {
            let message = format!("Not taking part in the election: {why}");
            match &why {
                Ineligible::DifferentHistory { .. }
                | Ineligible::UnrecordedNewer { .. }
                | Ineligible::LatestUnknown(_) => self.log_timeline_problem(message),
                _ => info!("{message}"),
            }
            let result = format!("not taking part in the election: {why}");
            if identity == PgIdentity::Primary {
                // L5: not eligible while observed writable without the lock.
                // standby.signal + marker first; a failed stop sets
                // stop_pending, so the next cycle fences it again.
                warn!(
                    "Not eligible for the election while PostgreSQL is a primary without the leader lock — stopping PostgreSQL"
                );
                if let Err(e) = self
                    .fence_primary(
                        "fast",
                        "stopped: not eligible for the election while running as a primary",
                        None,
                    )
                    .await
                {
                    return CycleResult::Error(format!(
                        "stop failed for an ineligible primary: {e}"
                    ));
                }
                return CycleResult::Follower(format!("{result}; stopped PostgreSQL"));
            }
            let _ = self.touch_member().await;
            return CycleResult::Follower(result);
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

        // A designated candidate is the only node that may acquire the lock.
        // A25: one that has the marker and may not campaign is ignored, and
        // the election continues as if no candidate were designated — which
        // still excludes the leader the switchover asked to step down.
        let raw_candidate = self
            .cluster
            .failover
            .as_ref()
            .and_then(|f| f.candidate.clone());
        let designated_candidate = self.honoured_candidate().map(str::to_owned);
        if let Some(c) = raw_candidate.as_deref()
            && designated_candidate.is_none()
        {
            warn!(
                candidate = %c,
                "Designated candidate {c} may not take part in the election (it is not shown to be on the latest timeline); proceeding as if no candidate were designated"
            );
        }

        let should_attempt = if let Some(candidate) = designated_candidate.as_deref() {
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
        } else if self.excluded_switchover_leader() == Some(self.config.name.as_str()) {
            info!(
                "Not taking part in the election: this node is the leader switchover asked to step down"
            );
            false
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
            match designated_candidate.as_deref() {
                Some(candidate) if candidate == self.config.name => CycleResult::Follower(
                    "designated candidate is not sync-eligible — not acquiring lock".into(),
                ),
                Some(candidate) => CycleResult::Follower(format!(
                    "deferring to designated candidate ({candidate})"
                )),
                None => {
                    if self.excluded_switchover_leader() == Some(self.config.name.as_str()) {
                        CycleResult::Follower(
                            "switchover asked this node to step down; not taking part in the election"
                                .into(),
                        )
                    } else {
                        CycleResult::Follower(
                            "not the healthiest node, waiting for election".into(),
                        )
                    }
                }
            }
        }
    }

    /// Designated candidate this node will defer to. `None` when the key has
    /// no candidate, or the candidate is in the member list and the marked-node
    /// rule says it will not campaign (A25). A missing member is still honoured.
    fn honoured_candidate(&self) -> Option<&str> {
        let candidate = self.cluster.failover.as_ref()?.candidate.as_deref()?;
        let may = self
            .cluster
            .get_member(candidate)
            .map(|m| self.member_eligibility(m).is_ok());
        identity::honour_designated_candidate(candidate == self.config.name, may)
            .then_some(candidate)
    }

    /// Leader a switchover named when this election is not reserved for a
    /// honoured candidate. Peers must not defer to it, and it must not
    /// acquire the lock: its WAL is the highest and its timeline still
    /// matches `/timeline`, so neither the healthiest-node comparison nor
    /// the rewind-check marker keeps it out.
    fn excluded_switchover_leader(&self) -> Option<&str> {
        if self.honoured_candidate().is_some() {
            return None;
        }
        self.cluster
            .failover
            .as_ref()
            .and_then(|f| f.leader.as_deref())
            .filter(|name| !name.is_empty())
    }

    /// Would the election that follows a no-candidate switchover have someone
    /// other than this node who is allowed to campaign? Checked before
    /// demotion: the stepped-down leader is excluded, so "no" means the
    /// cluster would be left with no one who may take the lock.
    pub(super) fn has_switchover_successor(&self) -> bool {
        let sync_mode = self
            .dynamic_config_state
            .last_config
            .synchronous_mode
            .unwrap_or(false);
        self.cluster.members.iter().any(|member| {
            member.name != self.config.name
                && !member.is_nofailover()
                && member.failover_priority() != 0
                && member.state == crate::cluster::MemberState::Running
                && sync_failover_allowed(&member.name, sync_mode, self.cluster.sync_state.as_ref())
                && self.member_eligibility(member).is_ok()
        })
    }

    /// Determine if this node is the healthiest and should attempt promotion.
    ///
    /// A node is "healthiest" if:
    /// - It is not tagged nofailover and failover_priority > 0
    /// - It has PostgreSQL running
    /// - It is not the leader a no-candidate switchover asked to step down
    /// - It may campaign under D6 / I3 (on the latest timeline, or the
    ///   operator override); peers that may not are not compared
    /// - Among eligible nodes, all on the latest timeline: highest WAL
    ///   position, then higher failover_priority, then alphabetical name.
    ///   The stepped-down switchover leader is not compared.
    pub(super) fn is_healthiest_node(&self) -> bool {
        // Self-check first (cheap)
        if self.config.tags.nofailover || self.config.tags.failover_priority == 0 {
            return false;
        }
        if !matches!(
            self.own_eligibility(),
            OwnEligibility::Eligible | OwnEligibility::Override(_)
        ) {
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

        // Owned so the member loop can borrow `self.cluster` afterwards.
        // A switchover with no honoured candidate names this leader so it
        // cannot win on WAL: it was the primary, and its timeline still
        // equals `/timeline`, so the rewind-check marker does not exclude it.
        let excluded_leader = self.excluded_switchover_leader().map(str::to_owned);
        if excluded_leader.as_deref() == Some(self.config.name.as_str()) {
            return false;
        }

        let my_name = &self.config.name;
        let my_member = self.cluster.get_member(my_name);
        let my_wal = my_member.and_then(|m| m.wal_position).unwrap_or(0);
        let my_priority = self.config.tags.failover_priority;

        // Compare against all other members
        for member in &self.cluster.members {
            if member.name == *my_name {
                continue;
            }
            if excluded_leader.as_deref() == Some(member.name.as_str()) {
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
            // A18 / I3: do not defer to a peer that may not campaign (the
            // same rule it applies to itself).
            if self.member_eligibility(member).is_err() {
                continue;
            }

            let their_wal = member.wal_position.unwrap_or(0);
            let their_priority = member.failover_priority();

            // Every eligible peer is on the latest timeline, like this node:
            // LSNs are comparable. Higher LSN wins.
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
