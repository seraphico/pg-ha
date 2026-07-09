//! Cascading Replication Support
//!
//! Enables replicas to stream WAL from other replicas instead of directly
//! from the primary. This is controlled by the `replicatefrom` tag on a node.
//!
//! Key behaviors:
//! - If a node has `replicatefrom` set to another member's name, it streams from that member
//! - If the replicatefrom source is unavailable, it falls back to the primary
//! - The cascade topology is exposed in the /cluster API response
//!
//! Equivalent to Patroni's cascading replication via the `replicatefrom` tag.

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::cluster::{Cluster, Member, MemberState};

/// A node in the cascade topology tree, used for /cluster API response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CascadeNode {
    /// Member name
    pub name: String,

    /// Role of this member (primary, replica, standby_leader)
    pub role: String,

    /// The upstream node this member replicates from (None for primary)
    pub upstream: Option<String>,

    /// The configured replicatefrom tag value (None if not set or if following primary)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replicatefrom: Option<String>,
}

/// Manages cascading replication decisions.
///
/// Responsible for:
/// - Selecting the correct upstream for a replica based on its replicatefrom tag
/// - Falling back to the primary when the cascade source is unavailable
/// - Detecting cascade source failures
/// - Building the cascade topology for API responses
pub struct CascadeManager;

impl CascadeManager {
    /// Select the upstream node for a given member.
    ///
    /// Logic:
    /// 1. Check if the member has a `replicatefrom` tag
    /// 2. If yes, look up that member in the cluster and verify it's healthy
    /// 3. If the tagged source is available, return it
    /// 4. Otherwise, fall back to the primary (leader)
    ///
    /// Returns the name of the upstream node, or None if no valid upstream exists.
    pub fn select_upstream<'a>(
        member_name: &str,
        replicatefrom: Option<&str>,
        cluster: &'a Cluster,
    ) -> Option<&'a Member> {
        // If replicatefrom is set, try to use that member as upstream
        if let Some(source_name) = replicatefrom {
            if source_name == member_name {
                warn!(
                    member = member_name,
                    "replicatefrom points to self, falling back to primary"
                );
            } else if let Some(source) = cluster.get_member(source_name) {
                if Self::is_cascade_source_healthy(source_name, cluster) {
                    info!(
                        member = member_name,
                        upstream = source_name,
                        "using cascade source from replicatefrom tag"
                    );
                    return Some(source);
                } else {
                    warn!(
                        member = member_name,
                        source = source_name,
                        "cascade source is not healthy, falling back to primary"
                    );
                }
            } else {
                warn!(
                    member = member_name,
                    source = source_name,
                    "cascade source not found in cluster, falling back to primary"
                );
            }
        }

        // Fall back to primary (leader)
        Self::get_primary_member(cluster)
    }

    /// Build a primary_conninfo string pointing to the selected upstream node.
    ///
    /// This connection string is used in PostgreSQL recovery configuration
    /// to establish streaming replication from the upstream.
    pub fn build_primary_conninfo(
        upstream: &Member,
        replication_user: &str,
        replication_password: Option<&str>,
        application_name: &str,
    ) -> String {
        // Extract host and port from the member's conn_url
        // conn_url format: "host=X port=Y dbname=Z user=W"
        let (host, port) = Self::parse_host_port_from_conn_url(&upstream.conn_url);

        let mut conninfo = format!(
            "host={} port={} user={} application_name={}",
            host, port, replication_user, application_name
        );
        if let Some(password) = replication_password {
            conninfo.push_str(&format!(" password={password}"));
        }
        conninfo
    }

    /// Check if a cascade source member is healthy and suitable for streaming.
    ///
    /// A cascade source is considered healthy if:
    /// - It exists in the cluster members list
    /// - Its state is Running
    pub fn is_cascade_source_healthy(source_name: &str, cluster: &Cluster) -> bool {
        cluster
            .get_member(source_name)
            .is_some_and(|m| m.state == MemberState::Running)
    }

    /// Build the cascade topology for the /cluster API response.
    ///
    /// For each member, determines its upstream based on:
    /// - Primary has no upstream
    /// - Members with replicatefrom tag point to that source (if healthy) or primary
    /// - Other replicas point to the primary
    pub fn build_cascade_topology(cluster: &Cluster) -> Vec<CascadeNode> {
        let primary_name = cluster
            .leader
            .as_ref()
            .map(|l| l.name.as_str());

        cluster
            .members
            .iter()
            .map(|member| {
                let is_primary = primary_name == Some(member.name.as_str());

                if is_primary {
                    CascadeNode {
                        name: member.name.clone(),
                        role: format!("{}", member.role),
                        upstream: None,
                        replicatefrom: None,
                    }
                } else {
                    // Check replicatefrom tag
                    let replicatefrom_tag = member
                        .tags
                        .get("replicatefrom")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());

                    let upstream = if let Some(ref source_name) = replicatefrom_tag {
                        // If the source is healthy, use it; otherwise fall back to primary
                        if Self::is_cascade_source_healthy(source_name, cluster)
                            && source_name.as_str() != member.name.as_str()
                        {
                            Some(source_name.clone())
                        } else {
                            primary_name.map(|n| n.to_string())
                        }
                    } else {
                        // No replicatefrom tag — replicate from primary
                        primary_name.map(|n| n.to_string())
                    };

                    CascadeNode {
                        name: member.name.clone(),
                        role: format!("{}", member.role),
                        upstream,
                        replicatefrom: replicatefrom_tag,
                    }
                }
            })
            .collect()
    }

    // ─────────────────── Private helpers ───────────────────

    /// Get the primary member from the cluster (based on leader lock).
    fn get_primary_member(cluster: &Cluster) -> Option<&Member> {
        cluster
            .leader
            .as_ref()
            .and_then(|leader| cluster.get_member(&leader.name))
    }

    /// Parse host and port from a PostgreSQL connection string.
    /// Expected format: "host=X port=Y ..."
    fn parse_host_port_from_conn_url(conn_url: &str) -> (String, u16) {
        let mut host = "127.0.0.1".to_string();
        let mut port: u16 = 5432;

        for part in conn_url.split_whitespace() {
            if let Some(val) = part.strip_prefix("host=") {
                host = val.to_string();
            } else if let Some(val) = part.strip_prefix("port=") {
                port = val.parse().unwrap_or(5432);
            }
        }

        (host, port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{Leader, Member, MemberRole, MemberState};
    use std::collections::HashMap;

    fn make_member(name: &str, state: MemberState, role: MemberRole, tags: HashMap<String, serde_json::Value>) -> Member {
        Member {
            name: name.to_string(),
            conn_url: format!("host={name} port=5432 dbname=postgres user=postgres"),
            api_url: format!("http://{name}:8008"),
            state,
            role,
            wal_position: Some(1000),
            timeline: Some(1),
            tags,
            version: None,
        }
    }

    fn make_cluster_3_nodes() -> Cluster {
        Cluster {
            leader: Some(Leader {
                name: "node1".to_string(),
                version: 1,
            }),
            members: vec![
                make_member("node1", MemberState::Running, MemberRole::Primary, HashMap::new()),
                make_member("node2", MemberState::Running, MemberRole::Replica, HashMap::new()),
                make_member(
                    "node3",
                    MemberState::Running,
                    MemberRole::Replica,
                    HashMap::from([("replicatefrom".to_string(), serde_json::json!("node2"))]),
                ),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn test_select_upstream_with_replicatefrom_tag() {
        let cluster = make_cluster_3_nodes();

        // node3 has replicatefrom=node2, should select node2
        let upstream = CascadeManager::select_upstream("node3", Some("node2"), &cluster);
        assert!(upstream.is_some());
        assert_eq!(upstream.unwrap().name, "node2");
    }

    #[test]
    fn test_select_upstream_fallback_to_primary() {
        let mut cluster = make_cluster_3_nodes();
        // Make node2 unhealthy
        cluster.members[1].state = MemberState::Stopped;

        // node3 has replicatefrom=node2 but node2 is stopped, should fall back to node1 (primary)
        let upstream = CascadeManager::select_upstream("node3", Some("node2"), &cluster);
        assert!(upstream.is_some());
        assert_eq!(upstream.unwrap().name, "node1");
    }

    #[test]
    fn test_select_upstream_source_not_found() {
        let cluster = make_cluster_3_nodes();

        // replicatefrom points to non-existent node
        let upstream = CascadeManager::select_upstream("node3", Some("node99"), &cluster);
        assert!(upstream.is_some());
        assert_eq!(upstream.unwrap().name, "node1"); // falls back to primary
    }

    #[test]
    fn test_select_upstream_self_reference() {
        let cluster = make_cluster_3_nodes();

        // replicatefrom points to self — should fall back to primary
        let upstream = CascadeManager::select_upstream("node3", Some("node3"), &cluster);
        assert!(upstream.is_some());
        assert_eq!(upstream.unwrap().name, "node1");
    }

    #[test]
    fn test_select_upstream_no_replicatefrom() {
        let cluster = make_cluster_3_nodes();

        // No replicatefrom — should use primary
        let upstream = CascadeManager::select_upstream("node2", None, &cluster);
        assert!(upstream.is_some());
        assert_eq!(upstream.unwrap().name, "node1");
    }

    #[test]
    fn test_select_upstream_no_primary() {
        let cluster = Cluster {
            leader: None,
            members: vec![
                make_member("node1", MemberState::Running, MemberRole::Replica, HashMap::new()),
                make_member("node2", MemberState::Running, MemberRole::Replica, HashMap::new()),
            ],
            ..Default::default()
        };

        // No leader — no upstream available
        let upstream = CascadeManager::select_upstream("node2", None, &cluster);
        assert!(upstream.is_none());
    }

    #[test]
    fn test_build_primary_conninfo() {
        let member = make_member("node2", MemberState::Running, MemberRole::Replica, HashMap::new());

        let conninfo = CascadeManager::build_primary_conninfo(
            &member,
            "replicator",
            Some("secret"),
            "node3",
        );

        assert!(conninfo.contains("host=node2"));
        assert!(conninfo.contains("port=5432"));
        assert!(conninfo.contains("user=replicator"));
        assert!(conninfo.contains("password=secret"));
        assert!(conninfo.contains("application_name=node3"));
    }

    #[test]
    fn test_build_primary_conninfo_no_password() {
        let member = make_member("node1", MemberState::Running, MemberRole::Primary, HashMap::new());

        let conninfo = CascadeManager::build_primary_conninfo(
            &member,
            "replicator",
            None,
            "node2",
        );

        assert!(conninfo.contains("host=node1"));
        assert!(conninfo.contains("port=5432"));
        assert!(conninfo.contains("user=replicator"));
        assert!(!conninfo.contains("password="));
        assert!(conninfo.contains("application_name=node2"));
    }

    #[test]
    fn test_is_cascade_source_healthy_running() {
        let cluster = make_cluster_3_nodes();
        assert!(CascadeManager::is_cascade_source_healthy("node2", &cluster));
    }

    #[test]
    fn test_is_cascade_source_healthy_stopped() {
        let mut cluster = make_cluster_3_nodes();
        cluster.members[1].state = MemberState::Stopped;
        assert!(!CascadeManager::is_cascade_source_healthy("node2", &cluster));
    }

    #[test]
    fn test_is_cascade_source_healthy_not_found() {
        let cluster = make_cluster_3_nodes();
        assert!(!CascadeManager::is_cascade_source_healthy("node99", &cluster));
    }

    #[test]
    fn test_build_cascade_topology() {
        let cluster = make_cluster_3_nodes();
        let topology = CascadeManager::build_cascade_topology(&cluster);

        assert_eq!(topology.len(), 3);

        // node1 is primary — no upstream
        let node1 = topology.iter().find(|n| n.name == "node1").unwrap();
        assert_eq!(node1.role, "primary");
        assert_eq!(node1.upstream, None);
        assert_eq!(node1.replicatefrom, None);

        // node2 is replica — upstream is primary (node1)
        let node2 = topology.iter().find(|n| n.name == "node2").unwrap();
        assert_eq!(node2.role, "replica");
        assert_eq!(node2.upstream, Some("node1".to_string()));
        assert_eq!(node2.replicatefrom, None);

        // node3 is replica with replicatefrom=node2 — upstream is node2
        let node3 = topology.iter().find(|n| n.name == "node3").unwrap();
        assert_eq!(node3.role, "replica");
        assert_eq!(node3.upstream, Some("node2".to_string()));
        assert_eq!(node3.replicatefrom, Some("node2".to_string()));
    }

    #[test]
    fn test_build_cascade_topology_unhealthy_source() {
        let mut cluster = make_cluster_3_nodes();
        // Make node2 unhealthy
        cluster.members[1].state = MemberState::Stopped;

        let topology = CascadeManager::build_cascade_topology(&cluster);

        // node3 has replicatefrom=node2 but node2 is stopped — falls back to primary
        let node3 = topology.iter().find(|n| n.name == "node3").unwrap();
        assert_eq!(node3.upstream, Some("node1".to_string()));
        assert_eq!(node3.replicatefrom, Some("node2".to_string()));
    }

    #[test]
    fn test_parse_host_port_from_conn_url() {
        let (host, port) = CascadeManager::parse_host_port_from_conn_url(
            "host=10.0.0.5 port=5433 dbname=postgres user=postgres"
        );
        assert_eq!(host, "10.0.0.5");
        assert_eq!(port, 5433);
    }

    #[test]
    fn test_parse_host_port_from_conn_url_defaults() {
        let (host, port) = CascadeManager::parse_host_port_from_conn_url("dbname=postgres");
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 5432);
    }

    #[test]
    fn test_cascade_node_serialization() {
        let node = CascadeNode {
            name: "node3".to_string(),
            role: "replica".to_string(),
            upstream: Some("node2".to_string()),
            replicatefrom: Some("node2".to_string()),
        };

        let json = serde_json::to_value(&node).unwrap();
        assert_eq!(json["name"], "node3");
        assert_eq!(json["role"], "replica");
        assert_eq!(json["upstream"], "node2");
        assert_eq!(json["replicatefrom"], "node2");
    }

    #[test]
    fn test_cascade_node_serialization_no_replicatefrom() {
        let node = CascadeNode {
            name: "node2".to_string(),
            role: "replica".to_string(),
            upstream: Some("node1".to_string()),
            replicatefrom: None,
        };

        let json = serde_json::to_value(&node).unwrap();
        assert_eq!(json["name"], "node2");
        assert_eq!(json["upstream"], "node1");
        // replicatefrom should be absent (skip_serializing_if)
        assert!(json.get("replicatefrom").is_none());
    }
}
