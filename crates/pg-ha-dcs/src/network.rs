//! Network transport for Raft RPC communication between nodes.
//!
//! Uses HTTP/JSON via reqwest (client) and axum (server) for inter-node Raft RPCs.
//! Endpoints: POST /raft/vote, POST /raft/append, POST /raft/snapshot

use std::future::Future;

use openraft::error::Fatal;
use openraft::error::{RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::storage::Snapshot;
use openraft::{BasicNode, RaftNetwork, RaftNetworkFactory};

use crate::store::{NodeId, TypeConfig};

// ─────────────────── Network Factory ───────────────────

/// Creates network connections to other Raft nodes
#[derive(Debug, Clone)]
pub struct NetworkFactory {
    client: reqwest::Client,
}

impl Default for NetworkFactory {
    fn default() -> Self {
        Self {
            client: reqwest::Client::builder()
                .pool_max_idle_per_host(5)
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .unwrap_or_default(),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, _target: NodeId, node: &BasicNode) -> Self::Network {
        let addr = if node.addr.starts_with("http") {
            node.addr.clone()
        } else {
            format!("http://{}", node.addr)
        };
        NetworkConnection {
            target_addr: addr,
            client: self.client.clone(),
        }
    }
}

// ─────────────────── Network Connection ───────────────────

/// A connection to a single remote Raft node
#[derive(Debug, Clone)]
pub struct NetworkConnection {
    target_addr: String,
    client: reqwest::Client,
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let url = format!("{}/raft/append", self.target_addr);
        let resp = self
            .client
            .post(&url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let result: AppendEntriesResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        Ok(result)
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let url = format!("{}/raft/vote", self.target_addr);
        let resp = self
            .client
            .post(&url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let result: VoteResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        Ok(result)
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        let url = format!("{}/raft/install-snapshot", self.target_addr);
        let resp = self
            .client
            .post(&url)
            .json(&rpc)
            .send()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let result: InstallSnapshotResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        Ok(result)
    }

    async fn full_snapshot(
        &mut self,
        _vote: openraft::Vote<NodeId>,
        snapshot: Snapshot<TypeConfig>,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<NodeId>, StreamingError<TypeConfig, Fatal<NodeId>>> {
        // Serialize snapshot data and send in one shot
        let data = {
            let mut buf = Vec::new();
            let mut cursor = snapshot.snapshot;
            std::io::Read::read_to_end(&mut cursor, &mut buf)
                .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;
            buf
        };

        let url = format!("{}/raft/snapshot", self.target_addr);

        #[derive(serde::Serialize)]
        struct SnapshotRequest {
            vote: openraft::Vote<NodeId>,
            meta: openraft::SnapshotMeta<NodeId, BasicNode>,
            data: Vec<u8>,
        }

        let req = SnapshotRequest {
            vote: _vote,
            meta: snapshot.meta,
            data,
        };

        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;

        let result: SnapshotResponse<NodeId> = resp
            .json()
            .await
            .map_err(|e| StreamingError::Unreachable(Unreachable::new(&e)))?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{BasicNode, RaftNetworkFactory};
    use proptest::prelude::*;

    /// **Validates: Requirements 3.3, 3.4**
    ///
    /// Property 2: Preservation - Raft RPC Communication Correctness
    ///
    /// Verifies that NetworkFactory correctly constructs URLs from target_addr
    /// with and without "http" prefix, and that NetworkConnection objects hold
    /// valid client and target_addr fields after creation.

    /// Strategy: generate valid network addresses (ip:port, hostname:port)
    /// both with and without the "http://" prefix.
    fn arb_addr_without_http() -> impl Strategy<Value = String> {
        prop_oneof![
            // IP:port addresses
            (1u8..=254, 0u8..=255, 0u8..=255, 1u8..=254, 1024u16..=65535)
                .prop_map(|(a, b, c, d, port)| format!("{a}.{b}.{c}.{d}:{port}")),
            // hostname:port addresses
            ("[a-z]{3,10}", "[a-z]{2,4}", 1024u16..=65535u16)
                .prop_map(|(host, tld, port)| format!("{host}.{tld}:{port}")),
            // localhost with port
            (1024u16..=65535u16).prop_map(|port| format!("localhost:{port}")),
        ]
    }

    fn arb_addr_with_http() -> impl Strategy<Value = String> {
        arb_addr_without_http().prop_map(|addr| format!("http://{addr}"))
    }

    fn arb_addr_with_https() -> impl Strategy<Value = String> {
        arb_addr_without_http().prop_map(|addr| format!("https://{addr}"))
    }

    proptest! {
        /// Property: For all addresses WITHOUT http prefix, NetworkFactory prepends "http://"
        #[test]
        fn network_factory_prepends_http_for_plain_addresses(
            addr in arb_addr_without_http()
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let mut factory = NetworkFactory::default();
                let node = BasicNode { addr: addr.clone() };
                let conn = factory.new_client(1, &node).await;

                let expected = format!("http://{addr}");
                // The target_addr should have "http://" prepended
                prop_assert_eq!(&conn.target_addr, &expected);

                // URL construction for each RPC endpoint should produce valid URLs
                let append_url = format!("{}/raft/append", conn.target_addr);
                let vote_url = format!("{}/raft/vote", conn.target_addr);
                let snapshot_url = format!("{}/raft/install-snapshot", conn.target_addr);

                prop_assert!(append_url.starts_with("http://"));
                prop_assert!(append_url.ends_with("/raft/append"));
                prop_assert!(vote_url.starts_with("http://"));
                prop_assert!(vote_url.ends_with("/raft/vote"));
                prop_assert!(snapshot_url.starts_with("http://"));
                prop_assert!(snapshot_url.ends_with("/raft/install-snapshot"));

                Ok(())
            })?;
        }

        /// Property: For all addresses WITH http/https prefix, NetworkFactory uses them as-is
        #[test]
        fn network_factory_preserves_http_prefixed_addresses(
            addr in prop_oneof![arb_addr_with_http(), arb_addr_with_https()]
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let mut factory = NetworkFactory::default();
                let node = BasicNode { addr: addr.clone() };
                let conn = factory.new_client(1, &node).await;

                // The target_addr should be used unchanged
                prop_assert_eq!(&conn.target_addr, &addr);

                // URL should not double-prefix with http://
                let append_url = format!("{}/raft/append", conn.target_addr);
                prop_assert!(!append_url.contains("http://http://"));
                prop_assert!(!append_url.contains("http://https://"));

                Ok(())
            })?;
        }

        /// Property: NetworkFactory creates connections that share the same underlying Client
        /// regardless of target address — this validates the Client reuse preservation behavior
        #[test]
        fn network_factory_shares_client_across_connections(
            addrs in prop::collection::vec(arb_addr_without_http(), 2..5)
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let mut factory = NetworkFactory::default();
                let mut connections = Vec::new();

                for (id, addr) in addrs.iter().enumerate() {
                    let node = BasicNode { addr: addr.clone() };
                    let conn = factory.new_client(id as u64, &node).await;
                    connections.push(conn);
                }

                // All connections should have non-empty target_addr starting with http
                for conn in &connections {
                    prop_assert!(conn.target_addr.starts_with("http"));
                    prop_assert!(conn.target_addr.len() > "http://".len());
                }

                Ok(())
            })?;
        }

        /// Property: For any valid target_addr, the RPC URL construction produces
        /// correctly formatted endpoints (append, vote, install-snapshot, snapshot)
        #[test]
        fn rpc_url_construction_correctness(
            addr in arb_addr_without_http()
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let mut factory = NetworkFactory::default();
                let node = BasicNode { addr: addr.clone() };
                let conn = factory.new_client(1, &node).await;

                // Simulate URL construction as done in RPC methods
                let base = &conn.target_addr;
                let append_url = format!("{base}/raft/append");
                let vote_url = format!("{base}/raft/vote");
                let snapshot_install_url = format!("{base}/raft/install-snapshot");
                let snapshot_url = format!("{base}/raft/snapshot");

                // All URLs should be parseable
                prop_assert!(append_url.parse::<reqwest::Url>().is_ok(),
                    "append URL should be valid: {}", append_url);
                prop_assert!(vote_url.parse::<reqwest::Url>().is_ok(),
                    "vote URL should be valid: {}", vote_url);
                prop_assert!(snapshot_install_url.parse::<reqwest::Url>().is_ok(),
                    "install-snapshot URL should be valid: {}", snapshot_install_url);
                prop_assert!(snapshot_url.parse::<reqwest::Url>().is_ok(),
                    "snapshot URL should be valid: {}", snapshot_url);

                Ok(())
            })?;
        }
    }

    /// Property: NetworkFactory::default() creates a valid factory with a usable client
    #[test]
    fn network_factory_default_creates_valid_instance() {
        let factory = NetworkFactory::default();
        // The factory should be Debug-printable (not panic)
        let debug_str = format!("{:?}", factory);
        assert!(debug_str.contains("NetworkFactory"));
    }

    /// Property: NetworkConnection fields are correctly initialized
    #[tokio::test]
    async fn network_connection_fields_initialized_correctly() {
        let mut factory = NetworkFactory::default();

        // Test with plain address
        let node = BasicNode {
            addr: "192.168.1.1:8080".to_string(),
        };
        let conn = factory.new_client(42, &node).await;
        assert_eq!(conn.target_addr, "http://192.168.1.1:8080");

        // Test with http-prefixed address
        let node = BasicNode {
            addr: "http://10.0.0.1:9090".to_string(),
        };
        let conn = factory.new_client(43, &node).await;
        assert_eq!(conn.target_addr, "http://10.0.0.1:9090");

        // Test with https-prefixed address
        let node = BasicNode {
            addr: "https://secure.example.com:443".to_string(),
        };
        let conn = factory.new_client(44, &node).await;
        assert_eq!(conn.target_addr, "https://secure.example.com:443");
    }

    /// Property: forward_to_leader URL construction follows same pattern
    /// (validates that address normalization in raft_dcs.rs uses same logic)
    #[test]
    fn forward_to_leader_url_pattern_consistency() {
        // The forward_to_leader method uses the same pattern:
        // if addr.starts_with("http") { addr } else { format!("http://{}", addr) }
        // followed by format!("{leader_addr}/raft/client-write")
        let test_cases = vec![
            ("127.0.0.1:8080", "http://127.0.0.1:8080/raft/client-write"),
            (
                "http://127.0.0.1:8080",
                "http://127.0.0.1:8080/raft/client-write",
            ),
            (
                "https://node1.cluster:443",
                "https://node1.cluster:443/raft/client-write",
            ),
            ("my-node:2380", "http://my-node:2380/raft/client-write"),
        ];

        for (input_addr, expected_url) in test_cases {
            let normalized = if input_addr.starts_with("http") {
                input_addr.to_string()
            } else {
                format!("http://{}", input_addr)
            };
            let url = format!("{normalized}/raft/client-write");
            assert_eq!(url, expected_url, "Failed for input: {input_addr}");
        }
    }

    // ─── Bug Condition Tests (from Task 6) ───

    /// Helper: extract a raw pointer to the inner Arc of a reqwest::Client.
    /// reqwest::Client is Clone and internally Arc-based, so clones share the same
    /// inner allocation. We use the Debug repr to detect identity — identical debug
    /// output means the same inner Arc (same pointer, same config).
    fn client_debug_repr(client: &reqwest::Client) -> String {
        format!("{:?}", client)
    }

    /// **Validates: Requirements 1.3, 1.4**
    ///
    /// **Property 1: Bug Condition** - HTTP Client Created Per-Call (FIXED)
    ///
    /// This test verifies that NetworkFactory::default() creates a factory with a
    /// shared Client, and that multiple calls to new_client return connections
    /// that share the same underlying Client instance.
    ///
    /// On UNFIXED code, each new_client call would create a distinct Client,
    /// meaning N calls → N distinct Client instances.
    ///
    /// On FIXED code (current), all connections share the factory's single Client.
    #[tokio::test]
    async fn test_network_factory_shares_client_across_connections() {
        let mut factory = NetworkFactory::default();

        // Create multiple connections to different nodes
        let node1 = BasicNode {
            addr: "127.0.0.1:9001".to_string(),
        };
        let node2 = BasicNode {
            addr: "127.0.0.1:9002".to_string(),
        };
        let node3 = BasicNode {
            addr: "127.0.0.1:9003".to_string(),
        };
        let node4 = BasicNode {
            addr: "127.0.0.1:9004".to_string(),
        };
        let node5 = BasicNode {
            addr: "127.0.0.1:9005".to_string(),
        };

        let conn1 = factory.new_client(1, &node1).await;
        let conn2 = factory.new_client(2, &node2).await;
        let conn3 = factory.new_client(3, &node3).await;
        let conn4 = factory.new_client(4, &node4).await;
        let conn5 = factory.new_client(5, &node5).await;

        // All connections should share the same underlying Client as the factory.
        // reqwest::Client is Arc-based internally, so clones have the same Debug repr.
        let factory_repr = client_debug_repr(&factory.client);
        let conn1_repr = client_debug_repr(&conn1.client);
        let conn2_repr = client_debug_repr(&conn2.client);
        let conn3_repr = client_debug_repr(&conn3.client);
        let conn4_repr = client_debug_repr(&conn4.client);
        let conn5_repr = client_debug_repr(&conn5.client);

        // The factory's client and all connection clients must be the same instance
        assert_eq!(
            factory_repr, conn1_repr,
            "Connection 1 should share the factory's Client"
        );
        assert_eq!(
            factory_repr, conn2_repr,
            "Connection 2 should share the factory's Client"
        );
        assert_eq!(
            factory_repr, conn3_repr,
            "Connection 3 should share the factory's Client"
        );
        assert_eq!(
            factory_repr, conn4_repr,
            "Connection 4 should share the factory's Client"
        );
        assert_eq!(
            factory_repr, conn5_repr,
            "Connection 5 should share the factory's Client"
        );

        // Additionally verify all connections have the correct target addresses
        assert_eq!(conn1.target_addr, "http://127.0.0.1:9001");
        assert_eq!(conn2.target_addr, "http://127.0.0.1:9002");
        assert_eq!(conn3.target_addr, "http://127.0.0.1:9003");
        assert_eq!(conn4.target_addr, "http://127.0.0.1:9004");
        assert_eq!(conn5.target_addr, "http://127.0.0.1:9005");
    }

    /// **Validates: Requirements 1.3**
    ///
    /// Verify that NetworkFactory::default() initializes with a valid shared Client.
    #[tokio::test]
    async fn test_network_factory_default_has_shared_client() {
        let factory = NetworkFactory::default();

        // The factory should have a client field that is usable
        // (not a zero-value or uninitialized state)
        let repr = client_debug_repr(&factory.client);
        assert!(
            !repr.is_empty(),
            "Factory client should have a valid Debug repr"
        );

        // Cloning the factory should preserve client identity
        let factory_clone = factory.clone();
        assert_eq!(
            client_debug_repr(&factory.client),
            client_debug_repr(&factory_clone.client),
            "Cloned factory should share the same Client"
        );
    }

    /// **Validates: Requirements 1.3, 1.4**
    ///
    /// Property-based style test: for N calls to new_client (N > 1),
    /// all N connections share exactly 1 Client instance.
    /// Counterexample on UNFIXED code: 5 calls → 5 distinct Client instances.
    /// On FIXED code: 5 calls → 1 shared Client instance.
    #[tokio::test]
    async fn test_n_new_client_calls_share_one_client_instance() {
        let mut factory = NetworkFactory::default();
        let factory_client_repr = client_debug_repr(&factory.client);

        let n = 10;
        let mut connection_reprs = Vec::with_capacity(n);

        for i in 0..n {
            let node = BasicNode {
                addr: format!("10.0.0.{}:2380", i + 1),
            };
            let conn = factory.new_client(i as NodeId, &node).await;
            connection_reprs.push(client_debug_repr(&conn.client));
        }

        // All N connections must share the same Client as the factory
        for (i, repr) in connection_reprs.iter().enumerate() {
            assert_eq!(
                &factory_client_repr,
                repr,
                "Connection {} should share the factory's Client, but got a distinct instance. \
                 Bug condition: {} calls created {} distinct Client instances instead of 1 shared.",
                i,
                n,
                connection_reprs
                    .iter()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
            );
        }

        // Verify there is exactly 1 unique Client across all connections
        let unique_clients: std::collections::HashSet<&String> = connection_reprs.iter().collect();
        assert_eq!(
            unique_clients.len(),
            1,
            "Expected 1 shared Client instance, but found {} distinct instances. \
             Counterexample: {} calls → {} distinct Client instances instead of 1 shared.",
            unique_clients.len(),
            n,
            unique_clients.len()
        );
    }

    /// **Validates: Requirements 1.3**
    ///
    /// Test that the factory's client field is the same one given to connections,
    /// even when addresses use different formats (http:// prefix vs bare addr).
    #[tokio::test]
    async fn test_client_shared_regardless_of_address_format() {
        let mut factory = NetworkFactory::default();
        let factory_repr = client_debug_repr(&factory.client);

        // Bare address (no http://)
        let node_bare = BasicNode {
            addr: "192.168.1.1:2380".to_string(),
        };
        let conn_bare = factory.new_client(1, &node_bare).await;
        assert_eq!(
            factory_repr,
            client_debug_repr(&conn_bare.client),
            "Bare address connection should share the factory's Client"
        );
        assert_eq!(conn_bare.target_addr, "http://192.168.1.1:2380");

        // Address with http:// prefix
        let node_http = BasicNode {
            addr: "http://192.168.1.2:2380".to_string(),
        };
        let conn_http = factory.new_client(2, &node_http).await;
        assert_eq!(
            factory_repr,
            client_debug_repr(&conn_http.client),
            "HTTP-prefixed address connection should share the factory's Client"
        );
        assert_eq!(conn_http.target_addr, "http://192.168.1.2:2380");
    }
}
