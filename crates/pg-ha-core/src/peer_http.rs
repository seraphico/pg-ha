//! HTTP helpers shared by components that call a peer's REST API
//! (the proxy health checker and the quorum vote probe).
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;

use crate::config::RestApiTlsConfig;
use crate::quorum::WalPosition;
use crate::quorum::round::{PEER_REQUEST_TIMEOUT, PeerProbeError, PeerTarget, PeerWalProbe};

/// Build a client that talks to peers' REST API with this node's REST TLS
/// material. mTLS presents this node's own server certificate; peers verify
/// it with the same CA. `what` names the client in the build-failure message.
pub fn build_peer_client(
    tls: Option<&RestApiTlsConfig>,
    timeout: Duration,
    what: &str,
) -> anyhow::Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder().timeout(timeout);
    if let Some(tls) = tls {
        if let Some(ca_path) = &tls.ca_cert {
            let pem = std::fs::read(ca_path).map_err(|err| {
                anyhow::anyhow!(
                    "failed to read REST CA certificate {}: {err}",
                    ca_path.display()
                )
            })?;
            let cert = reqwest::Certificate::from_pem(&pem).map_err(|err| {
                anyhow::anyhow!("invalid REST CA certificate {}: {err}", ca_path.display())
            })?;
            builder = builder.add_root_certificate(cert);
        }
        if tls.require_client_cert {
            let cert_pem = std::fs::read(&tls.cert).map_err(|err| {
                anyhow::anyhow!(
                    "failed to read REST certificate {}: {err}",
                    tls.cert.display()
                )
            })?;
            let key_pem = std::fs::read(&tls.key).map_err(|err| {
                anyhow::anyhow!("failed to read REST key {}: {err}", tls.key.display())
            })?;
            let mut identity_pem = cert_pem;
            if !identity_pem.ends_with(b"\n") {
                identity_pem.push(b'\n');
            }
            identity_pem.extend(key_pem);
            let identity = reqwest::Identity::from_pem(&identity_pem)
                .map_err(|err| anyhow::anyhow!("invalid REST client identity: {err}"))?;
            builder = builder.identity(identity);
        }
    }
    builder
        .build()
        .map_err(|err| anyhow::anyhow!("failed to build {what}: {err}"))
}

/// Published member URLs may still say `http://` after REST TLS is turned on.
/// Callers have to use the scheme this node is actually listening with.
pub fn align_api_scheme(url: &str, https: bool) -> String {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);
    let scheme = if https { "https" } else { "http" };
    format!("{scheme}://{rest}")
}

/// Body of a peer's `POST /vote` answer (same fields as `GET /wal`).
#[derive(Debug, Deserialize)]
struct WalBody {
    name: String,
    timeline: Option<u64>,
    wal_position: Option<u64>,
    pg_in_recovery: bool,
    /// SB-37: the peer's pg-ha version (`GET /wal`; absent before 0.1.26).
    #[serde(default)]
    pg_ha_version: Option<String>,
}

/// Asks a peer for its vote at `POST /vote` (SB-40): the peer stops streaming
/// from its upstream, confirms that, and answers with its live WAL position.
/// The request is never retried: it is not a plain read.
///
/// Every failure is returned as a value; none aborts the election cycle.
pub struct ReqwestPeerWalProbe {
    client: reqwest::Client,
    /// REST Basic Auth credentials; `/vote` requires them when configured.
    basic_auth: Option<(String, String)>,
}

impl ReqwestPeerWalProbe {
    /// `tls` is this node's own REST TLS material, used to verify the peer
    /// (CA) and, with mTLS, to present a client certificate. `basic_auth` is
    /// this node's `restapi` username/password, sent on every vote request
    /// (peers share the same REST credentials).
    pub fn new(
        tls: Option<&RestApiTlsConfig>,
        basic_auth: Option<(String, String)>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            client: build_peer_client(tls, PEER_REQUEST_TIMEOUT, "vote client")?,
            basic_auth,
        })
    }
}

fn map_reqwest_error(err: reqwest::Error) -> PeerProbeError {
    if err.is_timeout() {
        PeerProbeError::Timeout
    } else {
        PeerProbeError::Transport(err.to_string())
    }
}

#[async_trait]
impl PeerWalProbe for ReqwestPeerWalProbe {
    async fn read(&self, target: &PeerTarget) -> Result<WalPosition, PeerProbeError> {
        let url = format!("{}/vote", target.api_url.trim_end_matches('/'));
        let mut request = self.client.post(&url);
        if let Some((user, pass)) = &self.basic_auth {
            request = request.basic_auth(user, Some(pass));
        }
        let response = request.send().await.map_err(map_reqwest_error)?;
        let status = response.status();
        if matches!(status.as_u16(), 401 | 403) {
            tracing::error!(
                peer = %target.name,
                url = %url,
                status = status.as_u16(),
                "peer rejected POST /vote credentials; check restapi.username/password and REST mTLS \
                 on every node, otherwise no vote can succeed and quorum cannot be reached"
            );
        }
        wal_answer(response, target).await
    }
}
/// A `/wal` or `/vote` answer as a [`WalPosition`] of `target`.
async fn wal_answer(
    response: reqwest::Response,
    target: &PeerTarget,
) -> Result<WalPosition, PeerProbeError> {
    wal_answer_with_version(response, target)
        .await
        .map(|(p, _)| p)
}
/// [`wal_answer`] plus the version the peer reports.
async fn wal_answer_with_version(
    response: reqwest::Response,
    target: &PeerTarget,
) -> Result<(WalPosition, Option<String>), PeerProbeError> {
    let status = response.status();
    if status != reqwest::StatusCode::OK {
        return Err(PeerProbeError::Status(status.as_u16()));
    }
    let bytes = response.bytes().await.map_err(map_reqwest_error)?;
    let body: WalBody =
        serde_json::from_slice(&bytes).map_err(|err| PeerProbeError::BadBody(err.to_string()))?;
    let timeline = body
        .timeline
        .ok_or_else(|| PeerProbeError::BadBody("timeline missing".into()))?;
    let lsn = body
        .wal_position
        .ok_or_else(|| PeerProbeError::BadBody("wal_position missing".into()))?;
    if !body.name.eq_ignore_ascii_case(&target.name) {
        return Err(PeerProbeError::Mismatch);
    }
    Ok((
        WalPosition {
            name: body.name,
            in_recovery: body.pg_in_recovery,
            timeline,
            lsn,
        },
        body.pg_ha_version,
    ))
}
/// SB-37: reads a peer's live position at `GET /wal`, which changes nothing
/// on the peer (unlike [`ReqwestPeerWalProbe`]). The follow path uses it to
/// check that the node it would restart PostgreSQL to follow is reachable and,
/// for the lock holder, runs as a primary.
pub struct ReqwestPeerWalReader {
    client: reqwest::Client,
}
impl ReqwestPeerWalReader {
    /// `tls`: this node's REST TLS material, as for [`ReqwestPeerWalProbe`].
    pub fn new(tls: Option<&RestApiTlsConfig>) -> anyhow::Result<Self> {
        Ok(Self {
            client: build_peer_client(tls, PEER_REQUEST_TIMEOUT, "upstream /wal client")?,
        })
    }
}
#[async_trait]
impl PeerWalProbe for ReqwestPeerWalReader {
    async fn read(&self, target: &PeerTarget) -> Result<WalPosition, PeerProbeError> {
        self.read_with_version(target).await.map(|(p, _)| p)
    }
    async fn read_with_version(
        &self,
        target: &PeerTarget,
    ) -> Result<(WalPosition, Option<String>), PeerProbeError> {
        let url = format!("{}/wal", target.api_url.trim_end_matches('/'));
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(map_reqwest_error)?;
        wal_answer_with_version(response, target).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn tls(ca: Option<&str>, mtls: bool, key: &str) -> RestApiTlsConfig {
        RestApiTlsConfig {
            cert: PathBuf::from("/nonexistent/pg-ha-test/server.crt"),
            key: PathBuf::from(key),
            ca_cert: ca.map(PathBuf::from),
            require_client_cert: mtls,
        }
    }

    /// Serve `reply` (a full HTTP response) to one connection; returns the base URL.
    async fn serve_once(reply: String) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut sock, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                let _ = sock.write_all(reply.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        format!("http://{addr}")
    }

    fn http(status: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    }

    async fn read_from(reply: String, name: &str) -> Result<WalPosition, PeerProbeError> {
        let api_url = serve_once(reply).await;
        let probe = ReqwestPeerWalProbe::new(None, None).unwrap();
        probe
            .read(&PeerTarget {
                name: name.into(),
                api_url,
            })
            .await
    }

    #[tokio::test]
    async fn probe_parses_ok_answer() {
        let body = r#"{"name":"Node2","scope":"c","timeline":3,"wal_position":4096,"pg_in_recovery":true}"#;
        let got = read_from(http("200 OK", body), "node2").await.unwrap();
        assert_eq!(got.timeline, 3);
        assert_eq!(got.lsn, 4096);
        assert!(got.in_recovery);
    }

    /// SB-37: the follow-path reader only reads: `GET /wal`, never `/vote`.
    #[tokio::test]
    async fn reader_asks_with_get_wal_and_parses_the_answer() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let n = sock.read(&mut buf).await.unwrap();
            let body = r#"{"name":"node3","timeline":4,"wal_position":7,"pg_in_recovery":false}"#;
            let _ = sock.write_all(http("200 OK", body).as_bytes()).await;
            let _ = sock.shutdown().await;
            String::from_utf8_lossy(&buf[..n]).to_string()
        });
        let got = ReqwestPeerWalReader::new(None)
            .unwrap()
            .read(&PeerTarget {
                name: "node3".into(),
                api_url: format!("http://{addr}"),
            })
            .await
            .unwrap();
        assert!(!got.in_recovery);
        assert_eq!(got.timeline, 4);
        let request = seen.await.unwrap();
        assert!(request.starts_with("GET /wal "), "{request}");
    }
    /// SB-37: the reader returns the version a peer reports, None when an
    /// older peer reports none.
    #[tokio::test]
    async fn reader_returns_the_reported_version() {
        let reader = ReqwestPeerWalReader::new(None).unwrap();
        for (body, want) in [
            (
                r#"{"name":"node3","timeline":4,"wal_position":7,"pg_in_recovery":true,"pg_ha_version":"9.9.9"}"#,
                Some("9.9.9".to_string()),
            ),
            (
                r#"{"name":"node3","timeline":4,"wal_position":7,"pg_in_recovery":true}"#,
                None,
            ),
        ] {
            let api_url = serve_once(http("200 OK", body)).await;
            let (p, v) = reader
                .read_with_version(&PeerTarget {
                    name: "node3".into(),
                    api_url,
                })
                .await
                .unwrap();
            assert_eq!(p.lsn, 7);
            assert_eq!(v, want);
        }
    }
    #[tokio::test]
    async fn probe_asks_with_post_vote() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let n = sock.read(&mut buf).await.unwrap();
            let body = r#"{"name":"node2","timeline":3,"wal_position":1,"pg_in_recovery":true}"#;
            let _ = sock.write_all(http("200 OK", body).as_bytes()).await;
            let _ = sock.shutdown().await;
            String::from_utf8_lossy(&buf[..n]).to_string()
        });
        let probe = ReqwestPeerWalProbe::new(None, None).unwrap();
        probe
            .read(&PeerTarget {
                name: "node2".into(),
                api_url: format!("http://{addr}"),
            })
            .await
            .unwrap();
        let request = seen.await.unwrap();
        assert!(request.starts_with("POST /vote "), "{request}");
    }
    /// Capture the raw request of one `POST /vote` sent by a probe built with `auth`.
    async fn captured_request(auth: Option<(String, String)>) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 2048];
            let n = sock.read(&mut buf).await.unwrap();
            let _ = sock
                .write_all(http("401 Unauthorized", "{}").as_bytes())
                .await;
            let _ = sock.shutdown().await;
            String::from_utf8_lossy(&buf[..n]).to_ascii_lowercase()
        });
        let got = ReqwestPeerWalProbe::new(None, auth)
            .unwrap()
            .read(&PeerTarget {
                name: "node2".into(),
                api_url: format!("http://{addr}"),
            })
            .await;
        assert_eq!(got.unwrap_err(), PeerProbeError::Status(401));
        seen.await.unwrap()
    }

    #[tokio::test]
    async fn vote_request_carries_basic_auth_header() {
        let request = captured_request(Some(("alice".into(), "secret".into()))).await;
        // base64("alice:secret")
        assert!(
            request.contains("authorization: basic ywxpy2u6c2vjcmv0"),
            "{request}"
        );
    }

    #[tokio::test]
    async fn vote_request_without_credentials_has_no_auth_header() {
        let request = captured_request(None).await;
        assert!(!request.contains("authorization:"), "{request}");
    }

    #[test]
    fn vote_client_with_mtls_loads_the_client_certificate() {
        // require_client_cert makes the vote client read cert+key; missing
        // files prove the identity path is taken.
        let cfg = tls(None, true, "/nonexistent/pg-ha-test/server.key");
        let err = ReqwestPeerWalProbe::new(Some(&cfg), None)
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("failed to read REST certificate"), "{err}");
    }

    #[tokio::test]
    async fn probe_maps_503_to_status() {
        let got = read_from(http("503 Service Unavailable", "{}"), "node2").await;
        assert_eq!(got.unwrap_err(), PeerProbeError::Status(503));
    }

    #[tokio::test]
    async fn probe_rejects_name_mismatch() {
        let body = r#"{"name":"node9","timeline":3,"wal_position":1,"pg_in_recovery":true}"#;
        let got = read_from(http("200 OK", body), "node2").await;
        assert_eq!(got.unwrap_err(), PeerProbeError::Mismatch);
    }

    #[tokio::test]
    async fn probe_rejects_garbage_body() {
        let got = read_from(http("200 OK", "not json"), "node2").await;
        assert!(matches!(got.unwrap_err(), PeerProbeError::BadBody(_)));
    }

    #[tokio::test]
    async fn probe_rejects_missing_timeline() {
        let body = r#"{"name":"node2","wal_position":1,"pg_in_recovery":true}"#;
        let got = read_from(http("200 OK", body), "node2").await;
        assert!(matches!(got.unwrap_err(), PeerProbeError::BadBody(_)));
    }

    #[tokio::test]
    async fn probe_rejects_null_timeline() {
        let body = r#"{"name":"node2","timeline":null,"wal_position":1,"pg_in_recovery":true}"#;
        let got = read_from(http("200 OK", body), "node2").await;
        assert!(matches!(got.unwrap_err(), PeerProbeError::BadBody(_)));
    }

    #[tokio::test]
    async fn probe_connection_refused_is_transport() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let probe = ReqwestPeerWalProbe::new(None, None).unwrap();
        let got = probe
            .read(&PeerTarget {
                name: "node2".into(),
                api_url: format!("http://{addr}"),
            })
            .await;
        assert!(matches!(got.unwrap_err(), PeerProbeError::Transport(_)));
    }

    #[test]
    fn align_api_scheme_table() {
        assert_eq!(align_api_scheme("http://a:8008", true), "https://a:8008");
        assert_eq!(align_api_scheme("https://a:8008", false), "http://a:8008");
        assert_eq!(align_api_scheme("a:8008", true), "https://a:8008");
        assert_eq!(align_api_scheme("a:8008", false), "http://a:8008");
        assert_eq!(align_api_scheme("http://a:8008", false), "http://a:8008");
    }

    #[test]
    fn build_without_tls_is_ok() {
        assert!(build_peer_client(None, Duration::from_secs(1), "peer client").is_ok());
    }

    #[test]
    fn unreadable_ca_reports_path() {
        let cfg = tls(Some("/nonexistent/pg-ha-test/ca.crt"), false, "/x/key");
        let err = build_peer_client(Some(&cfg), Duration::from_secs(1), "peer client")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("failed to read REST CA certificate /nonexistent/pg-ha-test/ca.crt"),
            "{err}"
        );
    }

    #[test]
    fn missing_client_material_reports_path() {
        let cfg = tls(None, true, "/nonexistent/pg-ha-test/server.key");
        let err = build_peer_client(Some(&cfg), Duration::from_secs(1), "peer client")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("failed to read REST certificate /nonexistent/pg-ha-test/server.crt"),
            "{err}"
        );
    }
}
