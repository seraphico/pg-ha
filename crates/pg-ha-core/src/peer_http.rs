//! HTTP helpers shared by components that call a peer's REST API
//! (the proxy health checker and the quorum vote probe).
use std::time::Duration;

use crate::config::RestApiTlsConfig;

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
