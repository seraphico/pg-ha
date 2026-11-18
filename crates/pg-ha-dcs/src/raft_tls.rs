//! Mutual TLS for the Raft RPC port.
//!
//! The server requires a client certificate that (1) chains to the configured
//! CA and (2) carries a DNS or IP SAN equal to the host of a cluster member.
//! The member set is the union of the configured `self_addr` and
//! `partner_addrs` hosts and the hosts in the live Raft membership, evaluated
//! on every handshake, so identity follows membership changes.
//!
//! The Common Name is never used.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use openraft::{BasicNode, RaftMetrics};
use pg_ha_core::config::RaftTlsConfig;
use rustls::client::verify_server_name;
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ParsedCertificate, WebPkiClientVerifier};
use rustls::{
    CertificateError, DigitallySignedStruct, DistinguishedName, RootCertStore, SignatureScheme,
};
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use tokio::sync::watch;

use crate::store::NodeId;

/// Extract the host from `host:port`, `[v6]:port`, or a URL-style address.
pub fn host_of(addr: &str) -> String {
    let a = addr
        .strip_prefix("https://")
        .or_else(|| addr.strip_prefix("http://"))
        .unwrap_or(addr);
    let a = a.split('/').next().unwrap_or(a);
    if let Some(rest) = a.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest).to_string();
    }
    match a.rsplit_once(':') {
        // A bare IPv6 literal has several colons and no port.
        Some((host, port)) if !host.contains(':') && port.chars().all(|c| c.is_ascii_digit()) => {
            host.to_string()
        }
        _ => a.to_string(),
    }
}

type DynamicHosts = Arc<dyn Fn() -> Vec<String> + Send + Sync>;

/// The set of hosts whose certificates are accepted as cluster members.
#[derive(Clone)]
pub struct MemberHosts {
    static_hosts: Vec<String>,
    dynamic: Option<DynamicHosts>,
}

impl std::fmt::Debug for MemberHosts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemberHosts")
            .field("static_hosts", &self.static_hosts)
            .finish_non_exhaustive()
    }
}

impl MemberHosts {
    /// Members known from configuration only (`self_addr` + `partner_addrs`).
    pub fn new(config_addrs: &[String]) -> Self {
        Self {
            static_hosts: config_addrs.iter().map(|a| host_of(a)).collect(),
            dynamic: None,
        }
    }

    /// Configuration hosts plus the hosts of the live Raft membership.
    pub fn with_raft_metrics(
        config_addrs: &[String],
        metrics: watch::Receiver<RaftMetrics<NodeId, BasicNode>>,
    ) -> Self {
        let dynamic: DynamicHosts = Arc::new(move || {
            metrics
                .borrow()
                .membership_config
                .membership()
                .nodes()
                .map(|(_, node)| host_of(&node.addr))
                .collect()
        });
        Self {
            dynamic: Some(dynamic),
            ..Self::new(config_addrs)
        }
    }

    /// Configuration hosts plus hosts returned by `source` on every call.
    pub fn with_source(
        config_addrs: &[String],
        source: impl Fn() -> Vec<String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            dynamic: Some(Arc::new(source)),
            ..Self::new(config_addrs)
        }
    }

    /// All member hosts right now.
    pub fn hosts(&self) -> Vec<String> {
        let mut hosts = self.static_hosts.clone();
        if let Some(dynamic) = &self.dynamic {
            hosts.extend(dynamic());
        }
        hosts
    }
}

/// Does `cert` carry a DNS/IP SAN matching `host`?
fn cert_matches_host(cert: &ParsedCertificate<'_>, host: &str) -> bool {
    ServerName::try_from(host)
        .map(|name| verify_server_name(cert, &name).is_ok())
        .unwrap_or(false)
}

/// Wraps the stock WebPKI client verifier and additionally requires the leaf
/// to carry a SAN naming a cluster member.
#[derive(Debug)]
pub struct MemberCertVerifier {
    inner: Arc<dyn ClientCertVerifier>,
    members: MemberHosts,
}

impl MemberCertVerifier {
    pub fn new(inner: Arc<dyn ClientCertVerifier>, members: MemberHosts) -> Self {
        Self { inner, members }
    }
}

impl ClientCertVerifier for MemberCertVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let verified = self
            .inner
            .verify_client_cert(end_entity, intermediates, now)?;
        let parsed = ParsedCertificate::try_from(end_entity)?;
        if self
            .members
            .hosts()
            .iter()
            .any(|host| cert_matches_host(&parsed, host))
        {
            Ok(verified)
        } else {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn read_certs(path: &Path, what: &str) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let pem = std::fs::read(path)
        .with_context(|| format!("Failed to read {what} '{}'", path.display()))?;
    let certs = rustls_pemfile::certs(&mut pem.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("Invalid {what} '{}'", path.display()))?;
    if certs.is_empty() {
        bail!("{what} '{}' contains no certificates", path.display());
    }
    Ok(certs)
}

fn read_key(path: &Path, what: &str) -> anyhow::Result<PrivateKeyDer<'static>> {
    let pem = std::fs::read(path)
        .with_context(|| format!("Failed to read {what} '{}'", path.display()))?;
    rustls_pemfile::private_key(&mut pem.as_slice())
        .with_context(|| format!("Invalid {what} '{}'", path.display()))?
        .ok_or_else(|| anyhow!("no private key found in {what} '{}'", path.display()))
}

fn require_ca(tls: &RaftTlsConfig) -> anyhow::Result<&Path> {
    tls.ca_cert.as_deref().ok_or_else(|| {
        anyhow!("raft.tls.ca_cert is required: a CA certificate is needed to verify peers")
    })
}

/// Build the rustls server config for the Raft RPC listener: client
/// certificates are mandatory, must chain to `ca_cert`, and must name a member.
pub fn build_raft_server_config(
    tls: &RaftTlsConfig,
    members: MemberHosts,
) -> anyhow::Result<rustls::ServerConfig> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let ca_path = require_ca(tls)?;
    let mut roots = RootCertStore::empty();
    for ca in read_certs(ca_path, "ca_cert")? {
        roots
            .add(ca)
            .map_err(|e| anyhow!("Failed to add ca_cert to trust store: {e}"))?;
    }
    let inner = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| anyhow!("Failed to build client certificate verifier: {e}"))?;
    let verifier = Arc::new(MemberCertVerifier::new(inner, members));
    let chain = read_certs(&tls.cert, "server cert")?;
    let key = read_key(&tls.key, "server key")?;
    let mut config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, key)
        .map_err(|e| anyhow!("Failed to build server TLS config: {e}"))?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

/// Fail if the certificate this node presents to peers does not name
/// `self_host`: peers would reject it.
pub fn check_own_identity(tls: &RaftTlsConfig, self_host: &str) -> anyhow::Result<()> {
    let (cert_path, _) = tls.client_identity();
    let chain = read_certs(cert_path, "client cert")?;
    let parsed = ParsedCertificate::try_from(&chain[0])
        .map_err(|e| anyhow!("Invalid client cert '{}': {e}", cert_path.display()))?;
    if !cert_matches_host(&parsed, self_host) {
        bail!(
            "raft client certificate '{}' has no DNS/IP SAN matching this node's raft.self_addr \
             host '{self_host}'; peers would reject it",
            cert_path.display()
        );
    }
    Ok(())
}

/// Build the HTTPS client used for outbound Raft RPCs and leader forwarding.
/// Presents the member identity and trusts only the Raft CA. Unreadable or
/// invalid CA, certificate, or key files are errors.
pub fn build_raft_http_client(
    tls: &RaftTlsConfig,
    pool_max_idle_per_host: usize,
    timeout: Duration,
) -> anyhow::Result<reqwest::Client> {
    let ca_path = require_ca(tls)?;
    let ca_pem = std::fs::read(ca_path)
        .with_context(|| format!("Failed to read CA cert '{}'", ca_path.display()))?;
    let ca = reqwest::tls::Certificate::from_pem(&ca_pem)
        .with_context(|| format!("Invalid CA cert '{}'", ca_path.display()))?;

    let (cert_path, key_path) = tls.client_identity();
    let mut identity_pem = std::fs::read(cert_path)
        .with_context(|| format!("Failed to read client cert '{}'", cert_path.display()))?;
    identity_pem.push(b'\n');
    identity_pem.extend(
        std::fs::read(key_path)
            .with_context(|| format!("Failed to read client key '{}'", key_path.display()))?,
    );
    let identity = reqwest::tls::Identity::from_pem(&identity_pem)
        .with_context(|| format!("Invalid client identity '{}'", cert_path.display()))?;

    reqwest::Client::builder()
        .pool_max_idle_per_host(pool_max_idle_per_host)
        .timeout(timeout)
        .tls_built_in_root_certs(false)
        .add_root_certificate(ca)
        .identity(identity)
        .build()
        .context("Failed to build TLS HTTP client")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn openssl(args: &[&str], dir: &Path) {
        let out = std::process::Command::new("openssl")
            .args(args)
            .current_dir(dir)
            .output()
            .expect("openssl CLI is required to run the raft_tls tests");
        assert!(
            out.status.success(),
            "openssl {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn temp_dir(tag: &str) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pg-ha-raft-tls-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn gen_ca(dir: &Path) {
        openssl(&["genrsa", "-out", "ca.key", "2048"], dir);
        openssl(
            &[
                "req",
                "-x509",
                "-new",
                "-key",
                "ca.key",
                "-sha256",
                "-days",
                "1",
                "-subj",
                "/CN=raft-test-ca",
                "-out",
                "ca.crt",
            ],
            dir,
        );
    }

    /// Leaf signed by the CA in `dir` with the given SAN list (`DNS:a,IP:1.2.3.4`).
    fn gen_leaf(dir: &Path, name: &str, san: &str) {
        std::fs::write(
            dir.join(format!("{name}.ext")),
            format!(
                "subjectAltName={san}\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=CA:FALSE\n"
            ),
        )
        .unwrap();
        openssl(&["genrsa", "-out", &format!("{name}.key"), "2048"], dir);
        openssl(
            &[
                "req",
                "-new",
                "-key",
                &format!("{name}.key"),
                "-subj",
                &format!("/CN={name}-not-a-hostname"),
                "-out",
                &format!("{name}.csr"),
            ],
            dir,
        );
        openssl(
            &[
                "x509",
                "-req",
                "-in",
                &format!("{name}.csr"),
                "-CA",
                "ca.crt",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-days",
                "1",
                "-sha256",
                "-extfile",
                &format!("{name}.ext"),
                "-out",
                &format!("{name}.crt"),
            ],
            dir,
        );
    }

    fn tls_cfg(dir: &Path, name: &str) -> RaftTlsConfig {
        RaftTlsConfig {
            cert: dir.join(format!("{name}.crt")),
            key: dir.join(format!("{name}.key")),
            ca_cert: Some(dir.join("ca.crt")),
            client_cert: None,
            client_key: None,
            require_client_cert: None,
        }
    }

    /// In-memory TLS handshake; Err when the server refuses the client.
    fn handshake(
        server: &Arc<rustls::ServerConfig>,
        dir: &Path,
        client_identity: Option<&str>,
        server_name: &str,
    ) -> Result<(), rustls::Error> {
        let mut roots = RootCertStore::empty();
        for c in read_certs(&dir.join("ca.crt"), "ca").unwrap() {
            roots.add(c).unwrap();
        }
        let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
        let client_cfg = match client_identity {
            Some(name) => builder
                .with_client_auth_cert(
                    read_certs(&dir.join(format!("{name}.crt")), "c").unwrap(),
                    read_key(&dir.join(format!("{name}.key")), "k").unwrap(),
                )
                .unwrap(),
            None => builder.with_no_client_auth(),
        };
        let mut srv = rustls::ServerConnection::new(server.clone()).unwrap();
        let mut cli = rustls::ClientConnection::new(
            Arc::new(client_cfg),
            ServerName::try_from(server_name.to_string()).unwrap(),
        )
        .unwrap();
        for _ in 0..16 {
            let mut buf = Vec::new();
            cli.write_tls(&mut buf).ok();
            if !buf.is_empty() {
                srv.read_tls(&mut buf.as_slice()).unwrap();
                srv.process_new_packets()?;
            }
            let mut buf = Vec::new();
            srv.write_tls(&mut buf).ok();
            if !buf.is_empty() {
                cli.read_tls(&mut buf.as_slice()).unwrap();
                cli.process_new_packets()?;
            }
            if !cli.is_handshaking() && !srv.is_handshaking() {
                return Ok(());
            }
        }
        Err(rustls::Error::General("handshake did not complete".into()))
    }

    fn server_for(dir: &Path, members: MemberHosts) -> Arc<rustls::ServerConfig> {
        Arc::new(build_raft_server_config(&tls_cfg(dir, "node1"), members).unwrap())
    }

    fn addrs(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn host_of_handles_addr_forms() {
        assert_eq!(host_of("node1:2380"), "node1");
        assert_eq!(host_of("node1"), "node1");
        assert_eq!(host_of("10.0.0.1:2380"), "10.0.0.1");
        assert_eq!(host_of("[fe80::1]:2380"), "fe80::1");
        assert_eq!(host_of("fe80::1"), "fe80::1");
        assert_eq!(host_of("https://node1:2380/x"), "node1");
    }

    #[test]
    fn rejects_client_without_certificate() {
        let dir = temp_dir("nocert");
        gen_ca(&dir);
        gen_leaf(&dir, "node1", "DNS:node1");
        let server = server_for(&dir, MemberHosts::new(&addrs(&["node1:2380"])));
        assert!(handshake(&server, &dir, None, "node1").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_ca_signed_cert_without_member_san() {
        let dir = temp_dir("ctl");
        gen_ca(&dir);
        gen_leaf(&dir, "node1", "DNS:node1");
        gen_leaf(&dir, "ctl", "DNS:ctl.example.com");
        let server = server_for(
            &dir,
            MemberHosts::new(&addrs(&["node1:2380", "node2:2380"])),
        );
        assert!(handshake(&server, &dir, Some("ctl"), "node1").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn accepts_cert_with_member_san() {
        let dir = temp_dir("member");
        gen_ca(&dir);
        gen_leaf(&dir, "node1", "DNS:node1");
        gen_leaf(&dir, "node2", "DNS:node2");
        let server = server_for(
            &dir,
            MemberHosts::new(&addrs(&["node1:2380", "node2:2380"])),
        );
        assert!(handshake(&server, &dir, Some("node2"), "node1").is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn identity_follows_membership() {
        let dir = temp_dir("follow");
        gen_ca(&dir);
        gen_leaf(&dir, "node1", "DNS:node1");
        gen_leaf(&dir, "node4", "DNS:node4");
        let live = Arc::new(Mutex::new(Vec::<String>::new()));
        let source = live.clone();
        let members = MemberHosts::with_source(&addrs(&["node1:2380"]), move || {
            source.lock().unwrap().clone()
        });
        let server = server_for(&dir, members);
        assert!(handshake(&server, &dir, Some("node4"), "node1").is_err());
        live.lock().unwrap().push("node4".into());
        assert!(handshake(&server, &dir, Some("node4"), "node1").is_ok());
        live.lock().unwrap().clear();
        assert!(handshake(&server, &dir, Some("node4"), "node1").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn accepts_ip_san_member() {
        let dir = temp_dir("ip");
        gen_ca(&dir);
        gen_leaf(&dir, "node1", "DNS:node1,IP:127.0.0.1");
        gen_leaf(&dir, "peer", "IP:10.1.2.3");
        let server = server_for(
            &dir,
            MemberHosts::new(&addrs(&["127.0.0.1:2380", "10.1.2.3:2380"])),
        );
        assert!(handshake(&server, &dir, Some("peer"), "node1").is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn accepts_multi_san_cert_when_one_name_matches() {
        let dir = temp_dir("multi");
        gen_ca(&dir);
        gen_leaf(&dir, "node1", "DNS:node1");
        gen_leaf(&dir, "shared", "DNS:other,DNS:node3,DNS:another");
        let server = server_for(
            &dir,
            MemberHosts::new(&addrs(&["node1:2380", "node3:2380"])),
        );
        assert!(handshake(&server, &dir, Some("shared"), "node1").is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn server_config_requires_ca_cert() {
        let dir = temp_dir("noca");
        gen_ca(&dir);
        gen_leaf(&dir, "node1", "DNS:node1");
        let mut cfg = tls_cfg(&dir, "node1");
        cfg.ca_cert = None;
        let err = build_raft_server_config(&cfg, MemberHosts::new(&[])).unwrap_err();
        assert!(err.to_string().contains("ca_cert"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn http_client_errors_on_unreadable_client_cert() {
        let dir = temp_dir("badclient");
        gen_ca(&dir);
        gen_leaf(&dir, "node1", "DNS:node1");
        let mut cfg = tls_cfg(&dir, "node1");
        cfg.client_cert = Some(dir.join("missing.crt"));
        cfg.client_key = Some(dir.join("node1.key"));
        let err = build_raft_http_client(&cfg, 3, Duration::from_secs(5)).unwrap_err();
        assert!(format!("{err:#}").contains("missing.crt"), "{err:#}");
        cfg.client_cert = None;
        cfg.client_key = Some(dir.join("missing.key"));
        assert!(build_raft_http_client(&cfg, 3, Duration::from_secs(5)).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn own_identity_must_match_self_host() {
        let dir = temp_dir("own");
        gen_ca(&dir);
        gen_leaf(&dir, "node1", "DNS:node1");
        let cfg = tls_cfg(&dir, "node1");
        assert!(check_own_identity(&cfg, "node1").is_ok());
        let err = check_own_identity(&cfg, "node2").unwrap_err();
        assert!(err.to_string().contains("node2"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Real sockets: a member client gets a response, a non-member is refused.
    #[tokio::test]
    async fn socket_member_accepted_non_member_refused() {
        let dir = temp_dir("socket");
        gen_ca(&dir);
        gen_leaf(&dir, "node1", "DNS:localhost,IP:127.0.0.1");
        gen_leaf(&dir, "ctl", "DNS:ctl.example.com");
        let server_cfg = Arc::new(
            build_raft_server_config(
                &tls_cfg(&dir, "node1"),
                MemberHosts::new(&addrs(&["127.0.0.1:2380"])),
            )
            .unwrap(),
        );
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let app = axum::Router::new().route("/ping", axum::routing::get(|| async { "pong" }));
        let server = tokio::spawn(
            axum_server::from_tcp_rustls(
                listener,
                axum_server::tls_rustls::RustlsConfig::from_config(server_cfg),
            )
            .serve(app.into_make_service()),
        );
        let url = format!("https://127.0.0.1:{port}/ping");

        let member =
            build_raft_http_client(&tls_cfg(&dir, "node1"), 1, Duration::from_secs(5)).unwrap();
        let resp = member.get(&url).send().await.expect("member must connect");
        assert_eq!(resp.status(), 200);

        let ctl = build_raft_http_client(&tls_cfg(&dir, "ctl"), 1, Duration::from_secs(5)).unwrap();
        assert!(
            ctl.get(&url).send().await.is_err(),
            "non-member must be refused"
        );

        server.abort();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
