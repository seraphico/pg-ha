//! pg-ha-ctl: CLI tool for managing pg-ha clusters
//!
//! Communicates exclusively via the REST API.

mod credentials;
mod tls;

use clap::{Parser, Subcommand};
use credentials::{CredentialError, apply_basic_auth, format_http_result, resolve_credentials};
use serde_json::Value;
use tls::{TlsPathError, build_client, resolve_tls_paths};

/// pg-ha-ctl: Command-line tool for managing pg-ha clusters
#[derive(Parser)]
#[command(name = "pg-ha-ctl", version, about)]
struct Cli {
    /// REST API endpoint of a cluster member
    #[arg(short, long, default_value = "http://127.0.0.1:8008")]
    endpoint: String,

    /// REST username. Overrides the PG_HA_USERNAME environment variable.
    #[arg(long)]
    username: Option<String>,

    /// REST password. Overrides the PG_HA_PASSWORD environment variable.
    #[arg(long)]
    password: Option<String>,

    /// CA certificate for the REST server. Overrides PG_HA_CACERT.
    /// Required when the server certificate is signed by a private CA.
    #[arg(long)]
    cacert: Option<String>,

    /// Client certificate for REST mTLS. Overrides PG_HA_CERT.
    #[arg(long)]
    cert: Option<String>,

    /// Client private key for REST mTLS. Overrides PG_HA_KEY.
    #[arg(long)]
    key: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show cluster members and their status
    List,
    /// Trigger a switchover to a specified candidate
    Switchover {
        #[arg(long)]
        leader: Option<String>,
        #[arg(long)]
        candidate: Option<String>,
    },
    /// Trigger a manual failover
    Failover {
        #[arg(long)]
        candidate: Option<String>,
    },
    /// Restart PostgreSQL on this node
    Restart,
    /// Reinitialize a node (wipe data and re-clone)
    Reinitialize,
    /// Pause automatic failover
    Pause,
    /// Resume automatic failover
    Resume,
    /// Quorum recovery for voters that are gone for good. Without flags it
    /// only explains (reads, never writes). Exit codes: 0 done or nothing to
    /// do, 1 error, 2 usage, 3 will heal by itself, 4 refused or needs
    /// acceptance.
    Recover {
        /// Voters declared lost for good (comma separated), exactly as the
        /// explain lists them. Needs --digest.
        #[arg(
            long,
            value_delimiter = ',',
            requires = "digest",
            conflicts_with = "cancel"
        )]
        lost: Option<Vec<String>>,
        /// The digest the explain printed.
        #[arg(long, requires = "lost", conflicts_with = "cancel")]
        digest: Option<String>,
        /// Delete an unused grant.
        #[arg(long)]
        cancel: bool,
        /// After the apply, do not wait for the candidate to become primary.
        #[arg(long, requires = "lost")]
        no_wait: bool,
    },
}

/// How long `recover --lost` waits for the candidate to become primary.
const RECOVER_WAIT: std::time::Duration = std::time::Duration::from_secs(60);

/// Exit code of an explain answer (`assessment.outcome`).
fn explain_exit_code(assessment: &Value) -> i32 {
    match assessment["outcome"].as_str() {
        Some("nothing_to_do") => 0,
        Some("will_heal") => 3,
        Some("blocked") | Some("needs_acceptance") => 4,
        _ => 1,
    }
}

/// The explain as text, with the exact apply command when acceptance is
/// needed.
fn render_explain(endpoint: &str, message: &str, a: &Value) -> String {
    let mut out = format!("Quorum recovery: {message}\n");
    if let Some(q) = a["quorum"].as_u64() {
        out.push_str(&format!("  /sync q={q}\n"));
    }
    if let Some(voters) = a["voters"].as_array().filter(|v| !v.is_empty()) {
        out.push_str(&format!(
            "  {:<12} {:>8} {:>14}  {}\n",
            "voter", "timeline", "lsn", "status"
        ));
        for v in voters {
            let num = |k: &str| v[k].as_u64().map_or("-".to_string(), |n| n.to_string());
            out.push_str(&format!(
                "  {:<12} {:>8} {:>14}  {}\n",
                v["name"].as_str().unwrap_or("?"),
                num("timeline"),
                num("lsn"),
                v["status"].as_str().unwrap_or("")
            ));
        }
    }
    if let Some(g) = a.get("grant").filter(|g| g.is_object()) {
        out.push_str(&format!(
            "  grant: candidate {}, lost {}, expires in {} s{}\n",
            g["candidate"].as_str().unwrap_or("?"),
            join(&g["lost"]),
            g["expires_in_s"].as_u64().unwrap_or(0),
            if g["current"].as_bool() == Some(false) {
                " (stale: the state changed, it is ignored)"
            } else {
                ""
            }
        ));
    }
    if a["outcome"] == "needs_acceptance" {
        let risk = &a["risk"];
        let yes = |k: &str| {
            if risk[k].as_bool() == Some(true) {
                "yes"
            } else {
                "no"
            }
        };
        out.push_str(&format!(
            "  candidate: {}\n  lost: {}\n  commits only the lost voters hold may be lost: {}\n  everything acknowledged before the current timeline began is on the candidate: {}\n  newer timeline history abandoned: {}\n",
            a["candidate"].as_str().unwrap_or("?"),
            join(&a["lost"]),
            yes("possible"),
            yes("pre_timeline_safe"),
            yes("timeline_loss"),
        ));
        out.push_str(&format!(
            "To accept this loss, run:\n  pg-ha-ctl --endpoint {endpoint} recover --lost {} --digest {}\n",
            join(&a["lost"]),
            a["digest"].as_str().unwrap_or("?")
        ));
    }
    out
}

fn join(names: &Value) -> String {
    names
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default()
}

/// Does `GET /cluster` show `candidate` as the primary?
fn shows_primary(cluster: &Value, candidate: &str) -> bool {
    cluster["members"].as_array().is_some_and(|m| {
        m.iter().any(|n| {
            n["name"]
                .as_str()
                .is_some_and(|x| x.eq_ignore_ascii_case(candidate))
                && n["role"] == "primary"
        })
    })
}

#[tokio::main]
async fn main() {
    if let Err(code) = run().await {
        std::process::exit(code);
    }
}

async fn run() -> Result<(), i32> {
    let cli = Cli::parse();
    let env_username = std::env::var("PG_HA_USERNAME").ok();
    let env_password = std::env::var("PG_HA_PASSWORD").ok();
    let creds = match resolve_credentials(
        cli.username.as_deref(),
        cli.password.as_deref(),
        env_username.as_deref(),
        env_password.as_deref(),
    ) {
        Ok(creds) => creds,
        Err(CredentialError::MissingUsername) => {
            eprintln!(
                "username is required when a password is set; pass --username or set PG_HA_USERNAME"
            );
            return Err(2);
        }
        Err(CredentialError::MissingPassword) => {
            eprintln!(
                "password is required when a username is set; pass --password or set PG_HA_PASSWORD"
            );
            return Err(2);
        }
    };

    let env_cacert = std::env::var("PG_HA_CACERT").ok();
    let env_cert = std::env::var("PG_HA_CERT").ok();
    let env_key = std::env::var("PG_HA_KEY").ok();
    let tls = match resolve_tls_paths(
        cli.cacert.as_deref(),
        cli.cert.as_deref(),
        cli.key.as_deref(),
        env_cacert.as_deref(),
        env_cert.as_deref(),
        env_key.as_deref(),
    ) {
        Ok(tls) => tls,
        Err(TlsPathError::MissingCert) => {
            eprintln!(
                "client certificate is required when a client key is set; pass --cert or set PG_HA_CERT"
            );
            return Err(2);
        }
        Err(TlsPathError::MissingKey) => {
            eprintln!(
                "client key is required when a client certificate is set; pass --key or set PG_HA_KEY"
            );
            return Err(2);
        }
    };
    let client = match build_client(&tls) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("{err}");
            return Err(1);
        }
    };
    let sent_auth = creds.is_some();

    match cli.command {
        Commands::List => {
            let body = send_json(
                apply_basic_auth(
                    client.get(format!("{}/patroni", cli.endpoint)),
                    creds.as_ref(),
                ),
                sent_auth,
            )
            .await?;
            println!("+ Cluster: {}", body["scope"].as_str().unwrap_or("?"));
            println!("  {:<10} {:<10} {:<10}", "Name", "Role", "State");
            println!(
                "  {:<10} {:<10} {:<10}",
                body["name"].as_str().unwrap_or("?"),
                body["role"].as_str().unwrap_or("?"),
                body["state"].as_str().unwrap_or("?"),
            );
        }
        Commands::Switchover { leader, candidate } => {
            let payload = serde_json::json!({ "leader": leader, "candidate": candidate });
            let line = send_line(
                apply_basic_auth(
                    client
                        .post(format!("{}/switchover", cli.endpoint))
                        .json(&payload),
                    creds.as_ref(),
                ),
                sent_auth,
            )
            .await?;
            println!("{line}");
        }
        Commands::Failover { candidate } => {
            let payload = serde_json::json!({ "candidate": candidate });
            let line = send_line(
                apply_basic_auth(
                    client
                        .post(format!("{}/failover", cli.endpoint))
                        .json(&payload),
                    creds.as_ref(),
                ),
                sent_auth,
            )
            .await?;
            println!("{line}");
        }
        Commands::Restart => {
            let line = send_line(
                apply_basic_auth(
                    client.post(format!("{}/restart", cli.endpoint)),
                    creds.as_ref(),
                ),
                sent_auth,
            )
            .await?;
            println!("{line}");
        }
        Commands::Reinitialize => {
            let line = send_line(
                apply_basic_auth(
                    client.post(format!("{}/reinitialize", cli.endpoint)),
                    creds.as_ref(),
                ),
                sent_auth,
            )
            .await?;
            println!("{line}");
        }
        Commands::Pause => {
            let payload = serde_json::json!({ "pause": true });
            let line = send_line(
                apply_basic_auth(
                    client
                        .patch(format!("{}/config", cli.endpoint))
                        .json(&payload),
                    creds.as_ref(),
                ),
                sent_auth,
            )
            .await?;
            println!("{}", with_fallback(line, "Pause requested"));
        }
        Commands::Resume => {
            let payload = serde_json::json!({ "pause": false });
            let line = send_line(
                apply_basic_auth(
                    client
                        .patch(format!("{}/config", cli.endpoint))
                        .json(&payload),
                    creds.as_ref(),
                ),
                sent_auth,
            )
            .await?;
            println!("{}", with_fallback(line, "Resume requested"));
        }
        Commands::Recover {
            lost,
            digest,
            cancel,
            no_wait,
        } => {
            let url = format!("{}/recovery", cli.endpoint);
            if cancel {
                let line = send_line(
                    apply_basic_auth(client.delete(&url), creds.as_ref()),
                    sent_auth,
                )
                .await?;
                println!("{line}");
                return Ok(());
            }
            let (Some(lost), Some(digest)) = (lost, digest) else {
                // Explain: read-only.
                let (status, body) =
                    read_body(apply_basic_auth(client.get(&url), creds.as_ref())).await?;
                if let Err(line) = format_http_result(status, &body, sent_auth) {
                    eprintln!("{line}");
                    return Err(1);
                }
                let a = &body["assessment"];
                print!(
                    "{}",
                    render_explain(&cli.endpoint, body["message"].as_str().unwrap_or(""), a)
                );
                return match explain_exit_code(a) {
                    0 => Ok(()),
                    code => Err(code),
                };
            };
            let payload = serde_json::json!({ "lost": lost, "digest": digest });
            let (status, body) = read_body(apply_basic_auth(
                client.post(&url).json(&payload),
                creds.as_ref(),
            ))
            .await?;
            match format_http_result(status, &body, sent_auth) {
                Ok(line) => println!("{line}"),
                Err(line) => {
                    eprintln!("{line}");
                    // 409: refused (state changed, digest, existing grant).
                    return Err(if status.as_u16() == 409 { 4 } else { 1 });
                }
            }
            if no_wait {
                return Ok(());
            }
            let candidate = body["assessment"]["candidate"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let started = std::time::Instant::now();
            while started.elapsed() < RECOVER_WAIT {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let Ok((status, cluster)) = read_body(apply_basic_auth(
                    client.get(format!("{}/cluster", cli.endpoint)),
                    creds.as_ref(),
                ))
                .await
                else {
                    continue;
                };
                if status.is_success() && shows_primary(&cluster, &candidate) {
                    println!(
                        "{candidate} is the primary ({} s after the grant)",
                        started.elapsed().as_secs()
                    );
                    return Ok(());
                }
            }
            eprintln!(
                "{candidate} did not become primary within {} s; run pg-ha-ctl recover to see the state",
                RECOVER_WAIT.as_secs()
            );
            return Err(1);
        }
    }
    Ok(())
}

fn with_fallback(line: String, fallback: &str) -> String {
    if line.contains("] ") {
        line
    } else {
        format!("{line} {fallback}")
    }
}

async fn read_body(builder: reqwest::RequestBuilder) -> Result<(reqwest::StatusCode, Value), i32> {
    let response = match builder.send().await {
        Ok(response) => response,
        Err(err) => {
            eprintln!("{err}");
            return Err(1);
        }
    };
    let status = response.status();
    let text = match response.text().await {
        Ok(text) => text,
        Err(err) => {
            eprintln!("{err}");
            return Err(1);
        }
    };
    let body = serde_json::from_str(&text).unwrap_or(Value::Null);
    Ok((status, body))
}

async fn send_line(builder: reqwest::RequestBuilder, sent_auth: bool) -> Result<String, i32> {
    let (status, body) = read_body(builder).await?;
    match format_http_result(status, &body, sent_auth) {
        Ok(line) => Ok(line),
        Err(line) => {
            eprintln!("{line}");
            Err(1)
        }
    }
}

async fn send_json(builder: reqwest::RequestBuilder, sent_auth: bool) -> Result<Value, i32> {
    let (status, body) = read_body(builder).await?;
    if let Err(line) = format_http_result(status, &body, sent_auth) {
        eprintln!("{line}");
        return Err(1);
    }
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn recover(args: &[&str]) -> Result<Commands, clap::Error> {
        let mut all = vec!["pg-ha-ctl", "recover"];
        all.extend_from_slice(args);
        Cli::try_parse_from(all).map(|c| c.command)
    }
    #[test]
    fn recover_without_flags_is_the_explain() {
        match recover(&[]).unwrap() {
            Commands::Recover {
                lost: None,
                digest: None,
                cancel: false,
                no_wait: false,
            } => {}
            _ => panic!("not an explain"),
        }
    }
    #[test]
    fn recover_apply_takes_the_list_and_the_digest_unchanged() {
        match recover(&["--lost", "node1,node5", "--digest", "AbC123"]).unwrap() {
            Commands::Recover {
                lost: Some(lost),
                digest: Some(digest),
                ..
            } => {
                assert_eq!(lost, ["node1", "node5"]);
                assert_eq!(digest, "AbC123");
            }
            _ => panic!("not an apply"),
        }
    }
    #[test]
    fn recover_usage_errors_exit_2() {
        for args in [
            &["--lost", "node1"][..],
            &["--digest", "ab"],
            &["--cancel", "--lost", "node1", "--digest", "ab"],
            &["--no-wait"],
            &["--force"],
        ] {
            let e = match recover(args) {
                Err(e) => e,
                Ok(_) => panic!("{args:?} accepted"),
            };
            assert_eq!(e.exit_code(), 2, "{args:?}");
        }
        assert!(recover(&["--cancel"]).is_ok());
    }
    #[test]
    fn explain_exit_codes() {
        for (outcome, code) in [
            ("nothing_to_do", 0),
            ("will_heal", 3),
            ("blocked", 4),
            ("needs_acceptance", 4),
            ("something_else", 1),
        ] {
            assert_eq!(
                explain_exit_code(&serde_json::json!({ "outcome": outcome })),
                code,
                "{outcome}"
            );
        }
        assert_eq!(explain_exit_code(&Value::Null), 1);
    }
    #[test]
    fn explain_prints_the_exact_apply_command() {
        let a = serde_json::json!({
            "outcome": "needs_acceptance",
            "candidate": "node2",
            "lost": ["node1", "node5"],
            "risk": {"possible": true, "pre_timeline_safe": true, "timeline_loss": false},
            "digest": "ab".repeat(32),
            "quorum": 2,
            "voters": [
                {"name": "node1", "timeline": null, "lsn": null, "status": "unreachable: timed out"},
                {"name": "node2", "timeline": 2, "lsn": 100, "status": "candidate"},
            ],
        });
        let text = render_explain("http://n2:8008", "needs acceptance", &a);
        assert!(
            text.contains(&format!(
                "pg-ha-ctl --endpoint http://n2:8008 recover --lost node1,node5 --digest {}",
                "ab".repeat(32)
            )),
            "{text}"
        );
        assert!(text.contains("unreachable: timed out"), "{text}");
        assert!(text.contains("may be lost: yes"), "{text}");
        let healed = serde_json::json!({"outcome": "will_heal", "voters": []});
        assert!(!render_explain("e", "m", &healed).contains("--digest"));
    }
    #[test]
    fn waiting_reads_the_primary_from_the_cluster() {
        let c = serde_json::json!({"members": [
            {"name": "node2", "role": "replica"},
            {"name": "node3", "role": "primary"},
        ]});
        assert!(shows_primary(&c, "node3"));
        assert!(!shows_primary(&c, "node2"));
        assert!(!shows_primary(&Value::Null, "node3"));
    }

    #[test]
    fn root_flags_parse_on_list() {
        let cli = Cli::try_parse_from([
            "pg-ha-ctl",
            "--username",
            "alice",
            "--password",
            "secret",
            "list",
        ])
        .unwrap();
        assert_eq!(cli.username.as_deref(), Some("alice"));
        assert_eq!(cli.password.as_deref(), Some("secret"));
        assert_eq!(cli.cacert.as_deref(), None);
    }

    #[test]
    fn tls_flags_parse_on_list() {
        let cli = Cli::try_parse_from([
            "pg-ha-ctl",
            "--cacert",
            "ca.crt",
            "--cert",
            "client.crt",
            "--key",
            "client.key",
            "list",
        ])
        .unwrap();
        assert_eq!(cli.cacert.as_deref(), Some("ca.crt"));
        assert_eq!(cli.cert.as_deref(), Some("client.crt"));
        assert_eq!(cli.key.as_deref(), Some("client.key"));
    }
}
