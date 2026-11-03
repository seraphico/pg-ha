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
