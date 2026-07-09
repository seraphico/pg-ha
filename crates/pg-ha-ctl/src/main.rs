//! pg-ha-ctl: CLI tool for managing pg-ha clusters
//!
//! Communicates exclusively via the REST API.

use clap::{Parser, Subcommand};
use serde_json::Value;

/// pg-ha-ctl: Command-line tool for managing pg-ha clusters
#[derive(Parser)]
#[command(name = "pg-ha-ctl", version, about)]
struct Cli {
    /// REST API endpoint of a cluster member
    #[arg(short, long, default_value = "http://127.0.0.1:8008")]
    endpoint: String,

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
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let client = reqwest::Client::new();

    match cli.command {
        Commands::List => {
            let resp: Value = client
                .get(format!("{}/patroni", cli.endpoint))
                .send()
                .await?
                .json()
                .await?;
            println!("+ Cluster: {}", resp["scope"].as_str().unwrap_or("?"));
            println!(
                "  {:<10} {:<10} {:<10}",
                "Name", "Role", "State"
            );
            println!(
                "  {:<10} {:<10} {:<10}",
                resp["name"].as_str().unwrap_or("?"),
                resp["role"].as_str().unwrap_or("?"),
                resp["state"].as_str().unwrap_or("?"),
            );
        }
        Commands::Switchover { leader, candidate } => {
            let body = serde_json::json!({ "leader": leader, "candidate": candidate });
            let resp = client
                .post(format!("{}/switchover", cli.endpoint))
                .json(&body)
                .send()
                .await?;
            let status = resp.status();
            let body: Value = resp.json().await?;
            println!("[{}] {}", status.as_u16(), body["message"].as_str().unwrap_or(""));
        }
        Commands::Failover { candidate } => {
            let body = serde_json::json!({ "candidate": candidate });
            let resp = client
                .post(format!("{}/failover", cli.endpoint))
                .json(&body)
                .send()
                .await?;
            let status = resp.status();
            let body: Value = resp.json().await?;
            println!("[{}] {}", status.as_u16(), body["message"].as_str().unwrap_or(""));
        }
        Commands::Restart => {
            let resp = client
                .post(format!("{}/restart", cli.endpoint))
                .send()
                .await?;
            let status = resp.status();
            let body: Value = resp.json().await?;
            println!("[{}] {}", status.as_u16(), body["message"].as_str().unwrap_or(""));
        }
        Commands::Reinitialize => {
            let resp = client
                .post(format!("{}/reinitialize", cli.endpoint))
                .send()
                .await?;
            let status = resp.status();
            let body: Value = resp.json().await?;
            println!("[{}] {}", status.as_u16(), body["message"].as_str().unwrap_or(""));
        }
        Commands::Pause => {
            let body = serde_json::json!({ "pause": true });
            let resp = client
                .patch(format!("{}/config", cli.endpoint))
                .json(&body)
                .send()
                .await;
            match resp {
                Ok(r) => println!("[{}] Pause requested", r.status().as_u16()),
                Err(_) => println!("Pause: config endpoint not yet implemented"),
            }
        }
        Commands::Resume => {
            let body = serde_json::json!({ "pause": false });
            let resp = client
                .patch(format!("{}/config", cli.endpoint))
                .json(&body)
                .send()
                .await;
            match resp {
                Ok(r) => println!("[{}] Resume requested", r.status().as_u16()),
                Err(_) => println!("Resume: config endpoint not yet implemented"),
            }
        }
    }

    Ok(())
}
