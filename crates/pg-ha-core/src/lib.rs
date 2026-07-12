//! pg-ha-core: Core HA logic for PostgreSQL High Availability
//!
//! This crate provides:
//! - Configuration parsing and validation
//! - Cluster state types (Cluster, Leader, Member, SyncState, etc.)
//! - DCS adapter trait (implemented by pg-ha-dcs)
//! - HA decision engine (the loop that manages failover/promotion)
//! - PostgreSQL lifecycle management (start/stop/promote/rewind)
//! - Watchdog integration
//! - Failsafe mode logic
//! - Replication slot and synchronous replication management

pub mod bootstrap;
pub mod callbacks;
pub mod cascading;
pub mod cluster;
pub mod commands;
pub mod config;
pub mod dcs;
pub mod dynamic_config;
pub mod error;
pub mod failsafe;
pub mod ha;
pub mod history;
pub mod postgresql;
pub mod slots;
pub mod standby_cluster;
pub mod sync;
pub mod watchdog;

pub use bootstrap::{Bootstrap, BootstrapResult};
pub use callbacks::{CallbackEvent, CallbackExecutor, CallbacksConfig};
pub use cascading::{CascadeManager, CascadeNode};
pub use cluster::{
    Cluster, ClusterConfig, Failover, Leader, Member, MemberRole, MemberState, SyncState,
};
pub use commands::{CommandResponse, CommandStatus, ManagementCommand};
pub use config::Config;
pub use dcs::DcsAdapter;
pub use dynamic_config::{
    ConfigChanges, DynamicConfigState, GlobalConfig, PgParamClassification,
    PostgresqlDynamicConfig, classify_pg_param, detect_changes, patch_config,
};
pub use error::{Error, Result};
pub use ha::{CommandSender, CycleResult, Ha};
pub use history::{History, HistoryEntry, HistoryEventType};
pub use postgresql::{PgState, Postgresql, WalStatus, parse_lsn};
pub use slots::SlotManager;
pub use standby_cluster::{StandbyCluster, StandbyClusterConfig};
pub use watchdog::Watchdog;
