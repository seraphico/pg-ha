//! Files pg-ha writes into PGDATA to make PostgreSQL start as a standby.
//!
//! One writer for every path that configures a standby (clone, rejoin,
//! reconfigure, switchover): `standby.signal` is created and fsync'd, and
//! `postgresql.auto.conf` is rewritten through a temp file + rename, replacing
//! only `primary_conninfo` / `recovery_target_timeline`. Every other line
//! (`ALTER SYSTEM` content such as `synchronous_standby_names`) is kept.

use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::config::ConnectionParams;

pub(crate) const STANDBY_SIGNAL: &str = "standby.signal";
pub(crate) const AUTO_CONF: &str = "postgresql.auto.conf";
const AUTO_CONF_TMP: &str = "postgresql.auto.conf.pg-ha.tmp";

/// Header pg-ha writes before its own lines. Any line starting with
/// `# pg-ha managed` is treated as a pg-ha header and dropped on rewrite, which
/// also removes the headers older pg-ha versions wrote.
const PG_HA_HEADER: &str = "# pg-ha managed standby configuration";
const PG_HA_HEADER_PREFIX: &str = "# pg-ha managed";

/// host and port from a member conn_url (`host=X port=Y ...`). None when there
/// is no non-empty `host=`. A missing port means 5432 (the PostgreSQL default).
pub(crate) fn conn_url_host_port(conn_url: &str) -> Option<(String, String)> {
    let mut host = None;
    let mut port = None;
    for part in conn_url.split_whitespace() {
        if let Some(val) = part.strip_prefix("host=") {
            host = Some(val.to_string());
        } else if let Some(val) = part.strip_prefix("port=") {
            port = Some(val.to_string());
        }
    }
    let host = host.filter(|h| !h.is_empty())?;
    let port = port
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| "5432".to_string());
    Some((host, port))
}

/// True when a member conn_url carries a non-empty `host=` key.
pub(crate) fn conn_url_has_host(conn_url: &str) -> bool {
    conn_url_host_port(conn_url).is_some()
}

/// host/port from a member conn_url, user/password from the local replication
/// config. None when the conn_url has no `host=` (the leader's address is
/// unknown; never guess 127.0.0.1). An unset password is omitted.
pub(crate) fn build_primary_conninfo(
    upstream_conn_url: &str,
    replication: &ConnectionParams,
    application_name: &str,
) -> Option<String> {
    if !conn_url_has_host(upstream_conn_url) {
        return None;
    }
    let (host, port) = conn_url_host_port(upstream_conn_url)?;
    // The member conn_url carries superuser credentials; only host/port are
    // taken from it.
    let mut conninfo = format!(
        "host={} port={} user={}",
        libpq_quote(&host),
        libpq_quote(&port),
        libpq_quote(&replication.username)
    );
    if let Some(password) = replication.password.as_deref() {
        conninfo.push_str(&format!(" password={}", libpq_quote(password)));
    }
    conninfo.push_str(&format!(
        " application_name={}",
        libpq_quote(application_name)
    ));
    Some(conninfo)
}

/// Key of a configuration-file line (`name = value` or `name value`), or ""
/// for comments, blank lines and anything that does not start with a name.
fn conf_line_key(line: &str) -> &str {
    let trimmed = line.trim_start();
    let end = trimmed
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
        .unwrap_or(trimmed.len());
    &trimmed[..end]
}

/// New postgresql.auto.conf content.
///
/// Drops lines whose key is `primary_conninfo` (always) or
/// `recovery_target_timeline` (only when a conninfo is written), and pg-ha's
/// own header lines; keeps every other line in order. With a conninfo, appends
/// the pg-ha header, `primary_conninfo` and `recovery_target_timeline = 'latest'`.
pub(crate) fn rewrite_auto_conf(existing: &str, primary_conninfo: Option<&str>) -> String {
    let mut out = String::new();
    for line in existing.lines() {
        if line.trim_start().starts_with(PG_HA_HEADER_PREFIX) {
            continue;
        }
        let key = conf_line_key(line);
        if key.eq_ignore_ascii_case("primary_conninfo") {
            continue;
        }
        if primary_conninfo.is_some() && key.eq_ignore_ascii_case("recovery_target_timeline") {
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    if let Some(conninfo) = primary_conninfo {
        out.push_str(PG_HA_HEADER);
        out.push('\n');
        out.push_str(&format!("primary_conninfo = {}\n", conf_quote(conninfo)));
        out.push_str("recovery_target_timeline = 'latest'\n");
    }
    out
}

/// Create/truncate standby.signal, fsync it and the data directory.
pub(crate) fn ensure_standby_signal(data_dir: &Path) -> std::io::Result<()> {
    let file = File::create(data_dir.join(STANDBY_SIGNAL))?;
    file.sync_all()?;
    drop(file);
    fsync_dir(data_dir)
}

/// ensure_standby_signal, then rewrite postgresql.auto.conf via temp file
/// (write + sync_all) + rename + fsync(data_dir). A missing auto.conf counts as
/// empty. On error the temp file is removed; nothing is half-written.
pub(crate) fn write_standby_config(
    data_dir: &Path,
    primary_conninfo: Option<&str>,
) -> std::io::Result<()> {
    ensure_standby_signal(data_dir)?;

    let auto_conf = data_dir.join(AUTO_CONF);
    let (existing, permissions) = match std::fs::read_to_string(&auto_conf) {
        Ok(content) => (content, Some(std::fs::metadata(&auto_conf)?.permissions())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (String::new(), None),
        Err(e) => return Err(e),
    };
    let content = rewrite_auto_conf(&existing, primary_conninfo);

    let tmp = data_dir.join(AUTO_CONF_TMP);
    let result = (|| {
        let mut file = File::create(&tmp)?;
        file.write_all(content.as_bytes())?;
        file.sync_all()?;
        drop(file);
        if let Some(permissions) = permissions {
            std::fs::set_permissions(&tmp, permissions)?;
        }
        std::fs::rename(&tmp, &auto_conf)
    })();
    if let Err(e) = result {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    fsync_dir(data_dir)
}

/// Rewind-check marker (D4): this node may hold WAL beyond a fork point and
/// must be verified with pg_rewind before it follows a leader.
pub(crate) const REWIND_CHECK_MARKER: &str = "pg-ha.rewind_check";

/// Create PGDATA/pg-ha.rewind_check (content: reason + UTC time), fsync the
/// file and the data directory. Idempotent (overwrites).
pub(crate) fn create_rewind_check_marker(data_dir: &Path, reason: &str) -> std::io::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut file = File::create(data_dir.join(REWIND_CHECK_MARKER))?;
    file.write_all(format!("reason: {reason}\ncreated: {}\n", format_utc(now)).as_bytes())?;
    file.sync_all()?;
    drop(file);
    fsync_dir(data_dir)
}

/// Remove pg-ha's own state files copied from another node by pg_basebackup
/// (A16): they describe the source, not this node. A missing file is fine.
pub(crate) fn remove_node_state_files(data_dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(data_dir.join(REWIND_CHECK_MARKER)) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Unix seconds as `YYYY-MM-DDTHH:MM:SSZ` (proleptic Gregorian, UTC).
fn format_utc(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    // Days since 1970-01-01 to civil date (H. Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
}

/// libpq conninfo value: quoted when empty or containing whitespace, `'` or
/// `\`; inside quotes `\` and `'` are backslash-escaped.
fn libpq_quote(value: &str) -> String {
    let needs_quotes = value.is_empty()
        || value
            .chars()
            .any(|c| c.is_whitespace() || c == '\'' || c == '\\');
    if !needs_quotes {
        return value.to_string();
    }
    let escaped = value.replace('\\', "\\\\").replace('\'', "\\'");
    format!("'{escaped}'")
}

/// Configuration-file string literal: `\` doubled, `'` doubled, single-quoted.
fn conf_quote(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('\'', "''");
    format!("'{escaped}'")
}

fn fsync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn temp_data_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("pg-ha-sc-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn replication(password: Option<&str>) -> ConnectionParams {
        ConnectionParams {
            username: "replicator".to_string(),
            password: password.map(str::to_string),
            dbname: "postgres".to_string(),
        }
    }

    /// Minimal GUC string-literal unescape (`''` → `'`, `\\` → `\`), the
    /// inverse of conf_quote.
    fn conf_unquote(quoted: &str) -> String {
        let inner = quoted
            .strip_prefix('\'')
            .and_then(|s| s.strip_suffix('\''))
            .expect("quoted literal");
        let mut out = String::new();
        let mut chars = inner.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '\'' => {
                    assert_eq!(chars.next(), Some('\''), "lone quote in {quoted}");
                    out.push('\'');
                }
                '\\' => out.push(chars.next().expect("dangling backslash")),
                c => out.push(c),
            }
        }
        out
    }

    fn count_lines(content: &str, prefix: &str) -> usize {
        content.lines().filter(|l| l.starts_with(prefix)).count()
    }

    const PG_HEADER: &str = "# Do not edit this file manually!\n\
                             # It will be overwritten by the ALTER SYSTEM command.\n";

    #[test]
    fn rewrite_keeps_alter_system_lines() {
        let existing = format!(
            "{PG_HEADER}synchronous_standby_names = 'FIRST 1 (node2)'\n\
             primary_conninfo = 'host=old port=5432'\n\
             recovery_target_timeline = 'latest'\n\
             work_mem = '64MB'\n"
        );
        let out = rewrite_auto_conf(&existing, Some("host=node2 port=5432"));
        assert_eq!(
            out,
            format!(
                "{PG_HEADER}synchronous_standby_names = 'FIRST 1 (node2)'\n\
                 work_mem = '64MB'\n\
                 # pg-ha managed standby configuration\n\
                 primary_conninfo = 'host=node2 port=5432'\n\
                 recovery_target_timeline = 'latest'\n"
            )
        );
        assert_eq!(count_lines(&out, "primary_conninfo"), 1);
        assert_eq!(count_lines(&out, "recovery_target_timeline"), 1);
    }

    #[test]
    fn rewrite_none_removes_only_primary_conninfo() {
        let existing = format!(
            "{PG_HEADER}synchronous_standby_names = 'FIRST 1 (node2)'\n\
             primary_conninfo = 'host=old port=5432'\n\
             recovery_target_timeline = 'latest'\n"
        );
        let out = rewrite_auto_conf(&existing, None);
        assert_eq!(
            out,
            format!(
                "{PG_HEADER}synchronous_standby_names = 'FIRST 1 (node2)'\n\
                 recovery_target_timeline = 'latest'\n"
            )
        );
        assert!(!out.contains("pg-ha managed"));
    }

    #[test]
    fn rewrite_is_idempotent() {
        let existing = format!(
            "{PG_HEADER}synchronous_standby_names = 'FIRST 1 (node2)'\n\
             primary_conninfo = 'host=old'\n"
        );
        for conninfo in [Some("host=node2 port=5432"), None] {
            let once = rewrite_auto_conf(&existing, conninfo);
            let twice = rewrite_auto_conf(&once, conninfo);
            assert_eq!(once, twice, "conninfo {conninfo:?}");
        }
    }

    #[test]
    fn rewrite_drops_legacy_pg_ha_headers() {
        for header in [
            "# pg-ha managed standby configuration",
            "# pg-ha managed standby configuration (rejoin)",
            "# pg-ha managed (switchover demotion)",
        ] {
            let existing = format!(
                "{header}\nprimary_conninfo = 'host=old'\nrecovery_target_timeline = 'latest'\n"
            );
            let out = rewrite_auto_conf(&existing, Some("host=new"));
            assert_eq!(count_lines(&out, "# pg-ha managed"), 1, "{header}: {out}");
            assert!(!out.contains(header) || header == PG_HA_HEADER, "{out}");
            let out = rewrite_auto_conf(&existing, None);
            assert_eq!(count_lines(&out, "# pg-ha managed"), 0, "{header}: {out}");
        }
    }

    #[test]
    fn rewrite_matches_keys_case_insensitively() {
        let existing = "PRIMARY_CONNINFO='x'\n  primary_conninfo   =  'y'\n\
                        primary_conninfo 'z'\n\
                        primary_slot_name = 's'\n\
                        Recovery_Target_Timeline = '3'\n\
                        # primary_conninfo = 'commented out'\n";
        let out = rewrite_auto_conf(existing, None);
        assert_eq!(
            out,
            "primary_slot_name = 's'\nRecovery_Target_Timeline = '3'\n\
             # primary_conninfo = 'commented out'\n"
        );
        let out = rewrite_auto_conf(existing, Some("host=n"));
        assert_eq!(
            out,
            "primary_slot_name = 's'\n# primary_conninfo = 'commented out'\n\
             # pg-ha managed standby configuration\n\
             primary_conninfo = 'host=n'\nrecovery_target_timeline = 'latest'\n"
        );
    }

    #[test]
    fn rewrite_of_empty_file() {
        assert_eq!(rewrite_auto_conf("", None), "");
        assert_eq!(
            rewrite_auto_conf("", Some("host=n")),
            "# pg-ha managed standby configuration\n\
             primary_conninfo = 'host=n'\nrecovery_target_timeline = 'latest'\n"
        );
    }

    #[test]
    fn conninfo_quotes_special_values() {
        let conninfo = build_primary_conninfo(
            "host=n1 port=5433",
            &replication(Some("pa ss'w\\rd")),
            "node2",
        )
        .unwrap();
        assert_eq!(
            conninfo,
            "host=n1 port=5433 user=replicator password='pa ss\\'w\\\\rd' application_name=node2"
        );
        for s in [
            conninfo.as_str(),
            "",
            "a'b",
            "a\\b",
            "''\\\\",
            "it's \\ fine",
        ] {
            assert_eq!(conf_unquote(&conf_quote(s)), s, "round trip of {s:?}");
        }
        // The config-file line PostgreSQL reads back yields the libpq string.
        let line = rewrite_auto_conf("", Some(&conninfo));
        let value = line
            .lines()
            .find_map(|l| l.strip_prefix("primary_conninfo = "))
            .unwrap();
        assert_eq!(conf_unquote(value), conninfo);
    }

    #[test]
    fn libpq_quote_table() {
        for (input, expected) in [
            ("plain", "plain"),
            ("", "''"),
            ("a b", "'a b'"),
            ("a\tb", "'a\tb'"),
            ("a'b", "'a\\'b'"),
            ("a\\b", "'a\\\\b'"),
        ] {
            assert_eq!(libpq_quote(input), expected, "input {input:?}");
        }
    }

    #[test]
    fn conninfo_omits_password_when_none() {
        let conninfo =
            build_primary_conninfo("host=n1 port=5432", &replication(None), "node2").unwrap();
        assert_eq!(
            conninfo,
            "host=n1 port=5432 user=replicator application_name=node2"
        );
        assert!(!conninfo.contains("password"));
    }

    #[test]
    fn conninfo_empty_password_is_quoted() {
        let conninfo =
            build_primary_conninfo("host=n1 port=5432", &replication(Some("")), "node2").unwrap();
        assert!(
            conninfo.contains("password='' application_name=node2"),
            "{conninfo}"
        );
    }

    #[test]
    fn conninfo_defaults_port_only() {
        let conninfo = build_primary_conninfo("host=n1", &replication(None), "node2").unwrap();
        assert!(conninfo.starts_with("host=n1 port=5432 "), "{conninfo}");
        let conninfo = build_primary_conninfo(
            "host=n1 port=5432 dbname=postgres user=postgres password=secret",
            &replication(Some("r")),
            "node2",
        )
        .unwrap();
        // The superuser credentials in the member conn_url are never used.
        assert_eq!(
            conninfo,
            "host=n1 port=5432 user=replicator password=r application_name=node2"
        );
    }

    #[test]
    fn conninfo_none_without_host() {
        for url in [
            "",
            "port=5433 dbname=postgres",
            "hostaddr=10.0.0.1",
            "host= port=1",
        ] {
            assert_eq!(
                build_primary_conninfo(url, &replication(Some("p")), "node2"),
                None,
                "conn_url {url:?}"
            );
        }
    }

    #[test]
    fn conn_url_has_host_table() {
        for (url, expected) in [
            ("host=n1 port=1", true),
            ("port=1 host=n1", true),
            ("host=n1", true),
            ("", false),
            ("host= port=1", false),
            ("hostaddr=10.0.0.1", false),
            ("port=5432", false),
        ] {
            assert_eq!(conn_url_has_host(url), expected, "conn_url {url:?}");
        }
    }

    #[test]
    fn write_creates_signal_and_preserves_lines() {
        let dir = temp_data_dir("write-preserve");
        std::fs::write(
            dir.join(AUTO_CONF),
            format!(
                "{PG_HEADER}synchronous_standby_names = 'FIRST 1 (node2)'\n\
                 primary_conninfo = 'host=old'\n"
            ),
        )
        .unwrap();
        std::fs::write(dir.join(STANDBY_SIGNAL), "stale").unwrap();

        write_standby_config(&dir, Some("host=node2 port=5432 user=replicator")).unwrap();

        assert_eq!(std::fs::read(dir.join(STANDBY_SIGNAL)).unwrap(), b"");
        let content = std::fs::read_to_string(dir.join(AUTO_CONF)).unwrap();
        assert!(content.starts_with(PG_HEADER), "{content}");
        assert!(content.contains("synchronous_standby_names = 'FIRST 1 (node2)'\n"));
        assert!(content.contains("primary_conninfo = 'host=node2 port=5432 user=replicator'\n"));
        assert!(!content.contains("host=old"));
        assert!(!dir.join(AUTO_CONF_TMP).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_none_removes_conninfo() {
        let dir = temp_data_dir("write-none");
        write_standby_config(&dir, None).unwrap();
        assert!(dir.join(STANDBY_SIGNAL).exists());
        assert_eq!(std::fs::read_to_string(dir.join(AUTO_CONF)).unwrap(), "");

        std::fs::write(
            dir.join(AUTO_CONF),
            "work_mem = '4MB'\nprimary_conninfo = 'host=old'\n",
        )
        .unwrap();
        write_standby_config(&dir, None).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.join(AUTO_CONF)).unwrap(),
            "work_mem = '4MB'\n"
        );
        assert!(!dir.join(AUTO_CONF_TMP).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn write_keeps_auto_conf_permissions() {
        use std::os::unix::fs::PermissionsExt;
        let dir = temp_data_dir("write-perms");
        let auto_conf = dir.join(AUTO_CONF);
        std::fs::write(&auto_conf, "work_mem = '4MB'\n").unwrap();
        std::fs::set_permissions(&auto_conf, std::fs::Permissions::from_mode(0o600)).unwrap();

        write_standby_config(&dir, Some("host=n")).unwrap();

        let mode = std::fs::metadata(&auto_conf).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_fails_cleanly_when_signal_cannot_be_created() {
        let dir = temp_data_dir("write-signal-dir");
        std::fs::create_dir(dir.join(STANDBY_SIGNAL)).unwrap();
        let original =
            "synchronous_standby_names = 'FIRST 1 (node2)'\nprimary_conninfo = 'host=old'\n";
        std::fs::write(dir.join(AUTO_CONF), original).unwrap();

        assert!(write_standby_config(&dir, Some("host=n")).is_err());
        assert!(write_standby_config(&dir, None).is_err());
        assert!(ensure_standby_signal(&dir).is_err());

        assert_eq!(
            std::fs::read_to_string(dir.join(AUTO_CONF)).unwrap(),
            original
        );
        assert!(!dir.join(AUTO_CONF_TMP).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_fails_cleanly_when_auto_conf_cannot_be_replaced() {
        let dir = temp_data_dir("write-auto-conf-dir");
        // A directory where auto.conf should be: reading it fails, so nothing
        // is renamed over it and no temp file is left behind.
        std::fs::create_dir(dir.join(AUTO_CONF)).unwrap();

        assert!(write_standby_config(&dir, Some("host=n")).is_err());

        assert!(dir.join(AUTO_CONF).is_dir());
        assert!(!dir.join(AUTO_CONF_TMP).exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_fails_for_missing_data_dir() {
        let dir = std::env::temp_dir().join(format!("pg-ha-sc-missing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        assert!(write_standby_config(&dir, Some("host=n")).is_err());
        assert!(ensure_standby_signal(&dir).is_err());

        assert!(!dir.exists(), "nothing may be created");
    }

    #[test]
    fn marker_is_created_and_fsynced() {
        let dir = temp_data_dir("marker");
        create_rewind_check_marker(&dir, "demoted after losing the leader lock").unwrap();
        let content = std::fs::read_to_string(dir.join(REWIND_CHECK_MARKER)).unwrap();
        assert!(
            content.contains("reason: demoted after losing the leader lock\n"),
            "{content}"
        );
        assert!(content.contains("created: "), "{content}");
        // Idempotent: a second call overwrites.
        create_rewind_check_marker(&dir, "second").unwrap();
        let content = std::fs::read_to_string(dir.join(REWIND_CHECK_MARKER)).unwrap();
        assert!(content.starts_with("reason: second\n"), "{content}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn marker_fails_for_missing_dir() {
        let dir =
            std::env::temp_dir().join(format!("pg-ha-sc-marker-missing-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        assert!(create_rewind_check_marker(&dir, "x").is_err());
        assert!(!dir.exists(), "nothing may be created");
    }

    #[test]
    fn remove_node_state_files_drops_copied_marker() {
        let dir = temp_data_dir("state-files");
        create_rewind_check_marker(&dir, "copied from the clone source").unwrap();
        remove_node_state_files(&dir).unwrap();
        assert!(!dir.join(REWIND_CHECK_MARKER).exists());
        // Nothing to remove is not an error.
        remove_node_state_files(&dir).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn format_utc_table() {
        for (secs, expected) in [
            (0, "1970-01-01T00:00:00Z"),
            (951_782_400, "2000-02-29T00:00:00Z"),
            (1_709_251_199, "2024-02-29T23:59:59Z"),
            (4_102_444_800, "2100-01-01T00:00:00Z"),
        ] {
            assert_eq!(format_utc(secs), expected, "{secs}");
        }
    }
}
