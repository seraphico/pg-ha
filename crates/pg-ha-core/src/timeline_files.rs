//! D6: local timeline evidence read from the data directory.
//!
//! PostgreSQL names a new timeline `findNewestTimeLine(recoveryTargetTLI) + 1`
//! by probing `pg_wal/<TLI>.history` files, so the newest history file on disk
//! is the highest timeline this node knows exists. A history file's last line
//! is the switchpoint where that timeline began. Before writing a new history
//! file, a promote copies the last segment under the new timeline's name
//! (`XLogInitNewTimeline`), and a WAL receiver names the segments it writes
//! with the timeline it streams; a WAL segment named with a timeline means
//! the node has WAL on it, not only its history file.
//!
//! Also the pg-ha files that belong to the data directory's history:
//! `pg-ha.generation` (D6 generation) and `pg-ha.accept_timeline_loss`
//! (operator override).

use crate::cluster::HistoryEvidence;
use std::io::Write;
use std::path::Path;

/// History generation of this data directory (missing = 0). Copied with the
/// data by pg_basebackup and pg_rewind, which is right: it describes the
/// history, which the copy shares with its source.
pub(crate) const GENERATION_FILE: &str = "pg-ha.generation";

/// Operator override (D6): this node may campaign although it is not on the
/// latest timeline, accepting the loss of the newer history.
pub(crate) const ACCEPT_TIMELINE_LOSS_FILE: &str = "pg-ha.accept_timeline_loss";

/// One line of a timeline history file: `parent` ended at `switchpoint`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HistoryLine {
    pub(crate) parent: u64,
    pub(crate) switchpoint: u64,
}

/// Parse a history file. Blank lines and `#` comments are skipped, like
/// PostgreSQL's readTimeLineHistory. Parents must increase and switchpoints
/// must not decrease; anything else is an error, never a guess.
pub(crate) fn parse_history(content: &str) -> Result<Vec<HistoryLine>, String> {
    let mut lines: Vec<HistoryLine> = Vec::new();
    for (n, raw) in content.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split_whitespace();
        let (Some(tli), Some(lsn)) = (fields.next(), fields.next()) else {
            return Err(format!("line {}: expected '<timeline> <lsn>'", n + 1));
        };
        let parent: u64 = tli
            .parse()
            .map_err(|_| format!("line {}: bad timeline '{tli}'", n + 1))?;
        let switchpoint = crate::postgresql::parse_lsn(lsn)
            .ok_or_else(|| format!("line {}: bad LSN '{lsn}'", n + 1))?;
        if let Some(prev) = lines.last()
            && (parent <= prev.parent || switchpoint < prev.switchpoint)
        {
            return Err(format!("line {}: timelines out of order", n + 1));
        }
        lines.push(HistoryLine {
            parent,
            switchpoint,
        });
    }
    Ok(lines)
}

/// `<TLI>.history` -> TLI.
fn history_file_timeline(name: &str) -> Option<u64> {
    let hex = name.strip_suffix(".history")?;
    (hex.len() == 8 && hex.chars().all(|c| c.is_ascii_hexdigit()))
        .then(|| u64::from_str_radix(hex, 16).ok())
        .flatten()
}

/// A 24-hex-digit WAL segment name -> its TLI.
fn segment_timeline(name: &str) -> Option<u64> {
    (name.len() == 24 && name.chars().all(|c| c.is_ascii_hexdigit()))
        .then(|| u64::from_str_radix(&name[..8], 16).ok())
        .flatten()
}

/// Begin LSN of `timeline` from its history file (0 for timeline 1).
/// `Ok(None)` when the file does not exist.
pub(crate) fn timeline_begin(data_dir: &Path, timeline: u64) -> Result<Option<u64>, String> {
    if timeline <= 1 {
        return Ok(Some(0));
    }
    let path = data_dir
        .join("pg_wal")
        .join(format!("{timeline:08X}.history"));
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    let lines = parse_history(&content).map_err(|e| format!("{}: {e}", path.display()))?;
    match lines.last() {
        Some(last) if last.parent < timeline => Ok(Some(last.switchpoint)),
        Some(_) => Err(format!(
            "{}: parent timeline not below {timeline}",
            path.display()
        )),
        None => Err(format!("{}: no entries", path.display())),
    }
}

/// SB-37: the ancestors of `timeline` and where each ended, from its own
/// history file (empty for timeline 1). `Ok(None)` when the file does not
/// exist. Every line must name a parent below `timeline`.
pub(crate) fn timeline_ancestors(
    data_dir: &Path,
    timeline: u64,
) -> Result<Option<Vec<HistoryLine>>, String> {
    if timeline <= 1 {
        return Ok(Some(Vec::new()));
    }
    let path = data_dir
        .join("pg_wal")
        .join(format!("{timeline:08X}.history"));
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    let lines = parse_history(&content).map_err(|e| format!("{}: {e}", path.display()))?;
    match lines.last() {
        Some(last) if last.parent < timeline => Ok(Some(lines)),
        Some(_) => Err(format!(
            "{}: parent timeline not below {timeline}",
            path.display()
        )),
        None => Err(format!("{}: no entries", path.display())),
    }
}

/// Newest history on disk: highest `<TLI>.history` (timeline 1 when none),
/// its begin LSN, and whether a WAL segment named with it exists.
pub(crate) fn newest_history(data_dir: &Path) -> Result<HistoryEvidence, String> {
    let wal_dir = data_dir.join("pg_wal");
    let entries = std::fs::read_dir(&wal_dir).map_err(|e| format!("{}: {e}", wal_dir.display()))?;
    let mut newest = 1u64;
    let mut segment_timelines = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|e| format!("{}: {e}", wal_dir.display()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(tli) = history_file_timeline(name) {
            newest = newest.max(tli);
        } else if let Some(tli) = segment_timeline(name) {
            segment_timelines.push(tli);
        }
    }
    let begin = timeline_begin(data_dir, newest)?
        .ok_or_else(|| format!("history file of timeline {newest} vanished"))?;
    Ok(HistoryEvidence {
        timeline: newest,
        begin,
        wal: segment_timelines.contains(&newest),
    })
}

/// SB-37 / SB-31: this standby's WAL left timeline `replay_timeline` after the
/// point where the newest timeline on disk (`newest`) branched off it, so
/// PostgreSQL cannot follow `newest` ("new timeline N forked off current
/// database system timeline M before current recovery point").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ForkedOff {
    pub(crate) newest: u64,
    pub(crate) replay_timeline: u64,
    pub(crate) switchpoint: u64,
    pub(crate) replay_lsn: u64,
}

impl std::fmt::Display for ForkedOff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "replayed timeline {} up to {} but timeline {} left timeline {} at {}",
            self.replay_timeline,
            crate::postgresql::format_lsn(self.replay_lsn),
            self.newest,
            self.replay_timeline,
            crate::postgresql::format_lsn(self.switchpoint),
        )
    }
}

/// The pure check. `newest_ancestors` is the content of `<newest>.history`.
/// Forked off when `newest` is above the replay timeline, its history lists
/// the replay timeline with switchpoint `S`, and the replay LSN is past `S`.
/// A replay LSN at `S` holds exactly the WAL the new timeline began with.
pub(crate) fn forked_off(
    replay_timeline: u64,
    replay_lsn: u64,
    newest: u64,
    newest_ancestors: &[HistoryLine],
) -> Option<ForkedOff> {
    if newest <= replay_timeline {
        return None;
    }
    let line = newest_ancestors
        .iter()
        .find(|l| l.parent == replay_timeline)?;
    (replay_lsn > line.switchpoint).then_some(ForkedOff {
        newest,
        replay_timeline,
        switchpoint: line.switchpoint,
        replay_lsn,
    })
}

/// Timelines named in `pg_wal`: the highest `<TLI>.history` (1 when none)
/// and the highest timeline of a WAL segment file (0 when none).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LocalTimelines {
    pub(crate) newest_history: u64,
    pub(crate) highest_segment: u64,
}

pub(crate) fn local_timelines(data_dir: &Path) -> Result<LocalTimelines, String> {
    let wal_dir = data_dir.join("pg_wal");
    let entries = std::fs::read_dir(&wal_dir).map_err(|e| format!("{}: {e}", wal_dir.display()))?;
    let mut t = LocalTimelines {
        newest_history: 1,
        highest_segment: 0,
    };
    for entry in entries {
        let entry = entry.map_err(|e| format!("{}: {e}", wal_dir.display()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(tli) = history_file_timeline(name) {
            t.newest_history = t.newest_history.max(tli);
        } else if let Some(tli) = segment_timeline(name) {
            t.highest_segment = t.highest_segment.max(tli);
        }
    }
    Ok(t)
}

/// [`forked_off`] on this data directory. `timeline_lower_bound` and
/// `replay_lsn` describe the replayed WAL (live: `pg_last_wal_replay_lsn()`;
/// before a start: pg_control's minimum recovery point, which never exceeds
/// what was replayed).
///
/// The replay timeline is `max(timeline_lower_bound, highest segment
/// timeline)`. That is exact: the segment holding the last replayed record
/// is named with that record's timeline and is kept (it is past the last
/// restartpoint), and a segment named with a higher timeline means WAL of
/// that timeline was received, in which case nothing has forked off it.
pub(crate) fn local_fork(
    data_dir: &Path,
    timeline_lower_bound: u64,
    replay_lsn: u64,
) -> Result<Option<ForkedOff>, String> {
    let local = local_timelines(data_dir)?;
    let replay_timeline = timeline_lower_bound.max(local.highest_segment);
    if local.newest_history <= replay_timeline {
        return Ok(None);
    }
    let Some(ancestors) = timeline_ancestors(data_dir, local.newest_history)? else {
        return Ok(None);
    };
    Ok(forked_off(
        replay_timeline,
        replay_lsn,
        local.newest_history,
        &ancestors,
    ))
}

/// Generation of the data directory. A missing file is generation 0; an
/// unreadable one is an error (never assumed current).
pub(crate) fn read_generation(data_dir: &Path) -> Result<u64, String> {
    let path = data_dir.join(GENERATION_FILE);
    match std::fs::read_to_string(&path) {
        Ok(s) => s
            .trim()
            .parse()
            .map_err(|_| format!("{}: not a number", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// Write the generation durably: temp file, fsync, rename, fsync the dir.
pub(crate) fn write_generation(data_dir: &Path, generation: u64) -> std::io::Result<()> {
    let tmp = data_dir.join(format!("{GENERATION_FILE}.tmp"));
    let result = (|| {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(format!("{generation}\n").as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, data_dir.join(GENERATION_FILE))?;
        std::fs::File::open(data_dir)?.sync_all()
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// Identity of the running server: PID and start time from the first and
/// third lines of `postmaster.pid`, which the postmaster writes once when it
/// starts (a crash restart of the backends keeps them). `Ok(None)` when the
/// file does not exist (server stopped).
pub(crate) fn server_run(data_dir: &Path) -> Result<Option<String>, String> {
    let path = data_dir.join("postmaster.pid");
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    let mut lines = content.lines();
    let pid = lines.next().map(str::trim).unwrap_or_default();
    let start = lines.nth(1).map(str::trim).unwrap_or_default();
    if pid.parse::<u64>().is_err() || start.parse::<u64>().is_err() {
        return Err(format!("{}: unexpected content", path.display()));
    }
    Ok(Some(format!("pid={pid} start={start}")))
}

pub(crate) fn has_accept_timeline_loss(data_dir: &Path) -> bool {
    std::fs::symlink_metadata(data_dir.join(ACCEPT_TIMELINE_LOSS_FILE)).is_ok_and(|m| m.is_file())
}

pub(crate) fn remove_accept_timeline_loss(data_dir: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(data_dir.join(ACCEPT_TIMELINE_LOSS_FILE)) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_data_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pg-ha-tlfiles-{label}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("pg_wal")).unwrap();
        dir
    }

    fn put(dir: &Path, name: &str, content: &str) {
        std::fs::write(dir.join("pg_wal").join(name), content).unwrap();
    }

    /// Content PostgreSQL wrote on the 2026-10-26 docker cluster.
    const TL3: &str = "1\t0/404D660\tno recovery target specified\n\n\
                       2\t0/404D778\tno recovery target specified\n";

    #[test]
    fn parses_postgres_history_files() {
        let lines = parse_history(TL3).unwrap();
        assert_eq!(
            lines,
            vec![
                HistoryLine {
                    parent: 1,
                    switchpoint: 0x404D660
                },
                HistoryLine {
                    parent: 2,
                    switchpoint: 0x404D778
                },
            ]
        );
        assert_eq!(parse_history("# comment\n\n").unwrap(), vec![]);
    }

    #[test]
    fn rejects_malformed_history_files() {
        assert!(parse_history("1\n").is_err());
        assert!(parse_history("x\t0/1\treason\n").is_err());
        assert!(parse_history("1\tzz\treason\n").is_err());
        assert!(
            parse_history("2\t0/10\ta\n1\t0/20\tb\n").is_err(),
            "parent order"
        );
        assert!(
            parse_history("1\t0/20\ta\n2\t0/10\tb\n").is_err(),
            "switchpoint order"
        );
    }

    #[test]
    fn newest_history_without_files_is_timeline_one() {
        let dir = temp_data_dir("none");
        put(&dir, "000000010000000000000004", "");
        let h = newest_history(&dir).unwrap();
        assert_eq!(
            h,
            HistoryEvidence {
                timeline: 1,
                begin: 0,
                wal: true
            }
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn newest_history_reports_begin_and_wal() {
        let dir = temp_data_dir("newest");
        put(&dir, "00000002.history", "1\t0/404D660\tx\n");
        put(&dir, "00000003.history", TL3);
        put(&dir, "000000020000000000000004", "");
        put(&dir, "000000020000000000000004.partial", "");
        put(&dir, "archive_status", "");
        let h = newest_history(&dir).unwrap();
        assert_eq!(h.timeline, 3);
        assert_eq!(h.begin, 0x404D778);
        assert!(!h.wal, "history file only: no segment on timeline 3");

        put(&dir, "000000030000000000000004", "");
        assert!(newest_history(&dir).unwrap().wal);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn timeline_ancestors_reads_the_timelines_own_file() {
        let dir = temp_data_dir("ancestors");
        assert_eq!(timeline_ancestors(&dir, 1).unwrap(), Some(vec![]));
        assert_eq!(timeline_ancestors(&dir, 3).unwrap(), None, "missing file");
        put(&dir, "00000003.history", TL3);
        let lines = timeline_ancestors(&dir, 3).unwrap().unwrap();
        assert_eq!(
            lines.iter().map(|l| l.parent).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(lines[1].switchpoint, 0x404D778);
        put(&dir, "00000004.history", "5\t0/10\tx\n");
        assert!(timeline_ancestors(&dir, 4).is_err(), "parent above");
        put(&dir, "00000005.history", "");
        assert!(timeline_ancestors(&dir, 5).is_err(), "empty file");
        put(&dir, "00000006.history", "1\tzz\tx\n");
        assert!(timeline_ancestors(&dir, 6).is_err(), "malformed");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn timeline_begin_of_missing_or_first_timeline() {
        let dir = temp_data_dir("begin");
        assert_eq!(timeline_begin(&dir, 1).unwrap(), Some(0));
        assert_eq!(timeline_begin(&dir, 5).unwrap(), None);
        put(&dir, "00000005.history", "4\t0/10\tx\n");
        assert_eq!(timeline_begin(&dir, 5).unwrap(), Some(0x10));
        put(&dir, "00000006.history", "7\t0/10\tx\n");
        assert!(
            timeline_begin(&dir, 6).is_err(),
            "parent above the timeline"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ── SB-37 / SB-31: forked-off standby ──
    // Values of the PG 18.7 run (evidence/pr-b2-prestart-probe.md): the new
    // primary's 00000002.history is "1 0/8037260"; the named voter replayed
    // timeline 1 up to 0/8038750 (30 commits past the switchpoint), and its
    // pg_control minimum recovery point was 0/7000120.
    const S: u64 = 0x8037260;
    const REPLAY_PAST: u64 = 0x8038750;
    const TL2: &str = "1\t0/8037260\tno recovery target specified\n";

    fn line(parent: u64, switchpoint: u64) -> HistoryLine {
        HistoryLine {
            parent,
            switchpoint,
        }
    }

    #[test]
    fn forked_off_table() {
        let tl2 = [line(1, S)];
        let tl3 = [line(1, 0x10), line(2, S)];
        let f = forked_off(1, REPLAY_PAST, 2, &tl2).expect("past S");
        assert_eq!((f.newest, f.replay_timeline, f.switchpoint), (2, 1, S));
        assert_eq!(
            f.to_string(),
            "replayed timeline 1 up to 0/8038750 but timeline 2 left timeline 1 at 0/8037260"
        );
        assert_eq!(forked_off(1, S, 2, &tl2), None, "at S: the same WAL");
        assert_eq!(forked_off(1, S - 1, 2, &tl2), None, "behind S");
        assert_eq!(forked_off(2, REPLAY_PAST, 2, &tl2), None, "on the newest");
        assert_eq!(
            forked_off(3, REPLAY_PAST, 2, &tl2),
            None,
            "above the newest"
        );
        assert_eq!(
            forked_off(4, REPLAY_PAST, 5, &tl3),
            None,
            "timeline 4 is not an ancestor of 5's file"
        );
        // A grandparent: timeline 3 left 1 at 0x10; a node on 1 past 0x10
        // forked off 3 even when it is behind 2's switchpoint.
        assert_eq!(
            forked_off(1, 0x11, 3, &tl3).map(|f| f.switchpoint),
            Some(0x10)
        );
        assert_eq!(forked_off(2, S, 3, &tl3), None);
        assert!(forked_off(2, S + 1, 3, &tl3).is_some());
    }

    fn fork_dir(
        label: &str,
        history: Option<(&str, &str)>,
        segments: &[&str],
    ) -> std::path::PathBuf {
        let dir = temp_data_dir(label);
        for s in segments {
            put(&dir, s, "");
        }
        if let Some((name, content)) = history {
            put(&dir, name, content);
        }
        put(&dir, "000000010000000000000007.partial", "");
        dir
    }

    const SEG7: &str = "000000010000000000000007";
    const SEG8: &str = "000000010000000000000008";

    // Start: inputs from pg_controldata (minimum recovery point).
    #[test]
    fn start_check_cases() {
        let dir = fork_dir(
            "start-diverged",
            Some(("00000002.history", TL2)),
            &[SEG7, SEG8],
        );
        assert!(
            local_fork(&dir, 1, REPLAY_PAST).unwrap().is_some(),
            "diverged: marker"
        );
        assert_eq!(
            local_fork(&dir, 1, 0x7000120).unwrap(),
            None,
            "minimum recovery point not past S (the real run): no marker"
        );
        assert_eq!(local_fork(&dir, 1, S).unwrap(), None, "at S: no marker");
        let _ = std::fs::remove_dir_all(&dir);

        let dir = fork_dir("start-no-history", None, &[SEG7, SEG8]);
        assert_eq!(
            local_fork(&dir, 1, REPLAY_PAST).unwrap(),
            None,
            "no history file"
        );
        let _ = std::fs::remove_dir_all(&dir);

        // A node on timeline 4; timeline 5 descends from 1, 2, 3 only.
        let dir = fork_dir(
            "start-unrelated",
            Some(("00000005.history", "1\t0/10\tx\n2\t0/20\tx\n3\t0/30\tx\n")),
            &["000000040000000000000008"],
        );
        assert_eq!(
            local_fork(&dir, 4, REPLAY_PAST).unwrap(),
            None,
            "history of an unrelated timeline: no marker"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Running: inputs from pg_last_wal_replay_lsn() and the live lower bound.
    #[test]
    fn running_check_cases() {
        let dir = fork_dir(
            "run-diverged",
            Some(("00000002.history", TL2)),
            &[SEG7, SEG8],
        );
        let f = local_fork(&dir, 1, REPLAY_PAST)
            .unwrap()
            .expect("diverged running standby");
        assert_eq!(
            (f.newest, f.replay_timeline, f.switchpoint, f.replay_lsn),
            (2, 1, S, REPLAY_PAST)
        );
        assert_eq!(local_fork(&dir, 1, S).unwrap(), None, "replay at S: none");
        assert_eq!(
            local_fork(&dir, 1, S - 8).unwrap(),
            None,
            "replay behind S: none"
        );
        assert_eq!(
            local_fork(&dir, 2, REPLAY_PAST).unwrap(),
            None,
            "the receiver says timeline 2: none"
        );
        // A survivor that streams timeline 2 has a segment named with it.
        put(&dir, "000000020000000000000008", "");
        assert_eq!(
            local_fork(&dir, 1, REPLAY_PAST).unwrap(),
            None,
            "WAL on timeline 2 received (published timeline 1 is only a lower bound): none"
        );
        let _ = std::fs::remove_dir_all(&dir);

        let dir = fork_dir("run-no-history", None, &[SEG8]);
        assert_eq!(
            local_fork(&dir, 1, REPLAY_PAST).unwrap(),
            None,
            "no history: none"
        );
        let _ = std::fs::remove_dir_all(&dir);

        // A node on timeline 2; timeline 3 branched off timeline 1, not 2.
        let dir = fork_dir(
            "run-unrelated",
            Some(("00000003.history", "1\t0/10\tx\n")),
            &["000000020000000000000008"],
        );
        assert_eq!(
            local_fork(&dir, 2, REPLAY_PAST).unwrap(),
            None,
            "unrelated: none"
        );
        let _ = std::fs::remove_dir_all(&dir);

        let dir = fork_dir(
            "run-bad-history",
            Some(("00000002.history", "junk\n")),
            &[SEG8],
        );
        assert!(
            local_fork(&dir, 1, REPLAY_PAST).is_err(),
            "unreadable: error, never a guess"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn local_timelines_reads_names_only() {
        let dir = fork_dir(
            "local-tl",
            Some(("00000003.history", TL3)),
            &[SEG7, "000000020000000000000008"],
        );
        assert_eq!(
            local_timelines(&dir).unwrap(),
            LocalTimelines {
                newest_history: 3,
                highest_segment: 2
            }
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn generation_file_round_trip() {
        let dir = temp_data_dir("gen");
        assert_eq!(read_generation(&dir).unwrap(), 0);
        write_generation(&dir, 3).unwrap();
        assert_eq!(read_generation(&dir).unwrap(), 3);
        assert!(!dir.join(format!("{GENERATION_FILE}.tmp")).exists());
        std::fs::write(dir.join(GENERATION_FILE), "junk").unwrap();
        assert!(read_generation(&dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn server_run_from_postmaster_pid() {
        let dir = temp_data_dir("pid");
        assert_eq!(server_run(&dir).unwrap(), None);
        std::fs::write(
            dir.join("postmaster.pid"),
            "42\n/var/lib/postgresql/data\n1793043457\n5432\n/var/run/postgresql\n*\n",
        )
        .unwrap();
        assert_eq!(
            server_run(&dir).unwrap().as_deref(),
            Some("pid=42 start=1793043457")
        );
        std::fs::write(dir.join("postmaster.pid"), "42\n").unwrap();
        assert!(server_run(&dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn accept_timeline_loss_file() {
        let dir = temp_data_dir("accept");
        assert!(!has_accept_timeline_loss(&dir));
        std::fs::write(dir.join(ACCEPT_TIMELINE_LOSS_FILE), "").unwrap();
        assert!(has_accept_timeline_loss(&dir));
        remove_accept_timeline_loss(&dir).unwrap();
        remove_accept_timeline_loss(&dir).unwrap();
        assert!(!has_accept_timeline_loss(&dir));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
