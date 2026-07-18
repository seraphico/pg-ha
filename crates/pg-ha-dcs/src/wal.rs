//! WAL (Write-Ahead Log) persistence layer for Raft log entries.
//!
//! Replaces the previous full-JSON serialization approach with incremental
//! append-only writes and periodic compaction.
//!
//! # File Format
//!
//! A WAL file consists of consecutive binary records:
//!
//! ```text
//! ┌──────────┬──────────┬─────────────┬──────────────┬──────────┐
//! │ magic(2) │ type(1)  │ length(4)   │ payload(N)   │ crc32(4) │
//! └──────────┴──────────┴─────────────┴──────────────┴──────────┘
//!   0x52 0x41   u8        u32 LE        bincode bytes   u32 LE
//! ```
//!
//! - `magic`: Fixed bytes `0x52, 0x41` ("RA") to identify valid record boundaries
//! - `type`: Record type (Append=1, Purge=2, Truncate=3)
//! - `length`: Payload byte count (little-endian u32)
//! - `payload`: bincode-serialized data
//! - `crc32`: CRC32 checksum over `[type, length, payload]`
//!
//! # Crash Recovery
//!
//! During replay, incomplete or CRC-failed records cause replay to stop.
//! The trailing corrupted data is safely discarded — Raft guarantees that
//! uncommitted log entries can be dropped without data loss.
//!
//! [`WalWriter::open`] truncates any torn tail back to [`WalReplayResult::valid_len`]
//! before appending, so subsequent records are never written after garbage.
//!
//! # Compaction
//!
//! When WAL file size exceeds [`COMPACTION_THRESHOLD`], the current in-memory
//! BTreeMap is snapshot-written to a temporary file, then atomically renamed
//! to replace the old WAL.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use openraft::LogId;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::store::{LogEntry, NodeId};

// ─────────────────── Constants ───────────────────

/// Magic bytes "RA" (Raft Append) marking the start of a valid record.
const WAL_MAGIC: [u8; 2] = [0x52, 0x41];

/// Record header size: magic(2) + type(1) + length(4) = 7 bytes.
const HEADER_SIZE: usize = 2 + 1 + 4;

/// CRC32 checksum size in bytes.
const CRC_SIZE: usize = 4;

/// Reject payloads larger than this during replay (guards against corrupt length fields).
const MAX_PAYLOAD_SIZE: usize = 16 * 1024 * 1024;

/// Compaction is triggered when WAL file size exceeds this threshold (4 MB).
pub const COMPACTION_THRESHOLD: u64 = 4 * 1024 * 1024;

// ─────────────────── Record Type ───────────────────

/// WAL record type, corresponding to the three log mutation operations in openraft.
///
/// `#[repr(u8)]` ensures the discriminant can be directly written as a single byte.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordType {
    /// Append a Raft log entry (maps to `RaftStorage::append_to_log`).
    Append = 1,
    /// Purge all entries with index <= target (maps to `RaftStorage::purge_logs_upto`).
    Purge = 2,
    /// Truncate all entries with index >= target (maps to `RaftStorage::delete_conflict_logs_since`).
    Truncate = 3,
}

impl RecordType {
    /// Reconstruct from a raw byte read from disk.
    ///
    /// Returns `None` for invalid values, indicating file corruption.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Append),
            2 => Some(Self::Purge),
            3 => Some(Self::Truncate),
            _ => None,
        }
    }
}

// ─────────────────── Payload Structs ───────────────────

/// WAL payload for a Purge operation.
///
/// Records the fact that all log entries with index <= `log_id.index` have been removed.
#[derive(Serialize, Deserialize)]
pub struct PurgePayload {
    /// Full LogId required by openraft (includes leader_id); purge is inclusive of `index`.
    pub log_id: LogId<NodeId>,
}

/// WAL payload for a Truncate operation.
///
/// Records the removal of all log entries with index >= `since_index`
/// (caused by leader conflict rollback).
#[derive(Serialize, Deserialize)]
pub struct TruncatePayload {
    /// Truncate from this index onwards (inclusive).
    pub since_index: u64,
}

// ─────────────────── Replay Result ───────────────────

/// Result of replaying a WAL file, representing the reconstructed log state.
///
/// Used by `MemStore::new_persistent()` at startup to replace JSON-based loading.
pub struct WalReplayResult {
    /// Reconstructed Raft log entries (key = log index).
    pub log: BTreeMap<u64, LogEntry>,
    /// The LogId from the last Purge record, if any.
    pub last_purged: Option<LogId<NodeId>>,
    /// Byte offset after the last successfully validated record (for truncating torn tails).
    pub valid_len: u64,
}

// ─────────────────── WalWriter ───────────────────

/// Append-only WAL file writer for Raft log persistence.
///
/// Writes log mutations incrementally to disk and performs compaction
/// when the file grows beyond [`COMPACTION_THRESHOLD`].
///
/// # Durability
///
/// - [`append_record`](Self::append_record): write + flush + fsync (single-op durable)
/// - [`append_record_buffered`](Self::append_record_buffered) + [`sync`](Self::sync):
///   batch several records then fsync once
pub struct WalWriter {
    /// Buffered file writer to reduce syscall frequency.
    writer: BufWriter<File>,
    /// WAL file path (used for atomic rename during compaction).
    path: PathBuf,
    /// Current file size tracked in memory to avoid frequent stat calls.
    file_size: u64,
}

impl WalWriter {
    /// Open or create the WAL file in append mode.
    ///
    /// Replays existing content, truncates any torn tail, then opens for append.
    pub fn open(path: PathBuf) -> io::Result<(Self, WalReplayResult)> {
        let replayed = replay_wal(&path)?;

        if path.exists() {
            let meta_len = std::fs::metadata(&path)?.len();
            if meta_len > replayed.valid_len {
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(replayed.valid_len)?;
                f.sync_all()?;
            }
        }

        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let file_size = file.metadata()?.len();

        let writer = Self {
            writer: BufWriter::new(file),
            path,
            file_size,
        };

        Ok((writer, replayed))
    }

    /// Append a single WAL record and fsync to disk.
    pub fn append_record(&mut self, record_type: RecordType, payload: &[u8]) -> io::Result<()> {
        self.append_record_buffered(record_type, payload)?;
        self.sync()
    }

    /// Append a record to the buffer without fsync.
    ///
    /// Call [`sync`](Self::sync) after a batch to make writes durable.
    pub fn append_record_buffered(
        &mut self,
        record_type: RecordType,
        payload: &[u8],
    ) -> io::Result<()> {
        write_record_bytes(&mut self.writer, record_type, payload)?;
        self.file_size += (HEADER_SIZE + payload.len() + CRC_SIZE) as u64;
        Ok(())
    }

    /// Flush buffered writes and fsync the underlying file.
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    /// Returns the current WAL file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Compact the WAL by writing a fresh snapshot of the current log state.
    ///
    /// Process:
    /// 1. Flush any pending buffered writes on the live writer
    /// 2. Write all valid entries (and optionally a Purge record) to a temp file
    /// 3. fsync the temp file
    /// 4. Atomically rename it over the current WAL
    /// 5. fsync the parent directory
    /// 6. Reopen the new file in append mode
    ///
    /// If compaction fails at any step before rename, the old WAL remains intact.
    pub fn compact(
        &mut self,
        log: &BTreeMap<u64, LogEntry>,
        last_purged: Option<LogId<NodeId>>,
    ) -> io::Result<()> {
        // Ensure buffered appends are not lost when we drop/replace the writer.
        self.writer.flush()?;

        let compact_path = self.path.with_extension("compact");
        {
            let file = File::create(&compact_path)?;
            let mut tmp_writer = BufWriter::new(file);

            if let Some(log_id) = last_purged {
                let purge = PurgePayload { log_id };
                let payload = bincode::serialize(&purge)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                write_record_bytes(&mut tmp_writer, RecordType::Purge, &payload)?;
            }

            for entry in log.values() {
                let payload = bincode::serialize(entry)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                write_record_bytes(&mut tmp_writer, RecordType::Append, &payload)?;
            }

            tmp_writer.flush()?;
            tmp_writer.get_ref().sync_all()?;
        }

        std::fs::rename(&compact_path, &self.path)?;

        if let Some(parent) = self.path.parent() {
            // Open the directory (do NOT create — parent is a directory).
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }

        let file = OpenOptions::new().append(true).open(&self.path)?;
        let file_size = file.metadata()?.len();
        self.writer = BufWriter::new(file);
        self.file_size = file_size;

        Ok(())
    }
}

/// Write one record (header + payload + CRC) to `writer` without flush/fsync.
fn write_record_bytes(
    writer: &mut impl Write,
    record_type: RecordType,
    payload: &[u8],
) -> io::Result<()> {
    let length = payload.len() as u32;

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&[record_type as u8]);
    hasher.update(&length.to_le_bytes());
    hasher.update(payload);
    let crc = hasher.finalize();

    writer.write_all(&WAL_MAGIC)?;
    writer.write_all(&[record_type as u8])?;
    writer.write_all(&length.to_le_bytes())?;
    writer.write_all(payload)?;
    writer.write_all(&crc.to_le_bytes())?;
    Ok(())
}

// ─────────────────── Replay ───────────────────

/// Replay a WAL file from the beginning, reconstructing the log state.
///
/// Returns an empty result if the file does not exist (first startup).
/// Stops replay gracefully on EOF or any corrupted/incomplete trailing record.
pub fn replay_wal(path: &Path) -> io::Result<WalReplayResult> {
    let mut result = WalReplayResult {
        log: BTreeMap::new(),
        last_purged: None,
        valid_len: 0,
    };

    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(result),
        Err(e) => return Err(e),
    };

    let mut reader = BufReader::new(file);
    loop {
        match read_one_record(&mut reader) {
            Ok(Some((record_type, payload, record_len))) => {
                if let Err(e) = apply_record(&mut result, record_type, &payload) {
                    warn!("WAL replay stopped: {e}");
                    break;
                }
                result.valid_len += record_len;
            }
            Ok(None) => break, // Clean EOF
            Err(_) => break,   // Corrupted / incomplete tail — discard
        }
    }
    Ok(result)
}

/// Attempt to read one WAL record from the reader.
///
/// Returns:
/// - `Ok(Some((type, payload, record_len)))` — successfully read one record
/// - `Ok(None)` — reached end of file (no more data)
/// - `Err(_)` — data corruption or incomplete record
fn read_one_record(reader: &mut BufReader<File>) -> io::Result<Option<(RecordType, Vec<u8>, u64)>> {
    let mut header = [0u8; HEADER_SIZE];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    if header[0..2] != WAL_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }

    let record_type = RecordType::from_u8(header[2])
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown record type"))?;

    let length = u32::from_le_bytes(header[3..7].try_into().unwrap()) as usize;
    if length > MAX_PAYLOAD_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "payload too large",
        ));
    }

    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload)?;

    let mut crc_bytes = [0u8; CRC_SIZE];
    reader.read_exact(&mut crc_bytes)?;
    let stored_crc = u32::from_le_bytes(crc_bytes);

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&[header[2]]);
    hasher.update(&header[3..7]);
    hasher.update(&payload);
    let computed_crc = hasher.finalize();

    if computed_crc != stored_crc {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
    }

    let record_len = (HEADER_SIZE + length + CRC_SIZE) as u64;
    Ok(Some((record_type, payload, record_len)))
}

/// Apply a single WAL record to the replay result.
///
/// Deserialization failure is treated as corruption and returned as an error
/// so the caller can stop replay without advancing `valid_len`.
fn apply_record(
    result: &mut WalReplayResult,
    record_type: RecordType,
    payload: &[u8],
) -> io::Result<()> {
    match record_type {
        RecordType::Append => {
            let entry = bincode::deserialize::<LogEntry>(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.log.insert(entry.log_id.index, entry);
        }
        RecordType::Purge => {
            let purge = bincode::deserialize::<PurgePayload>(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.log.retain(|&idx, _| idx > purge.log_id.index);
            result.last_purged = Some(purge.log_id);
        }
        RecordType::Truncate => {
            let truncate = bincode::deserialize::<TruncatePayload>(payload)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            result.log.retain(|&idx, _| idx < truncate.since_index);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_wal_path(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("pg-ha-wal-{label}-{nanos}.wal"))
    }

    #[test]
    fn roundtrip_truncate_records() {
        let path = temp_wal_path("roundtrip");
        let _ = std::fs::remove_file(&path);

        {
            let (mut w, _) = WalWriter::open(path.clone()).unwrap();
            let p1 = bincode::serialize(&TruncatePayload { since_index: 9 }).unwrap();
            let p2 = bincode::serialize(&TruncatePayload { since_index: 3 }).unwrap();
            w.append_record_buffered(RecordType::Truncate, &p1).unwrap();
            w.append_record_buffered(RecordType::Truncate, &p2).unwrap();
            w.sync().unwrap();
            assert!(w.file_size() > 0);
        }

        let replayed = replay_wal(&path).unwrap();
        assert_eq!(replayed.valid_len, std::fs::metadata(&path).unwrap().len());
        // Truncate payloads do not insert into `log`, but both records must validate.
        assert!(replayed.valid_len > 0);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_truncates_torn_tail_then_allows_append() {
        let path = temp_wal_path("torn");
        let _ = std::fs::remove_file(&path);

        {
            let (mut w, _) = WalWriter::open(path.clone()).unwrap();
            let payload = bincode::serialize(&TruncatePayload { since_index: 9 }).unwrap();
            w.append_record(RecordType::Truncate, &payload).unwrap();
        }

        let good_len = std::fs::metadata(&path).unwrap().len();

        // Simulate a crash mid-write: append an incomplete header.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0x52, 0x41, 0x01, 0xff]).unwrap();
            f.sync_all().unwrap();
        }
        assert!(std::fs::metadata(&path).unwrap().len() > good_len);

        // Re-open must truncate the garbage before appending.
        {
            let (mut w, replayed) = WalWriter::open(path.clone()).unwrap();
            assert_eq!(replayed.valid_len, good_len);
            assert_eq!(std::fs::metadata(&path).unwrap().len(), good_len);

            let payload = bincode::serialize(&TruncatePayload { since_index: 3 }).unwrap();
            w.append_record(RecordType::Truncate, &payload).unwrap();
        }

        let again = replay_wal(&path).unwrap();
        assert_eq!(again.valid_len, std::fs::metadata(&path).unwrap().len());
        assert!(again.valid_len > good_len);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn rejects_oversized_payload_length() {
        let path = temp_wal_path("oversized");
        let _ = std::fs::remove_file(&path);

        // Craft a header claiming a huge payload (no valid body).
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(&WAL_MAGIC).unwrap();
            f.write_all(&[RecordType::Truncate as u8]).unwrap();
            f.write_all(&(u32::MAX).to_le_bytes()).unwrap();
            f.sync_all().unwrap();
        }

        let replayed = replay_wal(&path).unwrap();
        assert_eq!(replayed.valid_len, 0);

        let _ = std::fs::remove_file(&path);
    }
}
