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
//! # Compaction
//!
//! When WAL file size exceeds [`COMPACTION_THRESHOLD`], the current in-memory
//! BTreeMap is snapshot-written to a temporary file, then atomically renamed
//! to replace the old WAL.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::store::{LogEntry, LogId, NodeId};

// ─────────────────── Constants ───────────────────

/// Magic bytes "RA" (Raft Append) marking the start of a valid record.
const WAL_MAGIC: [u8; 2] = [0x52, 0x41];

/// Record header size: magic(2) + type(1) + length(4) = 7 bytes.
const HEADER_SIZE: usize = 2 + 1 + 4;

/// CRC32 checksum size in bytes.
const CRC_SIZE: usize = 4;

/// Compaction is triggered when WAL file size exceeds this threshold (4 MB).
pub const COMPACTION_THRESHOLD: u64 = 4 * 1024 * 1024;

// ─────────────────── Record Type ───────────────────

/// WAL record type, corresponding to the three log mutation operations in openraft.
///
/// `#[repr(u8)]` ensures the discriminant can be directly written as a single byte.
#[repr(u8)]
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
/// Records the fact that all log entries with index <= `up_to_index` have been removed.
#[derive(Serialize, Deserialize)]
pub struct PurgePayload {
    /// Purge up to this index (inclusive).
    pub up_to_index: u64,
    /// Full LogId required by openraft (includes leader_id information).
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
}

// ─────────────────── WalWriter ───────────────────

/// Append-only WAL file writer for Raft log persistence.
///
/// Writes log mutations incrementally to disk and performs compaction
/// when the file grows beyond [`COMPACTION_THRESHOLD`].
///
/// # Durability
///
/// Every [`append_record`](Self::append_record) call issues `flush()` + `fsync()`,
/// guaranteeing that a successfully returned write survives power loss.
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
    /// If the file already exists, new records will be appended after existing content.
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        let file_size = file.metadata()?.len();
        let writer = BufWriter::new(file);

        Ok(Self {
            writer,
            path,
            file_size,
        })
    }

    /// Append a single WAL record and fsync to disk.
    ///
    /// Writes the full record (header + payload + CRC) atomically from the
    /// application's perspective: either the entire record is durable, or
    /// it will be detected as incomplete during replay and discarded.
    pub fn append_record(&mut self, record_type: RecordType, payload: &[u8]) -> io::Result<()> {
        let length = payload.len() as u32;
        let mut crc_input = Vec::with_capacity(1 + 4 + payload.len());
        crc_input.push(record_type as u8);
        crc_input.extend_from_slice(&length.to_le_bytes());
        crc_input.extend_from_slice(payload);

        let crc = crc32fast::hash(&crc_input);

        self.writer.write_all(&WAL_MAGIC)?;
        self.writer.write_all(&[record_type as u8])?;
        self.writer.write_all(&length.to_le_bytes())?;
        self.writer.write_all(payload)?;
        self.writer.write_all(&crc.to_le_bytes())?;

        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        let bytes_written = (HEADER_SIZE + payload.len() + CRC_SIZE) as u64;
        self.file_size += bytes_written;

        Ok(())
    }

    /// Returns the current WAL file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Compact the WAL by writing a fresh snapshot of the current log state.
    ///
    /// Process:
    /// 1. Write all valid entries (and optionally a Purge record) to a temp file
    /// 2. fsync the temp file
    /// 3. Atomically rename it over the current WAL (POSIX atomic guarantee)
    /// 4. Reopen the new file in append mode
    ///
    /// If compaction fails at any step before rename, the old WAL remains intact.
    pub fn compact(
        &mut self,
        log: &BTreeMap<u64, LogEntry>,
        last_purged: Option<LogId<NodeId>>,
    ) -> io::Result<()> {
        let compact_path = self.path.with_extension("compact");
        {
            let file = File::create(&compact_path)?;
            let mut tmp_writer = BufWriter::new(file);

            // Write the purge marker so replay knows the starting point
            if let Some(log_id) = last_purged {
                let purge = PurgePayload {
                    up_to_index: log_id.index,
                    log_id,
                };
                let payload = bincode::serialize(&purge)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                Self::write_record_to(&mut tmp_writer, RecordType::Purge, &payload)?;
            }

            // Write all current entries
            for entry in log.values() {
                let payload = bincode::serialize(entry)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                Self::write_record_to(&mut tmp_writer, RecordType::Append, &payload)?;
            }

            tmp_writer.flush()?;
            tmp_writer.get_ref().sync_all()?;
        }

        // Atomic replace
        std::fs::rename(&compact_path, &self.path)?;

        // Reopen in append mode
        let file = OpenOptions::new().append(true).open(&self.path)?;
        let file_size = file.metadata()?.len();
        self.writer = BufWriter::new(file);
        self.file_size = file_size;

        Ok(())
    }

    /// Write a single record to an arbitrary writer (used by compaction).
    ///
    /// Does NOT flush or fsync — the caller is responsible for that.
    fn write_record_to(
        writer: &mut BufWriter<File>,
        record_type: RecordType,
        payload: &[u8],
    ) -> io::Result<()> {
        let length = payload.len() as u32;

        let mut crc_input = Vec::with_capacity(1 + 4 + payload.len());
        crc_input.push(record_type as u8);
        crc_input.extend_from_slice(&length.to_le_bytes());
        crc_input.extend_from_slice(payload);
        let crc = crc32fast::hash(&crc_input);

        writer.write_all(&WAL_MAGIC)?;
        writer.write_all(&[record_type as u8])?;
        writer.write_all(&length.to_le_bytes())?;
        writer.write_all(payload)?;
        writer.write_all(&crc.to_le_bytes())?;
        Ok(())
    }
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
    };

    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(result),
        Err(e) => return Err(e),
    };

    let mut reader = BufReader::new(file);
    loop {
        match read_one_record(&mut reader) {
            Ok(Some((record_type, payload))) => {
                apply_record(&mut result, record_type, &payload);
            }
            Ok(None) => break,  // Clean EOF
            Err(_) => break,    // Corrupted tail, safe to discard
        }
    }
    Ok(result)
}

/// Attempt to read one WAL record from the reader.
///
/// Returns:
/// - `Ok(Some((type, payload)))` — successfully read one record
/// - `Ok(None)` — reached end of file (no more data)
/// - `Err(_)` — data corruption or incomplete record
fn read_one_record(reader: &mut BufReader<File>) -> io::Result<Option<(RecordType, Vec<u8>)>> {
    // Read header: magic(2) + type(1) + length(4)
    let mut header = [0u8; HEADER_SIZE];
    match reader.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    // Validate magic bytes
    if header[0..2] != WAL_MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }

    // Parse record type
    let record_type = RecordType::from_u8(header[2])
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown record type"))?;

    // Parse payload length
    let length = u32::from_le_bytes([header[3], header[4], header[5], header[6]]) as usize;

    // Read payload
    let mut payload = vec![0u8; length];
    reader.read_exact(&mut payload)?;

    // Read and verify CRC32
    let mut crc_bytes = [0u8; CRC_SIZE];
    reader.read_exact(&mut crc_bytes)?;
    let stored_crc = u32::from_le_bytes(crc_bytes);

    let mut crc_input = Vec::with_capacity(1 + 4 + length);
    crc_input.push(header[2]);
    crc_input.extend_from_slice(&header[3..7]);
    crc_input.extend_from_slice(&payload);
    let computed_crc = crc32fast::hash(&crc_input);

    if stored_crc != computed_crc {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "CRC mismatch"));
    }

    Ok(Some((record_type, payload)))
}

/// Apply a single WAL record to the replay result.
///
/// Silently logs a warning and skips the record if deserialization fails
/// (should not happen with a valid WAL, but defensive against corruption).
fn apply_record(result: &mut WalReplayResult, record_type: RecordType, payload: &[u8]) {
    match record_type {
        RecordType::Append => match bincode::deserialize::<LogEntry>(payload) {
            Ok(entry) => {
                result.log.insert(entry.log_id.index, entry);
            }
            Err(e) => {
                warn!("WAL replay: failed to deserialize Append record: {e}");
            }
        },
        RecordType::Purge => match bincode::deserialize::<PurgePayload>(payload) {
            Ok(purge) => {
                result.log.retain(|&idx, _| idx > purge.up_to_index);
                result.last_purged = Some(purge.log_id);
            }
            Err(e) => {
                warn!("WAL replay: failed to deserialize Purge record: {e}");
            }
        },
        RecordType::Truncate => match bincode::deserialize::<TruncatePayload>(payload) {
            Ok(trunc) => {
                result.log.retain(|&idx, _| idx < trunc.since_index);
            }
            Err(e) => {
                warn!("WAL replay: failed to deserialize Truncate record: {e}");
            }
        },
    }
}
