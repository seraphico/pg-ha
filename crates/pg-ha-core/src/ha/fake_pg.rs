//! Test-only fake PostgreSQL server for the simple query protocol.
//!
//! It accepts connections without authentication, answers each `Query`
//! message from a list of `(substring, reply)` rules (first match wins) and
//! records every SQL string in order. A query no rule matches gets an
//! `ErrorResponse`, so an unexpected query fails the test that issued it.
//! Everything `Postgresql` runs on the promote path (`is_in_recovery`,
//! `primary_timeline`, `wal_status`, `replication_status`, `ALTER SYSTEM`)
//! is a simple query, so no production code needs a seam for it.

use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// What the server answers to a matching query.
#[derive(Clone)]
pub(super) enum Reply {
    /// One result set: column names and text rows.
    Rows(Vec<&'static str>, Vec<Vec<String>>),
    /// A command without rows (`ALTER SYSTEM`), with its command tag.
    Done(&'static str),
}

pub(super) struct FakePg {
    pub(super) port: u16,
    sql: Arc<Mutex<Vec<String>>>,
}

impl FakePg {
    /// Listen on 127.0.0.1 on a free port and serve `rules` until the test
    /// runtime ends.
    pub(super) async fn start(rules: Vec<(&'static str, Reply)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let sql = Arc::new(Mutex::new(Vec::new()));
        let rules = Arc::new(rules);
        let seen = sql.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let (rules, seen) = (rules.clone(), seen.clone());
                tokio::spawn(async move {
                    let _ = serve(stream, &rules, &seen).await;
                });
            }
        });
        FakePg { port, sql }
    }

    /// Every SQL string received so far, in order.
    pub(super) fn queries(&self) -> Vec<String> {
        self.sql.lock().unwrap().clone()
    }
}

fn message(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut m = vec![kind];
    m.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
    m.extend_from_slice(body);
    m
}

fn cstr(s: &str) -> Vec<u8> {
    let mut v = s.as_bytes().to_vec();
    v.push(0);
    v
}

fn ready() -> Vec<u8> {
    message(b'Z', b"I")
}

fn error(text: &str) -> Vec<u8> {
    let mut body = Vec::new();
    for (code, val) in [(b'S', "ERROR"), (b'C', "XX000"), (b'M', text)] {
        body.push(code);
        body.extend(cstr(val));
    }
    body.push(0);
    let mut out = message(b'E', &body);
    out.extend(ready());
    out
}

fn rows(cols: &[&str], data: &[Vec<String>]) -> Vec<u8> {
    let mut desc = (cols.len() as u16).to_be_bytes().to_vec();
    for c in cols {
        desc.extend(cstr(c));
        desc.extend(0u32.to_be_bytes()); // table oid
        desc.extend(0u16.to_be_bytes()); // column number
        desc.extend(25u32.to_be_bytes()); // type oid: text
        desc.extend((-1i16).to_be_bytes()); // type size
        desc.extend((-1i32).to_be_bytes()); // type modifier
        desc.extend(0u16.to_be_bytes()); // text format
    }
    let mut out = message(b'T', &desc);
    for row in data {
        let mut body = (row.len() as u16).to_be_bytes().to_vec();
        for v in row {
            body.extend((v.len() as u32).to_be_bytes());
            body.extend(v.as_bytes());
        }
        out.extend(message(b'D', &body));
    }
    out.extend(message(b'C', &cstr(&format!("SELECT {}", data.len()))));
    out.extend(ready());
    out
}

async fn serve(
    mut s: TcpStream,
    rules: &[(&'static str, Reply)],
    seen: &Mutex<Vec<String>>,
) -> std::io::Result<()> {
    // Startup. An SSLRequest (code 80877103) is declined with 'N'.
    loop {
        let len = s.read_u32().await? as usize;
        let mut body = vec![0u8; len - 4];
        s.read_exact(&mut body).await?;
        let code = u32::from_be_bytes([body[0], body[1], body[2], body[3]]);
        if code == 80_877_103 {
            s.write_all(b"N").await?;
            continue;
        }
        break;
    }
    let mut hello = message(b'R', &0u32.to_be_bytes());
    hello.extend(ready());
    s.write_all(&hello).await?;
    loop {
        let kind = s.read_u8().await?;
        let len = s.read_u32().await? as usize;
        let mut body = vec![0u8; len - 4];
        s.read_exact(&mut body).await?;
        if kind != b'Q' {
            return Ok(());
        }
        let sql = String::from_utf8_lossy(&body[..body.len().saturating_sub(1)]).into_owned();
        seen.lock().unwrap().push(sql.clone());
        let answer = match rules.iter().find(|(pat, _)| sql.contains(pat)) {
            Some((_, Reply::Rows(cols, data))) => rows(cols, data),
            Some((_, Reply::Done(tag))) => {
                let mut o = message(b'C', &cstr(tag));
                o.extend(ready());
                o
            }
            None => error(&format!("fake_pg: unexpected query: {sql}")),
        };
        s.write_all(&answer).await?;
    }
}
