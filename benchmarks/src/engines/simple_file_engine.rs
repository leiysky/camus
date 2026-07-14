use super::Engine;
use crate::model::{
    decode_value, encode_value, key_stream, kv_key, InputRecord, PendingRecord, Token,
};
use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use xxhash_rust::xxh3::xxh3_64;

const DATA_FILE: &str = "append.data";
const RELEASE_FILE: &str = "append.release";
const DATA_HEADER_MAGIC: [u8; 8] = *b"SAFDAT01";
const DATA_COMMIT_MAGIC: [u8; 8] = *b"SAFCMT01";
const RELEASE_HEADER_MAGIC: [u8; 8] = *b"SAFREL01";
const RELEASE_COMMIT_MAGIC: [u8; 8] = *b"SAFRCM01";
const HEADER_LEN: usize = 32;
const COMMIT_LEN: usize = 16;
const KEY_LEN: usize = 16;
const DATA_DESCRIPTOR_LEN: usize = KEY_LEN + 8;

#[derive(Clone, Copy)]
struct RecordPointer {
    value_offset: u64,
    value_len: u64,
}

struct State {
    data: File,
    releases: File,
    records: BTreeMap<(u64, u64), RecordPointer>,
    released: BTreeSet<(u64, u64)>,
}

pub(crate) struct SimpleAppendFileEngine {
    state: Arc<Mutex<State>>,
}

impl SimpleAppendFileEngine {
    pub(crate) async fn open(path: &Path) -> Result<Self> {
        let path = PathBuf::from(path);
        let state = tokio::task::spawn_blocking(move || open_state(&path))
            .await
            .context("join simple append-file open task")??;
        Ok(Self {
            state: Arc::new(Mutex::new(state)),
        })
    }
}

#[async_trait]
impl Engine for SimpleAppendFileEngine {
    async fn append_batch(&self, stream: u64, records: Vec<InputRecord>) -> Result<Vec<Token>> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            ensure!(!records.is_empty(), "append-file batch must not be empty");
            let prepared = prepare_data_frame(stream, records)?;
            let mut state = lock(&state)?;
            for (key, _, _) in &prepared.entries {
                ensure!(
                    !state.records.contains_key(key),
                    "append-file record key was reused"
                );
            }

            let base_offset = state
                .data
                .metadata()
                .context("read append-file data length")?
                .len();
            for (_, relative_offset, _) in &prepared.entries {
                base_offset
                    .checked_add(*relative_offset)
                    .context("append-file value offset overflow")?;
            }
            let tokens = prepared
                .entries
                .iter()
                .map(|(key, _, _)| Token::Kv(kv_key(key.0, key.1)))
                .collect();
            state
                .data
                .write_all(&prepared.bytes)
                .context("write append-file data frame")?;
            state
                .data
                .sync_data()
                .context("sync append-file data frame")?;

            for (key, relative_offset, value_len) in prepared.entries {
                state.records.insert(
                    key,
                    RecordPointer {
                        value_offset: base_offset + relative_offset,
                        value_len,
                    },
                );
            }
            Ok(tokens)
        })
        .await
        .context("join simple append-file write task")?
    }

    async fn read(
        &self,
        stream: u64,
        max_records: usize,
        max_payload_bytes: u64,
    ) -> Result<Vec<PendingRecord>> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let state = lock(&state)?;
            let mut output = Vec::with_capacity(max_records);
            let mut payload_bytes = 0_u64;
            for (&key, &pointer) in state.records.range((stream, 0)..=(stream, u64::MAX)) {
                if state.released.contains(&key) {
                    continue;
                }
                let value_len = usize::try_from(pointer.value_len)
                    .context("append-file value length does not fit usize")?;
                let mut value = vec![0_u8; value_len];
                read_exact_at(&state.data, &mut value, pointer.value_offset)
                    .context("read append-file value")?;
                let (metadata, payload) = decode_value(&value)?;
                let projected = payload_bytes
                    .checked_add(u64::try_from(payload.len()).context("payload length overflow")?)
                    .context("append-file read payload total overflow")?;
                if projected > max_payload_bytes {
                    if output.is_empty() {
                        bail!("first append-file record exceeds the read byte limit");
                    }
                    break;
                }
                output.push(PendingRecord {
                    token: Token::Kv(kv_key(key.0, key.1)),
                    metadata,
                    payload,
                });
                payload_bytes = projected;
                if output.len() == max_records {
                    break;
                }
            }
            Ok(output)
        })
        .await
        .context("join simple append-file read task")?
    }

    async fn release(&self, stream: u64, tokens: Vec<Token>) -> Result<()> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let mut state = lock(&state)?;
            let mut keys = BTreeSet::new();
            for token in tokens {
                let Token::Kv(key) = token else {
                    bail!("received a Camus token in the append-file adapter");
                };
                let parsed = parse_key(&key)?;
                ensure!(
                    parsed.0 == stream,
                    "append-file release token belongs to another stream"
                );
                ensure!(
                    state.records.contains_key(&parsed),
                    "append-file release token is unknown"
                );
                if !state.released.contains(&parsed) {
                    keys.insert(parsed);
                }
            }
            if keys.is_empty() {
                return Ok(());
            }

            let frame = prepare_release_frame(&keys)?;
            state
                .releases
                .write_all(&frame)
                .context("write append-file release frame")?;
            state
                .releases
                .sync_data()
                .context("sync append-file release frame")?;
            state.released.extend(keys);
            Ok(())
        })
        .await
        .context("join simple append-file release task")?
    }

    async fn pending_count(&self) -> Result<u64> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let state = lock(&state)?;
            let pending = state
                .records
                .len()
                .checked_sub(state.released.len())
                .context("append-file pending count underflow")?;
            u64::try_from(pending).context("append-file pending count does not fit u64")
        })
        .await
        .context("join simple append-file count task")?
    }

    async fn shutdown(&self) -> Result<()> {
        let state = self.state.clone();
        tokio::task::spawn_blocking(move || {
            let state = lock(&state)?;
            state.data.sync_data().context("sync append-file data")?;
            state
                .releases
                .sync_data()
                .context("sync append-file releases")?;
            Ok(())
        })
        .await
        .context("join simple append-file shutdown task")?
    }
}

struct PreparedDataFrame {
    bytes: Vec<u8>,
    entries: Vec<((u64, u64), u64, u64)>,
}

fn prepare_data_frame(stream: u64, records: Vec<InputRecord>) -> Result<PreparedDataFrame> {
    let mut encoded = Vec::with_capacity(records.len());
    let descriptor_capacity = records
        .len()
        .checked_mul(DATA_DESCRIPTOR_LEN)
        .context("append-file descriptor allocation overflow")?;
    let mut descriptor_bytes = Vec::with_capacity(descriptor_capacity);
    let mut body_len = 0_u64;
    for record in records {
        let key = kv_key(stream, record.sequence);
        let value = encode_value(&record)?;
        let value_len = u64::try_from(value.len()).context("append-file value length overflow")?;
        body_len = body_len
            .checked_add(u64::try_from(DATA_DESCRIPTOR_LEN).expect("fixed length fits u64"))
            .and_then(|bytes| bytes.checked_add(value_len))
            .context("append-file data body length overflow")?;
        descriptor_bytes.extend_from_slice(&key);
        descriptor_bytes.extend_from_slice(&value_len.to_le_bytes());
        encoded.push(((stream, record.sequence), key, value_len, value));
    }

    let count = u64::try_from(encoded.len()).context("append-file record count overflow")?;
    let frame_len = u64::try_from(HEADER_LEN + COMMIT_LEN)
        .expect("fixed lengths fit u64")
        .checked_add(body_len)
        .context("append-file frame length overflow")?;
    let mut bytes = Vec::with_capacity(
        usize::try_from(frame_len).context("append-file frame length does not fit usize")?,
    );
    bytes.extend_from_slice(&frame_header(DATA_HEADER_MAGIC, body_len, count));
    let mut entries = Vec::with_capacity(encoded.len());
    for (key, serialized_key, value_len, value) in encoded {
        bytes.extend_from_slice(&serialized_key);
        bytes.extend_from_slice(&value_len.to_le_bytes());
        let value_offset = u64::try_from(bytes.len()).context("value offset overflow")?;
        bytes.extend_from_slice(&value);
        entries.push((key, value_offset, value_len));
    }
    bytes.extend_from_slice(&frame_commit(DATA_COMMIT_MAGIC, xxh3_64(&descriptor_bytes)));
    Ok(PreparedDataFrame { bytes, entries })
}

fn prepare_release_frame(keys: &BTreeSet<(u64, u64)>) -> Result<Vec<u8>> {
    let count = u64::try_from(keys.len()).context("append-file release count overflow")?;
    let body_len = count
        .checked_mul(u64::try_from(KEY_LEN).expect("fixed length fits u64"))
        .context("append-file release body length overflow")?;
    let frame_len = u64::try_from(HEADER_LEN + COMMIT_LEN)
        .expect("fixed lengths fit u64")
        .checked_add(body_len)
        .context("append-file release frame length overflow")?;
    let mut body = Vec::with_capacity(
        usize::try_from(body_len).context("release body length does not fit usize")?,
    );
    for &(stream, sequence) in keys {
        body.extend_from_slice(&kv_key(stream, sequence));
    }
    let mut frame = Vec::with_capacity(
        usize::try_from(frame_len).context("release frame length does not fit usize")?,
    );
    frame.extend_from_slice(&frame_header(RELEASE_HEADER_MAGIC, body_len, count));
    frame.extend_from_slice(&body);
    frame.extend_from_slice(&frame_commit(RELEASE_COMMIT_MAGIC, xxh3_64(&body)));
    Ok(frame)
}

fn open_state(path: &Path) -> Result<State> {
    let data_path = path.join(DATA_FILE);
    let release_path = path.join(RELEASE_FILE);
    let created_data = !data_path.exists();
    let created_releases = !release_path.exists();
    let data = open_append_file(&data_path)?;
    let releases = open_append_file(&release_path)?;
    if created_data || created_releases {
        File::open(path)
            .and_then(|directory| directory.sync_all())
            .context("sync append-file directory")?;
    }

    let records = recover_data(&data)?;
    let released = recover_releases(&releases)?;
    for key in &released {
        ensure!(
            records.contains_key(key),
            "append-file release refers to an unknown record"
        );
    }
    Ok(State {
        data,
        releases,
        records,
        released,
    })
}

fn open_append_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(path)
        .with_context(|| format!("open simple append file {}", path.display()))
}

fn recover_data(file: &File) -> Result<BTreeMap<(u64, u64), RecordPointer>> {
    let length = file
        .metadata()
        .context("read append-file data metadata")?
        .len();
    let mut offset = 0_u64;
    let mut records = BTreeMap::new();
    while offset < length {
        let (body_len, count) = read_frame_header(file, offset, DATA_HEADER_MAGIC, length)?;
        ensure!(count > 0, "append-file data frame has no records");
        let body_start = offset
            .checked_add(u64::try_from(HEADER_LEN).expect("fixed length fits u64"))
            .context("append-file body offset overflow")?;
        let body_end = body_start
            .checked_add(body_len)
            .context("append-file body end overflow")?;
        let commit_end = body_end
            .checked_add(u64::try_from(COMMIT_LEN).expect("fixed length fits u64"))
            .context("append-file commit end overflow")?;
        ensure!(commit_end <= length, "append-file data frame is truncated");
        ensure!(
            count <= body_len / u64::try_from(DATA_DESCRIPTOR_LEN).expect("fixed length fits u64"),
            "append-file data descriptor count exceeds its body"
        );

        let mut cursor = body_start;
        let mut descriptors = Vec::with_capacity(
            usize::try_from(count)
                .context("append-file data count does not fit usize")?
                .checked_mul(DATA_DESCRIPTOR_LEN)
                .context("append-file descriptor allocation overflow")?,
        );
        let mut batch = Vec::new();
        for _ in 0..count {
            let mut descriptor = [0_u8; DATA_DESCRIPTOR_LEN];
            read_exact_at(file, &mut descriptor, cursor)
                .context("read append-file data descriptor")?;
            descriptors.extend_from_slice(&descriptor);
            let key = parse_key(&descriptor[..KEY_LEN])?;
            let value_len = parse_u64(&descriptor[KEY_LEN..]);
            let value_offset = cursor
                .checked_add(u64::try_from(DATA_DESCRIPTOR_LEN).expect("fixed length fits u64"))
                .context("append-file value offset overflow")?;
            let value_end = value_offset
                .checked_add(value_len)
                .context("append-file value end overflow")?;
            ensure!(
                value_end <= body_end,
                "append-file value exceeds its data frame"
            );
            batch.push((
                key,
                RecordPointer {
                    value_offset,
                    value_len,
                },
            ));
            cursor = value_end;
        }
        ensure!(
            cursor == body_end,
            "append-file data body has trailing bytes"
        );
        verify_commit(file, body_end, DATA_COMMIT_MAGIC, xxh3_64(&descriptors))?;
        for (key, pointer) in batch {
            ensure!(
                records.insert(key, pointer).is_none(),
                "append-file data contains a duplicate key"
            );
        }
        offset = commit_end;
    }
    Ok(records)
}

fn recover_releases(file: &File) -> Result<BTreeSet<(u64, u64)>> {
    let length = file
        .metadata()
        .context("read append-file release metadata")?
        .len();
    let mut offset = 0_u64;
    let mut released = BTreeSet::new();
    while offset < length {
        let (body_len, count) = read_frame_header(file, offset, RELEASE_HEADER_MAGIC, length)?;
        ensure!(count > 0, "append-file release frame has no records");
        let expected_body = count
            .checked_mul(u64::try_from(KEY_LEN).expect("fixed length fits u64"))
            .context("append-file release body overflow")?;
        ensure!(
            body_len == expected_body,
            "append-file release body length is invalid"
        );
        let body_start = offset
            .checked_add(u64::try_from(HEADER_LEN).expect("fixed length fits u64"))
            .context("append-file release body offset overflow")?;
        let body_end = body_start
            .checked_add(body_len)
            .context("append-file release body end overflow")?;
        let commit_end = body_end
            .checked_add(u64::try_from(COMMIT_LEN).expect("fixed length fits u64"))
            .context("append-file release commit end overflow")?;
        ensure!(
            commit_end <= length,
            "append-file release frame is truncated"
        );
        let body_size =
            usize::try_from(body_len).context("append-file release body does not fit usize")?;
        let mut body = vec![0_u8; body_size];
        read_exact_at(file, &mut body, body_start).context("read append-file release body")?;
        verify_commit(file, body_end, RELEASE_COMMIT_MAGIC, xxh3_64(&body))?;
        for key in body.chunks_exact(KEY_LEN) {
            released.insert(parse_key(key)?);
        }
        offset = commit_end;
    }
    Ok(released)
}

fn read_frame_header(
    file: &File,
    offset: u64,
    magic: [u8; 8],
    file_len: u64,
) -> Result<(u64, u64)> {
    let header_end = offset
        .checked_add(u64::try_from(HEADER_LEN).expect("fixed length fits u64"))
        .context("append-file header end overflow")?;
    ensure!(
        header_end <= file_len,
        "append-file frame header is truncated"
    );
    let mut header = [0_u8; HEADER_LEN];
    read_exact_at(file, &mut header, offset).context("read append-file frame header")?;
    ensure!(header[..8] == magic, "append-file frame magic is invalid");
    ensure!(
        parse_u64(&header[24..]) == xxh3_64(&header[..24]),
        "append-file frame header checksum mismatch"
    );
    Ok((parse_u64(&header[8..16]), parse_u64(&header[16..24])))
}

fn verify_commit(file: &File, offset: u64, magic: [u8; 8], checksum: u64) -> Result<()> {
    let mut commit = [0_u8; COMMIT_LEN];
    read_exact_at(file, &mut commit, offset).context("read append-file frame commit")?;
    ensure!(commit[..8] == magic, "append-file commit magic is invalid");
    ensure!(
        parse_u64(&commit[8..]) == checksum,
        "append-file descriptor checksum mismatch"
    );
    Ok(())
}

fn frame_header(magic: [u8; 8], body_len: u64, count: u64) -> [u8; HEADER_LEN] {
    let mut header = [0_u8; HEADER_LEN];
    header[..8].copy_from_slice(&magic);
    header[8..16].copy_from_slice(&body_len.to_le_bytes());
    header[16..24].copy_from_slice(&count.to_le_bytes());
    let checksum = xxh3_64(&header[..24]);
    header[24..].copy_from_slice(&checksum.to_le_bytes());
    header
}

fn frame_commit(magic: [u8; 8], checksum: u64) -> [u8; COMMIT_LEN] {
    let mut commit = [0_u8; COMMIT_LEN];
    commit[..8].copy_from_slice(&magic);
    commit[8..].copy_from_slice(&checksum.to_le_bytes());
    commit
}

fn parse_key(bytes: &[u8]) -> Result<(u64, u64)> {
    ensure!(bytes.len() == KEY_LEN, "append-file key length is invalid");
    let stream = key_stream(bytes)?;
    Ok((stream, parse_u64_be(&bytes[8..])))
}

fn parse_u64(bytes: &[u8]) -> u64 {
    let mut value = [0_u8; 8];
    value.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(value)
}

fn parse_u64_be(bytes: &[u8]) -> u64 {
    let mut value = [0_u8; 8];
    value.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(value)
}

fn read_exact_at(file: &File, mut buffer: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buffer.is_empty() {
        match file.read_at(buffer, offset) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            Ok(read) => {
                offset = offset
                    .checked_add(u64::try_from(read).expect("read length fits u64"))
                    .ok_or_else(|| io::Error::other("append-file read offset overflow"))?;
                buffer = &mut buffer[read..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn lock(state: &Arc<Mutex<State>>) -> Result<MutexGuard<'_, State>> {
    state
        .lock()
        .map_err(|_| anyhow::anyhow!("simple append-file mutex was poisoned"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::records;
    use tempfile::TempDir;

    #[tokio::test]
    async fn append_release_and_reopen() {
        let directory = TempDir::new().unwrap();
        let engine = SimpleAppendFileEngine::open(directory.path())
            .await
            .unwrap();
        let tokens = engine
            .append_batch(7, records(2, 7, 1, 8, 64))
            .await
            .unwrap();
        assert_eq!(engine.pending_count().await.unwrap(), 2);
        let pending = engine.read(7, 2, 128).await.unwrap();
        assert_eq!(pending.len(), 2);
        engine.release(7, vec![tokens[0]]).await.unwrap();
        engine.shutdown().await.unwrap();
        drop(engine);

        let reopened = SimpleAppendFileEngine::open(directory.path())
            .await
            .unwrap();
        assert_eq!(reopened.pending_count().await.unwrap(), 1);
        let pending = reopened.read(7, 2, 128).await.unwrap();
        assert_eq!(pending.len(), 1);
        reopened.release(7, vec![pending[0].token]).await.unwrap();
        assert_eq!(reopened.pending_count().await.unwrap(), 0);
    }
}
