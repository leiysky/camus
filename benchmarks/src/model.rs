use anyhow::{bail, Context, Result};
use bytes::Bytes;
use camus::RecordId;
use xxhash_rust::xxh3::xxh3_64;

const VALUE_HEADER_LEN: usize = 4;
const VALUE_CHECKSUM_LEN: usize = 8;

#[derive(Clone)]
pub(crate) struct InputRecord {
    pub(crate) sequence: u64,
    pub(crate) metadata: Bytes,
    pub(crate) payload: Bytes,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum Token {
    Camus(RecordId),
    Kv([u8; 16]),
}

pub(crate) struct PendingRecord {
    pub(crate) token: Token,
    pub(crate) metadata: Bytes,
    pub(crate) payload: Bytes,
}

pub(crate) fn records(
    count: usize,
    stream: u64,
    first_sequence: u64,
    metadata_bytes: usize,
    payload_bytes: usize,
) -> Vec<InputRecord> {
    (0..count)
        .map(|offset| {
            let sequence = first_sequence + u64::try_from(offset).expect("record count fits u64");
            InputRecord {
                sequence,
                metadata: deterministic_bytes(
                    metadata_bytes,
                    stream.rotate_left(17) ^ sequence ^ 0x6d65_7461_6461_7461,
                ),
                payload: deterministic_bytes(
                    payload_bytes,
                    stream.rotate_left(31) ^ sequence ^ 0x7061_796c_6f61_6421,
                ),
            }
        })
        .collect()
}

fn deterministic_bytes(len: usize, mut state: u64) -> Bytes {
    if state == 0 {
        state = 0x9e37_79b9_7f4a_7c15;
    }
    let mut output = vec![0_u8; len];
    for chunk in output.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let bytes = state.to_le_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    Bytes::from(output)
}

pub(crate) fn kv_key(stream: u64, sequence: u64) -> [u8; 16] {
    let mut key = [0_u8; 16];
    key[..8].copy_from_slice(&stream.to_be_bytes());
    key[8..].copy_from_slice(&sequence.to_be_bytes());
    key
}

pub(crate) fn key_stream(key: &[u8]) -> Result<u64> {
    if key.len() != 16 {
        bail!(
            "expected a 16-byte benchmark key, found {} bytes",
            key.len()
        );
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&key[..8]);
    Ok(u64::from_be_bytes(bytes))
}

pub(crate) fn encode_value(record: &InputRecord) -> Result<Vec<u8>> {
    let metadata_len = u32::try_from(record.metadata.len())
        .context("benchmark metadata length does not fit u32")?;
    let capacity = VALUE_HEADER_LEN
        .checked_add(record.metadata.len())
        .and_then(|bytes| bytes.checked_add(record.payload.len()))
        .and_then(|bytes| bytes.checked_add(VALUE_CHECKSUM_LEN))
        .context("benchmark value length overflow")?;
    let mut value = Vec::with_capacity(capacity);
    value.extend_from_slice(&metadata_len.to_le_bytes());
    value.extend_from_slice(&record.metadata);
    value.extend_from_slice(&record.payload);
    let checksum = xxh3_64(&value);
    value.extend_from_slice(&checksum.to_le_bytes());
    Ok(value)
}

pub(crate) fn decode_value(value: &[u8]) -> Result<(Bytes, Bytes)> {
    if value.len() < VALUE_HEADER_LEN + VALUE_CHECKSUM_LEN {
        bail!("benchmark value is truncated");
    }
    let checksum_offset = value.len() - VALUE_CHECKSUM_LEN;
    let mut checksum_bytes = [0_u8; VALUE_CHECKSUM_LEN];
    checksum_bytes.copy_from_slice(&value[checksum_offset..]);
    let expected = u64::from_le_bytes(checksum_bytes);
    let actual = xxh3_64(&value[..checksum_offset]);
    if actual != expected {
        bail!("benchmark value checksum mismatch");
    }

    let mut metadata_len_bytes = [0_u8; VALUE_HEADER_LEN];
    metadata_len_bytes.copy_from_slice(&value[..VALUE_HEADER_LEN]);
    let metadata_len = usize::try_from(u32::from_le_bytes(metadata_len_bytes))
        .context("metadata length does not fit usize")?;
    let metadata_end = VALUE_HEADER_LEN
        .checked_add(metadata_len)
        .context("benchmark metadata end overflow")?;
    if metadata_end > checksum_offset {
        bail!("benchmark metadata length exceeds the encoded value");
    }

    Ok((
        Bytes::copy_from_slice(&value[VALUE_HEADER_LEN..metadata_end]),
        Bytes::copy_from_slice(&value[metadata_end..checksum_offset]),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_key_orders_stream_then_sequence() {
        assert!(kv_key(1, 9) < kv_key(2, 0));
        assert!(kv_key(7, 9) < kv_key(7, 10));
        assert_eq!(key_stream(&kv_key(42, 99)).unwrap(), 42);
    }

    #[test]
    fn value_round_trip_verifies_checksum() {
        let record = records(1, 3, 8, 17, 257).remove(0);
        let mut value = encode_value(&record).unwrap();
        let (metadata, payload) = decode_value(&value).unwrap();
        assert_eq!(metadata, record.metadata);
        assert_eq!(payload, record.payload);

        value[8] ^= 0x40;
        assert!(decode_value(&value).is_err());
    }
}
