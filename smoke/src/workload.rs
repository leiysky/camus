use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use camus::{PendingRecord, ReadLimits, Record, Stream};
use hdrhistogram::Histogram;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::watch;

pub(crate) const METADATA_HEADER_BYTES: usize = 24;
const RECORD_MAGIC: &[u8; 8] = b"CAMUSSM1";
const METADATA_SEED: u64 = 0x6d65_7461_6461_7461;
const PAYLOAD_SEED: u64 = 0x7061_796c_6f61_6421;
const MAX_LATENCY_NS: u64 = 3_600_000_000_000;

pub(crate) const LATENCY_BUCKETS_NS: &[(&str, u64)] = &[
    ("0.00025", 250_000),
    ("0.0005", 500_000),
    ("0.001", 1_000_000),
    ("0.002", 2_000_000),
    ("0.005", 5_000_000),
    ("0.01", 10_000_000),
    ("0.02", 20_000_000),
    ("0.05", 50_000_000),
    ("0.1", 100_000_000),
    ("0.25", 250_000_000),
    ("0.5", 500_000_000),
    ("1", 1_000_000_000),
    ("2", 2_000_000_000),
    ("5", 5_000_000_000),
    ("10", 10_000_000_000),
    ("30", 30_000_000_000),
    ("60", 60_000_000_000),
    ("120", 120_000_000_000),
    ("300", 300_000_000_000),
    ("+Inf", u64::MAX),
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Phase {
    Warmup,
    Steady,
    PressureFill,
    PressureHold,
    Recovery,
    FinalDrain,
}

impl Phase {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Warmup => "warmup",
            Self::Steady => "steady",
            Self::PressureFill => "pressure_fill",
            Self::PressureHold => "pressure_hold",
            Self::Recovery => "recovery",
            Self::FinalDrain => "final_drain",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Control {
    pub(crate) producers_enabled: bool,
    pub(crate) consumers_enabled: bool,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ProducerConfig {
    pub(crate) producer_id: usize,
    pub(crate) metadata_bytes: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) batch_records: usize,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ConsumerConfig {
    pub(crate) stream_index: usize,
    pub(crate) stream_count: usize,
    pub(crate) max_records: usize,
    pub(crate) max_bytes: u64,
}

impl Control {
    pub(crate) const fn new(producers_enabled: bool, consumers_enabled: bool) -> Self {
        Self {
            producers_enabled,
            consumers_enabled,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Measurements {
    pub(crate) append: Arc<OperationMeasurement>,
    pub(crate) read: Arc<OperationMeasurement>,
    pub(crate) release: Arc<OperationMeasurement>,
    pub(crate) active_append_calls: Arc<AtomicUsize>,
    pub(crate) integrity_errors: Arc<AtomicU64>,
}

impl Measurements {
    pub(crate) fn new() -> Result<Self> {
        Ok(Self {
            append: Arc::new(OperationMeasurement::new("append")?),
            read: Arc::new(OperationMeasurement::new("read")?),
            release: Arc::new(OperationMeasurement::new("release")?),
            active_append_calls: Arc::new(AtomicUsize::new(0)),
            integrity_errors: Arc::new(AtomicU64::new(0)),
        })
    }

    pub(crate) fn take_intervals(&self) -> [OperationInterval; 3] {
        [
            self.append.take_interval(),
            self.read.take_interval(),
            self.release.take_interval(),
        ]
    }
}

pub(crate) struct OperationMeasurement {
    name: &'static str,
    interval: Mutex<IntervalAccumulator>,
}

impl OperationMeasurement {
    fn new(name: &'static str) -> Result<Self> {
        Ok(Self {
            name,
            interval: Mutex::new(IntervalAccumulator::new()?),
        })
    }

    fn success(&self, elapsed_ns: u64, records: u64, payload_bytes: u64) {
        let mut interval = self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        interval.calls = interval.calls.saturating_add(1);
        interval.records = interval.records.saturating_add(records);
        interval.payload_bytes = interval.payload_bytes.saturating_add(payload_bytes);
        interval.histogram.saturating_record(elapsed_ns.max(1));
    }

    fn failure(&self) {
        let mut interval = self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        interval.failures = interval.failures.saturating_add(1);
    }

    fn take_interval(&self) -> OperationInterval {
        let mut guard = self
            .interval
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let replacement = IntervalAccumulator::new().expect("valid fixed histogram configuration");
        let interval = std::mem::replace(&mut *guard, replacement);
        OperationInterval::from_accumulator(self.name, interval)
    }
}

struct IntervalAccumulator {
    calls: u64,
    records: u64,
    payload_bytes: u64,
    failures: u64,
    histogram: Histogram<u64>,
}

impl IntervalAccumulator {
    fn new() -> Result<Self> {
        Ok(Self {
            calls: 0,
            records: 0,
            payload_bytes: 0,
            failures: 0,
            histogram: Histogram::new_with_max(MAX_LATENCY_NS, 3)
                .context("create operation latency histogram")?,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OperationInterval {
    pub(crate) name: &'static str,
    pub(crate) calls: u64,
    pub(crate) records: u64,
    pub(crate) payload_bytes: u64,
    pub(crate) failures: u64,
    pub(crate) p50_ns: u64,
    pub(crate) p95_ns: u64,
    pub(crate) p99_ns: u64,
    pub(crate) max_ns: u64,
    pub(crate) cumulative_buckets: Vec<u64>,
}

impl OperationInterval {
    fn from_accumulator(name: &'static str, interval: IntervalAccumulator) -> Self {
        let cumulative_buckets = LATENCY_BUCKETS_NS
            .iter()
            .map(|(_, upper)| {
                if *upper == u64::MAX {
                    interval.histogram.len()
                } else {
                    interval.histogram.count_between(0, *upper)
                }
            })
            .collect();
        Self {
            name,
            calls: interval.calls,
            records: interval.records,
            payload_bytes: interval.payload_bytes,
            failures: interval.failures,
            p50_ns: interval.histogram.value_at_quantile(0.50),
            p95_ns: interval.histogram.value_at_quantile(0.95),
            p99_ns: interval.histogram.value_at_quantile(0.99),
            max_ns: interval.histogram.max(),
            cumulative_buckets,
        }
    }

    pub(crate) fn records_per_second(&self, elapsed: std::time::Duration) -> f64 {
        if elapsed.is_zero() {
            return 0.0;
        }
        self.records as f64 / elapsed.as_secs_f64()
    }
}

pub(crate) async fn producer(
    config: ProducerConfig,
    stream: Stream,
    mut control: watch::Receiver<Control>,
    mut stop: watch::Receiver<bool>,
    measurements: Measurements,
) -> Result<()> {
    let producer_id = u64::try_from(config.producer_id).context("producer ID overflow")?;
    let mut next_sequence = 1_u64;
    loop {
        if !wait_for_gate(&mut control, &mut stop, true).await? {
            return Ok(());
        }
        let mut records = Vec::with_capacity(config.batch_records);
        for _ in 0..config.batch_records {
            records.push(make_record(
                producer_id,
                next_sequence,
                config.metadata_bytes,
                config.payload_bytes,
            ));
            next_sequence = next_sequence
                .checked_add(1)
                .context("producer sequence overflow")?;
        }
        let payload_total = u64::try_from(config.payload_bytes)
            .context("payload length overflow")?
            .checked_mul(u64::try_from(config.batch_records).context("batch length overflow")?)
            .context("batch payload byte count overflow")?;
        let started = Instant::now();
        measurements
            .active_append_calls
            .fetch_add(1, Ordering::AcqRel);
        let result = stream.append_batch(records).await;
        measurements
            .active_append_calls
            .fetch_sub(1, Ordering::AcqRel);
        match result {
            Ok(ids) => {
                ensure!(
                    ids.len() == config.batch_records,
                    "append returned an unexpected record ID count"
                );
                measurements.append.success(
                    duration_ns(started.elapsed()),
                    u64::try_from(ids.len()).unwrap_or(u64::MAX),
                    payload_total,
                );
            }
            Err(error) => {
                measurements.append.failure();
                return Err(error).context("append batch");
            }
        }
    }
}

pub(crate) async fn consumer(
    config: ConsumerConfig,
    stream: Stream,
    mut control: watch::Receiver<Control>,
    mut stop: watch::Receiver<bool>,
    measurements: Measurements,
) -> Result<()> {
    let expected_stream = u64::try_from(config.stream_index).context("stream ID overflow")?;
    let stream_count = u64::try_from(config.stream_count).context("stream count overflow")?;
    let mut last_released_sequences = BTreeMap::<u64, u64>::new();
    loop {
        if !wait_for_gate(&mut control, &mut stop, false).await? {
            return Ok(());
        }
        let started = Instant::now();
        let read = tokio::select! {
            result = stream.read(ReadLimits::new(config.max_records, config.max_bytes)) => Some(result),
            changed = stop.changed() => {
                changed.context("stop signal closed")?;
                None
            }
        };
        let Some(read) = read else {
            return Ok(());
        };
        let snapshot = match read {
            Ok(snapshot) => snapshot,
            Err(error) => {
                measurements.read.failure();
                return Err(error).context("read pending snapshot");
            }
        };
        let payload_bytes = snapshot.iter().fold(0_u64, |total, record| {
            total.saturating_add(u64::try_from(record.payload.len()).unwrap_or(u64::MAX))
        });
        measurements.read.success(
            duration_ns(started.elapsed()),
            u64::try_from(snapshot.len()).unwrap_or(u64::MAX),
            payload_bytes,
        );

        let mut ids = Vec::with_capacity(snapshot.len());
        let mut observed_sequences = last_released_sequences.clone();
        for record in &snapshot {
            let validated = validate_record(record, expected_stream, stream_count).and_then(
                |(producer_id, sequence)| {
                    let previous = observed_sequences.entry(producer_id).or_default();
                    ensure!(
                        sequence > *previous,
                        "record was delivered after its durable release or out of producer order"
                    );
                    *previous = sequence;
                    Ok(())
                },
            );
            if let Err(error) = validated {
                measurements.integrity_errors.fetch_add(1, Ordering::AcqRel);
                return Err(error);
            }
            ids.push(record.id);
        }
        let released = u64::try_from(ids.len()).unwrap_or(u64::MAX);
        let started = Instant::now();
        match stream.release(ids).await {
            Ok(()) => {
                measurements
                    .release
                    .success(duration_ns(started.elapsed()), released, 0);
                last_released_sequences = observed_sequences;
            }
            Err(error) => {
                measurements.release.failure();
                return Err(error).context("release pending snapshot");
            }
        }
    }
}

async fn wait_for_gate(
    control: &mut watch::Receiver<Control>,
    stop: &mut watch::Receiver<bool>,
    producer: bool,
) -> Result<bool> {
    loop {
        if *stop.borrow() {
            return Ok(false);
        }
        let state = *control.borrow();
        let enabled = if producer {
            state.producers_enabled
        } else {
            state.consumers_enabled
        };
        if enabled {
            return Ok(true);
        }
        tokio::select! {
            changed = control.changed() => changed.context("control signal closed")?,
            changed = stop.changed() => changed.context("stop signal closed")?,
        }
    }
}

fn make_record(
    producer_id: u64,
    sequence: u64,
    metadata_bytes: usize,
    payload_bytes: usize,
) -> Record {
    let mut metadata = vec![0_u8; metadata_bytes];
    metadata[..8].copy_from_slice(RECORD_MAGIC);
    metadata[8..16].copy_from_slice(&producer_id.to_le_bytes());
    metadata[16..24].copy_from_slice(&sequence.to_le_bytes());
    fill_pattern(
        &mut metadata[METADATA_HEADER_BYTES..],
        record_seed(producer_id, sequence, METADATA_SEED),
    );
    let mut payload = vec![0_u8; payload_bytes];
    fill_pattern(
        &mut payload,
        record_seed(producer_id, sequence, PAYLOAD_SEED),
    );
    Record {
        metadata: Bytes::from(metadata),
        payload: Bytes::from(payload),
    }
}

fn validate_record(
    record: &PendingRecord,
    expected_stream: u64,
    stream_count: u64,
) -> Result<(u64, u64)> {
    ensure!(
        record.metadata.len() >= METADATA_HEADER_BYTES,
        "record metadata is truncated"
    );
    ensure!(
        &record.metadata[..8] == RECORD_MAGIC,
        "record metadata magic mismatch"
    );
    let producer_id = read_u64(&record.metadata[8..16]);
    let sequence = read_u64(&record.metadata[16..24]);
    ensure!(
        producer_id % stream_count == expected_stream,
        "record payload belongs to the wrong logical stream"
    );
    ensure!(
        check_pattern(
            &record.metadata[METADATA_HEADER_BYTES..],
            record_seed(producer_id, sequence, METADATA_SEED),
        ),
        "record metadata pattern mismatch"
    );
    ensure!(
        check_pattern(
            &record.payload,
            record_seed(producer_id, sequence, PAYLOAD_SEED),
        ),
        "record payload pattern mismatch"
    );
    Ok((producer_id, sequence))
}

fn record_seed(producer_id: u64, sequence: u64, domain: u64) -> u64 {
    producer_id.rotate_left(17) ^ sequence.rotate_left(31) ^ domain
}

fn fill_pattern(output: &mut [u8], mut state: u64) {
    if state == 0 {
        state = 0x9e37_79b9_7f4a_7c15;
    }
    for chunk in output.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
    }
}

fn check_pattern(input: &[u8], mut state: u64) -> bool {
    if state == 0 {
        state = 0x9e37_79b9_7f4a_7c15;
    }
    for chunk in input.chunks(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        if chunk != &state.to_le_bytes()[..chunk.len()] {
            return false;
        }
    }
    true
}

fn read_u64(input: &[u8]) -> u64 {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(input);
    u64::from_le_bytes(bytes)
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use camus::RecordId;

    #[test]
    fn generated_record_validates_without_allocation() {
        let record = make_record(7, 42, 32, 4096);
        let pending = PendingRecord {
            id: RecordId::from_bytes([1; RecordId::BYTE_LEN]),
            metadata: record.metadata,
            payload: record.payload,
        };
        validate_record(&pending, 3, 4).unwrap();
    }

    #[test]
    fn validation_detects_payload_damage_and_redelivery() {
        let record = make_record(7, 42, 32, 128);
        let mut payload = record.payload.to_vec();
        payload[31] ^= 1;
        let pending = PendingRecord {
            id: RecordId::from_bytes([1; RecordId::BYTE_LEN]),
            metadata: record.metadata,
            payload: Bytes::from(payload),
        };
        assert!(validate_record(&pending, 3, 4).is_err());
        assert!(validate_record(&pending, 2, 4).is_err());
    }

    #[test]
    fn interval_histogram_is_reset_after_snapshot() {
        let measurement = OperationMeasurement::new("append").unwrap();
        measurement.success(1_000_000, 4, 16);
        let first = measurement.take_interval();
        let second = measurement.take_interval();
        assert_eq!(first.calls, 1);
        assert_eq!(first.records, 4);
        assert!(first.p50_ns.abs_diff(1_000_000) < 1_000);
        assert_eq!(second.calls, 0);
        assert_eq!(second.cumulative_buckets.last(), Some(&0));
    }
}
