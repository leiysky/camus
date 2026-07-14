# Long-running smoke protocol

The repository-only `camus-long-smoke` runner validates that one bounded Camus
root can sustain repeated production and consumption while preserving its
capacity, durability, and delivery invariants. It records a time series rather
than reducing the run to one end-of-process number. The runner belongs to the
standalone `smoke/` crate and is excluded from the published Camus crate.

The long-running workload is manually triggered on a controlled development
host and is not a CI gate. The crate's formatting, lint, unit tests, and
dependency policy are ordinary CI checks. It does not compare Camus with a KV
engine and has no RocksDB or redb dependency, so its build cannot resolve,
download, or compile either engine.

## Workload and phases

The default workload uses a 1 GiB bounded root with `FullPolicy::Block`, 64 MiB
segments, 16 logical streams, 32 producers, 16 records per atomic append, and
512 records per read and exact release. Every stream has exactly one consumer.
Producers embed a deterministic producer ID, sequence, and byte pattern in
opaque metadata and payload. Consumers verify the pattern before release and
track producer sequence order with memory proportional to producer count, not
record count.

The controller repeats these phases:

1. `warmup` fills the root to the upper steady-state waterline with consumers
   paused;
2. `steady` keeps producers active and enables or disables consumers around a
   configured waterline band;
3. `pressure_fill` pauses consumers until at least one append is blocked by
   capacity and safe-capacity utilization reaches the pressure target;
4. `pressure_hold` keeps consumers paused so blocked append latency and waiter
   behavior are observable;
5. `recovery` pauses new production and drains until in-flight appends finish,
   released segments are reclaimed, and the low waterline is reached; and
6. after the requested run duration, `final_drain` stops production, releases
   every pending record, requests one maintenance pass, shuts down, and reopens
   the same root for recovery validation.

Safe-capacity utilization is:

```text
(actual_file_bytes + maintenance_headroom_bytes) / configured_capacity_bytes
```

It deliberately includes the dynamic reserve Camus needs to make maintenance
progress. Pending payload bytes alone are not a valid capacity waterline.

## Pass criteria

A normal run succeeds only when all of the following hold:

- at least one complete pressure and recovery cycle finished;
- capacity blocking was observed under `FullPolicy::Block`;
- no append, read, or release returned an error and no mutation Future was
  cancelled;
- every deterministic record passed content and producer-order validation;
- caller-observed read and release record totals equal the unique durable
  append and release totals, detecting delivery after a successful release;
- durable append and release record totals are equal;
- the final drain leaves zero pending records;
- a clean reopen recovers zero pending records; and
- VictoriaMetrics reports no failed or locally dropped metric batch.

Stopping the process externally is not a successful run. The disposable data
directory remains available for diagnosis when the runner exits with an error.
On success it is removed unless `--keep-data` is set.

The separate `backlog` qualification command intentionally kills its internal
writer subprocess; that expected kill is not a capacity-cycle run failure.

## Metrics

The runner samples every five seconds by default and writes the exact pushed
Prometheus exposition lines, including millisecond timestamps, to ignored
`target/long-smoke-results/<run-id>/metrics.prom`. It pushes the same samples
to a single-node VictoriaMetrics
[`/api/v1/import/prometheus`](https://docs.victoriametrics.com/victoriametrics/single-server-victoriametrics/#how-to-import-time-series-data)
endpoint. The generated report reads them back through `/api/v1/export`; it is
therefore based on values accepted by VictoriaMetrics rather than only the
runner's local aggregation.

Every series has a bounded `run_id` label. Workload interval series also use
the low-cardinality `phase` and `operation` labels. No stream ID, record ID,
path, failure message, hostname, or device identifier becomes a metric label.

The main metric families are:

| Family | Interpretation |
| --- | --- |
| `camus_long_smoke_capacity_utilization_ratio` | maintenance-safe root waterline |
| `actual_file_bytes`, `maintenance_headroom_bytes`, `data_admissible_bytes` | capacity accounting components |
| `pending_records`, `pending_payload_bytes`, `segments`, `reclaimable_bytes` | logical backlog and physical lifecycle |
| `waiters`, `waits_total`, `duration_seconds_*` | queue, readiness, capacity, and storage-job pressure |
| `interval_operation_records`, `operation_records_per_second` | interval append, read, and release throughput |
| `interval_operation_latency_seconds` | interval p50, p95, and p99 caller-observed latency |
| `interval_operation_latency_bucket` | cumulative fixed buckets per interval for whole-run quantiles |
| `commit_*` | durable group count, units, records, encoded bytes, and maxima |
| `maintenance_*`, `recovery_*` | reclamation, rollover, compaction, and reopen work |
| `filesystem_available_bytes`, `process_resident_memory_bytes` | host safety context without host labels |
| `integrity_errors_total`, `validation_passed` | workload correctness outcome |

The latency report aggregates fixed interval buckets, so its whole-run p50,
p95, and p99 are bucket upper bounds rather than interpolated exact values.
The interval quantile series remain available for tail-latency drift analysis.
Detailed Camus operation and storage-job timing is enabled for this test; the
report therefore measures the deliberately instrumented configuration.

## Running on a development host

VictoriaMetrics must be reachable from the test process. A local single-node
instance on the default port needs no additional configuration:

```sh
curl -fsS http://127.0.0.1:8428/health

cargo run --locked --release --manifest-path smoke/Cargo.toml -- run \
  --duration-seconds 21600 \
  --data-directory /path/on/device/camus-long-smoke-data \
  --output-directory target/long-smoke-results \
  --victoria-metrics-url http://127.0.0.1:8428
```

The default six-hour run normally contains many pressure and recovery cycles.
Use a unique explicit `--run-id` when a stable report label is useful. Keep the
data directory on the filesystem being tested, reserve materially more device
free space than the configured Camus capacity, and avoid unrelated I/O-heavy
work during a reference run. The capacity bound limits live Camus files, not
cumulative device writes: repeated fill and reclaim cycles may write many
times the configured capacity. Choose duration and payload rate with the
device's write budget in mind.

For a short harness check, shrink the capacity and phase durations while still
requiring one complete cycle:

```sh
cargo run --locked --release --manifest-path smoke/Cargo.toml -- run \
  --duration-seconds 30 --sample-interval-seconds 1 \
  --steady-seconds 3 --pressure-hold-seconds 2 \
  --fill-timeout-seconds 30 --recovery-timeout-seconds 30 \
  --capacity-bytes 67108864 --segment-bytes 8388608 \
  --streams 4 --producers 8 --read-batch-records 128
```

`--no-victoria-metrics` retains the timestamped local metric stream but cannot
produce the VictoriaMetrics-backed Markdown report. `--allow-no-capacity-cycle`
exists only for diagnostic runs too short to exercise the normal pass
criterion.

## Large-backlog kill and reopen

The manual `backlog` command qualifies recovery cost and correctness with a
large pending root. Its internal writer creates deterministic records across
all configured streams and waits after every append has reported durable
success. The parent sends an external `SIGKILL`, opens the same root, records
open and first-read latency, topology, recovery work, and resident memory,
then verifies and releases every record. A final reopen must recover zero
pending records.

The default creates one million 1 KiB payloads plus framing and metadata:

```sh
cargo run --locked --release --manifest-path smoke/Cargo.toml -- backlog \
  --records 1000000 \
  --data-directory /path/on/device/camus-backlog-data \
  --output-directory target/backlog-recovery-results
```

This is an unclean large-backlog reopen, not a claim that the kill landed
inside a particular syscall. Exact storage-transition boundaries are covered
by the deterministic library crash matrix; a release candidate still needs
repeated externally timed kills while live append, release, rollover,
checkpoint, and reclamation work is active.

The command writes an ignored `backlog-report.json` containing revision state,
configuration, recovered counts and bytes, segment topology, wall-clock open,
first-read and drain durations, Camus recovery duration and scan counts, and
RSS after open. It may contain local paths and dirty-worktree state, so keep the
raw report outside commits and copy only a sanitized aggregate into a release
summary.

## Output and retention

Successful VictoriaMetrics-backed runs create these ignored local artifacts:

| File | Purpose |
| --- | --- |
| `metrics.prom` | exact local copy of every submitted sample |
| `run.json` | arguments, revision state, exporter counts, and final invariants |
| `victoria-export.jsonl` | raw time series read back from VictoriaMetrics |
| `report.md` | sanitized capacity, phase throughput, latency, group-commit, and integrity summary |

Regenerate the two report artifacts from retained `run.json` metadata and the
VictoriaMetrics time series without rerunning the workload:

```sh
cargo run --locked --release --manifest-path smoke/Cargo.toml -- report \
  --run-metadata target/long-smoke-results/<run-id>/run.json \
  --victoria-metrics-url http://127.0.0.1:8428
```

Keep these artifacts outside commits. `run.json` and the raw export are
diagnostic data and may contain local paths or revision state. Only explicitly
sanitized aggregate conclusions should be copied into repository
documentation.
