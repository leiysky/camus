# Camus long-running smoke runner

This standalone, unpublished, repository-only crate exercises one bounded
Camus root through repeated steady-state, capacity-pressure, recovery, and
final-drain phases. It publishes time-series observations to VictoriaMetrics
and validates record integrity and drained recovery before reporting success.

It has its own manifest and lockfile, is not a workspace member, and is
excluded from the published Camus package. Its dependencies are therefore
resolved only when a command explicitly names `smoke/Cargo.toml`.

Run it from the repository root:

```sh
cargo run --locked --release --manifest-path smoke/Cargo.toml -- run \
  --duration-seconds 21600 \
  --victoria-metrics-url http://127.0.0.1:8428
```

See [`docs/long-running-smoke.md`](../docs/long-running-smoke.md) for the
workload model, metric definitions, pass criteria, and report handling.
