# Runnable examples

Each example is self-contained, uses a temporary storage root, and asserts the
property it demonstrates. Run them from the repository root:

```sh
cargo run --locked --example durable_replay
cargo run --locked --example multi_stream
cargo run --locked --example readiness
cargo run --locked --example maintenance
```

| Example | Typical use | Main point |
| --- | --- | --- |
| `durable_replay` | Local outbox or write-behind buffer | Reopen discovers pending records; a crash before release replays the effect |
| `multi_stream` | Independent upload, audit, tenant, or destination buffers | Lightweight handles share one root while pending state and release remain stream-scoped |
| `readiness` | Notify an async task or application reactor | Waiting `read` is the readiness Future; it observes rather than claims work |
| `maintenance` | Capacity-aware storage maintenance | Reclamation is automatic and explicit `reclaim` is an optional maintenance barrier |

The examples deliberately keep application policy outside Camus. In a real
application:

- put application idempotency keys in opaque metadata;
- make repeated downstream effects safe when duplicates matter;
- release only after the effect is durably represented elsewhere;
- bound each drain attempt and coordinate workers above the storage layer;
- coordinate multiple readers above Camus when duplicate concurrent effects
  are undesirable; and
- use explicit `reclaim` only when the application needs to await a pass.

See the [usage guide](../docs/usage.md) for production ownership, retry,
failure, and capacity patterns.
