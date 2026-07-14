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
| `multi_stream` | Independent upload, audit, tenant, or destination buffers | IDs, pending state, release, and rollover policy are stream-scoped |
| `readiness` | Notify an async task or application reactor | `wait_for` is level-triggered observation, not callback execution or record assignment |
| `maintenance` | Periodic storage maintenance | The application schedules age rollover and reclaim; active reclaim is explicit pressure policy |

The examples deliberately keep application policy outside Camus. In a real
application:

- use a stable record ID and never reuse it in the same stream;
- make repeated downstream effects safe when duplicates matter;
- release only after the effect is durably represented elsewhere;
- bound each drain attempt and coordinate workers above the storage layer;
- rearm readiness after a drain attempt, not while work remains pending; and
- schedule idle age rollover and reclamation from the application's own timer.

See the [usage guide](../docs/usage.md) for production ownership, retry,
failure, and capacity patterns.
