# AGENTS.md

Guidance for AI coding agents working in this repository.

## Commands

```sh
go build ./...
go test ./... -race
go vet ./...
golangci-lint run ./...
govulncheck ./...
```

Run a single test:

```sh
go test ./internal/wal/... -run TestCrashRecovery -v
```

Regenerate gRPC code after editing `proto/tallyd/v1/events.proto` (requires `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc` on `PATH`):

```sh
protoc -I proto -I "$(brew --prefix protobuf)/include" \
  --go_out=. --go_opt=module=github.com/tallyd/tallyd \
  --go-grpc_out=. --go-grpc_opt=module=github.com/tallyd/tallyd \
  proto/tallyd/v1/events.proto
```

Run the daemon locally:

```sh
go build -o tallyd ./cmd/tallyd
cp config.example.yaml config.yaml   # edit buffer.dir etc.
./tallyd -config config.yaml
```

Commits require DCO sign-off (`git commit -s`) and Conventional Commits
subject lines (`feat:`, `fix:`, `chore:`, `test:`, `docs:`, `refactor:`) —
see CONTRIBUTING.md. CI enforces both on PRs.

## Architecture

tallyd is a durability-first pipeline: `receiver -> WAL -> dispatcher ->
batcher -> adapter`. The full wiring lives in `internal/pipeline.Build`;
`cmd/tallyd/main.go` is a thin flag/signal wrapper around it.

**The core invariant**: `internal/wal.WAL.Append` only returns after its
record is fsync'd to disk. The HTTP receiver (`internal/receiver`) acks
the caller with 2xx *only* after every event in the request durably
appends — never before. This is what makes it safe to treat a 2xx as "this
event will survive a crash." `internal/wal/wal_test.go`'s
`TestCrashRecovery` proves this by SIGKILLing a real subprocess mid-run
and asserting replay recovers everything that was ever acked.

**WAL size is bounded, not unbounded**: `Config.Buffer.MaxBytes` (via
`wal.WithMaxBytes`) caps total on-disk size across every segment,
checked in `Append` via `WAL.TotalBytes()` before submitting the write;
once over the cap, `Append` returns `wal.ErrBufferFull` and the receiver
surfaces that as `503`/`Unavailable` rather than growing the WAL forever.
`<= 0` (unset) means unlimited. `pipeline.Build` fails fast at startup if
`Buffer.OnFull` is set to anything other than `"reject"` — the only
implemented policy. Note that ack records consume WAL space too (written
to whichever segment is currently active, not necessarily the segment the
original event lives in) — `TestBufferSpaceFreesAfterSegmentGC` in
`internal/wal/wal_test.go` probes real on-disk sizes empirically rather
than assuming event/ack framing overhead, precisely because this is easy
to get wrong by hand.

**Two transports, one core**: `receiver.Receiver.Ingest([]adapter.Event) error` is the transport-agnostic core (validate → route → durably append) — both the HTTP handler (`handleEvents`, JSON decode) and `internal/grpcserver.Server` (protobuf decode, generated from `proto/tallyd/v1/events.proto`) are thin shims that convert into `[]adapter.Event` and call it, so validation/routing/durability behave identically no matter which transport an event arrived through. `Ingest` returns typed `*receiver.ValidationError` / `*receiver.UnavailableError` so each transport maps them to its own status codes (HTTP 400/503, gRPC `InvalidArgument`/`Unavailable`) without duplicating the classification logic. The gRPC listener is optional and off by default (`Config.Listen.GRPC` empty, or `pipeline.Pipeline.GRPCServer` nil) — `cmd/tallyd/main.go` only starts it if configured, and treats its shutdown the same way as the HTTP server's (`GracefulStop()` alongside `server.Shutdown()`). One easy-to-miss detail if you touch the gRPC conversion path: `(*timestamppb.Timestamp)(nil).AsTime()` returns Unix epoch, not Go's zero `time.Time{}` — `grpcserver.toEvents` explicitly checks for a nil `Timestamp` first, otherwise a client that omits it would silently skip the "timestamp is required" validation that the HTTP path correctly enforces.

**Dual delivery paths, same durability boundary**: once an event is
durable, `pipeline.walDispatchSink.Append` does two things — it's the
`receiver.Sink` the HTTP layer talks to, and after a successful
`wal.Append` it also hands the event to the `dispatcher` for live
delivery. On restart, `dispatcher.ReplayPending(wal.Pending())` re-enqueues
anything left unresolved from a prior crash before the receiver accepts
new traffic. So live delivery and crash-recovery delivery are two
different code paths converging on the same `dispatcher` — but not on the
same strictness: `Dispatch` (live path) errors on an unknown provider,
while `ReplayPending` deliberately *skips + logs* one and leaves the entry
pending. That asymmetry is intentional. An unknown provider must never
reach the live path in the first place — `receiver.Receiver.KnownProviders`
validates every resolved route, including a client-supplied `event.Route`
(which bypasses `pipeline.validateRouting`, since that only checks the
config's own routing), and rejects an unknown name with a 400 *before* the
`wal.Append`. But a WAL written by an older/misconfigured build could still
hold such an entry, and there `ReplayPending` must tolerate it: erroring
would make `pipeline.Build` fail on every restart, permanently wedging the
daemon over one bad entry. Leaving it pending (visible via
`wal_unacked_entries`) is the lesser evil.

**Per-provider ack state, not per-event**: each WAL entry tracks which
providers have acked/dead-lettered it independently (`wal.Entry.Pending`).
An entry is only garbage-collected once *every* target provider has
resolved it — this is what makes dual-write (sending the same event to
Orb and Metronome, say) correct instead of racy.

**One queue+batch+retry engine per provider**: `internal/dispatcher` just
fans an event out to the named provider `Enqueuer`s (normally
`*batcher.Batcher`); it does no batching itself. Each `Batcher` owns its
own flush-on-size-or-linger loop, exponential backoff+jitter retry (capped
below the provider's dedup window via `RetryPolicy.MaxElapsed` — retrying
past that window risks double-counting, so exhausted retries dead-letter
instead of looping forever), and DLQ handoff. A slow/down provider only
backs up its own queue, never a healthy one's.

**Dead-lettering is terminal, and the DLQ isn't just an append-only
log**: `batcher.deadLetter` passes a `permanent bool` into
`DeadLetterSink.Put` — `true` for a provider-rejected or encode-error
failure (unlikely to succeed on replay without intervention), `false` for
retry-budget-exhausted (failed on time, not necessarily on the payload).
`internal/dlq.DLQ` tracks a cumulative `Attempts` count per (provider,
event ID), reconstructed from disk on restart, not just in-memory —
once it crosses its category's threshold (`permanentPoisonThreshold=2`,
`exhaustedPoisonThreshold=3`, neither configurable yet), the event moves
to a separate `<provider>.poison.jsonl` file and its earlier non-poisoned
record is purged from the regular file (`List()` must never return a
stale, superseded entry for an ID that's since been poisoned — this was
a real bug caught by testing the poison-threshold logic itself). Replay
is `POST /v1/dlq/replay?provider=X[&include_poison=true]`
(`internal/dlqreplay`) or the `tallyd dlq replay` CLI wrapper around it:
reads `List()` (and `ListPoison()` if requested), re-`Append`s each event
through the same `Sink` the receiver uses — targeting *only* the provider
being replayed, not the event's original full route — and `Remove()`s
whichever ones were durably re-queued. A successful replay means
"durably re-queued," the same "accepted" guarantee as a fresh
`POST /v1/events`, not confirmation of delivery; if it fails again it
produces a brand-new DLQ record with the attempt count continuing from
where it left off. `dlqreplay.Handler.KnownProviders` rejects an unknown
provider name before ever touching the DLQ or WAL — otherwise the event
would get durably re-appended before dispatch failed on it, leaving a
permanently-stuck WAL entry, the same failure mode
`pipeline.validateRouting` exists to prevent on the normal ingest path.
`internal/dlqshow` is the read-only counterpart: `GET /v1/dlq?provider=X
[&include_poison=true]` (`tallyd dlq show` CLI wrapper) returns the same
records replay would act on, without re-injecting anything — lets you see
what's dead-lettered before deciding whether to replay it. Same
`KnownProviders` gate, same `Store` interface subset (`List`/`ListPoison`)
as `dlqreplay.Handler`.

**`internal/status` answers "what's backed up" without parsing Prometheus
text**: `GET /v1/status` (`tallyd status` CLI wrapper) returns
`wal_unacked_entries` plus a per-provider `{depth, poison_depth}` breakdown
computed from `DLQ.List`/`DLQ.ListPoison` at request time (not the
periodically-refreshed `/metrics` gauges) — `status.Handler.Providers` is
the full configured provider list so every provider shows up even at zero
depth, not just ones that happen to have DLQ files on disk yet.

**Up to `defaultMaxInFlight` (4) sends run concurrently per provider,
bounded, not unbounded**: `run()`'s single goroutine still owns `pending`
and the linger timer, but `flush()` spawns each Send as its own goroutine
gated by a semaphore, so a slow provider response no longer blocks
everything queued behind it — `run()` keeps accumulating and dispatching
new batches while earlier ones are still in flight, up to the concurrency
cap; beyond that, acquiring the semaphore slot blocks `run()` itself,
which is the intended back-pressure (never spawn unbounded concurrent
requests at a struggling provider). Verified against a real slow HTTP
endpoint: 12 events produced exactly 3 waves of 4 concurrent requests,
not 12 serial ones. This is precisely why `Batcher.Close()` needs a
*second* wait group (`flushWG`, alongside `wg` for the `run()` loop
itself) — without it, `Close()` could return while sends spawned during
shutdown are still running, and the caller (`pipeline.Close`) would go on
to close the DLQ/WAL out from under them. Not yet configurable per
provider; a fixed default for this first cut.

**Structural typing keeps packages decoupled**: `receiver`, `batcher`, and
`dispatcher` never import `internal/wal`, `internal/dlq`, or
`internal/metrics` directly (dispatcher imports `wal` only for the
`wal.Entry` data type, not behavior). Instead each package declares small
local interfaces it needs (`receiver.Sink`, `receiver.MetricsRecorder`,
`batcher.Acker`, `batcher.DeadLetterSink`, `batcher.MetricsRecorder`,
`dispatcher.Enqueuer`), and the concrete types (`*wal.WAL`, `*dlq.DLQ`,
`*metrics.Metrics`, `*batcher.Batcher`) satisfy them structurally. When
adding a new producer/consumer relationship between packages, prefer
adding a narrow interface at the consumer over introducing a new import.
Metrics fields are always optional (nil-checked before use) so tests don't
need to wire a `*metrics.Metrics` just to exercise business logic.

**Two adapters exist so far**: `adapter.Adapter` is the vendor seam
(`Encode`/`Send`/`Classify`/`MaxBatchSize`). `adapter/stdout` prints
batches instead of calling a real billing API, which is enough to
exercise the whole pipeline without vendor credentials. `adapter/metronome`
calls Metronome's real ingest API (`POST /v1/ingest`, Bearer token, JSON
array of `transaction_id`/`customer_id`/`event_type`/`timestamp`/
`properties`) — batch size is hard-capped at 100 regardless of config
(`MaxBatchSize` constant) since that's Metronome's documented limit, not a
tunable, and every property value gets stringified before sending
(`stringifyProperties`) since Metronome requires that even for numbers and
booleans. Its `Send` treats a 2xx as the whole batch accepted (Metronome's
docs don't specify a per-event result body) and its `Classify` maps
429/5xx/network errors to `Retry` and other 4xx to `DeadLetter`. Orb is
the next adapter to add. `internal/pipeline.buildAdapter` is the factory
switch — adding Orb means adding a case there plus a new `adapter/<name>`
package, not touching the pipeline wiring itself.
`internal/pipeline.ProviderConfig`'s `Endpoint`/`TokenEnv` fields (the
latter resolved via `os.Getenv` in `buildAdapter`, erroring if unset) are
what make this config-driven without code changes per provider.

**Config**: `internal/pipeline/config.go` defines a YAML schema with a
custom `Duration` type (`UnmarshalYAML` via `time.ParseDuration`, since
`encoding/yaml` doesn't parse `"10s"`-style strings for the stdlib type
out of the box). `Config.applyDefaults()` fills in unset fields
per-provider; `cmd/tallyd/main.go` calls `pipeline.LoadConfig` if `-config`
is given, otherwise builds a zero-value `Config` and lets `pipeline.Build`
apply defaults. `pipeline.Build` also validates before doing anything
else: `Buffer.OnFull` must be `"reject"`, and every provider name in
`Routing.Default`/`Routing.Rules` must exist in `Providers` —
`validateRouting` catches a typo'd provider name at startup instead of
letting it surface later as a `503` on whichever request first matches
the bad rule.

**Testing patterns worth reusing**: fake `Acker`/`DeadLetterSink`/`Adapter`
implementations in `internal/batcher/batcher_test.go` and
`internal/dispatcher/dispatcher_test.go` (structural typing makes these
trivial); `internal/pipeline/pipeline_test.go` captures real `os.Stdout`
via `os.Pipe` to assert on what the stdout adapter printed; retry/linger
tests use a `waitFor(t, timeout, cond)` polling helper rather than fixed
sleeps, since the batcher's flush loop runs on its own goroutine.
