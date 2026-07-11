# tallyd

Durable, vendor-agnostic daemon for forwarding usage-based billing events to any provider.

`tallyd` buffers local usage events durably (fsync'd write-ahead log)
before forwarding them to billing providers, so an accepted event is
never lost even if the process crashes mid-flight. Same pattern as an
OpenTelemetry Collector or Fluent Bit, specialized for billing ingestion.

## Status

Early. The receiver, WAL, dispatcher, batcher, retry/DLQ, and metrics
pipeline work end-to-end today, but only a `stdout` adapter exists so
far — it prints batches instead of calling a real billing API, which is
enough to exercise the whole pipeline without vendor credentials. Orb
and Metronome adapters are the next unit of work.

## Quickstart

```sh
go build -o tallyd ./cmd/tallyd
cp config.example.yaml config.yaml   # adjust buffer.dir etc.
./tallyd -config config.yaml
```

```sh
curl -X POST http://127.0.0.1:8999/v1/events \
  -H "Content-Type: application/json" \
  -d '{"id":"evt-1","customer_id":"cust_1","event_name":"api_call","timestamp":"2026-07-11T12:00:00Z"}'
```

Metrics are served at `/metrics`.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Commits require DCO sign-off
(`git commit -s`).

## License

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
