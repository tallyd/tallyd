# Contributing to tallyd

Thanks for considering a contribution.

## Developer Certificate of Origin (DCO)

Every commit must be signed off, certifying you wrote it or otherwise have
the right to submit it under this project's license (the [DCO text](https://developercertificate.org/)).
Add a sign-off with `-s`:

```sh
git commit -s -m "your message"
```

This appends a `Signed-off-by: Your Name <you@example.com>` trailer to
the commit. PRs with unsigned commits will fail CI and can't be merged.

We do not require a CLA.

## Development

```sh
go build ./...
go test ./...
golangci-lint run
```

- Go 1.26+ (see `go.mod`).
- The WAL, dispatcher, and batcher packages favor correctness over
  throughput in this first pass — see the `TODO` comments in
  `internal/wal`, `internal/batcher`, and `internal/dispatcher` for known
  simplifications before touching that code.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/) style
subject lines (`feat:`, `fix:`, `chore:`, `test:`, `docs:`, `refactor:`).

## Pull requests

- Keep PRs focused; unrelated cleanup belongs in a separate PR.
- Add or update tests for behavior changes.
- `go build ./...` and `go test ./...` must pass locally before you push.
