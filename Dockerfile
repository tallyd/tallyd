# syntax=docker/dockerfile:1

FROM golang:1.26 AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/tallyd ./cmd/tallyd

# distroless/static has no shell, so the WAL directory can't be mkdir/chown'd
# in the final stage — prep it here (owned by the nonroot UID/GID the final
# image runs as: 65532) and COPY it over instead.
RUN mkdir -p /out/var-lib-tallyd && chown -R 65532:65532 /out/var-lib-tallyd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/tallyd /usr/local/bin/tallyd

# Matches BufferConfig's default dir (see internal/pipeline/config.go), so
# the container works out of the box without a mounted config. Mount a
# volume here in production so the WAL survives container restarts —
# losing it defeats the whole point of the daemon.
COPY --from=builder --chown=65532:65532 /out/var-lib-tallyd /var/lib/tallyd
VOLUME ["/var/lib/tallyd"]

EXPOSE 8999

ENTRYPOINT ["/usr/local/bin/tallyd"]
# 0.0.0.0 (not the config default's 127.0.0.1) so `docker run -p` can
# actually reach it. Override entirely to pass -config, e.g.:
#   docker run -p 8999:8999 -v ./config.yaml:/etc/tallyd/config.yaml \
#     -e METRONOME_API_KEY=... myimage -config /etc/tallyd/config.yaml -listen 0.0.0.0:8999
CMD ["-listen", "0.0.0.0:8999"]
