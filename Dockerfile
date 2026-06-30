# Multi-stage build for fsserver.
#
# Build:
#   docker build -t fsserver .
#
# Run (local storage):
#   docker run -p 8080:8080 -v $(pwd)/data:/data fsserver
#
# Run (MinIO storage + Redis):
#   docker run -p 8080:8080 -v $(pwd)/config.yaml:/app/config.yaml fsserver /app/config.yaml

# ---------- build stage ----------
FROM golang:1.25-alpine AS builder

# git is required by go modules for VCS stamping; ca-certificates for TLS.
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Cache module downloads by copying go.mod/go.sum first.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source and build a static binary.
COPY . .

# CGO_DISABLED=1 produces a fully static binary that runs on scratch/alpine
# without depending on libc. The linker flags strip debug info to shrink the
# image.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/fsserver ./cmd/fsserver

# ---------- runtime stage ----------
FROM alpine:3.20

# ca-certificates for outbound TLS (MinIO/PostgreSQL/Redis over TLS).
# tzdata for correct timestamp handling across timezones.
RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S app && adduser -S app -G app

WORKDIR /app

COPY --from=builder /out/fsserver /app/fsserver

# Default data directory for the local storage backend. Mount a volume here in
# production so uploaded files persist across container restarts.
RUN mkdir -p /data && chown -R app:app /data /app

USER app

ENV TZ=UTC \
    GIN_MODE=release

EXPOSE 8080 9090

# Default config lives next to the binary; override by passing a config path
# as the first argument (e.g. `docker run ... fsserver /path/to/config.yaml`).
ENTRYPOINT ["/app/fsserver"]
CMD ["config.yaml"]
