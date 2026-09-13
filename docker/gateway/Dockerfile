# syntax=docker/dockerfile:1
#
# Multi-stage build for cmd/gateway, TariPay Gateway's single binary.
#
# Structure/reasoning copied from this org's established convention in
# go-crypto-pool's docker/backend/Dockerfile (golang:<minor>-bookworm builder,
# CGO_ENABLED=0 static build, distroless/static-debian12 nonroot runtime) — see
# AGENTS.md for the pointer to that precedent. Adapted here to this repo's single
# binary (cmd/gateway) instead of go-crypto-pool's four.
#
# Builder Go version pinned to golang:1.25-bookworm to match this repo's go.mod
# `go 1.25.14` directive's minor version (checked directly, not assumed) — bump
# this alongside any future go.mod bump.
FROM golang:1.25-bookworm AS builder

WORKDIR /src

# Cache module downloads separately from source so `go build` reruns don't
# re-fetch dependencies on every source change.
COPY go.mod go.sum ./
RUN go mod download

# COPY . . must happen before `go build` below so cmd/gateway's transitive
# dependency on internal/admin's `//go:embed templates/*.html` (internal/admin/
# admin.go) can actually see internal/admin/templates/*.html at build time — Go's
# embed directive resolves relative to the package directory on disk at compile
# time, so the templates must already be present in this build context.
COPY . .

# CGO_ENABLED=0 + GOOS=linux: fully static binary, no libc dependency, so it runs
# unmodified on the distroless static runtime base below. -trimpath drops local
# build-path strings from the binary; -ldflags="-s -w" strips the symbol table and
# DWARF debug info to shrink the final image.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway

# Runtime stage: distroless static, nonroot. cmd/gateway makes outbound HTTPS
# webhook-delivery calls (internal/webhook.Sender) and outbound wallet gRPC calls
# (github.com/Snipa22/go-tari-lib's walletGRPC package) — both need the system CA
# bundle to verify TLS certificates, which is why distroless's "static" variant
# (not "static" stripped of ca-certificates, and not "scratch") is used: it ships
# /etc/ssl/certs's CA bundle out of the box, same justification as go-crypto-pool's
# backend Dockerfile.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/gateway /usr/local/bin/gateway

USER nonroot:nonroot

# No default TARIPAY_HTTP_LISTEN_ADDR is baked in here — per AGENTS.md's
# no-hardcoded-defaults rule, the listen address is config.DefaultHTTPListenAddr
# (see internal/config/config.go) unless overridden by flag/env/config file at
# runtime. EXPOSE below documents that default without hardcoding a redefinition
# of it here.
EXPOSE 8090

ENTRYPOINT ["/usr/local/bin/gateway"]
