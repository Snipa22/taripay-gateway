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

# APP_VERSION is a build ARG (task brief part 4, I13/I27's version-stamp fix) —
# defaults to "dev" for a plain `docker build` with no override, expected to be
# set via `--build-arg APP_VERSION=<value>` (e.g. a git describe/tag) by CI. Baked
# into the binary via -ldflags -X main.version=..., not a runtime env var, since a
# version stamp describes what was BUILT, not how it's currently configured.
ARG APP_VERSION=dev

# CGO_ENABLED=0 + GOOS=linux: fully static binary, no libc dependency, so it runs
# unmodified on the distroless static runtime base below. -trimpath drops local
# build-path strings from the binary; -ldflags="-s -w" strips the symbol table and
# DWARF debug info to shrink the final image; -X main.version=$APP_VERSION stamps
# the version var cmd/gateway/main.go prints at startup and exposes via GET
# /health.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w -X main.version=${APP_VERSION}" -o /out/gateway ./cmd/gateway

# cmd/healthcheck (task brief part 4, I13/I27): a tiny standalone binary built
# alongside gateway specifically for the HEALTHCHECK directive below — see that
# command's own doc comment for why (distroless has no shell/curl to shell out to
# instead). No version stamp needed on this one; it's an internal plumbing detail,
# not a user-facing binary.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/healthcheck ./cmd/healthcheck

# Runtime stage: distroless static, nonroot. cmd/gateway makes outbound HTTPS
# webhook-delivery calls (internal/webhook.Sender) and outbound wallet gRPC calls
# (github.com/Snipa22/go-tari-lib's walletGRPC package) — both need the system CA
# bundle to verify TLS certificates, which is why distroless's "static" variant
# (not "static" stripped of ca-certificates, and not "scratch") is used: it ships
# /etc/ssl/certs's CA bundle out of the box, same justification as go-crypto-pool's
# backend Dockerfile.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/gateway /usr/local/bin/gateway
COPY --from=builder /out/healthcheck /usr/local/bin/healthcheck

USER nonroot:nonroot

# No default TARIPAY_HTTP_LISTEN_ADDR is baked in here — per AGENTS.md's
# no-hardcoded-defaults rule, the listen address is config.DefaultHTTPListenAddr
# (see internal/config/config.go) unless overridden by flag/env/config file at
# runtime. EXPOSE below documents that default without hardcoding a redefinition
# of it here.
#
# TARIPAY_ADMIN_AUTH_TOKEN has no default at all (not even a placeholder) and MUST be
# set for the admin UI (/admin, /admin/invoices, /admin/webhooks/*/retry) to be
# reachable — if unset, cmd/gateway logs a startup warning and does not register
# those routes on the mux (they 404), per the S1/I19 admin-auth fix. The
# merchant-facing /invoice API is unaffected either way.
EXPOSE 8090

# HEALTHCHECK (task brief part 4, I13/I27): hits cmd/gateway's GET /health via the
# cmd/healthcheck binary copied in above, since distroless has no shell/curl for
# Docker's usual shell-out-to-curl HEALTHCHECK pattern — see that command's doc
# comment. cmd/healthcheck defaults to http://127.0.0.1:8090/health, matching
# config.DefaultHTTPListenAddr; if TARIPAY_HTTP_LISTEN_ADDR is overridden away from
# that default at runtime, also set TARIPAY_HEALTHCHECK_URL to match (cmd/
# healthcheck reads that env var — see its own doc comment), or this HEALTHCHECK
# will report unhealthy against the wrong address.
#
# --interval/--timeout/--start-period/--retries are all PLACEHOLDER, UNCONFIRMED
# values, same "reasonable-sounding guess, not project-owner-approved" caveat as
# this repo's other tunables (see cmd/gateway/main.go's ttlSweepTickerInterval
# etc.) — a generous but bounded guess for a small self-hosted gateway process,
# not load-tested or project-owner-approved.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/healthcheck"]

ENTRYPOINT ["/usr/local/bin/gateway"]
