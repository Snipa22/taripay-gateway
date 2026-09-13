# AGENTS.md

Instructions for AI coding agents (OpenCode, Claude Code, or any `agents.md`-compatible tool) working in this repository. Read this before making changes.

## Project

- **What this repo is:** TariPay Gateway — a self-hosted Tari (XTM) payment gateway service. Merchants run their own instance against their own `minotari_console_wallet` (daemon-mode, gRPC). It issues per-order invoices tagged with a payment ID, hands out a wallet address, watches the wallet's event stream for payment, and fires signed webhooks back to the store (e.g. a WooCommerce plugin, built separately). Modeled on BTCPay Server's architecture: thin cart-side plugin, all wallet logic lives here, no third-party custody.
- **Module path:** `github.com/Snipa22/taripay-gateway`
- **Depends on:** `github.com/Snipa22/go-tari-lib` (wallet gRPC wrapper — use its `walletGRPC` package's `InitWalletGRPC`, `GetPaymentIdAddress`, `StreamTransactionEvents`, `GetCompletedTransactionsByPaymentID`, `Identify`, `GetBalances` functions; do not reimplement wallet gRPC calls here), `github.com/Snipa22/go-tari-grpc-lib/v3` (transitive, for `tari_generated` types).
- **Deployment model:** self-hosted-per-merchant by default — one gateway + one wallet per merchant, always. A merchant may have Ara/Alex (jagtech.io) administer their instance as an operational convenience, but that never means pooling multiple merchants into one wallet or one gateway process. Design as if every deploy is single-tenant, because it always is.

## Commands

- **Build:** `go build ./...`
- **Test:** `go test ./...`
- **Vet:** `go vet ./...`
- **Format:** `gofmt -l .` (should return nothing; `gofmt -w .` to fix)
- **Tidy:** `go mod tidy`

Run build + vet + gofmt + test before considering any change complete.

## Conventions

- **Conventional Commits** required — commit type drives semver via release-please (set up in a later task, not this one).
- **Rebase, never merge.** No merge commits in PR branches.
- **No direct commits/pushes to `main`** once branch protection is applied (the very first scaffold commit is an explicit exception, applied by the orchestrating agent under admin-bypass — you will be told explicitly if you're doing that one).
- **No static/hardcoded assumptions** — wallet gRPC address, Postgres DSN, HTTP listen address, confirmation-depth threshold, invoice TTL, webhook HMAC secret: all config-driven (flag > env var > default), never hardcoded. Follow the config-resolution-order pattern used in sibling repo `go-tari-ootle-explorer`'s `internal/config/config.go` (CLI flag > env var > config file > default, with defaults documented and cited where they mirror an upstream default).
- **Migrations**: plain numbered SQL migration files in `internal/db/migrations/`, `NNNN_description.up.sql` / `NNNN_description.down.sql` pairs (see `go-tari-ootle-explorer`'s `internal/db/migrations/` for the exact pattern to copy). Use `github.com/golang-migrate/migrate/v4` if not already decided otherwise — check `go.sum` conventions of sibling repos first.
- **HTMX + plain Go templates for any UI** — no SPA/JS framework. Admin-panel convention (used across this org's other internal tools): plain HTML forms, `confirm()` before destructive actions, flash-message banners (`.flash-message` CSS class pattern). See `go-crypto-pool-web`'s `templates/layout.html` for the base-layout style to match if useful, though that repo is a read-only frontend and this one has real forms/actions.
- Package layout: mirror `go-tari-ootle-explorer`'s `cmd/<binary>/main.go` + `internal/<package>/` layout — don't invent a new top-level layout convention.

## Don't

- Don't touch anything in the `go-tari-lib` or `go-tari-grpc-lib` dependencies — if a wallet gRPC call you need isn't already wrapped in `go-tari-lib`'s `walletGRPC` package, STOP and flag it back rather than vendoring/reimplementing it here.
- Don't pool multiple merchants' funds/wallets into one process or one DB — single-tenant only, always.
- Don't build refund logic, multi-currency support, or hosted-multi-tenant admin tooling in this pass — explicitly out of scope for v1 (see project plan).
- Don't skip tests because "there weren't any before."
- Don't silently pick a confirmation-depth / invoice-TTL / under-overpayment-tolerance default without citing it clearly in a code comment AND your final summary — these are real open design decisions the human operator (Alex) has not locked in yet; ship sane, clearly-flagged defaults, don't treat them as settled.

## Disclosure

If you (the agent) are making a substantial autonomous contribution, note it in your final summary so the human operator can add a disclosure note per this repo's future CONTRIBUTING.md (not yet written — that's fine, just flag it).
