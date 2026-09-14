# taripay-gateway

Self-hosted Tari (XTM) payment gateway. Self-hosted-per-merchant, BTCPay-Server-style
architecture: this service wraps a merchant-operated `minotari_console_wallet`
(daemon-mode gRPC), issues per-order invoices, and fires signed webhooks to cart
plugins (WooCommerce first) on confirmed payment. No third-party custody.

See `AGENTS.md` for architecture/contribution conventions.

The admin UI (`/admin`, `/admin/invoices`, `/admin/webhooks/*/retry`) requires the
`TARIPAY_ADMIN_AUTH_TOKEN` env var to be set (shared bearer token); if unset, those
routes are not registered at all.

## Configuration

Every setting is resolved in `CLI flag > env var > TOML config file > default` order
(run `gateway -h` for the full list of flags). Two settings are the exception:
**Postgres DSN** (`TARIPAY_POSTGRES_DSN` / `postgres_dsn` in the config file) and the
**webhook HMAC signing secret** (`TARIPAY_WEBHOOK_HMAC_SECRET` /
`webhook_hmac_secret`) have no corresponding CLI flag — they are env-var/config-file
only. This is deliberate: a CLI flag's value is visible to any other local user via
`/proc/<pid>/cmdline` or `ps`, and typically ends up in shell history too, which is a
real exposure for two genuinely secret values. Every other setting (wallet gRPC
address, HTTP listen address, webhook callback URL, admin auth token, etc.) remains
flag-settable as usual — this fix covers only the two values called out above.

