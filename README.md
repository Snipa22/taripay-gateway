# taripay-gateway

Self-hosted Tari (XTM) payment gateway. Self-hosted-per-merchant, BTCPay-Server-style
architecture: this service wraps a merchant-operated `minotari_console_wallet`
(daemon-mode gRPC), issues per-order invoices, and fires signed webhooks to cart
plugins (WooCommerce first) on confirmed payment. No third-party custody.

See `AGENTS.md` for architecture/contribution conventions.

The admin UI (`/admin`, `/admin/invoices`, `/admin/webhooks/*/retry`) requires the
`TARIPAY_ADMIN_AUTH_TOKEN` env var to be set (shared bearer token); if unset, those
routes are not registered at all.
