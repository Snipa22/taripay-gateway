# taripay-gateway

Self-hosted Tari (XTM) payment gateway. Self-hosted-per-merchant, BTCPay-Server-style
architecture: this service wraps a merchant-operated `minotari_console_wallet`
(daemon-mode gRPC), issues per-order invoices, and fires signed webhooks to cart
plugins (WooCommerce first) on confirmed payment. No third-party custody.

See `AGENTS.md` for architecture/contribution conventions.
