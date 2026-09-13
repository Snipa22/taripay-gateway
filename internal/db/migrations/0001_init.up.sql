-- 0001_init: v1 schema for TariPay Gateway's invoice tracking, exactly as specified in
-- the Phase 1a scope brief (section 3). One table only — event-watcher/webhook state
-- (Phase 1b) is expected to extend this incrementally, not be designed speculatively
-- here.

CREATE TABLE invoices (
    id             UUID PRIMARY KEY,
    payment_id     TEXT NOT NULL UNIQUE,   -- the UTF8 payment ID string sent to the wallet
    order_ref      TEXT NOT NULL,          -- merchant's own order/cart reference
    amount_utari   BIGINT NOT NULL,        -- amount in microTari (uT), matches go-tari-lib's uint64 convention
    address        TEXT NOT NULL,          -- resolved payment address from GetPaymentIdAddress
    status         TEXT NOT NULL DEFAULT 'pending', -- pending | seen | confirmed | expired | cancelled
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at     TIMESTAMPTZ NOT NULL,
    confirmed_at   TIMESTAMPTZ
);

CREATE INDEX idx_invoices_payment_id ON invoices(payment_id);
CREATE INDEX idx_invoices_status ON invoices(status);
