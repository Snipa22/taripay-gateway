-- 0002_webhook_deliveries: Phase 1b schema addition for the webhook delivery log
-- (internal/webhook.Store) — exactly as specified in the Phase 1b scope brief
-- (section "What to build" > 1).

CREATE TABLE webhook_deliveries (
    id           UUID PRIMARY KEY,
    invoice_id   UUID NOT NULL REFERENCES invoices(id),
    callback_url TEXT NOT NULL,
    payload      JSONB NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending', -- pending | delivered | failed
    attempts     INT NOT NULL DEFAULT 0,
    last_error   TEXT,
    response_code INT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ
);

CREATE INDEX idx_webhook_deliveries_invoice_id ON webhook_deliveries(invoice_id);
CREATE INDEX idx_webhook_deliveries_status ON webhook_deliveries(status);
