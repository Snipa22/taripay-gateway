-- 0003_amount_received: adds cumulative-received-amount tracking to invoices, per the
-- C2 finding fix (task brief part 1) — internal/eventwatcher's handleEvent must
-- compare the amount actually received against the invoiced amount before confirming,
-- and needs a place to accumulate received amounts across multiple events/transfers
-- for the same invoice (e.g. an underpayment followed by a top-up transfer).

ALTER TABLE invoices
    ADD COLUMN amount_received_utari BIGINT NOT NULL DEFAULT 0;
