-- 0003_amount_received down: drop the amount_received_utari column added by
-- 0003_amount_received.up.sql.

ALTER TABLE invoices
    DROP COLUMN IF EXISTS amount_received_utari;
