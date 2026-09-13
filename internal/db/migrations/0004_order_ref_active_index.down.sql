-- 0004_order_ref_active_index down: drop the partial index added by
-- 0004_order_ref_active_index.up.sql.

DROP INDEX IF EXISTS idx_invoices_order_ref_active;
