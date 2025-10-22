-- ==========================================================
-- Upsert invoices
-- ==========================================================
WITH w AS (
  SELECT last_loaded_ts FROM invoicing_olap.etl_watermarks WHERE stream_name='invoices'
),
src AS (
  SELECT i.invoice_id, i.invoice_no, i.invoice_date, i.seller_id, i.client_id,
         i.total_net_worth, i.total_vat, i.total_gross_worth, i.status, i.updated_at
  FROM invoices i, w
  WHERE i.updated_at >= w.last_loaded_ts
),
keys AS (
  SELECT s.invoice_id,
         invoicing_olap.ensure_date_key(s.invoice_date) AS invoice_date_key,
         ds.seller_key, dc.client_key,
         s.invoice_no, s.total_net_worth, s.total_vat, s.total_gross_worth, s.status, s.updated_at
  FROM src s
  JOIN invoicing_olap.dim_seller ds ON ds.seller_id = s.seller_id
  JOIN invoicing_olap.dim_client dc ON dc.client_id = s.client_id
)
INSERT INTO invoicing_olap.fact_invoice (
  invoice_id, invoice_no, invoice_date_key, seller_key, client_key,
  total_net, total_vat, total_gross, status, updated_at
)
SELECT invoice_id, invoice_no, invoice_date_key, seller_key, client_key,
       total_net_worth, total_vat, total_gross_worth, status, updated_at
FROM keys
ON CONFLICT (invoice_id)
DO UPDATE SET
  total_gross = EXCLUDED.total_gross,
  status = EXCLUDED.status,
  updated_at = EXCLUDED.updated_at;

UPDATE invoicing_olap.etl_watermarks
SET last_loaded_ts = NOW()
WHERE stream_name = 'invoices';

-- ==========================================================
-- Upsert invoice_items
-- ==========================================================
WITH w AS (
  SELECT last_loaded_ts FROM invoicing_olap.etl_watermarks WHERE stream_name='invoice_items'
),
src AS (
  SELECT ii.item_id, ii.invoice_id, ii.product_id,
         ii.quantity, ii.net_price, ii.net_worth, ii.vat_rate, ii.gross_worth, ii.created_at
  FROM invoice_items ii, w
  WHERE ii.created_at >= w.last_loaded_ts
),
keys AS (
  SELECT s.item_id, s.invoice_id,
         dp.product_key,
         s.quantity, s.net_price, s.net_worth, s.vat_rate, s.gross_worth, s.created_at
  FROM src s
  JOIN invoicing_olap.dim_product dp ON dp.product_id = s.product_id
)
INSERT INTO invoicing_olap.fact_invoice_item (
  invoice_id, product_key, qty, net_price, net_worth, vat_rate, gross_worth, created_at
)
SELECT invoice_id, product_key, quantity, net_price, net_worth, vat_rate, gross_worth, created_at
FROM keys
ON CONFLICT DO NOTHING;

UPDATE invoicing_olap.etl_watermarks
SET last_loaded_ts = NOW()
WHERE stream_name='invoice_items';

-- ==========================================================
-- Upsert payments
-- ==========================================================
WITH w AS (
  SELECT last_loaded_ts FROM invoicing_olap.etl_watermarks WHERE stream_name='payments'
),
src AS (
  SELECT p.payment_id, p.invoice_id, p.payment_date,
         p.amount, p.method, p.reference, p.created_at
  FROM payments p, w
  WHERE p.created_at >= w.last_loaded_ts
),
keys AS (
  SELECT s.payment_id, s.invoice_id,
         invoicing_olap.ensure_date_key(s.payment_date) AS payment_date_key,
         s.amount, s.method, s.reference, s.created_at
  FROM src s
)
INSERT INTO invoicing_olap.fact_payment (
  invoice_id, payment_date_key, amount, method, reference, created_at
)
SELECT invoice_id, payment_date_key, amount, method, reference, created_at
FROM keys
ON CONFLICT DO NOTHING;

UPDATE invoicing_olap.etl_watermarks
SET last_loaded_ts = NOW()
WHERE stream_name='payments';
