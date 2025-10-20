-- Upsert invoices
WITH w AS (SELECT last_loaded_ts FROM invoicing_olap.etl_watermarks WHERE stream_name='invoices'),
src AS (
  SELECT i.invoice_id, i.invoice_no, i.invoice_date, i.seller_id, i.client_id,
         i.total_net_worth, i.total_vat, i.total_gross_worth, i.status, i.updated_at
  FROM invoicing.invoices i, w WHERE i.updated_at >= w.last_loaded_ts
),
keys AS (
  SELECT s.invoice_id, invoicing_olap.ensure_date_key(s.invoice_date) AS invoice_date_key,
         ds.seller_key, dc.client_key,
         s.invoice_no, s.total_net_worth, s.total_vat, s.total_gross_worth, s.status, s.updated_at
  FROM src s
  JOIN invoicing_olap.dim_seller ds ON ds.seller_id=s.seller_id
  JOIN invoicing_olap.dim_client dc ON dc.client_id=s.client_id
)
INSERT INTO invoicing_olap.fact_invoice (invoice_id,invoice_no,invoice_date_key,seller_key,client_key,total_net,total_vat,total_gross,status,updated_at)
SELECT invoice_id,invoice_no,invoice_date_key,seller_key,client_key,total_net_worth,total_vat,total_gross_worth,status,updated_at
FROM keys
ON CONFLICT (invoice_id) DO UPDATE SET total_gross=EXCLUDED.total_gross, status=EXCLUDED.status, updated_at=EXCLUDED.updated_at;

UPDATE invoicing_olap.etl_watermarks SET last_loaded_ts = NOW() WHERE stream_name='invoices';

-- Append invoice_items and payments similarly