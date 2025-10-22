-- dim_date
INSERT INTO invoicing_olap.dim_date (
    date_key,
    "date",
    year,
    quarter,
    month,
    month_name,
    day,
    day_of_week,
    day_name,
    week_of_year
)
SELECT DISTINCT
    EXTRACT(YEAR FROM i.invoice_date)::INT * 10000 +
    EXTRACT(MONTH FROM i.invoice_date)::INT * 100 +
    EXTRACT(DAY FROM i.invoice_date)::INT AS date_key,
    DATE(i.invoice_date) AS "date",
    EXTRACT(YEAR FROM i.invoice_date)::INT AS year,
    EXTRACT(QUARTER FROM i.invoice_date)::INT AS quarter,
    EXTRACT(MONTH FROM i.invoice_date)::INT AS month,
    TO_CHAR(i.invoice_date, 'Month') AS month_name,
    EXTRACT(DAY FROM i.invoice_date)::INT AS day,
    EXTRACT(DOW FROM i.invoice_date)::INT AS day_of_week,
    TO_CHAR(i.invoice_date, 'Day') AS day_name,
    EXTRACT(WEEK FROM i.invoice_date)::INT AS week_of_year
FROM invoices i
ON CONFLICT (date_key)
DO NOTHING;

-- Upsert clients
WITH w AS (SELECT last_loaded_ts FROM invoicing_olap.etl_watermarks WHERE stream_name='clients')
INSERT INTO invoicing_olap.dim_client (client_id, client_name, client_addr, client_tax_id, iban, updated_at)
SELECT c.client_id, c.client_name, c.client_address, c.client_tax_id, c.iban, c.updated_at
FROM clients c, w WHERE c.updated_at >= w.last_loaded_ts
ON CONFLICT (client_id) DO UPDATE SET client_name=EXCLUDED.client_name, client_addr=EXCLUDED.client_addr,
client_tax_id=EXCLUDED.client_tax_id, iban=EXCLUDED.iban, updated_at=EXCLUDED.updated_at;

UPDATE invoicing_olap.etl_watermarks SET last_loaded_ts = NOW() WHERE stream_name='clients';

-- Upsert sellers
WITH w AS (SELECT last_loaded_ts FROM invoicing_olap.etl_watermarks WHERE stream_name='sellers')
INSERT INTO invoicing_olap.dim_seller (seller_id, seller_name, seller_addr, seller_tax_id, iban, updated_at)
SELECT s.seller_id, s.seller_name, s.seller_address, s.seller_tax_id, s.iban, s.updated_at
FROM sellers s, w WHERE s.updated_at >= w.last_loaded_ts
ON CONFLICT (seller_id) DO UPDATE SET seller_name=EXCLUDED.seller_name, seller_addr=EXCLUDED.seller_addr,
seller_tax_id=EXCLUDED.seller_tax_id, iban=EXCLUDED.iban, updated_at=EXCLUDED.updated_at;

UPDATE invoicing_olap.etl_watermarks SET last_loaded_ts = NOW() WHERE stream_name='sellers';

-- Upsert products
WITH w AS (SELECT last_loaded_ts FROM invoicing_olap.etl_watermarks WHERE stream_name='products')
INSERT INTO invoicing_olap.dim_product (product_id, product_name, product_desc, unit_price, vat_rate, updated_at)
SELECT p.product_id, p.product_name, p.product_description, p.unit_price, p.vat_rate, p.updated_at
FROM products p, w WHERE p.updated_at >= w.last_loaded_ts
ON CONFLICT (product_id) DO UPDATE SET product_name=EXCLUDED.product_name,
unit_price=EXCLUDED.unit_price, vat_rate=EXCLUDED.vat_rate, updated_at=EXCLUDED.updated_at;

UPDATE invoicing_olap.etl_watermarks SET last_loaded_ts = NOW() WHERE stream_name='products';