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