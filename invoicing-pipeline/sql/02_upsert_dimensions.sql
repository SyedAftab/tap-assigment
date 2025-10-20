-- Upsert clients
WITH w AS (SELECT last_loaded_ts FROM invoicing_olap.etl_watermarks WHERE stream_name='clients')
INSERT INTO invoicing_olap.dim_client (client_id, client_name, client_addr, client_tax_id, iban, updated_at)
SELECT c.client_id, c.client_name, c.client_address, c.client_tax_id, c.iban, c.updated_at
FROM invoicing.clients c, w WHERE c.updated_at >= w.last_loaded_ts
ON CONFLICT (client_id) DO UPDATE SET client_name=EXCLUDED.client_name, client_addr=EXCLUDED.client_addr,
client_tax_id=EXCLUDED.client_tax_id, iban=EXCLUDED.iban, updated_at=EXCLUDED.updated_at;

UPDATE invoicing_olap.etl_watermarks SET last_loaded_ts = NOW() WHERE stream_name='clients';

-- Repeat same pattern for sellers and products