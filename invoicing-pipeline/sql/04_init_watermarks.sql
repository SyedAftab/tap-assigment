INSERT INTO invoicing_olap.etl_watermarks(stream_name,last_loaded_ts)
VALUES ('clients','1900-01-01'),('sellers','1900-01-01'),('products','1900-01-01'),('invoices','1900-01-01'),('invoice_items','1900-01-01'),('payments','1900-01-01')
ON CONFLICT (stream_name) DO NOTHING;