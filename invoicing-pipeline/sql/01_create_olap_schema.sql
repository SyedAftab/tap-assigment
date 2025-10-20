-- OLAP schema for invoicing_olap
CREATE SCHEMA IF NOT EXISTS invoicing_olap;
SET search_path TO invoicing_olap;

CREATE TABLE IF NOT EXISTS etl_watermarks (
  stream_name TEXT PRIMARY KEY,
  last_loaded_ts TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
  date_key INT PRIMARY KEY,
  "date" DATE NOT NULL UNIQUE,
  year INT, quarter INT, month INT, month_name TEXT, day INT,
  day_of_week INT, day_name TEXT, week_of_year INT
);

CREATE OR REPLACE FUNCTION ensure_date_key(d DATE)
RETURNS INT AS $$
DECLARE k INT := (EXTRACT(YEAR FROM d)::INT * 10000 + EXTRACT(MONTH FROM d)::INT * 100 + EXTRACT(DAY FROM d)::INT);
BEGIN
  INSERT INTO dim_date(date_key,"date",year,quarter,month,month_name,day,day_of_week,day_name,week_of_year)
  VALUES (k,d,EXTRACT(YEAR FROM d)::INT,EXTRACT(QUARTER FROM d)::INT,EXTRACT(MONTH FROM d)::INT,TO_CHAR(d,'Mon'),EXTRACT(DAY FROM d)::INT,EXTRACT(DOW FROM d)::INT+1,TO_CHAR(d,'Dy'),EXTRACT(WEEK FROM d)::INT)
  ON CONFLICT (date_key) DO NOTHING;
  RETURN k;
END; $$ LANGUAGE plpgsql;

CREATE TABLE dim_client (
  client_key BIGSERIAL PRIMARY KEY,
  client_id INT UNIQUE NOT NULL,
  client_name TEXT NOT NULL,
  client_addr TEXT,
  client_tax_id TEXT,
  iban TEXT,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_seller (
  seller_key BIGSERIAL PRIMARY KEY,
  seller_id INT UNIQUE NOT NULL,
  seller_name TEXT NOT NULL,
  seller_addr TEXT,
  seller_tax_id TEXT,
  iban TEXT,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE dim_product (
  product_key BIGSERIAL PRIMARY KEY,
  product_id INT UNIQUE NOT NULL,
  product_name TEXT NOT NULL,
  product_desc TEXT,
  unit_price NUMERIC(12,2),
  vat_rate NUMERIC(5,2),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fact_invoice (
  invoice_key BIGSERIAL PRIMARY KEY,
  invoice_id INT UNIQUE NOT NULL,
  invoice_no TEXT NOT NULL,
  invoice_date_key INT REFERENCES dim_date(date_key),
  seller_key BIGINT REFERENCES dim_seller(seller_key),
  client_key BIGINT REFERENCES dim_client(client_key),
  total_net NUMERIC(14,2), total_vat NUMERIC(14,2), total_gross NUMERIC(14,2),
  status TEXT, updated_at TIMESTAMP
);

CREATE TABLE fact_invoice_item (
  invoice_item_key BIGSERIAL PRIMARY KEY,
  invoice_id INT,
  product_key BIGINT REFERENCES dim_product(product_key),
  qty NUMERIC(10,2), net_price NUMERIC(12,2), net_worth NUMERIC(12,2),
  vat_rate NUMERIC(5,2), gross_worth NUMERIC(12,2), created_at TIMESTAMP
);

CREATE TABLE fact_payment (
  payment_key BIGSERIAL PRIMARY KEY,
  invoice_id INT,
  payment_date_key INT REFERENCES dim_date(date_key),
  amount NUMERIC(14,2), method TEXT, reference TEXT, created_at TIMESTAMP
);