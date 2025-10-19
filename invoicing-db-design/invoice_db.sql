-- ==========================================================
-- PostgreSQL OLTP Schema: invoicing
-- Author: Syed Aftab Alam
-- Purpose: Full CRUD-enabled schema for invoice management
-- ==========================================================

-- ========================================
-- Table: Clients
-- ========================================
CREATE TABLE clients (
    client_id SERIAL PRIMARY KEY,
    client_name VARCHAR(255) NOT NULL,
    client_address TEXT NOT NULL,
    client_tax_id VARCHAR(30) UNIQUE NOT NULL,
    iban VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- Table: Sellers
-- ========================================
CREATE TABLE sellers (
    seller_id SERIAL PRIMARY KEY,
    seller_name VARCHAR(255) NOT NULL,
    seller_address TEXT NOT NULL,
    seller_tax_id VARCHAR(30) UNIQUE NOT NULL,
    iban VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- Table: Products
-- ========================================
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_description TEXT,
    unit_price NUMERIC(12,2) NOT NULL,
    vat_rate NUMERIC(5,2) DEFAULT 10.00,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- Table: Invoices
-- ========================================
CREATE TABLE invoices (
    invoice_id SERIAL PRIMARY KEY,
    invoice_no VARCHAR(50) UNIQUE NOT NULL,
    invoice_date DATE NOT NULL,
    seller_id INT NOT NULL REFERENCES sellers(seller_id) ON DELETE CASCADE,
    client_id INT NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
    total_net_worth NUMERIC(14,2) DEFAULT 0.00,
    total_vat NUMERIC(14,2) DEFAULT 0.00,
    total_gross_worth NUMERIC(14,2) DEFAULT 0.00,
    status VARCHAR(20) DEFAULT 'UNPAID',  -- UNPAID | PARTIALLY_PAID | PAID
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- Table: Invoice Items (Many-to-One → Invoices, Many-to-One → Products)
-- ========================================
CREATE TABLE invoice_items (
    item_id SERIAL PRIMARY KEY,
    invoice_id INT NOT NULL REFERENCES invoices(invoice_id) ON DELETE CASCADE,
    product_id INT NOT NULL REFERENCES products(product_id) ON DELETE RESTRICT,
    item_qty NUMERIC(10,2) NOT NULL CHECK (item_qty > 0),
    item_net_price NUMERIC(12,2) NOT NULL,
    item_net_worth NUMERIC(12,2),
    item_vat_rate NUMERIC(5,2) DEFAULT 10.00,
    item_gross_worth NUMERIC(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- Table: Payments (Many-to-One → Invoices)
-- ========================================
CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    invoice_id INT NOT NULL REFERENCES invoices(invoice_id) ON DELETE CASCADE,
    payment_date DATE NOT NULL DEFAULT CURRENT_DATE,
    payment_method VARCHAR(50) CHECK (payment_method IN ('CASH', 'BANK_TRANSFER', 'CREDIT_CARD', 'CHECK', 'ONLINE')),
    payment_reference VARCHAR(100),
    amount NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    remarks TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ========================================
-- Indexes
-- ========================================
CREATE INDEX idx_invoices_date ON invoices(invoice_date);
CREATE INDEX idx_invoice_items_invoice_id ON invoice_items(invoice_id);
CREATE INDEX idx_payments_invoice_id ON payments(invoice_id);
CREATE INDEX idx_products_name ON products(product_name);

-- ========================================
-- Trigger for auto-update timestamps
-- ========================================
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_clients
BEFORE UPDATE ON clients
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER trg_update_sellers
BEFORE UPDATE ON sellers
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER trg_update_products
BEFORE UPDATE ON products
FOR EACH ROW EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER trg_update_invoices
BEFORE UPDATE ON invoices
FOR EACH ROW EXECUTE FUNCTION update_timestamp();