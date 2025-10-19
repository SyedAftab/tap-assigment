-- Create Seller
INSERT INTO sellers (seller_name, seller_address, seller_tax_id, iban)
VALUES ('Whitaker, Gray and Green', '39588 Thomas Brook Suite 147 Sullivanshire, NH 40856', '987-82-9377', 'GB50UPCM86772611595760');

-- Create Client
INSERT INTO clients (client_name, client_address, client_tax_id, iban)
VALUES ('Davidson, Smith and Gill', '3577 Michael Fields Marquezstad, IL 48510', '967-95-5221', 'GB50UPCM86772611595760');

-- Add Products
INSERT INTO products (product_name, product_description, unit_price, vat_rate)
VALUES 
('36" Round Marble Dining Table', 'Marble Lapis Mosaic Floral Inlay', 1490.00, 10),
('12" Marble Side Coffee Table', 'Lapis Peacock Floral Inlay', 248.78, 10),
('6x3 Oval Marble Dining Table', 'Marquetry White Top Decor', 7193.76, 10);

-- Create Invoice
INSERT INTO invoices (invoice_no, invoice_date, seller_id, client_id, total_net_worth, total_vat, total_gross_worth)
VALUES ('15686725', '2021-11-03', 1, 1, 85850.39, 8585.04, 94435.43);

-- Add Invoice Items
INSERT INTO invoice_items (invoice_id, product_id, item_qty, item_net_price, item_vat_rate)
VALUES
(1, 1, 1.00, 1490.00, 10),
(1, 2, 3.00, 248.78, 10),
(1, 3, 2.00, 7193.76, 10);

-- Add Payment
INSERT INTO payments (invoice_id, payment_method, payment_reference, amount, remarks)
VALUES (1, 'BANK_TRANSFER', 'TXN-20211103-889', 50000.00, 'Partial payment received');


-- Fetch full invoice with products and payment details
SELECT
    i.invoice_no,
    i.invoice_date,
    s.seller_name,
    c.client_name,
    p.product_name,
    ii.item_qty,
    ii.item_net_price,
    ii.item_gross_worth,
    pay.amount AS payment_amount,
    pay.payment_method,
    pay.payment_date
FROM invoices i
JOIN sellers s ON s.seller_id = i.seller_id
JOIN clients c ON c.client_id = i.client_id
JOIN invoice_items ii ON ii.invoice_id = i.invoice_id
JOIN products p ON p.product_id = ii.product_id
LEFT JOIN payments pay ON pay.invoice_id = i.invoice_id
WHERE i.invoice_no = '15686725';


-- Update invoice totals after payment
UPDATE invoices
SET total_gross_worth = 94435.43,
    status = 'PARTIALLY_PAID'
WHERE invoice_no = '15686725';

-- Update product price
UPDATE products
SET unit_price = 1550.00
WHERE product_name = '36" Round Marble Dining Table';


-- Delete an invoice (cascades to items and payments)
DELETE FROM invoices WHERE invoice_no = '15686725';

-- Delete a product (will fail if referenced in invoice_items)
DELETE FROM products WHERE product_id = 1;