import psycopg2
import json
import random
from decimal import Decimal
from datetime import datetime, timedelta

# ===============================
# PostgreSQL Connection Config
# ===============================
DB_CONFIG = {
    "dbname": "invoicing",
    "user": "syed",
    "password": "$yo01d2zedgloef",
    "host": "tap-assignments.cqncol5yfsyy.eu-west-1.rds.amazonaws.com",
    "port": "5432"
}

# ===============================
# Helper Functions
# ===============================

def get_or_create(cursor, table, unique_field, unique_value, insert_data):
    """Insert record if not exists, return id."""
    cursor.execute(f"SELECT {table[:-1]}_id FROM {table} WHERE {unique_field} = %s", (unique_value,))
    record = cursor.fetchone()
    if record:
        return record[0]
    columns = ', '.join(insert_data.keys())
    placeholders = ', '.join(['%s'] * len(insert_data))
    cursor.execute(f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) RETURNING {table[:-1]}_id",
                   list(insert_data.values()))
    return cursor.fetchone()[0]

def random_payment_method():
    return random.choice(['CASH', 'BANK_TRANSFER', 'CREDIT_CARD', 'ONLINE'])

# ===============================
# Main Script
# ===============================

def insert_invoices_from_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data_lines = f.readlines()

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for line in data_lines:
        try:
            invoice_data = json.loads(line.strip())
            header = invoice_data['gt_parse']['header']
            items = invoice_data['gt_parse']['items']
            summary = invoice_data['gt_parse']['summary']

            # --- Seller ---
            seller_id = get_or_create(
                cur,
                "sellers",
                "seller_tax_id",
                header['seller_tax_id'],
                {
                    "seller_name": header['seller'],
                    "seller_address": header['seller'],
                    "seller_tax_id": header['seller_tax_id'],
                    "iban": header['iban']
                }
            )

            # --- Client ---
            client_id = get_or_create(
                cur,
                "clients",
                "client_tax_id",
                header['client_tax_id'],
                {
                    "client_name": header['client'],
                    "client_address": header['client'],
                    "client_tax_id": header['client_tax_id'],
                    "iban": header['iban']
                }
            )

            # --- Invoice ---
            cur.execute("""
                INSERT INTO invoices (invoice_no, invoice_date, seller_id, client_id,
                                      total_net_worth, total_vat, total_gross_worth, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'UNPAID')
                ON CONFLICT (invoice_no) DO NOTHING
                RETURNING invoice_id
            """, (
                header['invoice_no'],
                datetime.strptime(header['invoice_date'], "%m/%d/%Y"),
                seller_id,
                client_id,
                Decimal(summary['total_net_worth'].replace('$', '').replace(',', '').strip()),
                Decimal(summary['total_vat'].replace('$', '').replace(',', '').strip()),
                Decimal(summary['total_gross_worth'].replace('$', '').replace(',', '').strip())
            ))

            row = cur.fetchone()
            if row:
                invoice_id = row[0]
            else:
                cur.execute("SELECT invoice_id FROM invoices WHERE invoice_no = %s", (header['invoice_no'],))
                invoice_id = cur.fetchone()[0]

            # --- Invoice Items ---
            for item in items:
                try:
                    product_id = get_or_create(
                        cur,
                        "products",
                        "product_name",
                        item['item_desc'],
                        {
                            "product_name": item['item_desc'],
                            "product_description": item['item_desc'],
                            "unit_price": Decimal(item['item_net_price'].replace(',', '.')),
                            "vat_rate": Decimal(item.get('item_vat', '10%').replace('%', '').strip())
                        }
                    )
                    item_vat_rate = Decimal(item.get('item_vat', '10%').replace('%', '').strip())
                    item_qty = Decimal(item['item_qty'].replace(',', '.'))
                    item_net_price = Decimal(item['item_net_price'].replace(',', '.'))
                    item_net_worth = item_qty*item_net_price
                    item_gross_worth = item_net_worth + (item_net_worth * item_vat_rate / 100)
                    cur.execute("""
                        INSERT INTO invoice_items (invoice_id, product_id, item_qty, item_net_price, item_vat_rate, item_net_worth, item_gross_worth)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        invoice_id,
                        product_id,
                        Decimal(item['item_qty'].replace(',', '.')),
                        Decimal(item['item_net_price'].replace(',', '.')),
                        Decimal(item.get('item_vat', '10%').replace('%', '').strip()),
                        item_net_worth,
                        item_gross_worth
                    ))
                except Exception as e:
                    print(f"Skipped item: {e}")

            # --- Generate Random Payment ---
            gross_total = Decimal(summary['total_gross_worth'].replace('$', '').replace(',', '').strip())
            paid_amount = gross_total if random.random() > 0.6 else gross_total * Decimal(random.uniform(0.4, 0.9))
            payment_status = 'PAID' if paid_amount >= gross_total else 'PARTIALLY_PAID'

            cur.execute("""
                INSERT INTO payments (invoice_id, payment_date, payment_method, payment_reference, amount, remarks)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                invoice_id,
                datetime.strptime(header['invoice_date'], "%m/%d/%Y") + timedelta(days=random.randint(1, 30)),
                random_payment_method(),
                f"TXN-{header['invoice_no']}-{random.randint(100,999)}",
                round(paid_amount, 2),
                "Auto-generated payment"
            ))

            # --- Update Invoice Status ---
            cur.execute("""
                UPDATE invoices SET status = %s WHERE invoice_id = %s
            """, (payment_status, invoice_id))

            print(f"Invoice {header['invoice_no']} inserted with payment: {payment_status}")

        except Exception as e:
            print(f"Error: {e}")
            conn.rollback()
        else:
            conn.commit()

    cur.close()
    conn.close()


if __name__ == "__main__":
    insert_invoices_from_json("ivoicing-data.txt")
