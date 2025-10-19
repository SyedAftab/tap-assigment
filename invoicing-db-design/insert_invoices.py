import psycopg2
import json
from decimal import Decimal
from datetime import datetime

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

            # --- Insert Seller ---
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

            # --- Insert Client ---
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

            # --- Insert Invoice ---
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

            invoice_row = cur.fetchone()
            if invoice_row:
                invoice_id = invoice_row[0]
            else:
                cur.execute("SELECT invoice_id FROM invoices WHERE invoice_no = %s", (header['invoice_no'],))
                invoice_id = cur.fetchone()[0]

            # --- Insert Items ---
            for item in items:
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

            print(f"Inserted invoice {header['invoice_no']} successfully.")

        except Exception as e:
            print(f"Error processing line: {e}")
            conn.rollback()
        else:
            conn.commit()

    cur.close()
    conn.close()


if __name__ == "__main__":
    insert_invoices_from_json("ivoicing-data.txt")
