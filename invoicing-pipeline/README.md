# Invoicing OLTP → OLAP Data Pipeline

This project provides a **PostgreSQL-based OLTP → OLAP incremental ETL pipeline** for invoicing data using **Apache Airflow**.

---

### Folder Structure
```
project_root/
├── sql/                         # SQL scripts for OLAP schema and incremental upserts
│   ├── 01_create_olap_schema.sql
│   ├── 02_upsert_dimensions.sql
│   ├── 03_upsert_facts.sql
│   └── 04_init_watermarks.sql
├── dags/
│   └── invoice_elt_olap.py      # Airflow DAG performing incremental loads
└── README.md                    # This documentation
```

---

### Setup Instructions

#### 1️.Prerequisites
- **PostgreSQL** 14+.
- **Apache Airflow** 2.7+ with the **Postgres Provider** installed.

#### 2️.Create Schema
Create PostgreSQL schema:
```sql
CREATE SCHEMA invoicing_olap; -- OLAP target
```

#### 3️.Run Initial Schema Scripts
Execute these in order:
```bash
psql -d invoicing_olap -f sql/01_create_olap_schema.sql
psql -d invoicing_olap -f sql/04_init_watermarks.sql
```

#### 4️.Airflow Connection Setup
In the **Airflow UI → Admin → Connections**, create:

| Conn Id   | Type        | Host        | Schema          | Login | Password | Port | Description |
|------------|--------------|--------------|------------------|--------|-----------|-------|--------------|
| `olap_pg` | Postgres     | your-host    | invoicing_olap   | user   | pass      | 5432  | Target OLAP |

Ensure both connections are tested successfully.

#### 5️.Deploy the DAG
Copy the `invoice_elt_olap.py` file into Airflow’s DAGs directory:
```bash
cp dags/invoice_elt_olap.py /airflow/dags/
```
Then restart Airflow:
```bash
docker compose down
docker compose up -d
```
### Running Incremental Sync
1. Trigger the DAG manually from the Airflow UI.
2. The DAG will:
   - Load new or updated **clients**, **sellers**, **products** into dimensions.
   - Upsert **invoices** (with date + foreign key resolution).
   - Append new **invoice_items** and **payments**.
3. After first successful run, it updates `etl_watermarks` to maintain incremental sync.

### Testing Locally

#### 1️.Run OLTP Inserts
```sql
INSERT INTO invoicing.clients (client_name, client_address, client_tax_id, iban)
VALUES ('Demo Client', 'Test Address', 'TAX-001', 'IBAN-DEMO');
```
#### 2️.Trigger the DAG
```bash
airflow dags trigger invoice_elt_olap
```

#### 3️.Verify Results
```sql
-- In OLAP DB
SELECT * FROM invoicing_olap.dim_client ORDER BY updated_at DESC;
SELECT * FROM invoicing_olap.fact_invoice LIMIT 5;
```

#### 4️.Force Incremental Test
Update a client or product record in OLTP and re-trigger the DAG to confirm it updates in OLAP.


### Maintenance Tips
- Periodically vacuum/analyze OLAP tables for performance.
- Consider archiving old watermarks if ETL runs over a year.
- Add more granular logging by wrapping SQL tasks with Airflow XCom pushes.

### Future Enhancements
- Add **SCD Type 2** for product price/version tracking.
- Use **Airflow sensors** to detect new OLTP updates before ETL.

**Author:** Syed Aftab Alam 
**Purpose:** Real-time incremental load from transactional PostgreSQL (OLTP) → analytical PostgreSQL (OLAP) using Airflow.
