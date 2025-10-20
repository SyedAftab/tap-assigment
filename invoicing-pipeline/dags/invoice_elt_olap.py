# ---------- dags/invoice_elt_olap.py ----------
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args={"owner":"data-eng","retries":1,"retry_delay":timedelta(minutes=2)}
with DAG(
    'invoice_elt_olap',
    start_date=datetime(2025,10,18),
    schedule_interval='*/5 * * * *',
    catchup=False,
    default_args=default_args,
    tags=['invoicing','oltp->olap']
) as dag:

    @task
def run_sql(file):
        sql=open(file).read()
        PostgresHook(postgres_conn_id='olap_pg').run(sql)

    dim=run_sql('/sql/02_upsert_dimensions.sql')
    fact=run_sql('/sql/03_upsert_facts.sql')
    dim >> fact