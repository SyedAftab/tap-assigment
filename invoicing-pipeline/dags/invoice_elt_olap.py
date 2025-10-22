from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id='invoice_elt_olap',
    start_date=datetime(2025, 10, 18),
    schedule='*/5 * * * *',  # corrected: 'schedule' → 'schedule_interval'
    catchup=False,
    default_args=default_args,
    tags=['invoicing', 'oltp->olap']
) as dag:

    @task
    def run_sql(file_path: str):
        """Reads and executes SQL file on Postgres connection"""
        hook = PostgresHook(postgres_conn_id='olap_pg')
        with open(file_path, 'r') as f:
            sql = f.read()
        hook.run(sql)

    dim = run_sql('e:\data-science\airflow-docker\dags\sql\02_upsert_dimensions.sql')
    fact = run_sql('e:\data-science\airflow-docker\dags\sql\sql\03_upsert_facts.sql')

    dim >> fact
