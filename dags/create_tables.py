import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {'owner': 'Dylan Bragdon'}

with DAG(
    dag_id = 'create_postgres_tables',
    start_date = days_ago(2),
    schedule_interval = None,
    default_args = default_args,
    catchup = False,
) as dag:
    create_post_table = PostgresOperator(
        task_id = 'create_tables',
        postgres_conn_id = 'hackernews_connect',
        sql = "sql/post_schema.sql"
    )
