from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

dag = DAG(
    "s70_hook",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 21 * * *",
    catchup=False,
)

sql_stmt = """SELECT pg_attribute.attname AS pkey
FROM pg_index, pg_class, pg_attribute, pg_namespace 
WHERE 
  pg_class.oid = 'dag'::regclass AND 
  indrelid = pg_class.oid AND 
  nspname = 'public' AND 
  pg_class.relnamespace = pg_namespace.oid AND 
  pg_attribute.attrelid = pg_class.oid AND 
  pg_attribute.attnum = any(pg_index.indkey)
 AND indisprimary
"""

# Define the Task
load_pk_op = PostgresOperator(
    task_id="load_pk_op",
    sql=sql_stmt,
    postgres_conn_id="postgres_conn",
    dag=dag,
)


def get_pk(table_name="dag", schema_name="public"):
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    pk = pg_hook.get_table_primary_key(table=table_name, schema=schema_name)
    print(pk)
    return pk


load_pk_hk = PythonOperator(
    task_id="load_pk_hk",
    python_callable=get_pk,
    dag=dag,
)

load_pk_op >> load_pk_hk
