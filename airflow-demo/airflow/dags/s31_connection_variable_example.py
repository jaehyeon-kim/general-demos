from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils import echo

dag = DAG(
    "s31_connection_variable_example",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 21 * * *",
    catchup=False,
)

sql_stmt = """SELECT * FROM dag
WHERE last_parsed_time BETWEEN '{{var.json.query_interval.start_date}}'::date AND '{{var.json.query_interval.end_date}}'::date
"""

# Define the Task
load_table = PostgresOperator(
    task_id="load_table",
    sql=sql_stmt,
    postgres_conn_id="postgres_conn",
    dag=dag,
)


send_email_task = PythonOperator(
    task_id="send_email",
    python_callable=echo,
    op_args=["{{ var.value.get('support_email') }}"],
    dag=dag,
)
