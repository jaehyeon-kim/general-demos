from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from utils import echo

dag = DAG(
    "s30_varaible_example",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 21 * * *",
    catchup=False,
)


# Define the Tasks
fetch_exchange_rates = BashOperator(
    task_id="fetch_exchange_rates",
    bash_command="curl '{{ var.value.get('web_api_key') }}' -o xrate.json",
    cwd="/tmp",
    dag=dag,
)


send_email_task = PythonOperator(
    task_id="send_email",
    python_callable=echo,
    op_args=["{{ var.value.get('support_email') }}"],
    dag=dag,
)


# Define the Dependencies

fetch_exchange_rates >> send_email_task
