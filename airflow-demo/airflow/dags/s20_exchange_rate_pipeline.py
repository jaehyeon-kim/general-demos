from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from clean_data import clean_data

# Define or Instantiate DAG
dag = DAG(
    "exchange_rate_etl",
    start_date=datetime(2023, 10, 1),
    # end_date=datetime(2023, 12, 31),
    schedule_interval="0 22 * * *",
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    catchup=False,
)

# Define or Instantiate Tasks
download_task = BashOperator(
    task_id="download_file",
    bash_command="curl -o xrate.csv https://data-api.ecb.europa.eu/service/data/EXR/M.USD.EUR.SP00.A?format=csvdata",
    cwd="/tmp",
    dag=dag,
)

clean_data_task = PythonOperator(
    task_id="clean_data",
    python_callable=clean_data,
    dag=dag,
)

send_email_task = BashOperator(
    task_id="send_email",
    bash_command="sleep 2; echo 'email sent...'",
    dag=dag,
)

# Define Task Dependencies
download_task >> clean_data_task >> send_email_task
