from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import subprocess

from utils import echo

dag = DAG(
    "s60_xcom",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 23 * * *",
    catchup=False,
)


def fetch_xrate_fn(task_instance):
    xrate_filename = f'xrate_{datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.json'
    curl_command = [
        "curl",
        "https://data-api.ecb.europa.eu/service/data/EXR/M.USD.EUR.SP00.A?format=csvdata",
        "-o",
        f"/tmp/{xrate_filename}",
    ]
    subprocess.run(curl_command)
    task_instance.xcom_push(key="xrate_file", value=xrate_filename)


fetch_xrate = PythonOperator(
    task_id="fetch_xrate", python_callable=fetch_xrate_fn, provide_context=True, dag=dag
)

upload_to_s3 = PythonOperator(
    task_id="upload_to_s3",
    python_callable=echo,
    op_args=["{{ task_instance.xcom_pull(task_ids='fetch_xrate', key='xrate_file') }}"],
    dag=dag,
)

# Define Dependencies
fetch_xrate >> upload_to_s3
