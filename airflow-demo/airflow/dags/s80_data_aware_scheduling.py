from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

dag = DAG(
    "s80_data_aware_scheduling_producer",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 23 * * *",
)

download_task = BashOperator(
    task_id="data_producer_task",
    bash_command="curl -o xrate.csv https://data-api.ecb.europa.eu/service/data/EXR/M.USD.EUR.SP00.A?format=csvdata && sleep 5",
    cwd="/tmp",
    dag=dag,
    outlets=[Dataset("/tmp/xrate.csv")],
)

# Define the DAG
dag2 = DAG(
    "s80_data_aware_scheduling_consumer",
    default_args={"start_date": days_ago(1)},
    schedule=[Dataset("/tmp/xrate.csv")],
    #   We usually code cron-based (or time based) schedule_interval as below:
    #   schedule_interval="0 23 * * *",
)

cat_task = BashOperator(
    task_id="data_consumer_task",
    bash_command="cat /tmp/xrate.csv",
    cwd="/tmp",
    dag=dag,
)
