from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime


def print_welcome():
    print("Welcome to Airflow!")


def print_date():
    print("Today is {}".format(datetime.today().date()))


def print_goodbye():
    print("Goodbye from Airflow!")


dag = DAG(
    "welcome_dag",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 23 * * *",
    catchup=False,
)


print_welcome_task = PythonOperator(
    task_id="print_welcome", python_callable=print_welcome, dag=dag
)


print_date_task = PythonOperator(
    task_id="print_date", python_callable=print_date, dag=dag
)


print_goodbye_task = PythonOperator(
    task_id="print_goodbye", python_callable=print_goodbye, dag=dag
)


# Set the dependencies between the tasks

print_welcome_task >> print_date_task >> print_goodbye_task
