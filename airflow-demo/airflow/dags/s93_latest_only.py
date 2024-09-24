from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.utils.dates import days_ago


def raise_exception(msg):
    raise Exception(msg)


def echo(msg):
    print(msg)


dag = DAG(
    "s93_latest_only",
    default_args={"start_date": days_ago(3)},
    schedule_interval="0 2 * * *",
    catchup=True,
)

# Define tasks
task_a = PythonOperator(
    task_id="task_a",
    python_callable=echo,
    op_args=["Executing Task A"],
    dag=dag,
)

task_b = PythonOperator(
    task_id="task_b",
    python_callable=echo,
    op_args=["Executing Task B"],
    dag=dag,
)

# Send email based on success
send_email_completed = PythonOperator(
    task_id="send_email_completed",
    python_callable=echo,
    op_args=["UK Sales Data Load - Successful"],
    dag=dag,
)

# Send email based on failure
send_email_failed = PythonOperator(
    task_id="send_email_failed",
    python_callable=echo,
    op_args=["UK Sales Data Load  - Failed"],
    dag=dag,
    trigger_rule="all_failed",
)


# Task for LatestOnlyOperator branch
latest_only = LatestOnlyOperator(task_id="latest_only", dag=dag)

# Connect tasks
task_a >> task_b >> send_email_failed
task_b >> latest_only >> send_email_completed
