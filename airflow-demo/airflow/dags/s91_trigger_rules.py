from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def raise_exception(msg):
    raise Exception(msg)


def echo(msg):
    print(msg)


dag = DAG(
    "s91_trigger_rules",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 21 * * *",
    catchup=False,
)

# Define tasks
task_a = PythonOperator(
    task_id="task_a",
    python_callable=raise_exception,
    op_args=["Failure in Query a"],
    dag=dag,
)

task_b = PythonOperator(
    task_id="task_b",
    python_callable=raise_exception,
    op_args=["Failure in Query b"],
    dag=dag,
)

task_c = PythonOperator(
    task_id="task_c",
    python_callable=raise_exception,
    op_args=["Failure in Query b"],
    dag=dag,
)

task_d = PythonOperator(
    task_id="task_d",
    python_callable=echo,
    op_args=["Executing Task D"],
    dag=dag,
    trigger_rule="all_failed",
)

task_e = PythonOperator(
    task_id="task_e",
    python_callable=echo,
    op_args=["Executing Task E"],
    dag=dag,
)

# Define task dependencies
task_a >> task_d
task_b >> task_d
task_c >> task_d
task_d >> task_e
