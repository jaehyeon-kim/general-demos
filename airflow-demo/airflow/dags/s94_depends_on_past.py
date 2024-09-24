from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def raise_exception(msg):
    raise Exception(msg)


def echo(msg):
    print(msg)


dag = DAG(
    "s94_depends_on_past",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 21 * * *",
    catchup=False,
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
    depends_on_past=True,
    dag=dag,
)

task_c = PythonOperator(
    task_id="task_c",
    python_callable=raise_exception,
    op_args=["Failure on Task C"],
    dag=dag,
)

task_d = PythonOperator(
    task_id="task_d",
    python_callable=echo,
    op_args=["Executing Task D"],
    depends_on_past=True,
    dag=dag,
)

task_e = PythonOperator(
    task_id="task_e",
    python_callable=echo,
    op_args=["Executing Task E"],
    depends_on_past=False,
    dag=dag,
)

# Connect tasks
task_a >> task_b >> task_c

task_c >> task_d
task_c >> task_e
