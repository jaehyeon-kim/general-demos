from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def raise_exception(msg):
    raise Exception(msg)


def echo(msg):
    print(msg)


dag = DAG(
    "s92_setup_teardown",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 21 * * *",
    catchup=False,
)

# Define tasks
create_cluster = PythonOperator(
    task_id="create_cluster",
    python_callable=echo,
    op_args=["Creating Cluster"],
    dag=dag,
)

run_query1 = PythonOperator(
    task_id="run_query1",
    python_callable=echo,
    op_args=["Running Query 1"],
    dag=dag,
)

run_query2 = PythonOperator(
    task_id="run_query2",
    python_callable=raise_exception,
    op_args=["Failure in Query 2"],
    dag=dag,
)


run_query3 = PythonOperator(
    task_id="run_query3",
    python_callable=echo,
    op_args=["Running Query 2"],
    dag=dag,
)

delete_cluster = PythonOperator(
    task_id="delete_cluster",
    python_callable=echo,
    op_args=["Deleting Cluster"],
    dag=dag,
)

# Define task dependencies
create_cluster >> [run_query1, run_query2]
[run_query1, run_query2] >> run_query3
run_query3 >> delete_cluster.as_teardown(setups=create_cluster)
