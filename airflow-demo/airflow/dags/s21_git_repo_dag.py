from airflow import DAG
from airflow.utils.dates import days_ago

# from airflow.providers.github.operators.github import GithubOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Define the DAG
dag = DAG(
    "git_repo_dag",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 21 * * *",
    catchup=False,
)

# Start Dummy Operator
start = EmptyOperator(task_id="start", dag=dag)

list_repo_tags = BashOperator(
    task_id="list_repo_tags",
    bash_command="sleep 4",
    dag=dag,
)

# End Dummy Operator
end = EmptyOperator(task_id="end", dag=dag)

# Define task dependencies
start >> list_repo_tags >> end
