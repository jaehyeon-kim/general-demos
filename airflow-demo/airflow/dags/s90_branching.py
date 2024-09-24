from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago


# Function to get file size
def get_file_size(file_path, ti):
    # Get file size in bytes
    with open(file_path, "rb") as f:
        file_size_bytes = len(f.read())

    # Convert bytes to gigabytes
    file_size_gb = file_size_bytes / (1024**3)

    # Push file size to XCom
    ti.xcom_push(key="file_size", value=file_size_gb)


# Function to decide the branch
def decide_branch(ti):
    file_size = ti.xcom_pull(task_ids="check_file_size", key="file_size")
    return "parallel_transform" if file_size > 10 else "serial_transform"


# Serial transformation task
def serial_transform():
    # Add your serial transformation logic here
    print("Executing serial transformation")


# Serial load task
def serial_load():
    # Add your serial load logic here
    print("Executing serial load")


# Parallel transformation task
def parallel_transform():
    # Add your parallel transformation logic here
    print("Executing parallel transformation")


# Parallel load task
def parallel_load():
    # Add your parallel load logic here
    print("Executing parallel load")


dag = DAG(
    "s90_branching",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 23 * * *",
    catchup=False,
)

# Task to check file size
check_file_size = PythonOperator(
    task_id="check_file_size",
    python_callable=get_file_size,
    op_args=["/tmp/requirements.txt"],
    provide_context=True,
    dag=dag,
)

# Task to decide the branch
decide_branch_task = BranchPythonOperator(
    task_id="decide_branch",
    python_callable=decide_branch,
    provide_context=True,
    dag=dag,
)

# Define tasks for serial execution
serial_transform_task = PythonOperator(
    task_id="serial_transform",
    python_callable=serial_transform,
    dag=dag,
)

serial_load_task = PythonOperator(
    task_id="serial_load",
    python_callable=serial_load,
    dag=dag,
)

# Define tasks for parallel execution
parallel_transform_task = PythonOperator(
    task_id="parallel_transform",
    python_callable=parallel_transform,
    dag=dag,
)

parallel_load_task = PythonOperator(
    task_id="parallel_load",
    python_callable=parallel_load,
    dag=dag,
)

# Set up task dependencies
check_file_size >> decide_branch_task

# Serial branch
decide_branch_task >> serial_transform_task >> serial_load_task

# Parallel branch
decide_branch_task >> parallel_transform_task >> parallel_load_task
