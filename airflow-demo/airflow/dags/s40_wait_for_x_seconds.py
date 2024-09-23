import random
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator

from utils import echo


def wait_for():
    if random.random() > 0.5:
        return True
    else:
        return False


dag = DAG(
    "wait_for_x_seconds",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 23 * * *",
    catchup=False,
)

# wait for log entry
wait_for_x_seconds = PythonSensor(
    task_id="wait_for_x_seconds",
    python_callable=wait_for,
    poke_interval=5,
    mode="reschedule",  # poke vs reschedule, poke occupies a worker slot
    timeout=60 * 60 * 5,
    soft_fail=True,
    dag=dag,
)

send_email_task = PythonOperator(
    task_id="send_email",
    python_callable=echo,
    op_args=["{{ var.value.get('support_email') }}"],
    dag=dag,
)

wait_for_x_seconds >> send_email_task

# wait_for_file = S3KeySensor(
#     task_id="wait_for_s3_file",
#     bucket_name="sleekdata",
#     bucket_key="oms/employee_details.csv",
#     aws_conn_id="aws_conn",
#     poke_interval=60 * 10,
#     mode="reschedule",  # poke vs reschedule, poke occupies a worker slot
#     timeout=60 * 60 * 5,
#     soft_fail=True,
#     dag=dag,
# )

# Parameter       | Default Value              #
# ---------------------------------------------#
# poke_interval   | 60 Seconds                 #
# mode            | poke                       #
# timeout         | 7 days (60 * 60 * 24 * 7)  #
# soft_fail       | False                      #
