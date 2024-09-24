from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.time_delta import TimeDeltaSensor, TimeDeltaSensorAsync
from airflow.operators.python import PythonOperator

from utils import echo

dag1 = DAG(
    "time_delta_sensor",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 23 * * *",
    catchup=False,
)

# wait for log entry
wait_for_x_seconds = TimeDeltaSensor(
    task_id="time_delta_sensor",
    delta=timedelta(seconds=10),
    timeout=60 * 60 * 5,
    soft_fail=True,
    dag=dag1,
)

send_email_task = PythonOperator(
    task_id="send_email",
    python_callable=echo,
    op_args=["completed..."],
    dag=dag1,
)

wait_for_x_seconds >> send_email_task

dag2 = DAG(
    "time_delta_sensor_async",
    default_args={"start_date": days_ago(1)},
    schedule_interval="0 23 * * *",
    catchup=False,
)

# wait for log entry
wait_for_x_seconds_aync = TimeDeltaSensorAsync(
    task_id="time_delta_sensor_async",
    delta=timedelta(seconds=10),
    timeout=60 * 60 * 5,
    soft_fail=True,
    dag=dag2,
)

send_email_task_sync = PythonOperator(
    task_id="send_email_aync",
    python_callable=echo,
    op_args=["completed..."],
    dag=dag2,
)

wait_for_x_seconds_aync >> send_email_task_sync
