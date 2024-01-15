import logging
from pprint import pprint

import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

with DAG(
    dag_id="demo_etl",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Australia/Sydney"),
    catchup=False,
    tags=["pizza"],
):

    def update_records(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return "Whatever you return gets printed in the logs"

    task_records_update = PythonOperator(
        task_id="update_records", python_callable=update_records
    )

    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --profiles-dir /opt/airflow/dbt-profiles --project-dir /tmp/pizza_shop",
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --profiles-dir /opt/airflow/dbt-profiles --project-dir /tmp/pizza_shop",
    )

    task_records_update >> task_dbt_run >> task_dbt_test
