from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import timezone
from datetime import datetime, timedelta

KYIV_TIMEZONE = timezone('Europe/Kiev')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 22, tzinfo=KYIV_TIMEZONE),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="process_customers_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_customers_raw_to_bronze = BashOperator(
        task_id="customers_raw_to_bronze",
        bash_command="python /opt/airflow/scripts/process_customers.py bronze"
    )

    task_customers_bronze_to_silver = BashOperator(
        task_id="customers_bronze_to_silver",
        bash_command="python /opt/airflow/scripts/process_customers.py silver"
    )

    task_customers_raw_to_bronze >> task_customers_bronze_to_silver