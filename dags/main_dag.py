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
    dag_id="process_sales_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # task_sales_raw_to_bronze = BashOperator(
    #     task_id="process_sales_raw_to_bronze",
    #     bash_command="python /opt/airflow/scripts/process_sales.py bronze"
    # )

    task_sales_bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command='python /opt/airflow/scripts/process_sales.py silver'
    )

    #task_sales_raw_to_bronze
    task_sales_bronze_to_silver