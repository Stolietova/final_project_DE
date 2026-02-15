from airflow import DAG
from airflow.operators.bash import BashOperator
from pendulum import timezone
from datetime import datetime, timedelta

KYIV_TIMEZONE = timezone('Europe/Kiev')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 22, tzinfo=KYIV_TIMEZONE),
    "retries": 1,
}

with DAG(
    dag_id="process_user_profiles_pipeline",
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,
) as dag:

    task_user_profiles_raw_to_bronze = BashOperator(
        task_id="user_profiles_raw_to_bronze",
        bash_command="python /opt/airflow/scripts/process_user_profiles.py bronze"
    )

    task_user_profiles_bronze_to_silver = BashOperator(
        task_id="user_profiles_bronze_to_silver",
        bash_command="python /opt/airflow/scripts/process_user_profiles.py silver"
    )

    task_user_profiles_raw_to_bronze >> task_user_profiles_bronze_to_silver