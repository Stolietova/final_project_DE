from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession

def run_spark_test():
    # Використовуємо локальний режим, щоб обійти мережеві обмеження Docker
    spark = SparkSession.builder \
        .appName("Airflow-Local-Spark-Test") \
        .master("local[*]") \
        .getOrCreate()

    try:
        data = [("Ihor", 25), ("Kateryna", 30), ("Oleg", 35)]
        df = spark.createDataFrame(data, ["Name", "Age"])
        
        print("--- SUCCESS: SPARK IS WORKING ---")
        df.show()
        print(f"Total rows: {df.count()}")
    except Exception as e:
        print(f"Final error: {e}")
        raise
    finally:
        spark.stop()

with DAG(
    dag_id='spark_connection_test',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    test_task = PythonOperator(
        task_id='test_pyspark_task',
        python_callable=run_spark_test
    )