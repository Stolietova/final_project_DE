import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

path_raw = "/opt/airflow/data/raw/user_profiles"
path_bronze = "/opt/airflow/data/bronze/user_profiles"
path_silver = "/opt/airflow/data/silver/user_profiles"

def get_spark():
    return SparkSession.builder \
        .appName("process_user_profiles") \
        .master("local[*]") \
        .getOrCreate()

def raw_to_bronze():
    spark = get_spark()
    print("--- Starting Raw to Bronze ---")
    
    df = spark.read \
        .option("recursiveFileLookup", "true") \
        .json(path_raw)    
    
    df.write.mode("overwrite").parquet(path_bronze)
    
    print(f"Successfully saved Bronze data to {path_bronze}")
    spark.stop()

def bronze_to_silver():
    spark = get_spark()
    print("--- Starting Bronze to Silver ---")

    df = spark.read.parquet(path_bronze)
    df.write.mode("overwrite").parquet(path_silver)

    print(f"Successfully saved Silver data to {path_silver}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python process_user_profiles.py [bronze|silver]")
        sys.exit(1)
        
    action = sys.argv[1]
    if action == "bronze":
        raw_to_bronze()
    elif action == "silver":
        bronze_to_silver()
    else:
        print(f"Unknown action: {action}")
        sys.exit(1)