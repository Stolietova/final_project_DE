import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType

path_raw = "/opt/airflow/data/raw/customers/*/*"
path_bronze = "/opt/airflow/data/bronze/customers"
path_silver = "/opt/airflow/data/silver/customers"

def get_spark():
    return SparkSession.builder \
        .appName("process_sales") \
        .master("local[*]") \
        .getOrCreate()

def raw_to_bronze():
    spark = get_spark()
    print("--- Starting Raw to Bronze ---")

    raw_schema = StructType([
        StructField("client_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("registration_date", DateType(), True),
        StructField("state", StringType(), True)
    ])
    
    df = spark.read.csv(path_raw, header=True, schema=raw_schema)
    df.write.mode("overwrite").parquet(path_bronze)
    print(f"Successfully saved Bronze data to {path_bronze}")
    spark.stop()


def bronze_to_silver():
    spark = get_spark()
    print("--- Starting Bronze to Silver ---")
    
    df_new = spark.read.parquet(path_bronze).dropDuplicates(["client_id"])
    
    if not os.path.exists(path_silver):
        print("Silver table doesn't exist. Creating new one...")
        df_new.write.mode("overwrite").parquet(path_silver)
    else:
        print("Performing logical MERGE...")
        df_old = spark.read.parquet(path_silver)

        df_old_filtered = df_old.join(df_new, on="client_id", how="left_anti")
        df_final = df_old_filtered.unionByName(df_new)
        
        df_final.write.mode("overwrite").parquet(path_silver)
        print(f"Successfully MERGED data into {path_silver}")
   
    print(f"Successfully saved Silver data to {path_silver}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python process_customers.py [bronze|silver]")
        sys.exit(1)
        
    action = sys.argv[1]
    if action == "bronze":
        raw_to_bronze()
    elif action == "silver":
        bronze_to_silver()
    else:
        print(f"Unknown action: {action}")
        sys.exit(1)