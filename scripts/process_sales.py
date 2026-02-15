import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
from data_quality import check_silver_sales_quality

raw_path = "/opt/airflow/data/raw/sales/*/*" 
bronze_path = "/opt/airflow/data/bronze/sales"
silver_path = "/opt/airflow/data/silver/sales"

def get_spark():
    return SparkSession.builder \
        .appName("process_sales") \
        .master("local[*]") \
        .getOrCreate()

def raw_to_bronze():
    spark = get_spark()
    print("--- Starting Raw to Bronze ---")
    
    raw_schema = StructType([
        StructField("CustomerId", StringType(), True),
        StructField("PurchaseDate", StringType(), True),
        StructField("Product", StringType(), True),
        StructField("Price", StringType(), True)
    ])
    
    df = spark.read.csv(raw_path, header=True, schema=raw_schema)
    df.write.mode("overwrite").parquet(bronze_path)
    print(f"Successfully saved Bronze data to {bronze_path}")
    spark.stop()


def bronze_to_silver():
    spark = get_spark()
    print("--- Starting Bronze to Silver ---")
    
    df_bronze = spark.read.parquet(bronze_path)
    
    clean_input = F.trim(F.col("PurchaseDate"))
    flexible_date = F.coalesce(
        F.to_date(clean_input, "yyyy-M-d"),
        F.to_date(clean_input, "yyyy/M/d"),
        F.to_date(clean_input, "yyyy-MMM-dd") 
    )

    df_silver = df_bronze.select(
        F.trim(F.col("CustomerId")).cast(IntegerType()).alias("client_id"),
        flexible_date.alias("purchase_date"),
        F.trim(F.col("Product")).alias("product_name"),
        F.regexp_replace(F.col("Price"), r"[^0-9.]", "").cast(DecimalType(10, 2)).alias("price")
    )

    if check_silver_sales_quality(df_silver, "Sales_Silver"):
        print("Data quality check passed. Writing to Silver.")
        df_silver.write.mode("overwrite").partitionBy("purchase_date").parquet(silver_path)
        print(f"Successfully saved Bronze data to {silver_path}")
    else:
        print("Data quality check failed. Aborting write.")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python process_sales.py [bronze|silver]")
        sys.exit(1)
        
    action = sys.argv[1]
    if action == "bronze":
        raw_to_bronze()
    elif action == "silver":
        bronze_to_silver()
    else:
        print(f"Unknown action: {action}")
        sys.exit(1)