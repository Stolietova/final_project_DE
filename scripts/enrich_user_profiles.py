import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

path_customers_silver = "/opt/airflow/data/silver/customers"
path_profiles_silver = "/opt/airflow/data/silver/user_profiles"
path_gold = "/opt/airflow/data/gold/user_profiles"

def get_spark():
    return SparkSession.builder \
        .appName("enrich_user_profiles") \
        .master("local[*]") \
        .getOrCreate()

def enrich_data():
    spark = get_spark()
    print("--- Starting Silver to Gold ---")

    df_customers = spark.read.parquet(path_customers_silver)
    df_profiles = spark.read.parquet(path_profiles_silver)

    enriched_df = df_customers.join(df_profiles, on="email", how="left")

    temp_full_name = F.coalesce(
        F.col("full_name"),
        F.concat_ws(" ", df_customers["first_name"], df_customers["last_name"])
    )

    df_golden = enriched_df.select(
        F.col("client_id"),
        F.col("email"),
        F.split(temp_full_name, " ").getItem(0).alias("first_name"),
        F.split(temp_full_name, " ").getItem(1).alias("last_name"),
        F.coalesce(df_profiles["state"], df_customers["state"]).alias("state"),
        F.col("birth_date"),
        F.col("phone_number"),
        F.col("registration_date")
    )

    df_golden.write.mode("overwrite").parquet(path_gold)
    
    print(f"Successfully saved Gold data to {path_gold}")
    spark.stop()

if __name__ == "__main__":
    enrich_data()

