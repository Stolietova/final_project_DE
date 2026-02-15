from pyspark.sql import functions as F


def check_silver_sales_quality(df, df_name):

    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    duplicate_count = df.count() - df.dropDuplicates().count()

    is_valid = True
    for col, count in null_counts.items():
        if count > 0:
            print(f"Column '{col}': found {count} NULL values")
            is_valid = False
        else:
            print(f"Column '{col}': No NULLs")
            
    if duplicate_count > 0:
        print(f"Warning: Found {duplicate_count} duplicate rows.")

    df.select(F.min("purchase_date"), F.max("purchase_date"), F.min("price"), F.max("price")).show()

    return is_valid