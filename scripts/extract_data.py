# extract_data.py
# Reads raw CSV into Spark and writes Bronze parquet (raw persisted)
import sys
from pyspark.sql import SparkSession

def start_spark(app_name="retail_extract"):
    spark = SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()
    return spark

def extract(input_csv, bronze_path):
    spark = start_spark("retail_extract")
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_csv)
    # basic write to bronze (raw)
    df.write.mode("overwrite").parquet(bronze_path)
    print(f"Wrote bronze parquet to: {bronze_path}")
    spark.stop()

if __name__ == '__main__':
    csv_path = sys.argv[1] if len(sys.argv)>1 else '../data/retail_sales_dataset.csv'
    bronze = sys.argv[2] if len(sys.argv)>2 else '../data/bronze/retail_bronze'
    extract(csv_path, bronze)
