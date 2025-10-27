# transform_data.py
# Reads Bronze parquet, applies cleaning and transformations, writes Silver parquet
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when, to_date

def start_spark(app_name="retail_transform"):
    spark = SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()
    return spark

def transform(bronze_path, silver_path):
    spark = start_spark("retail_transform")
    df = spark.read.parquet(bronze_path)

    # Example cleaning rules:
    # - Fill nulls for region/category/payment_method with 'UNKNOWN'
    df = df.withColumn('region', when(col('region').isNull(), 'UNKNOWN').otherwise(col('region')))
    df = df.withColumn('category', when(col('category').isNull(), 'UNKNOWN').otherwise(col('category')))
    df = df.withColumn('payment_method', when(col('payment_method').isNull(), 'UNKNOWN').otherwise(col('payment_method')))

    # Ensure numeric columns are cast correctly
    df = df.withColumn('quantity_sold', col('quantity_sold').cast('int'))
    df = df.withColumn('unit_price', col('unit_price').cast('double'))
    df = df.withColumn('discount_percent', col('discount_percent').cast('double'))
    df = df.withColumn('total_amount', col('total_amount').cast('double'))

    # Parse date column and derive year/month/day
    df = df.withColumn('date', to_date(col('date')))
    df = df.withColumn('year', expr('year(date)'))
    df = df.withColumn('month', expr('month(date)'))

    # Basic validation: remove rows with negative prices or quantities
    df = df.filter((col('unit_price') >= 0) & (col('quantity_sold') > 0))

    # Write silver (cleaned) parquet partitioned by year/month for efficient reads
    df.write.mode('overwrite').partitionBy('year','month').parquet(silver_path)
    print(f"Wrote silver parquet to: {silver_path}")
    spark.stop()

if __name__ == '__main__':
    bronze = sys.argv[1] if len(sys.argv)>1 else '../data/bronze/retail_bronze'
    silver = sys.argv[2] if len(sys.argv)>2 else '../data/silver/retail_silver'
    transform(bronze, silver)
