# load_data.py
# Reads Silver parquet, aggregates to Gold, and writes results (parquet + SQLite demo)
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg as _avg, col
import pandas as pd
import sqlite3
from pathlib import Path

def start_spark(app_name='retail_load'):
    spark = SparkSession.builder.appName(app_name).master('local[*]').getOrCreate()
    return spark

def load(silver_path, gold_path, sqlite_db):
    spark = start_spark('retail_load')
    df = spark.read.parquet(silver_path)

    # Aggregate: revenue by region and category monthly
    agg = df.groupBy('year','month','region','category').agg(
        _sum(col('total_amount')).alias('revenue'),
        _sum(col('quantity_sold')).alias('units_sold'),
        _avg(col('unit_price')).alias('avg_price')
    )

    # Write gold parquet partitioned by year/month
    agg.write.mode('overwrite').partitionBy('year','month').parquet(gold_path)
    print(f"Wrote gold parquet to: {gold_path}")

    # Also write a sample to SQLite for demo (convert to pandas)
    out_dir = Path(sqlite_db).parent
    out_dir.mkdir(parents=True, exist_ok=True)
    pandas_df = agg.toPandas()
    conn = sqlite3.connect(sqlite_db)
    pandas_df.to_sql('sales_summary', conn, if_exists='replace', index=False)
    conn.close()
    print(f"Wrote aggregated table to SQLite DB: {sqlite_db}")
    spark.stop()

if __name__ == '__main__':
    silver = sys.argv[1] if len(sys.argv)>1 else '../data/silver/retail_silver'
    gold = sys.argv[2] if len(sys.argv)>2 else '../data/gold/retail_gold'
    sqlite_db = sys.argv[3] if len(sys.argv)>3 else '../data/sqlite/retail_summary.db'
    load(silver, gold, sqlite_db)
