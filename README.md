# Retail Sales Data Pipeline Using PySpark & SQL

This project demonstrates an end-to-end retail sales data pipeline implemented with **PySpark**.
The pipeline follows Bronze → Silver → Gold stages and includes a small demo of loading aggregated results into SQLite.

## Structure
{
    'data/': 'raw CSV + parquet outputs (bronze/silver/gold)',
    'scripts/': 'pipeline scripts (extract, transform, load, run_pipeline)',
    'notebooks/': 'analysis notebook (sample)'
}

## How to run (local)
1. Install requirements:
   pip install -r requirements.txt
2. Run the orchestrator (this will run extract, transform, load):
   python scripts/run_pipeline.py
3. After completion:
   - Bronze parquet: data/bronze/retail_bronze
   - Silver parquet: data/silver/retail_silver (partitioned by year/month)
   - Gold parquet: data/gold/retail_gold (aggregated)
   - SQLite DB: data/sqlite/retail_summary.db (table: sales_summary)

## Notes
- Requires Java (JDK 11+) for PySpark to run locally.
- The dataset used is synthetic and included in `data/retail_sales_dataset.csv`.
- You can push parquet outputs to a data lake or migrate the pipeline to Databricks with minimal changes.

