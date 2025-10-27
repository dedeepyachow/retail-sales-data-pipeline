# run_pipeline.py
# Orchestrates extract -> transform -> load in sequence
import subprocess
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
data_csv = BASE / 'data' / 'retail_sales_dataset.csv'
bronze = BASE / 'data' / 'bronze' / 'retail_bronze'
silver = BASE / 'data' / 'silver' / 'retail_silver'
gold = BASE / 'data' / 'gold' / 'retail_gold'
sqlite_db = BASE / 'data' / 'sqlite' / 'retail_summary.db'

def run():
    print('--- Running Extract step ---')
    subprocess.check_call([sys.executable, str(BASE / 'scripts' / 'extract_data.py'), str(data_csv), str(bronze)])
    print('--- Running Transform step ---')
    subprocess.check_call([sys.executable, str(BASE / 'scripts' / 'transform_data.py'), str(bronze), str(silver)])
    print('--- Running Load step ---')
    subprocess.check_call([sys.executable, str(BASE / 'scripts' / 'load_data.py'), str(silver), str(gold), str(sqlite_db)])
    print('--- Pipeline finished ---')

if __name__ == '__main__':
    run()
