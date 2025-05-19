from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

def excel_to_parquet():
    df = pd.read_excel("/c/Users/TRAVEEND/OneDrive - Volvo Cars/Data/myproject/excell/source.xlsx")
    df.to_parquet("/c/Users/TRAVEEND/OneDrive - Volvo Cars/Data/myproject/excell/source.parquet", index=False)
    print("Excel file converted to 'source.parquet'.")

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='excel_to_parquet_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['savetopaarquet'],
    description='A simple DAG to convert Excel to Parquet',
) as dag:

    convert_task = PythonOperator(
        task_id='convert_excel_to_parquet',
        python_callable=excel_to_parquet,
    )