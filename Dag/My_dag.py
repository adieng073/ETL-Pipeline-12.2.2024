from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import extract
import transform
import load
import os
from dotenv import load_dotenv

load_dotenv()

email = os.getenv("EMAIL")
owner_name = os.getenv("NAME")

def_args = {
    'owner': owner_name,
    'depends_on_past': False,
    'email':email,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="ETL_dag_run",
    description="etl from source to redshift",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=def_args
) as dag:
    
    extract_data = PythonOperator(
        task_id="extract_data_task",
        python_callable=extract.extract
    )
    
    transform_data = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform.transform
    )
    
    load_data = PythonOperator(
        task_id="Load_data_Redshift",
        python_callable=load.load
    )
    
    extract_data >> transform_data >> load_data
    
    #encountering a symlink error
    # just learned that i might need a vc for this
    