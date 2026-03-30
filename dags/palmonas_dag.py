from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from datetime import datetime, timedelta
import sys
import os

# ✅ Add project root (NOT scripts folder directly)
BASE_DIR = "/mnt/c/Users/SAHUSHREE/OneDrive/Vision Project"
sys.path.append(BASE_DIR)
from scripts.step1_get_category_urls import run_step1
from scripts.step2_get_products_links import run_step2
from scripts.step3_remove_duplicates import run_step3
from scripts.step4_product_data import run_step4
from scripts.load_to_mongodb import load_to_mongodb


default_args = {
    "owner": "sahu",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 21),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="palmonas_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["etl", "palmonas"],
) as dag:

    step1 = PythonOperator(
        task_id="extract_categories",
        python_callable=run_step1,
        execution_timeout=timedelta(minutes=10),
    )

    step2 = PythonOperator(
        task_id="extract_product_links",
        python_callable=run_step2,
        execution_timeout=timedelta(minutes=15),
    )

    step3 = PythonOperator(
        task_id="remove_duplicates",
        python_callable=run_step3,
        execution_timeout=timedelta(minutes=5),
    )

    step4 = PythonOperator(
    task_id="extract_product_details",
    python_callable=run_step4,
    execution_timeout=timedelta(minutes=30),
    )

    load_db = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb,
        execution_timeout=timedelta(minutes=10),
    )

    step1 >> step2 >> step3 >> step4 >> load_db