import sys
import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(BASE_DIR)

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "sahu",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def step1():
    from scripts.step1_get_category_urls import run_step1
    run_step1()

def step2():
    from scripts.step2_get_products_links import run_step2
    run_step2()

def step3():
    from scripts.step3_remove_duplicates import run_step3
    run_step3()

def step4():
    from scripts.step4_product_data import run_step4
    run_step4()

def step5():
    from scripts.load_to_mongodb import run_step5
    run_step5()


with DAG(
    dag_id="palmonas_etl_pipeline",
    start_date=datetime(2026, 3, 21),
    schedule="@daily",
    catchup=False,
) as dag:

    t1 = PythonOperator(task_id="step1", python_callable=step1)
    t2 = PythonOperator(task_id="step2", python_callable=step2)
    t3 = PythonOperator(task_id="step3", python_callable=step3)
    t4 = PythonOperator(task_id="step4", python_callable=step4)
    t5 = PythonOperator(task_id="step5", python_callable=step5)

    t1 >> t2 >> t3 >> t4 >> t5