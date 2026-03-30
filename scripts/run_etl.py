import logging

from step1_get_category_urls import run_step1
from step2_get_products_links import run_step2
from step3_remove_duplicates import run_step3
from step4_product_data import run_step4
from load_to_mongodb import load_to_mongodb

# 🔥 Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def run_etl():
    logging.info("🚀 Starting ETL Pipeline...")

    try:
        logging.info("🔹 Step 1: Extract Categories")
        run_step1()

        logging.info("🔹 Step 2: Extract Product Links")
        run_step2()

        logging.info("🔹 Step 3: Remove Duplicates")
        run_step3()

        logging.info("🔹 Step 4: Extract Product Details")
        run_step4()

        logging.info("🔹 Step 5: Load to MongoDB")
        load_to_mongodb()

        logging.info("🎯 ETL Pipeline Completed Successfully!")

    except Exception as e:
        logging.error(f"❌ ETL Pipeline Failed: {e}")
        raise e  # 🔥 important for Airflow


if __name__ == "__main__":
    run_etl()