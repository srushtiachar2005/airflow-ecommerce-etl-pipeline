import json
import os
from pymongo import MongoClient, errors
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)


# ✅ FIX: Get latest file dynamically
def get_latest_product_details_file(base_path="Palmonas"):
    folders = sorted(os.listdir(base_path), reverse=True)

    for folder in folders:
        path = os.path.join(base_path, folder, "Products", "product_details.json")
        if os.path.exists(path):
            return folder, path

    raise FileNotFoundError("❌ No product_details.json found")


def load_to_mongodb():
    load_dotenv()

    MONGO_URI = os.getenv("MONGO_URI")

    if not MONGO_URI:
        raise ValueError("❌ MONGO_URI not found")

    # ✅ SAFE CONNECTION
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
    except Exception as e:
        raise Exception(f"❌ MongoDB connection failed: {e}")

    db = client["palmonas_db"]
    collection = db["products"]

    # ✅ FIXED FILE PATH
    latest_date, file_path = get_latest_product_details_file()

    logging.info(f"📂 Using file: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        logging.warning("⚠️ No data to insert")
        return

    logging.info(f"📦 Loaded {len(data)} products")

    # ✅ SAFE INDEX
    try:
        collection.create_index(
            [("product_id", 1), ("scrape_date", 1)],
            unique=True
        )
    except:
        pass

    inserted = 0
    skipped = 0

    for item in data:
        # ❌ DON'T overwrite scrape_date
        # item["scrape_date"] = today

        try:
            collection.insert_one(item)
            inserted += 1

        except errors.DuplicateKeyError:
            skipped += 1

        except Exception as e:
            logging.warning(f"⚠️ Insert error: {e}")
            skipped += 1

    logging.info(f"✅ Inserted: {inserted}")
    logging.info(f"⚠️ Skipped: {skipped}")
    logging.info("🎯 MongoDB load completed!")


def run_step5():
    load_to_mongodb()


if __name__ == "__main__":
    load_to_mongodb()