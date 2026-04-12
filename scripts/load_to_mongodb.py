import json
import os
import logging
from pymongo import MongoClient
from scripts.utils import build_path

from pymongo import MongoClient
import os
import logging

logging.basicConfig(level=logging.INFO)

def get_mongo_client():
    uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017/")
    return MongoClient(uri)

def run_step5():
    client = get_mongo_client()
    db = client["palmonas_db"]
    col = db["products"]

    base = build_path("Palmonas")
    folders = sorted(os.listdir(base), reverse=True)

    for f in folders:
        path = os.path.join(base, f, "Products", "product_details.json")
        if os.path.exists(path):
            break

    with open(path) as file:
        data = json.load(file)

    if not data:
        logging.warning("No data")
        return

    col.insert_many(data, ordered=False)

    logging.info("Data inserted")