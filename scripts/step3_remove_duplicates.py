import json
import os
import logging

logging.basicConfig(level=logging.INFO)


def get_latest_products_file(base_path="Palmonas"):
    folders = sorted(os.listdir(base_path), reverse=True)

    for folder in folders:
        path = os.path.join(base_path, folder, "Products", "products.json")
        if os.path.exists(path):
            return folder, path

    raise FileNotFoundError("❌ No products.json found")


def clean_products():
    latest_date, input_path = get_latest_products_file()

    output_path = os.path.join(
        "Palmonas", latest_date, "Products", "products_cleaned.json"
    )

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    global_seen = set()
    cleaned_data = {}

    for category, urls in data.items():

        if not urls:
            logging.warning(f"❌ Skipping empty category: {category}")
            continue

        unique_urls = []

        for url in urls:
            if not url or "/products/" not in url:
                continue

            if url not in global_seen:
                global_seen.add(url)
                unique_urls.append(url)

        if unique_urls:
            cleaned_data[category] = unique_urls

    if not cleaned_data:
        raise Exception("❌ No valid products after cleaning")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(cleaned_data, f, indent=2)

    logging.info(f"✅ Step3 completed: {len(cleaned_data)} categories")


def run_step3():
    clean_products()