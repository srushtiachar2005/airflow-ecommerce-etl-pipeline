import json
import logging
import os
import requests
from scripts.utils import build_path, get_project_root, get_today_folder, ensure_dir

logging.basicConfig(level=logging.INFO)


def get_latest_category_file():
    base = build_path("Palmonas")
    folders = sorted(os.listdir(base), reverse=True)

    for f in folders:
        path = os.path.join(base, f, "Category", "category_urls.json")
        if os.path.exists(path):
            return f, path

    raise FileNotFoundError("No category file")


def run_step2():
    today, category_path = get_latest_category_file()

    output = build_path("Palmonas", today, "Products", "products.json")
    ensure_dir(output)

    with open(category_path) as f:
        categories = json.load(f)

    final = {}

    for name, url in categories.items():
        handle = url.split("/collections/")[-1]

        api = f"https://www.palmonas.com/collections/{handle}/products.json"
        res = requests.get(api)

        if res.status_code != 200:
            continue

        data = res.json().get("products", [])

        links = [
            f"https://www.palmonas.com/products/{p['handle']}"
            for p in data if p.get("handle")
        ]

        if links:
            final[name] = links

    with open(output, "w") as f:
        json.dump(final, f, indent=2)

    logging.info("Step2 completed")