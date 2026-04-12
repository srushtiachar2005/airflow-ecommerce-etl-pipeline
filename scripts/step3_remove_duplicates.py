import json
import os
import logging
from scripts.utils import build_path

logging.basicConfig(level=logging.INFO)


def run_step3():
    base = build_path("Palmonas")
    folders = sorted(os.listdir(base), reverse=True)

    for f in folders:
        path = os.path.join(base, f, "Products", "products.json")
        if os.path.exists(path):
            break

    output = os.path.join(base, f, "Products", "products_cleaned.json")

    with open(path) as file:
        data = json.load(file)

    seen = set()
    cleaned = {}

    for cat, urls in data.items():
        unique = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                unique.append(u)

        if unique:
            cleaned[cat] = unique

    with open(output, "w") as f:
        json.dump(cleaned, f, indent=2)

    logging.info("Step3 done")