import asyncio
import aiohttp
import json
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

CONCURRENT_REQUESTS = 3
TIMEOUT = 30
BATCH_SIZE = 20

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://www.palmonas.com/"
}


def get_latest_cleaned_file(base_path="Palmonas"):
    folders = sorted(os.listdir(base_path), reverse=True)

    for folder in folders:
        path = os.path.join(base_path, folder, "Products", "products_cleaned.json")
        if os.path.exists(path):
            return folder, path

    raise FileNotFoundError("❌ No cleaned data found")


# ✅ SAFE FETCH WITH RETRY
async def fetch(session, url):
    for attempt in range(3):
        try:
            async with session.get(url, headers=HEADERS, timeout=TIMEOUT) as response:

                if response.status != 200:
                    logging.warning(f"❌ Status {response.status}: {url}")
                    continue

                try:
                    return await response.json()
                except:
                    logging.warning(f"⚠️ Not JSON response: {url}")
                    return None

        except Exception as e:
            logging.warning(f"⚠️ Attempt {attempt+1} failed: {url}")

        await asyncio.sleep(1)

    return None


# ✅ SAFE PRODUCT PROCESSING
async def process_product(session, sem, category, url):
    async with sem:
        try:
            api_url = url.rstrip("/") + ".json"

            data = await fetch(session, api_url)

            if not data or "product" not in data:
                return None, url

            product = data["product"]

            # ✅ Safe variant handling
            variants = product.get("variants") or []
            variant = variants[0] if variants else {}

            try:
                price = float(variant.get("price") or 0)
                mrp = float(variant.get("compare_at_price") or 0)
            except:
                price, mrp = 0, 0

            discount = round(((mrp - price) / mrp) * 100, 2) if mrp else 0

            # ✅ Image extraction
            images = [
                img.get("src")
                for img in product.get("images", [])
                if img.get("src")
            ]

            return {
                "category": category,
                "product_id": product.get("id"),
                "title": product.get("title"),
                "handle": product.get("handle"),
                "vendor": product.get("vendor"),
                "product_type": product.get("product_type"),
                "tags": product.get("tags"),

                "price": price,
                "mrp": mrp,
                "discount_percent": discount,

                "sku": variant.get("sku"),
                "available": variant.get("available"),

                "image_count": len(images),
                "images": images,

            }, None

        except Exception as e:
            logging.error(f"💥 Error processing {url}: {e}")
            return None, url


async def async_main():
    latest_date, input_path = get_latest_cleaned_file()

    output_path = os.path.join(
        "Palmonas", latest_date, "Products", "product_details.json"
    )

    logging.info(f"📥 Input: {input_path}")
    logging.info(f"📤 Output: {output_path}")

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    sem = asyncio.Semaphore(CONCURRENT_REQUESTS)

    all_results = []
    failed = []

    today = datetime.now().strftime("%Y-%m-%d")

    async with aiohttp.ClientSession(headers=HEADERS) as session:

        for category, urls in data.items():
            logging.info(f"🚀 {category}: {len(urls)} URLs")

            for i in range(0, len(urls), BATCH_SIZE):
                batch = urls[i:i + BATCH_SIZE]

                tasks = [
                    process_product(session, sem, category, url)
                    for url in batch
                ]

                # ✅ CRITICAL FIX: prevent crash
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for item in results:

                    # 🔥 Handle exception safely
                    if isinstance(item, Exception):
                        logging.warning(f"⚠️ Task failed: {item}")
                        continue

                    res, fail = item

                    if res:
                        res["scrape_date"] = today
                        all_results.append(res)

                    if fail:
                        failed.append(fail)

                logging.info(f"✅ Done: {i + len(batch)} / {len(urls)}")

                await asyncio.sleep(2)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2, ensure_ascii=False)

    if failed:
        fail_path = os.path.join(
            "Palmonas", latest_date, "Products", "failed_urls.txt"
        )
        with open(fail_path, "w") as f:
            f.write("\n".join(failed))

    logging.info(f"🎯 SUCCESS: {len(all_results)}")
    logging.info(f"⚠️ FAILED: {len(failed)}")


def run_step4():
    asyncio.run(async_main())