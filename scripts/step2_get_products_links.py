import requests
import json
import os
import logging
import asyncio
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def get_latest_category_file():
    base = "Palmonas"
    folders = sorted(os.listdir(base), reverse=True)

    for folder in folders:
        path = os.path.join(base, folder, "Category", "category_urls.json")
        if os.path.exists(path):
            return folder, path

    raise FileNotFoundError("❌ No category file found")


def extract_handle(url):
    return url.split("/collections/")[-1].split("?")[0]


# ✅ PLAYWRIGHT SCRAPER (SINGLE USE)
async def scrape_collection_page(page, url):
    product_links = set()

    try:
        await page.goto(url, timeout=60000)
        await page.wait_for_load_state("networkidle")

        for _ in range(5):
            await page.mouse.wheel(0, 3000)
            await page.wait_for_timeout(1000)

        elements = await page.locator("a[href*='/products/']").all()

        for el in elements:
            href = await el.get_attribute("href")

            if href and "/products/" in href:
                if href.startswith("/"):
                    href = "https://www.palmonas.com" + href

                href = href.split("?")[0]
                product_links.add(href)

    except Exception as e:
        logging.warning(f"⚠️ Playwright failed: {e}")

    return list(product_links)


def scrape_products():

    latest_date, category_path = get_latest_category_file()

    output_path = os.path.join("Palmonas", latest_date, "Products", "products.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(category_path, "r") as f:
        categories = json.load(f)

    final_data = {}
    global_seen = set()

    async def fallback_runner(categories):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            for category, url in categories.items():

                logging.info(f"🚀 {category}")

                collection_handle = extract_handle(url)
                urls = []
                page_no = 1

                # ✅ API scraping with retry
                while True:
                    api_url = f"https://www.palmonas.com/collections/{collection_handle}/products.json?page={page_no}"

                    try:
                        res = requests.get(api_url, headers=HEADERS, timeout=15)

                        if res.status_code != 200:
                            break

                        try:
                            data = res.json()
                        except:
                            break

                        products = data.get("products", [])

                        if not products:
                            break

                        for p in products:
                            product_handle = p.get("handle")

                            if not product_handle:
                                continue

                            product_url = f"https://www.palmonas.com/products/{product_handle}"

                            if product_url not in global_seen:
                                global_seen.add(product_url)
                                urls.append(product_url)

                        logging.info(f"📄 API Page {page_no}: {len(products)}")
                        page_no += 1

                    except Exception as e:
                        logging.warning(f"⚠️ API error: {e}")
                        break

                # 🔥 FALLBACK
                if not urls:
                    logging.warning(f"⚠️ Using fallback: {category}")

                    fallback_urls = await scrape_collection_page(page, url)

                    for u in fallback_urls:
                        if u not in global_seen:
                            global_seen.add(u)
                            urls.append(u)

                if urls:
                    final_data[category] = urls
                else:
                    logging.warning(f"❌ Skipped: {category}")

            await browser.close()

    # ✅ RUN ONCE (FIXED)
    asyncio.run(fallback_runner(categories))

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(final_data, f, indent=2)

    logging.info("🎯 Step2 completed (stable version)")


def run_step2():
    scrape_products()