import asyncio
import json
import logging
from urllib.parse import urljoin
from playwright.async_api import async_playwright
from scripts.utils import build_path, ensure_dir, get_today_folder

logging.basicConfig(level=logging.INFO)

BASE_URL = "https://www.palmonas.com"


def is_valid_category(name, href):
    if not name or len(name.strip()) < 3:
        return False

    invalid = ["sale", "offer", "free", "gift", "combo", "view", "all"]
    return "/collections/" in href and not any(k in name.lower() for k in invalid)


async def scrape():
    today = get_today_folder()

    output_file = build_path("Palmonas", today, "Category", "category_urls.json")
    ensure_dir(output_file)

    results = {}
    seen = set()

    async with async_playwright() as p:

        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled"
            ]
        )

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        )

        page = await context.new_page()

        # 🔥 Retry logic
        for attempt in range(3):
            try:
                logging.info(f"Loading page attempt {attempt+1}")
                await page.goto(BASE_URL, timeout=60000, wait_until="domcontentloaded")
                break
            except Exception as e:
                logging.warning(f"Retry failed: {e}")
                await asyncio.sleep(3)

        # 🔥 WAIT MORE (IMPORTANT)
        await page.wait_for_timeout(5000)

        elements = await page.locator("a[href*='/collections/']").all()

        logging.info(f"Total elements found: {len(elements)}")

        for el in elements:
            try:
                name = (await el.inner_text()).strip()
                href = await el.get_attribute("href")

                if not name or not href:
                    continue

                href = urljoin(BASE_URL, href).split("?")[0]

                handle = href.split("/collections/")[-1]

                if not is_valid_category(name, href):
                    continue

                if handle in seen:
                    continue

                seen.add(handle)
                results[name] = href

            except Exception as e:
                logging.warning(f"Element error: {e}")

        await browser.close()

    # 🔥 DO NOT CRASH DAG
    if len(results) == 0:
        logging.warning("⚠️ No categories found — but not failing")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    logging.info(f"✅ Categories saved: {len(results)}")
    logging.info(f"📁 File: {output_file}")


def run_step1():
    asyncio.run(scrape())