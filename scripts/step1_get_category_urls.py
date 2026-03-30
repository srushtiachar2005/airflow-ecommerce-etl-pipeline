import asyncio
import logging
import os
import json
from datetime import datetime
from urllib.parse import urljoin
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)

BASE_URL = "https://www.palmonas.com"


# ✅ VALID CATEGORY FILTER
def is_valid_category(name, href):

    if not name or len(name.strip()) < 3:
        return False

    name = name.lower()
    href = href.lower()

    # ❌ Remove junk / promo / navigation
    invalid_keywords = [
        "sale", "offer", "free", "gift", "combo",
        "view", "all", "new", "best",
        "|", "@", "₹", "%"
    ]

    if any(k in name for k in invalid_keywords):
        return False

    if any(k in href for k in invalid_keywords):
        return False

    # ❌ Must be proper collection
    if "/collections/" not in href:
        return False

    return True


async def scrape_categories():

    today = datetime.now().strftime("%Y-%m-%d")

    output_file = os.path.join(
        "Palmonas", today, "Category", "category_urls.json"
    )
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    results = {}
    seen_handles = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(BASE_URL, timeout=60000)
        await page.wait_for_load_state("networkidle")

        # ✅ Remove popup
        try:
            await page.evaluate("""
                let popup = document.querySelector('iframe, [id*="popup"]');
                if (popup) popup.remove();
            """)
        except:
            pass

        await page.wait_for_timeout(2000)

        elements = await page.locator("a[href*='/collections/']").all()

        logging.info(f"🔎 Raw links found: {len(elements)}")

        for el in elements:
            try:
                name = (await el.inner_text()).strip()
                href = await el.get_attribute("href")

                if not href:
                    continue

                href = urljoin(BASE_URL, href)
                href = href.split("?")[0]

                # ✅ Extract handle
                handle = href.split("/collections/")[-1]

                # ❌ Skip invalid
                if not is_valid_category(name, href):
                    continue

                # ❌ Skip duplicates (same collection)
                if handle in seen_handles:
                    continue

                seen_handles.add(handle)

                results[name] = href

            except:
                continue

        await browser.close()

    # ✅ FINAL SAFETY CHECK
    if len(results) < 5:
        raise Exception("❌ Too few valid categories extracted")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    logging.info(f"✅ Clean categories saved: {len(results)}")


def run_step1():
    asyncio.run(scrape_categories())