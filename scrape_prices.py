import os
import json
import time
import asyncio
from typing import List, Optional
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from google.cloud import firestore

REGIONS = os.getenv("REGIONS", "ZA-WC-CT").split(",")
STORE = os.getenv("STORE", "CHECKERS")
INGREDIENTS_JSON = os.getenv(
    "INGREDIENTS_JSON",
    '["cheddar cheese","milk","beef mince"]'
)

PAUSE = 1.0  # wait between items to avoid hammering
SEARCH_URL = "https://www.checkers.co.za/search/all?q={}"

def fs() -> firestore.Client:
    return firestore.Client()

def write_price(
    db: firestore.Client,
    region: str,
    store: str,
    ingredient: str,
    product_name: str,
    size: str,
    price: float,
    price_per_unit: float,
    unit_type: str,
    url: Optional[str] = None
) -> None:
    doc = (
        db.collection("prices").document(region)
          .collection("stores").document(store)
          .collection("items").document(ingredient.lower())
    )
    doc.set(
        {
            "ingredient": ingredient.lower(),
            "store": store,
            "region": region,
            "productName": product_name,
            "size": size,
            "price": price,
            "pricePerUnit": price_per_unit,
            "unitType": unit_type,
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "url": url,
        },
        merge=True,
    )

async def search_checkers(playwright, ingredient: str) -> Optional[dict]:
    browser = await playwright.chromium.launch(headless=True)
    page = await browser.new_page()
    url = SEARCH_URL.format(ingredient.replace(" ", "+"))
    await page.goto(url, timeout=60000)
    await page.wait_for_selector("div.product-list__item", timeout=15000)

    html = await page.content()
    await browser.close()

    soup = BeautifulSoup(html, "lxml")
    cards = soup.select("div.product-list__item")
    if not cards:
        print(f"[WARN] {ingredient}: no results found")
        return None

    card = cards[0]
    name = card.select_one("a.product-list__item-name")
    price = card.select_one("span.now")
    size = card.select_one("div.product-list__item-size")

    if not name or not price:
        print(f"[WARN] {ingredient}: could not parse card")
        return None

    try:
        raw_price = price.get_text(strip=True).replace("R", "").replace(",", ".")
        price_val = float(raw_price)
    except Exception:
        return None

    size_text = size.get_text(strip=True) if size else "unit"
    unit_type = "each"
    qty = 1.0
    if "kg" in size_text.lower():
        unit_type = "kg"
        try:
            qty = float(size_text.lower().replace("kg", "").strip())
        except:
            qty = 1.0
    elif "g" in size_text.lower():
        unit_type = "kg"
        try:
            grams = float(size_text.lower().replace("g", "").strip())
            qty = grams / 1000.0
        except:
            qty = 0.5
    elif "ml" in size_text.lower():
        unit_type = "L"
        try:
            ml = float(size_text.lower().replace("ml", "").strip())
            qty = ml / 1000.0
        except:
            qty = 1.0
    elif "l" in size_text.lower():
        unit_type = "L"
        try:
            qty = float(size_text.lower().replace("l", "").strip())
        except:
            qty = 1.0

    ppu = price_val / qty if qty > 0 else price_val
    return {
        "product_name": name.get_text(strip=True),
        "size": size_text,
        "price": price_val,
        "price_per_unit": ppu,
        "unit_type": unit_type,
        "url": url,
    }

async def run() -> None:
    db = fs()
    try:
        ingredients: List[str] = json.loads(INGREDIENTS_JSON)
    except Exception:
        ingredients = ["cheddar cheese", "milk", "beef mince"]

    async with async_playwright() as p:
        for ing in ingredients:
            best = await search_checkers(p, ing)
            if best:
                for region in REGIONS:
                    write_price(db, region, STORE, ing, **best)
                print(f"[OK] {ing} → {best['product_name']} | R{best['price']} | {best['size']} | {best['unit_type']}/unit: {best['price_per_unit']:.2f}")
            else:
                print(f"[FAIL] {ing}: no valid product")
            time.sleep(PAUSE)

if __name__ == "__main__":
    asyncio.run(run())
