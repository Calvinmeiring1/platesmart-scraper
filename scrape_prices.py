# scrape_prices.py
import os
import re
import json
import asyncio
from datetime import datetime, timezone

from google.cloud import firestore
from playwright.async_api import async_playwright

# --------- Config from env (set in workflow) ----------
REGIONS = os.getenv("REGIONS", "ZA-WC-CT")        # e.g. 'ZA-WC-CT'
STORE   = os.getenv("STORE", "CHECKERS")          # 'CHECKERS'
ING_JSON = os.getenv("INGREDIENTS_JSON", '["cheddar cheese","milk","beef mince"]')
INGREDIENTS = json.loads(ING_JSON)

# Where to dump debug if parser fails
DEBUG_DIR = os.path.join("debug")
os.makedirs(DEBUG_DIR, exist_ok=True)

# --------- Helpers -----------------------------------
def parse_price(text: str) -> float | None:
    """
    Pulls a number like R84.99 (or 84.99) from text.
    Returns float or None.
    """
    if not text:
        return None
    m = re.search(r"R?\s*([0-9]+(?:[.,][0-9]{2})?)", text, re.IGNORECASE)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))

def guess_size(name: str) -> str | None:
    """
    Extracts a rough size, e.g., '200 g', '500 g', '1 kg', '2 L', from product name.
    """
    if not name:
        return None
    m = re.search(r"(\b[0-9]+(?:\.[0-9]+)?\s*(?:g|kg|ml|l|L)\b)", name)
    return m.group(1) if m else None

async def accept_cookies(page):
    # Try a few common cookie buttons
    selectors = [
        'button:has-text("Accept")',
        'button:has-text("ACCEPT ALL")',
        'button:has-text("I Accept")',
        'button[aria-label*="Accept"]',
    ]
    for sel in selectors:
        try:
            btn = page.locator(sel)
            if await btn.first.is_visible():
                await btn.first.click(timeout=500)
                break
        except Exception:
            pass

async def search_checkers(pw, ingredient: str):
    """
    Returns dict with real values or None if not found:
      {
        "ingredient": ...,
        "productName": ...,
        "price": 84.99,
        "size": "200 g",
        "url": "https://www.sixty60.co.za/...."
      }
    """
    url = f"https://www.sixty60.co.za/search?search={ingredient.replace(' ', '%20')}"
    browser = await pw.chromium.launch(headless=True)
    context = await browser.new_context(
        # helps look less like a bot
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
    )
    page = await context.new_page()

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await accept_cookies(page)

        # Wait for any product card to show up
        cards = page.locator('div[class*="product-card_container"]')
        await cards.first.wait_for(state="visible", timeout=15000)

        # Take first reasonable product
        count = await cards.count()
        for i in range(min(count, 6)):  # look at the first few cards
            card = cards.nth(i)

            # Name
            name_el = card.locator('p[class*="product-card_product-name"]')
            name = (await name_el.text_content()) if await name_el.count() else None
            if not name:
                # last resort: any <p> inside card with decent length
                try:
                    name = await card.locator("p").nth(0).text_content()
                except Exception:
                    name = None

            # Price text usually inside price-display_* structure
            price_el = card.locator('p[class*="price-display_price-text"]')
            price_text = (await price_el.text_content()) if await price_el.count() else None
            # fallback: any text in card containing 'R'
            if not price_text:
                try:
                    price_text = await card.locator(":text-matches('R\\s*\\d+')").first.text_content()
                except Exception:
                    price_text = None

            price = parse_price(price_text or "")
            if not (name and price):
                # Skip cards with missing data
                continue

            size = guess_size(name)
            # Build product URL from current page + scroll target (best effort)
            product_url = page.url

            return {
                "ingredient": ingredient,
                "productName": name.strip(),
                "price": price,
                "size": size or "",
                "url": product_url,
            }

        # If we got here, parsing failed: dump debug
        html = await page.content()
        with open(os.path.join(DEBUG_DIR, f"{ingredient.replace(' ','_')}.html"), "w", encoding="utf-8") as f:
            f.write(html)
        await page.screenshot(path=os.path.join(DEBUG_DIR, f"{ingredient.replace(' ','_')}.png"), full_page=True)
        return None

    except Exception as e:
        # Keep a breadcrumb for this query
        try:
            with open(os.path.join(DEBUG_DIR, f"{ingredient.replace(' ','_')}_error.txt"), "w", encoding="utf-8") as f:
                f.write(repr(e))
        except Exception:
            pass
        return None
    finally:
        await context.close()
        await browser.close()

def write_to_firestore(client, region: str, store: str, item: dict):
    """
    Firestore path:
      prices/{region}/stores/{store}/items/{ingredient-lower}
    """
    ingredient_id = item["ingredient"].lower()
    doc_ref = (
        client.collection("prices")
        .document(region)
        .collection("stores")
        .document(store)
        .collection("items")
        .document(ingredient_id)
    )
    payload = {
        "ingredient": item["ingredient"],
        "productName": item["productName"],
        "price": item["price"],                # price of the pack on the site
        "size": item["size"],                  # best-effort pack size (e.g., '200 g')
        "store": store,
        "region": region,
        "url": item["url"],
        "updatedAt": datetime.now(timezone.utc),
    }
    doc_ref.set(payload, merge=True)

async def main():
    # Firebase setup: GOOGLE_APPLICATION_CREDENTIALS must be set
    db = firestore.Client()

    async with async_playwright() as pw:
        for ing in INGREDIENTS:
            print(f"Searching: {ing}")
            result = await search_checkers(pw, ing)
            if result is None:
                print(f"  ❌ No real result parsed for: {ing} (left unchanged). Check debug/ artifacts.")
                continue

            print(f"  ✅ {result['productName']} — R{result['price']:.2f} ({result['size']})")
            write_to_firestore(db, REGIONS, STORE, result)

if __name__ == "__main__":
    asyncio.run(main())
