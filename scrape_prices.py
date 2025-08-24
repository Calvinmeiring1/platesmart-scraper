# scrape_prices.py
# Real scraper for Checkers search pages.
# - For each ingredient (e.g., "cheddar cheese"), fetches search results
# - Parses product name, price, size (e.g., "500 g" or "1 L" or "6 x 125 g")
# - Computes price-per-kg/L/each
# - Picks best match by (text similarity DESC, then cheapest per unit)
# - Writes to Firestore at: prices/{REGION}/stores/CHECKERS/items/{ingredient}

import os
import re
import time
import json
from typing import List, Optional, Tuple, Dict, Any
import requests
from bs4 import BeautifulSoup
from google.cloud import firestore

# ------------ CONFIG ------------
REGIONS = os.getenv("REGIONS", "ZA-WC-CT").split(",")
STORE = os.getenv("STORE", "CHECKERS")
INGREDIENTS_JSON = os.getenv(
    "INGREDIENTS_JSON",
    '["cheddar cheese","milk","beef mince"]'
)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36"
TIMEOUT = 20
PAUSE = 1.0           # 1 req/sec – be polite
MAX_ITEMS = 12        # consider top N tiles
SEARCH_URL = "https://www.checkers.co.za/search/all?q={q}"

# ------------ FIRESTORE ------------
def fs() -> firestore.Client:
    return firestore.Client()

def write_price(db: firestore.Client, region: str, store: str, ingredient: str, best: Dict[str, Any]) -> None:
    doc = (db.collection("prices").document(region)
             .collection("stores").document(store)
             .collection("items").document(ingredient.lower()))
    payload = {
        "ingredient": ingredient.lower(),
        "store": store,
        "region": region,
        "productName": best["name"],
        "size": best.get("size_display"),
        "price": best["price"],
        "pricePerUnit": best["ppu"],
        "unitType": best["unit_type"],  # "kg" | "L" | "each"
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "url": best.get("url"),
        "imageUrl": best.get("image"),
    }
    doc.set(payload, merge=True)

# ------------ TEXT + UNITS ------------
def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def tokens(s: str) -> List[str]:
    return [t for t in re.split(r"[^\w]+", (s or "").lower()) if t]

def sim_words(a: str, b: str) -> float:
    A, B = set(tokens(a)), set(tokens(b))
    if not A:
        return 0.0
    return len(A & B) / len(A)

_NUM = r"(\d+(?:\.\d+)?)"
PACK_RE = re.compile(rf"{_NUM}\s*[x×]\s*{_NUM}\s*(g|kg|ml|l)\b", re.I)
SINGLE_RE = re.compile(rf"{_NUM}\s*(g|kg|ml|l)\b", re.I)

def parse_size(size_text: str) -> Tuple[float, str, str]:
    """
    Returns (qty_in_base_units, unit_type, display):
      - unit_type: "kg", "L", or "each"
      - qty_in_base_units: e.g., 0.5 for 500 g; 1.5 for 1.5 L; 1 for each
    """
    t = normalize_spaces((size_text or "").lower())
    # Pack: "6 x 125 g", "2×400 g"
    m = PACK_RE.search(t)
    if m:
        n = float(m.group(1))
        each = float(m.group(2))
        u = m.group(3).lower()
        scalar = 1.0 if u in ("kg", "l") else 0.001
        unit_type = "L" if u in ("ml", "l") else "kg"
        return (n * each * scalar, unit_type, f"{int(n) if n.is_integer() else n} x {each} {u}")

    # Single: "500 g", "1.5 L"
    m = SINGLE_RE.search(t)
    if m:
        v = float(m.group(1))
        u = m.group(2).lower()
        scalar = 1.0 if u in ("kg", "l") else 0.001
        unit_type = "L" if u in ("ml", "l") else "kg"
        return (v * scalar, unit_type, f"{v} {u}")

    return (1.0, "each", "each")

def price_per_unit(price: float, qty: float, unit_type: str) -> float:
    return price if unit_type == "each" else price / max(qty, 1e-6)

def parse_price_from_text(txt: str) -> Optional[float]:
    """
    Accepts strings like "R 34.99", "R34^99", "34,99" → float.
    """
    if not txt:
        return None
    t = txt.replace(" ", "")
    # Convert weird cents separator like "^99" → ".99"
    t = re.sub(r"\^(\d{2})", r".\1", t)
    # Keep only digits, dot, comma, and 'R'
    t = re.sub(r"[^0-9R\.,]", "", t)
    # Strip leading R
    t = t.lstrip("Rr")
    # If comma used as decimal
    t = t.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    return float(m.group(1)) if m else None

# ------------ SCRAPE CHECKERS ------------
def fetch_html(url: str) -> str:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text

def search_checkers(query: str) -> List[Dict[str, Any]]:
    """
    Returns list of hits: {name, price, size_display, ppu, unit_type, url?, image?}
    Tries several fallback selectors because the site’s HTML can change.
    """
    url = SEARCH_URL.format(q=requests.utils.quote(query))
    html = fetch_html(url)
    soup = BeautifulSoup(html, "lxml")

    # possible container selectors for a product card
    card_selectors = [
        ".product-tile",                # legacy
        ".product",                     # alt
        ".product-grid__item",          # alt
        ".product-card",                # alt
        "[data-component='product-card']"
    ]

    # within a card, try these for each field
    name_selectors = [".product-title", ".product__name", ".product-card__name", "[itemprop='name']"]
    price_selectors = [".product-price", ".product__price", ".price", "[data-price]"]
    size_selectors = [".product-size", ".product__size", ".size", ".uom"]
    link_selectors = ["a[href]"]
    img_selectors  = ["img[src]", "img[data-src]"]

    def first_text(el, sels) -> str:
        for s in sels:
            n = el.select_one(s)
            if n and n.get_text(strip=True):
                return n.get_text(" ", strip=True)
        return ""

    def first_attr(el, sels, attr) -> Optional[str]:
        for s in sels:
            n = el.select_one(s)
            if n and n.get(attr):
                return n.get(attr)
        return None

    # collect all cards
    cards = []
    for cs in card_selectors:
        found = soup.select(cs)
        if found:
            cards = found
            break
    if not cards:
        return []

    hits: List[Dict[str, Any]] = []
    for el in cards[:MAX_ITEMS]:
        name = normalize_spaces(first_text(el, name_selectors))
        price_txt = normalize_spaces(first_text(el, price_selectors))
        if not price_txt:
            # some sites store price in data attributes
            price_attr = first_attr(el, price_selectors, "data-price")
            price_txt = price_attr or ""
        price = parse_price_from_text(price_txt)
        if not name or price is None:
            continue

        size_txt = normalize_spaces(first_text(el, size_selectors)) or name
        qty, unit_type, display = parse_size(size_txt)
        ppu = price_per_unit(price, qty, unit_type)

        link = first_attr(el, link_selectors, "href")
        img  = first_attr(el, img_selectors, "src") or first_attr(el, img_selectors, "data-src")

        # Build absolute link if needed
        if link and link.startswith("/"):
            link = f"https://www.checkers.co.za{link}"

        hits.append({
            "name": name,
            "price": price,
            "size_display": display,
            "ppu": ppu,
            "unit_type": unit_type,
            "url": link,
            "image": img
        })
    return hits

def pick_best(ingredient: str, hits: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not hits:
        return None
    # Higher similarity first, then cheapest ppu
    hits_sorted = sorted(
        hits,
        key=lambda h: (-sim_words(ingredient, h["name"]), h["ppu"])
    )
    return hits_sorted[0]

# ------------ MAIN ------------
def main() -> None:
    db = fs()
    try:
        ingredients: List[str] = json.loads(INGREDIENTS_JSON)
    except Exception:
        ingredients = ["cheddar cheese", "milk", "beef mince"]

    for ing in ingredients:
        try:
            hits = search_checkers(ing)
            best = pick_best(ing, hits)
            if not best:
                print(f"[CHECKERS] No result for: {ing}")
            else:
                for region in REGIONS:
                    write_price(db, region, "CHECKERS", ing, best)
                unit = best['unit_type']
                print(f"[CHECKERS] {ing} → {best['name']} | R{best['price']:.2f} | {best['size_display']} | R/{unit}: {best['ppu']:.2f}")
        except Exception as e:
            print(f"[ERROR] {ing}: {e}")
        time.sleep(PAUSE)

if __name__ == "__main__":
    main()
