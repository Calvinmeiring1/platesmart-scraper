import os, re, json, time, asyncio
from typing import List, Optional, Tuple, Dict, Any
from google.cloud import firestore
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

REGIONS = os.getenv("REGIONS", "ZA-WC-CT").split(",")
STORE = os.getenv("STORE", "CHECKERS")
INGREDIENTS_JSON = os.getenv("INGREDIENTS_JSON", '["cheddar cheese","milk","beef mince"]')
TIMEOUT_MS = 20000
PAUSE = 1000  # ms between queries
SEARCH_URL = "https://www.checkers.co.za/search/all?q={q}"

def fs() -> firestore.Client:
    return firestore.Client()

def normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def parse_price_from_text(txt: str) -> Optional[float]:
    if not txt: return None
    t = txt.replace(" ", "")
    t = re.sub(r"\^(\d{2})", r".\1", t)
    t = re.sub(r"[^0-9R\.,]", "", t).lstrip("Rr").replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", t)
    return float(m.group(1)) if m else None

_NUM = r"(\d+(?:\.\d+)?)"
PACK_RE = re.compile(rf"{_NUM}\s*[x×]\s*{_NUM}\s*(g|kg|ml|l)\b", re.I)
SINGLE_RE = re.compile(rf"{_NUM}\s*(g|kg|ml|l)\b", re.I)

def parse_size(size_text: str) -> Tuple[float, str, str]:
    t = normalize_spaces((size_text or "").lower())
    m = PACK_RE.search(t)
    if m:
        n = float(m.group(1)); each = float(m.group(2)); u = m.group(3).lower()
        scalar = 1.0 if u in ("kg","l") else 0.001
        unit = "L" if u in ("ml","l") else "kg"
        return (n*each*scalar, unit, f"{int(n) if n.is_integer() else n} x {each} {u}")
    m = SINGLE_RE.search(t)
    if m:
        v = float(m.group(1)); u = m.group(2).lower()
        scalar = 1.0 if u in ("kg","l") else 0.001
        unit = "L" if u in ("ml","l") else "kg"
        return (v*scalar, unit, f"{v} {u}")
    return (1.0, "each", "each")

def price_per_unit(price: float, qty: float, unit: str) -> float:
    return price if unit == "each" else price / max(qty, 1e-6)

def tokens(s: str) -> List[str]:
    return [t for t in re.split(r"[^\w]+", (s or "").lower()) if t]

def sim_words(a: str, b: str) -> float:
    A, B = set(tokens(a)), set(tokens(b))
    return (len(A & B) / len(A)) if A else 0.0

def pick_best(ingredient: str, hits: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not hits: return None
    return sorted(hits, key=lambda h: (-sim_words(ingredient, h["name"]), h["ppu"]))[0]

def write_price(db: firestore.Client, region: str, store: str, ingredient: str, best: Dict[str, Any]):
    doc = (db.collection("prices").document(region)
             .collection("stores").document(store)
             .collection("items").document(ingredient.lower()))
    doc.set({
        "ingredient": ingredient.lower(),
        "store": store,
        "region": region,
        "productName": best["name"],
        "size": best.get("size_display"),
        "price": best["price"],
        "pricePerUnit": best["ppu"],
        "unitType": best["unit_type"],
        "updatedAt": firestore.SERVER_TIMESTAMP,
        "url": best.get("url"),
        "imageUrl": best.get("image"),
    }, merge=True)

CARD_SELECTORS = [".product-card", ".product-tile", ".product", ".product-grid__item"]
NAME_SELECTORS = [".product-card__name", ".product-title", ".product__name", "[itemprop='name']"]
PRICE_SELECTORS = [".price", ".product__price", ".product-price", "[data-price]"]
SIZE_SELECTORS = [".uom", ".product__size", ".product-size", ".size"]

async def fetch_search_html(play, url: str) -> str:
    browser = await play.chromium.launch()
    page = await browser.new_page()
    await page.goto(url, timeout=TIMEOUT_MS)
    # wait for any possible product card container
    try:
        await page.wait_for_selector(",".join(CARD_SELECTORS), timeout=TIMEOUT_MS)
    except:
        pass
    html = await page.content()
    await browser.close()
    return html

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

async def search_checkers(ingredient: str) -> List[Dict[str, Any]]:
    async with async_playwright() as p:
        url = SEARCH_URL.format(q=requests.utils.quote(ingredient))
        html = await fetch_search_html(p, url)
    soup = BeautifulSoup(html, "lxml")

    cards = []
    for cs in CARD_SELECTORS:
        found = soup.select(cs)
        if found:
            cards = found
            break
    if not cards:
        return []

    hits: List[Dict[str, Any]] = []
    for el in cards[:12]:
        name = normalize_spaces(first_text(el, NAME_SELECTORS))
        price_txt = normalize_spaces(first_text(el, PRICE_SELECTORS))
        if not price_txt:
            price_txt = first_attr(el, PRICE_SELECTORS, "data-price") or ""
        price = parse_price_from_text(price_txt)
        if not name or price is None:
            continue
        size_txt = normalize_spaces(first_text(el, SIZE_SELECTORS)) or name
        qty, unit_type, display = parse_size(size_txt)
        ppu = price_per_unit(price, qty, unit_type)
        link = first_attr(el, ["a[href]"], "href")
        img  = first_attr(el, ["img[src]", "img[data-src]"], "src") or first_attr(el, ["img[data-src]"], "data-src")
        if link and link.startswith("/"):
            link = f"https://www.checkers.co.za{link}"
        hits.append({
            "name": name, "price": price, "size_display": display, "ppu": ppu,
            "unit_type": unit_type, "url": link, "image": img
        })
    return hits

async def run() -> None:
    db = fs()
    try:
        ingredients: List[str] = json.loads(INGREDIENTS_JSON)
    except Exception:
        ingredients = ["cheddar cheese","milk","beef mince"]

    for ing in ingredients:
        try:
            hits = await search_checkers(ing)
            best = pick_best(ing, hits)
            if not best:
                print(f"[CHECKERS] No result for: {ing}")
            else:
                for region in REGIONS:
                    write_price(db, region, "CHECKERS", ing, best)
                u = best["unit_type"]
                print(f"[CHECKERS] {ing} → {best['name']} | R{best['price']:.2f} | {best['size_display']} | R/{u}: {best['ppu']:.2f}")
        except Exception as e:
            print(f"[ERROR] {ing}: {e}")
        time.sleep(PAUSE/1000)

def main():
    asyncio.run(run())

if __name__ == "__main__":
    main()
