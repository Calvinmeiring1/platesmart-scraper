import os, re, json, time, asyncio
from typing import List, Optional, Tuple, Dict, Any
from google.cloud import firestore
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

REGIONS = os.getenv("REGIONS", "ZA-WC-CT").split(",")
STORE = os.getenv("STORE", "CHECKERS")
INGREDIENTS_JSON = os.getenv("INGREDIENTS_JSON", '["cheddar cheese","milk","beef mince"]')
PAUSE_MS = 1000
SEARCH_URL = "https://www.checkers.co.za/search/all?q={}"

# ---------- Firestore ----------
def fs() -> firestore.Client:
    return firestore.Client()

def write_price(db: firestore.Client, region: str, store: str, ingredient: str, best: Dict[str, Any]):
    (db.collection("prices").document(region)
       .collection("stores").document(store)
       .collection("items").document(ingredient.lower())
       .set({
            "ingredient": ingredient.lower(),
            "store": store,
            "region": region,
            "productName": best["name"],
            "size": best.get("size_display"),
            "price": best["price"],
            "pricePerUnit": best["ppu"],
            "unitType": best["unit_type"],   # "kg" | "L" | "each"
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "url": best.get("url"),
            "imageUrl": best.get("image"),
       }, merge=True))

# ---------- helpers ----------
def normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def tokens(s: str) -> List[str]:
    return [t for t in re.split(r"[^\w]+", (s or "").lower()) if t]

def sim_words(a: str, b: str) -> float:
    A, B = set(tokens(a)), set(tokens(b))
    return len(A & B) / len(A) if A else 0.0

_NUM = r"(\d+(?:\.\d+)?)"
PACK_RE   = re.compile(rf"{_NUM}\s*[x×]\s*{_NUM}\s*(g|kg|ml|l)\b", re.I)
SINGLE_RE = re.compile(rf"{_NUM}\s*(g|kg|ml|l)\b", re.I)

def parse_size(text: str) -> Tuple[float, str, str]:
    """
    Returns (qty_in_base, unit_type, display)
    unit_type is "kg", "L" or "each"; base is kg or L or 1
    """
    t = normalize((text or "").lower())

    m = PACK_RE.search(t)  # e.g. "6 x 125 g"
    if m:
        n  = float(m.group(1)); each = float(m.group(2)); u = m.group(3).lower()
        scalar   = 1.0 if u in ("kg","l") else 0.001
        unit_out = "L" if u in ("ml","l") else "kg"
        return (n * each * scalar, unit_out, f"{int(n) if n.is_integer() else n} x {each} {u}")

    m = SINGLE_RE.search(t)  # e.g. "500 g", "1.5 L"
    if m:
        v = float(m.group(1)); u = m.group(2).lower()
        scalar   = 1.0 if u in ("kg","l") else 0.001
        unit_out = "L" if u in ("ml","l") else "kg"
        return (v * scalar, unit_out, f"{v} {u}")

    return (1.0, "each", "each")

def ppu(price: float, qty: float, unit_type: str) -> float:
    return price if unit_type == "each" else price / max(qty, 1e-6)

def pick_best(ingredient: str, hits: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not hits: return None
    return sorted(hits, key=lambda h: (-sim_words(ingredient, h["name"]), h["ppu"]))[0]

# ---------- selector helpers based on your snippet ----------
# container like: <div class="product-card_container__4EAMJ ...">
CARD_SEL  = 'div[class*="product-card_container__"]'
# name like: <p class="product-card_product-name__8wxGT">...</p>
NAME_SELS = ['[class*="product-card_product-name__"]',
             '.product-card__name', '.product-title', '.product__name', '[itemprop="name"]']
# price like: <p class="price-display_price-text__... product-card_product-price___...">
# inside it: <span class="price-display_full__...">R84</span><span class="price-display_half__...">.99</span>
PRICE_FULL_SEL = '[class*="price-display_full__"]'
PRICE_HALF_SEL = '[class*="price-display_half__"]'
PRICE_FALLBACKS = ['.price', '.product__price', '.product-price', '[data-price]']
# sometimes size is separate; if not we parse from name
SIZE_SELS  = ['[class*="product-list__item-size"]', '.uom', '.product__size', '.product-size', '.size']

async def get_rendered_html(p, url: str, debug_key: str) -> str:
    browser = await p.chromium.launch(headless=True)
    page = await browser.new_page()
    try:
        await page.goto(url, timeout=60000)
        # try cookie / promo banners
        for sel in ["button:has-text('Accept')", "button:has-text('GOT IT')", "button:has-text('I agree')"]:
            try: await page.locator(sel).click(timeout=2500)
            except: pass
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=20000)
            await page.wait_for_load_state("networkidle", timeout=20000)
        except PWTimeout:
            pass
        await page.wait_for_timeout(1500)
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight/2)")
        await page.wait_for_timeout(800)
        html = await page.content()

        # debug dump if we cannot find cards
        soup = BeautifulSoup(html, "lxml")
        if not soup.select_one(CARD_SEL):
            os.makedirs("debug", exist_ok=True)
            with open(f"debug/{debug_key}.html", "w", encoding="utf-8") as f:
                f.write(html[:150_000])
            await page.screenshot(path=f"debug/{debug_key}.png", full_page=True)
            print(f"[DEBUG] wrote debug/{debug_key}.html and .png (no product-card containers found)")
        return html
    finally:
        await browser.close()

def sel_text(el, selectors: List[str]) -> str:
    for s in selectors:
        n = el.select_one(s)
        if n and n.get_text(strip=True):
            return n.get_text(" ", strip=True)
    return ""

def get_price_from_card(card) -> Optional[float]:
    # Prefer the "full" + "half" split (R and cents separated)
    full = card.select_one(PRICE_FULL_SEL)
    half = card.select_one(PRICE_HALF_SEL)
    if full:
        ft = full.get_text(strip=True)
        ht = half.get_text(strip=True) if half else ""
        txt = f"{ft}{ht}"  # e.g., "R84" + ".99" -> "R84.99"
        t = txt.replace(" ", "").replace(",", ".")
        m = re.search(r"(\d+(?:\.\d+)?)", t.lstrip("Rr"))
        if m: return float(m.group(1))
    # Fallbacks
    for s in PRICE_FALLBACKS:
        n = card.select_one(s)
        if n:
            raw = n.get_text(" ", strip=True)
            raw = raw.replace(",", ".")
            m = re.search(r"(\d+(?:\.\d+)?)", raw)
            if m: return float(m.group(1))
        # attribute fallback (data-price)
        n = card.select_one(s)
        if n and n.has_attr("data-price"):
            try: return float(n["data-price"])
            except: pass
    return None

async def search_checkers(p, ingredient: str) -> List[Dict[str, Any]]:
    url = SEARCH_URL.format(ingredient.replace(" ", "+"))
    debug_key = re.sub(r"[^\w]+", "_", ingredient.lower())[:40]
    html = await get_rendered_html(p, url, debug_key)
    soup = BeautifulSoup(html, "lxml")

    cards = soup.select(CARD_SEL)
    if not cards:
        # try some older fallbacks too
        alt_cards = soup.select(".product-card, .product-grid__item, .product, .product-tile")
        cards = alt_cards

    hits: List[Dict[str, Any]] = []
    for card in cards[:12]:
        name = sel_text(card, NAME_SELS)
        if not name:
            # sometimes the name is the image alt/title
            img = card.select_one("img[alt]")
            if img and img.get("alt"):
                name = img["alt"]
        name = normalize(name)
        price_val = get_price_from_card(card)
        if not name or price_val is None:
            continue

        # size: explicit element or parse from name itself
        size_text = sel_text(card, SIZE_SELS) or name
        qty, unit_type, display = parse_size(size_text)

        # link + image
        link = None
        a = card.select_one("a[href]")
        if a and a.get("href"):
            link = a["href"]
            if link.startswith("/"): link = f"https://www.checkers.co.za{link}"
        img = card.select_one("img[src], img[data-src]")
        img_url = img.get("src") or img.get("data-src") if img else None

        hits.append({
            "name": name,
            "price": price_val,
            "size_display": display,
            "ppu": ppu(price_val, qty, unit_type),
            "unit_type": unit_type,
            "url": link,
            "image": img_url
        })
    return hits

# ---------- main ----------
async def run() -> None:
    db = fs()
    try:
        ingredients: List[str] = json.loads(INGREDIENTS_JSON)
    except Exception:
        ingredients = ["cheddar cheese","milk","beef mince"]

    async with async_playwright() as p:
        for ing in ingredients:
            try:
                hits = await search_checkers(p, ing)
                best = pick_best(ing, hits)
                if not best:
                    print(f"[MISS] {ing}: no products parsed")
                else:
                    for region in REGIONS:
                        write_price(db, region, STORE, ing, best)
                    u = best["unit_type"]
                    print(f"[OK] {ing} → {best['name']} | R{best['price']:.2f} | {best['size_display']} | R/{u}: {best['ppu']:.2f}")
            except Exception as e:
                print(f"[ERROR] {ing}: {e}")
            await asyncio.sleep(PAUSE_MS / 1000)

if __name__ == "__main__":
    asyncio.run(run())
