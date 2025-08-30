# add at top with others
from bs4 import BeautifulSoup

def _price_from_text(txt: str) -> float | None:
    if not txt:
        return None
    m = re.search(r"R?\s*([0-9]+(?:[.,][0-9]{2})?)", txt)
    return float(m.group(1).replace(",", ".")) if m else None

def _size_guess(name: str) -> str:
    m = re.search(r"\b(\d+(?:\.\d+)?\s*(?:g|kg|ml|l|L))\b", name or "")
    return m.group(1) if m else ""

def _best_hit(ingredient: str, hits: list[dict]) -> dict | None:
    if not hits:
        return None
    A = set(re.findall(r"\w+", ingredient.lower()))
    def score(h):
        B = set(re.findall(r"\w+", h["name"].lower()))
        overlap = len(A & B) / max(1, len(A))
        return (-overlap, h["price"])
    return sorted(hits, key=score)[0]

# ---------- checkers.co.za ----------
async def search_checkers_site(pw, ingredient: str) -> dict | None:
    # try both ?search= and ?Search=
    urls = [
        f"https://www.checkers.co.za/search/all?q={ingredient.replace(' ', '%20')}",
        f"https://www.checkers.co.za/search?search={ingredient.replace(' ', '%20')}",
        f"https://www.checkers.co.za/search?Search={ingredient.replace(' ', '%20')}",
    ]
    browser = await pw.chromium.launch(headless=True)
    ctx = await browser.new_context(
        viewport={"width": 1366, "height": 900},
        locale="en-ZA",
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"),
    )
    page = await ctx.new_page()
    try:
        for url in urls:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            except Exception:
                continue

            # small soft waits + light scroll to let lazy content render
            await page.wait_for_timeout(1500)
            for _ in range(2):
                await page.mouse.wheel(0, 1200)
                await page.wait_for_timeout(600)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Broad product tile selectors used on checkers web
            card_sel = [
                "[class*='product-list__item']",
                "[data-component='product-tile']",
                "[class*='product-grid__item']",
                "div.product, li.product",
            ]
            name_sel = [
                "[class*='item-name']",
                "[class*='product__name']",
                "[class*='product-title']",
                "[itemprop='name']",
                "img[alt]",
            ]
            price_sel = [
                "[class*='price']", ".price", ".now",
            ]

            cards = []
            for s in card_sel:
                cards.extend(soup.select(s))
            # de-dupe while preserving order
            cards = list(dict.fromkeys(cards))[:40]

            hits = []
            for c in cards:
                # name
                nm = None
                for ns in name_sel:
                    el = c.select_one(ns)
                    if el:
                        nm = el.get("alt") if el.name == "img" else el.get_text(" ", strip=True)
                        if nm:
                            break
                if not nm:
                    continue

                # price
                pr = None
                for ps in price_sel:
                    el = c.select_one(ps)
                    if el:
                        pr = _price_from_text(el.get_text(" ", strip=True))
                        if pr is not None:
                            break
                if pr is None:
                    pr = _price_from_text(c.get_text(" ", strip=True))
                if pr is None:
                    continue

                hits.append({
                    "name": nm.strip(),
                    "price": pr,
                    "size": _size_guess(nm),
                    "url": url,
                })

            best = _best_hit(ingredient, hits)
            if best:
                return best

        return None
    finally:
        await ctx.close()
        await browser.close()

# ---------- sixty60.co.za ----------
async def search_sixty60(pw, ingredient: str) -> dict | None:
    urls = [
        f"https://www.sixty60.co.za/search?search={ingredient.replace(' ', '%20')}",
        f"https://www.sixty60.co.za/search?Search={ingredient.replace(' ', '%20')}",
    ]
    browser = await pw.chromium.launch(headless=True)
    ctx = await browser.new_context(
        viewport={"width": 1366, "height": 900},
        locale="en-ZA",
        timezone_id="Africa/Johannesburg",
        geolocation={"latitude": -33.9249, "longitude": 18.4241},  # Cape Town
        permissions=["geolocation"],
        user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"),
        extra_http_headers={"Accept-Language": "en-ZA,en;q=0.9"},
    )
    page = await ctx.new_page()
    try:
        for url in urls:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            except Exception:
                continue

            # Dismiss cookie banners if present (best-effort)
            for sel in [
                "button:has-text('Accept')", "button:has-text('ACCEPT')",
                "button[aria-label*='Accept']"
            ]:
                try:
                    await page.locator(sel).first.click(timeout=800)
                    break
                except Exception:
                    pass

            # soft waits + light scroll
            await page.wait_for_timeout(1500)
            for _ in range(2):
                await page.mouse.wheel(0, 1200)
                await page.wait_for_timeout(600)

            html = await page.content()
            soup = BeautifulSoup(html, "lxml")

            # Sixty60 module class names – use contains()
            cards = soup.select('div[class*="product-card_container"]')
            hits = []
            for c in cards[:40]:
                # name
                nm_el = c.select_one('[class*="product-card_product-name"]')
                nm = nm_el.get_text(" ", strip=True) if nm_el else None
                if not nm:
                    img = c.select_one("img[alt]")
                    if img and img.get("alt"):
                        nm = img["alt"]
                if not nm:
                    continue

                # price — full + half spans or any text containing ‘R’
                full = c.select_one('[class*="price-display_full"]')
                half = c.select_one('[class*="price-display_half"]')
                txt = ""
                if full: txt += full.get_text(strip=True)
                if half: txt += half.get_text(strip=True)
                pr = _price_from_text(txt) or _price_from_text(c.get_text(" ", strip=True))
                if pr is None:
                    continue

                hits.append({
                    "name": nm.strip(),
                    "price": pr,
                    "size": _size_guess(nm),
                    "url": url,
                })

            best = _best_hit(ingredient, hits)
            if best:
                return best

        return None
    finally:
        await ctx.close()
        await browser.close()
