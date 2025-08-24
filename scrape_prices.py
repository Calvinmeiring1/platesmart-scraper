# scrape_prices.py
# Starter: writes fake prices to Firestore so the pipeline works end-to-end.

import os
import json
import time
from typing import List
from google.cloud import firestore

REGIONS = os.getenv("REGIONS", "ZA-WC-CT").split(",")
STORE = os.getenv("STORE", "CHECKERS")
# Example: '["cheddar cheese","milk","beef mince"]'
INGREDIENTS_JSON = os.getenv(
    "INGREDIENTS_JSON",
    '["cheddar cheese","milk","beef mince"]'
)

PAUSE = 0.2  # small delay between writes

def fs() -> firestore.Client:
    # Uses GOOGLE_APPLICATION_CREDENTIALS provided by the workflow
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
    url: str | None = None
) -> None:
    # Firestore path: prices/{region}/stores/{store}/items/{ingredient}
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
            "unitType": unit_type,  # "kg" | "L" | "each"
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "url": url,
        },
        merge=True,
    )

def fake_best(ingredient: str) -> dict:
    """
    Pretend we matched a 500 g pack at R 64.99 (so you can see R/kg math).
    Adjust as you like.
    """
    price = 64.99
    qty_kg = 0.5  # 500 g
    ppu = price / qty_kg
    return {
        "product_name": f"{ingredient.title()} 500 g",
        "size": "500 g",
        "price": price,
        "price_per_unit": ppu,
        "unit_type": "kg",
        "url": None,
    }

def main() -> None:
    db = fs()
    try:
        ingredients: List[str] = json.loads(INGREDIENTS_JSON)
    except Exception:
        ingredients = ["cheddar cheese", "milk", "beef mince"]

    print(f"Writing fake prices for {len(ingredients)} ingredients → Firestore")
    for ing in ingredients:
        best = fake_best(ing)
        for region in REGIONS:
            write_price(db, region, STORE, ing, **best)
        print(f"[{STORE}] {ing} -> {best['pricePerUnit'] if 'pricePerUnit' in best else best['price_per_unit']}")
        time.sleep(PAUSE)

if __name__ == "__main__":
    main()


