import json
from loguru import logger
import boto3
import requests
from bs4 import BeautifulSoup
from datetime import date, datetime

S3_BUCKET: str = "torbilote-dev-my-s3-bucket "

HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "pl-PL,pl;q=0.9",
}

# ── Parsowanie HTML ───────────────────────────────────────────────────────────
 
def scrape_listing_page(page: dict) -> dict:
    #TODO: add appropiate selectors to capture product information
    PRODUCT_CONTAINER = ".product-item"          # <-- zmień na właściwy selektor
    NAME_SELECTOR     = ".product-item__name"    # <-- zmień na właściwy selektor
    PRICE_SELECTOR    = ".product-item__price"   # <-- zmień na właściwy selektor
    PRODUCT_ID_ATTR   = "data-product-id"        # <-- zmień lub usuń jeśli brak

    url = page["url"]
    response = requests.get(url, headers=HEADERS, timeout=15)
    response.raise_for_status()
 
    soup     = BeautifulSoup(response.text, "lxml")
    products = soup.select(PRODUCT_CONTAINER)
 
    if not products:
        raise ValueError(
            f"Brak produktów na stronie {url} "
            f"(selektor: '{PRODUCT_CONTAINER}'). "
            f"Sprawdź czy selektor CSS jest aktualny."
        )
 
    results = []
    today   = str(date.today())
 
    for item in products:
        # Nazwa
        name_el = item.select_one(NAME_SELECTOR)
        if name_el is None:
            logger.warning("Brak nazwy produktu na %s — pomijam kafelek.", url)
            continue
 
        # Cena
        price_el = item.select_one(PRICE_SELECTOR)
        if price_el is None:
            logger.warning("Brak ceny dla '%s' — pomijam.", name_el.get_text(strip=True))
            continue
 
        # ID produktu — z atrybutu HTML lub z href linka
        product_id = item.get(PRODUCT_ID_ATTR)
        if not product_id:
            link = item.select_one("a[href]")
            product_id = link["href"].rstrip("/").split("/")[-1] if link else "unknown"
 
        # Czyszczenie ceny: "2 499,99 zł" -> 2499.99
        raw = price_el.get_text(strip=True)
        price = float(
            raw
            .replace("\xa0", "")
            .replace(" ", "")
            .replace("zł", "")
            .replace("PLN", "")
            .replace(",", ".")
        )
 
        results.append({
            "product_id":  f"{page['category_id']}#{product_id}",
            "category_id": page["category_id"],
            "name":        name_el.get_text(strip=True),
            "price":       price,
            "url":         url,
            "page":        page["page"],
            "date":        today,
            "scraped_at":  datetime.utcnow().isoformat(),
        })
 
    logger.info(
        "[%s] strona %d — znaleziono %d produktów",
        page["category_id"], page["page"], len(results),
    )
    return results

# ── Zapis do S3 (Parquet) ─────────────────────────────────────────────────────
 
def save_to_s3(records: list[dict], batch_index: int):
    import pandas as pd
    
    today    = date.today()
    s3_key   = (
        f"prices/year={today.year}/month={today.month:02d}"
        f"/day={today.day:02d}/batch_{batch_index:04d}.parquet"
    )
    tmp_path = f"/tmp/batch_{batch_index}.parquet"
 
    pd.DataFrame(records).to_parquet(tmp_path, index=False, engine="pyarrow")
    boto3.client("s3", region_name='eu-north-1').upload_file(tmp_path, S3_BUCKET, s3_key)
    logger.info("Zapisano do S3: s3://%s/%s", S3_BUCKET, s3_key)
 
 # ── Handler ───────────────────────────────────────────────────────────────────
 
def handler(event, context):
    failed_message_ids = []
 
    for record in event["Records"]:
        body        = json.loads(record["body"])
        batch_index = body["batch_index"]
        pages       = body["pages"]
 
        logger.info(
            "Przetwarzam batch %d (%d stron listingów).",
            batch_index, len(pages),
        )
 
        all_products = []
 
        for page in pages:
            try:
                products = scrape_listing_page(page)
                all_products.extend(products)
            except Exception as e:
                logger.error(
                    "BŁĄD [%s] strona %d: %s",
                    page["category_id"], page["page"], e,
                )
 
        if all_products:
            save_to_s3(all_products, batch_index)
            logger.info(
                "Batch %d zakończony — zapisano %d produktów.",
                batch_index, len(all_products),
            )
        else:
            logger.error("Batch %d — żaden produkt nie został scrapowany.", batch_index)
            failed_message_ids.append(record["messageId"])
 
    return {
        "batchItemFailures": [
            {"itemIdentifier": mid} for mid in failed_message_ids
        ]
    }