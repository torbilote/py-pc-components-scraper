import json
from loguru import logger
import boto3
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
import requests
import os


BATCH_SIZE = 100
SQS_QUEUE_URL = os.environ.get(
    "SQS_QUEUE_URL",
    "https://sqs.eu-central-1.amazonaws.com/123456789012/price-scraper-queue",
)
HEADERS: dict = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "pl-PL,pl;q=0.9",
}

CATEGORIES: list[dict] = [
    {
        "category_id": "gpu",
        "base_url": "https://www.x-kom.pl/g-5/c/345-karty-graficzne.html",
        "pagination_selector": "eqRsIL",
    },
    {
        "category_id": "cpu",
        "base_url": "https://www.x-kom.pl/g-5/c/11-procesory.html",
        "pagination_selector":  "eqRsIL",
    },
]

# ── Krok 1: odczyt liczby stron ────────────────────────────────────────────────
 
def fetch_page_count(category: dict) -> int:
    url = category["base_url"]
    logger.info("[%s] Pobieram stronę 1: %s", category["category_id"], url)

    response = requests.get(url, headers=HEADERS, timeout=15)
    response.raise_for_status()
 
    soup = BeautifulSoup(response.text, "lxml")
    elements = soup.select(selector=category["pagination_selector"], limit=6)
    
    if not elements:
        raise ValueError(
            f"[{category['category_id']}] Nie znaleziono elementow paginacji "
            f"(selektor: '{category['pagination_selector']}'). "
            f"Sprawdź czy selektor CSS jest aktualny."
        )
    
    numbers = [int(element.get_text(strip=True)) for element in elements]
    page_count = max(numbers)
 
    if not page_count:
        raise ValueError(
            f"[{category['category_id']}] Nie można znaleźć liczby stron w elementach paginacji. "
        )
 
    logger.info("[%s] Liczba stron: %d", category["category_id"], page_count)
    return page_count

# ── Krok 2: generowanie URL-i ──────────────────────────────────────────────────
 
def generate_urls(category: dict, page_count: int) -> list[dict]:
    urls: list[dict] = []

    for page in range(1, page_count + 1):
        urls.append({
            "category_id": category["category_id"],
            "page": page,
            "url": f"{category['base_url']}?page={page}",
        })

    return urls

# ── Krok 3: podział na batche i wysyłka do SQS ────────────────────────────────

def chunk_list(lst: list, chunk_size: int) -> list[list[dict]]:
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def send_batch_to_sqs(sqs_client, batch: list[dict], batch_index: int) -> dict:
    response = sqs_client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps({
            "batch_index": batch_index,
            "total_pages": len(batch),
            "pages": batch,
        }),
        MessageAttributes={
            "batch_index": {"StringValue": str(batch_index), "DataType": "Number"},
        },
    )
    return response

# ── Handler ────────────────────────────────────────────────────────────────────
 
def handler(event, context):
    all_urls: list = []
    discovery_errors: list = []

    for category in CATEGORIES:
        try:
            page_count = fetch_page_count(category)
            urls = generate_urls(category, page_count)
            all_urls.extend(urls)
            logger.info(
                "[%s] Wygenerowano %d URL-i (stron: %d)",
                category["category_id"], len(urls), page_count,
            )
        except Exception as e:
            msg = f"[{category['category_id']}] Discovery failed: {e}"
            logger.error(msg)
            discovery_errors.append(msg)
 
    if not all_urls:
        raise RuntimeError(
            "Discovery zakończone bez żadnych URL-i. "
            f"Błędy: {discovery_errors}"
        )
 
    logger.info(
        "Discovery zakończone. Łączna liczba URL-i: %d (kategorie: %d, błędy: %d)",
        len(all_urls), len(CATEGORIES), len(discovery_errors),
    )

    sqs = boto3.client("sqs", region_name="eu-north-1")
    batches = chunk_list(all_urls, BATCH_SIZE)
    logger.info("Podział na %d batchy po max %d URL-i.", len(batches), BATCH_SIZE)
 
    sent = 0
    failed = 0
 
    for index, batch in enumerate(batches):
        try:
            response = send_batch_to_sqs(sqs, batch, index)
            logger.info(
                "Batch %d/%d wysłany (%d URL-i). MessageId: %s",
                index + 1, len(batches), len(batch), response["MessageId"],
            )
            sent += 1
        except ClientError as e:
            logger.error("Błąd wysyłania batcha %d: %s", index, e)
            failed += 1
 
    summary = {
        "total_categories": len(CATEGORIES),
        "discovery_errors": len(discovery_errors),
        "total_urls": len(all_urls),
        "total_batches": len(batches),
        "sent": sent,
        "failed": failed,
    }
    logger.info("Podsumowanie: %s", summary)
 
    if failed > 0:
        raise RuntimeError(f"{failed} batchy nie zostało wysłanych do SQS.")
 
    return {"statusCode": 200, "body": json.dumps(summary)}