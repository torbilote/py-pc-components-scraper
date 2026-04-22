import csv
import json
import os
import time
from csv import QUOTE_ALL
from datetime import UTC, datetime

from botocore.client import BaseClient
from bs4 import BeautifulSoup, Tag
from cloudscraper import CloudScraper
from dotenv import load_dotenv
from loguru import logger

from app.common.aws import create_aws_client
from app.common.scraper import create_scraper

load_dotenv()

os.environ["AWS_REGION"] = "eu-north-1"

S3_BUCKET: str = os.environ["S3_BUCKET"]
URL_SCRAPE_SLEEP_SECONDS: float = 1.0

PROXIES: list[str] = [
    p for p in [os.environ["PROXY_URL_1"], os.getenv("PROXY_URL_2")] if p
]

PRODUCT_CONTAINER_CLASS: str = "gDPdFR"
PRODUCT_FULL_NAME_CLASS: str = "eyGQAu"
PRODUCT_PRICE_CLASS: str = "looiKE"
PRODUCT_ATTRIBUTES_CLASS: str = "hsNyNy"


def parse_sqs_record(record: dict) -> tuple[dict, datetime]:
    """Extract the message body and sent timestamp from a raw SQS record."""
    body = json.loads(record["body"])
    sent_at = datetime.fromtimestamp(
        int(record["attributes"]["SentTimestamp"]) / 1000, tz=UTC
    )
    return body, sent_at


def parse_product(item: Tag, category_name: str, date: str) -> dict | None:
    """Parse a single product element. Returns None if the name element is missing."""
    name_el = item.find(class_=PRODUCT_FULL_NAME_CLASS)

    if not name_el:
        logger.warning(
            f"[{category_name}] Skipping product - name element not found"
        )
        return None

    price_el = item.find(class_=PRODUCT_PRICE_CLASS)
    attributes_el = item.find(class_=PRODUCT_ATTRIBUTES_CLASS)

    return {
        "category": category_name,
        "date": date,
        "full_name": name_el.get_text(strip=True),
        "price": price_el.get_text(strip=True) if price_el else "n/a",
        "attributes": attributes_el.get_text(strip=True)
        if attributes_el
        else "n/a",
    }


def scrape_one_url(
    scraper: CloudScraper, url: str, category_name: str, date: str
) -> list[dict]:
    """Fetch a single listing page and return its parsed products."""
    response = scraper.get(url, timeout=10)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    items = soup.find_all(class_=PRODUCT_CONTAINER_CLASS)

    if not items:
        logger.warning(f"[{category_name}] No products found on {url}")
        return []

    products = [parse_product(item, category_name, date) for item in items]
    products_filtered = [p for p in products if p is not None]

    logger.info(
        f"[{category_name}] {url} -> {len(products_filtered)}/{len(items)} products parsed"
    )
    return products_filtered


def scrape_urls(
    scraper: CloudScraper, category_name: str, date: str, urls: list[str]
) -> list[dict]:
    """Scrape all URLs in a batch and return a flat list of products."""
    all_products: list[dict] = []

    for index, url in enumerate(urls, 1):
        logger.info(
            f"[{category_name}] Scraping URL {index}/{len(urls)}: {url}"
        )

        try:
            products_batch = scrape_one_url(scraper, url, category_name, date)
            all_products.extend(products_batch)

        except Exception as e:
            # One bad URL should not abort the entire batch
            logger.error(f"[{category_name}] Failed to scrape {url}: {e}")

        time.sleep(URL_SCRAPE_SLEEP_SECONDS)

    logger.info(
        f"[{category_name}] Scraped {len(all_products)} products across {len(urls)} URLs"
    )
    return all_products


def create_temp_csv_file(path: str, data: list[dict]) -> None:
    """Create a temporary CSV file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=data[0].keys(), quoting=QUOTE_ALL
        )
        writer.writeheader()
        writer.writerows(data)


def upload_to_s3(
    s3_client: BaseClient,
    category_name: str,
    date: str,
    batch_index: int,
    data: list[dict],
) -> None:
    """Write scraped data to a temp CSV file and upload it to S3."""

    s3_key = f"py-pc-components-scraper/scraped-data/{category_name}/{date}/{date}-{category_name}-{batch_index}.csv"
    tmp_path = f"/tmp/files/{category_name}-{date}-{batch_index}.csv"

    create_temp_csv_file(tmp_path, data)

    s3_client.upload_file(tmp_path, S3_BUCKET, s3_key)
    logger.info(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")

    os.remove(tmp_path)


def handler(event, _context) -> dict:
    """AWS Lambda entry point."""
    logger.info("Worker started")

    message, sent_at = parse_sqs_record(event["Records"][0])
    logger.info(
        f"SQS message received (sent at {sent_at.strftime('%Y-%m-%d %H:%M:%S %Z')}): {message}"
    )

    category_name: str = message.get("category_name")
    urls: list[str] = message.get("urls")
    batch_index: int = message.get("batch_index")

    if not category_name or not urls or batch_index is None:
        raise ValueError(
            f"Malformed SQS message — missing required fields: {message}"
        )

    logger.info(
        f"[{category_name}] Processing batch {batch_index} ({len(urls)} URLs)"
    )

    scraper = create_scraper(PROXIES)
    date = sent_at.strftime("%Y%m%d")
    products = scrape_urls(scraper, category_name, date, urls)

    if not products:
        raise ValueError(
            f"[{category_name}] No products scraped in batch {batch_index}"
        )

    s3_client = create_aws_client("s3")
    upload_to_s3(s3_client, category_name, date, batch_index, products)

    logger.info(
        f"[{category_name}] Batch {batch_index} complete — {len(products)} products uploaded"
    )
    return 0


if __name__ == "__main__":
    from app.common.utils import lambda_invocation_event_obj

    handler(lambda_invocation_event_obj, None)
