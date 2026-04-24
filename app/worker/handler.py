import json
import os
import time
from datetime import UTC, datetime

from bs4 import BeautifulSoup, Tag
from cloudscraper import CloudScraper
from dotenv import load_dotenv
from loguru import logger
from requests import HTTPError

from app.common.aws import create_aws_client, upload_to_s3
from app.common.scraper import create_scraper

load_dotenv()

os.environ["AWS_REGION"] = "eu-north-1"

S3_BUCKET: str = os.environ["S3_BUCKET"]
URL_SCRAPE_SLEEP_SECONDS: float = 1.0

PROXIES: list[str] = [
    p for p in [os.environ["PROXY_URL_1"], os.getenv("PROXY_URL_2")] if p
]

CLASSES: dict[str, str] = {
    "product_container": "gDPdFR",
    "product_full_name": "eyGQAu",
    "product_price": "looiKE",
    "product_attributes": "hsNyNy",
}

def parse_sqs_record(record: dict) -> tuple[dict, datetime]:
    """Extract the message body and sent timestamp from a raw SQS record."""
    body = json.loads(record["body"])
    sent_at = datetime.fromtimestamp(
        int(record["attributes"]["SentTimestamp"]) / 1000, tz=UTC
    )
    return body, sent_at


def parse_product(item: Tag, category_name: str, date: str, product_classes: dict) -> dict | None:
    """Parse a single product element. Returns None if the name element is missing."""
    name_el = item.find(class_=product_classes["product_full_name"])

    if not name_el:
        logger.warning(
            f"[{category_name}] Skipping product - name element not found"
        )
        return None

    price_el = item.find(class_=product_classes["product_price"])
    attributes_el = item.find(class_=product_classes["product_attributes"])

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
    scraper: CloudScraper, url: str, category_name: str, date: str, product_classes: dict
) -> list[dict]:
    """Fetch a single listing page and return its parsed products."""
    response = scraper.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    items = soup.find_all(class_=product_classes["product_container"])

    if not items:
        logger.warning(f"[{category_name}] No products found on {url}")
        return []

    products = [parse_product(item, category_name, date, product_classes) for item in items]
    products_filtered = [p for p in products if p is not None]

    logger.info(
        f"[{category_name}] {url} -> {len(products_filtered)}/{len(items)} products parsed"
    )
    return products_filtered


def scrape_urls(
    scraper: CloudScraper, category_name: str, date: str, urls: list[str], product_classes: dict, sleep_time: int
) -> list[dict]:
    """Scrape all URLs in a batch and return a flat list of products."""
    all_products: list[dict] = []

    for index, url in enumerate(urls, 1):
        logger.info(
            f"[{category_name}] Scraping URL {index}/{len(urls)}: {url}"
        )

        try:
            products_batch = scrape_one_url(scraper, url, category_name, date, product_classes)
            all_products.extend(products_batch)

        except (HTTPError, AttributeError, ValueError) as e:
            logger.error(f"[{category_name}] Failed to scrape {url}: {e}")

        time.sleep(sleep_time)

    logger.info(
        f"[{category_name}] Scraped {len(all_products)} products across {len(urls)} URLs"
    )
    return all_products


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
    products = scrape_urls(scraper, category_name, date, urls, CLASSES, URL_SCRAPE_SLEEP_SECONDS)

    if not products:
        raise ValueError(
            f"[{category_name}] No products scraped in batch {batch_index}"
        )

    s3_client = create_aws_client("s3")
    s3_key = f"py-pc-components-scraper/scraped-data/{category_name}/{date}/{date}-{category_name}-{batch_index}.csv"

    upload_to_s3(s3_client, s3_key, S3_BUCKET, products)
    logger.info(
        f"[{category_name}] Batch {batch_index} complete — {len(products)} products uploaded"
    )
    return 0


if __name__ == "__main__":
    from app.common.utils import lambda_invocation_event_obj

    handler(lambda_invocation_event_obj, None)
