import json
import random
import time

import boto3
import cloudscraper
from bs4 import BeautifulSoup
from loguru import logger

BATCH_SIZE: int = 3
SQS_QUEUE_URL: str = "https://sqs.eu-north-1.amazonaws.com/335721753558/py-pc-components-scraper-sqs"
CATEGORIES: list[dict] = [
    {
        "name": "gpu",
        "base_url": "https://www.x-kom.pl/g-5/c/345-karty-graficzne.html",
        "pagination_class": "eqRsIL",
    },
    {
        "name": "cpu",
        "base_url": "https://www.x-kom.pl/g-5/c/11-procesory.html",
        "pagination_class": "eqRsIL",
    },
    {
        "name": "ssd",
        "base_url": "https://www.x-kom.pl/g-5/c/1779-dyski-ssd.html",
        "pagination_class": "eqRsIL",
    },
]

sqs_client = boto3.client("sqs")
session = cloudscraper.create_scraper()


def fetch_page_count(
    url: str, pagination_class: str, session: cloudscraper.CloudScraper
) -> int:
    """Fetch the page count."""
    response = session.get(url, timeout=15)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    elements = soup.find_all(class_=pagination_class)

    if not elements:
        raise ValueError(
            f"No pagination elements found (class='{pagination_class}') on {url}"
        )

    page_numbers = [int(el.get_text(strip=True)) for el in elements]
    return max(page_numbers)


def generate_urls(base_url: str, page_count: int) -> list[str]:
    """Generate URLs for for provided page_count."""
    return [f"{base_url}?page={i}" for i in range(1, page_count + 1)]


def chunk_list(
    urls: list[str], batch_size: int = BATCH_SIZE
) -> list[list[str]]:
    """Chunk list of URLs into batches."""
    return [urls[i : i + batch_size] for i in range(0, len(urls), batch_size)]


def send_message_to_sqs(message: list[str], **kwargs) -> None:
    """Send a message to SQS."""
    sqs_client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps({"pages": message, **kwargs}),
    )


def handler(_event, _context):
    """AWS Lambda entry point."""

    for category in CATEGORIES:
        category_name = category["name"]
        base_url = category["base_url"]
        pagination_class = category["pagination_class"]

        page_count = fetch_page_count(base_url, pagination_class, session)
        logger.info(f"[{category_name}] {page_count} pages found.")

        urls = generate_urls(base_url, page_count)
        batches = chunk_list(urls)
        logger.info(
            f"[{category_name}] {len(urls)} URLs split into {len(batches)} batches."
        )

        for idx, batch in enumerate(batches, 1):
            send_message_to_sqs(batch, category_name=category_name)
            logger.info(f"[{category_name}] Sent batch {idx}/{len(batches)}")

        time.sleep(random.uniform(1.5, 3.5))

    return {"statusCode": 200, "body": "OK"}


# For local testing
if __name__ == "__main__":
    handler({}, {})
