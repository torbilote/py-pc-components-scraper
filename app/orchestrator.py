import json
import os
from time import sleep

import boto3
import cloudscraper
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from loguru import logger
from requests import Response

load_dotenv()

os.environ["AWS_REGION"] = "eu-north-1"

PROXY_URL_1: str = os.environ["PROXY_URL_1"] # necessary
PROXY_URL_2: str = os.getenv("PROXY_URL_2") # optional
PROXY_URL_3: str = os.getenv("PROXY_URL_3") # optional
SQS_QUEUE_URL: str = os.environ["SQS_QUEUE_URL"]

BATCH_SIZE: int = 30
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


def fetch_page_count(url: str, pagination_class: str) -> int:
    """Fetch the page count."""
    proxies = [proxy for proxy in [PROXY_URL_1, PROXY_URL_2, PROXY_URL_3] if proxy]

    scraper = cloudscraper.create_scraper(
        cookie_storage_dir="/tmp/cookies",
        rotating_proxies=proxies,
        circuit_failure_threshold=3,
        circuit_timeout=60,
    )

    response: Response = scraper.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    elements = soup.find_all(class_=pagination_class)

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


def send_message_to_sqs(message: dict) -> None:
    """Send a message to SQS."""
    client = boto3.client("sqs")
    client.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps(message),
    )


def handler(_event, _context) -> dict:
    """AWS Lambda entry point."""
    logger.info("Starting orchestrator handler")

    for category in CATEGORIES:
        category_name = category["name"]
        base_url = category["base_url"]
        pagination_class = category["pagination_class"]
        logger.info(f"Processing category: {category_name}")

        page_count = fetch_page_count(base_url, pagination_class)
        logger.info(f"Fetched {page_count} pages")

        urls = generate_urls(base_url, page_count)
        logger.info(f"Generated {len(urls)} URLs")

        batches = chunk_list(urls)
        logger.info(f"Created {len(batches)} batches")

        for idx, batch_of_url_list in enumerate(batches, 1):
            message = {
                "category_name": category_name,
                "urls": batch_of_url_list,
                "batch_index": idx,
            }
            send_message_to_sqs(message)
            logger.info(f"Sent batch {idx} with {len(batch_of_url_list)} URLs to SQS")

        sleep(5.0)
        logger.info("Sleeping for 5 seconds to avoid rate limiting")

    logger.info("Completed processing all categories")
    return {"completed": "true"}

if __name__ == "__main__":
    handler({}, {})
