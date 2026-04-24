import os
import time

from bs4 import BeautifulSoup
from cloudscraper import CloudScraper
from dotenv import load_dotenv
from loguru import logger
from requests import Response

from app.common.aws import create_aws_client, send_message_to_sqs
from app.common.scraper import create_scraper

load_dotenv()

os.environ["AWS_REGION"] = "eu-north-1"

BATCH_SIZE: int = 100
SQS_QUEUE_URL: str = os.environ["SQS_QUEUE_URL"]
CATEGORY_SLEEP_SECONDS: float = 5.0
PAGINATION_CLASS: str = "eqRsIL"

PROXIES: list[str] = [
    p for p in [os.environ["PROXY_URL_1"], os.getenv("PROXY_URL_2")] if p
]

CATEGORIES: list[dict] = [
    {
        "name": "gpu",
        "base_url": "https://www.x-kom.pl/g-5/c/345-karty-graficzne.html",
    },
    {
        "name": "cpu",
        "base_url": "https://www.x-kom.pl/g-5/c/11-procesory.html",
    },
    {
        "name": "ssd",
        "base_url": "https://www.x-kom.pl/g-5/c/1779-dyski-ssd.html",
    },
    {
        "name": "hdd",
        "base_url": "https://www.x-kom.pl/g-5/c/1580-dyski-hdd.html",
    },
    {
        "name": "ram",
        "base_url": "https://www.x-kom.pl/g-5/c/28-pamieci-ram.html",
    },
    {
        "name": "mobo",
        "base_url": "https://www.x-kom.pl/g-5/c/14-plyty-glowne.html",
    },
    {
        "name": "liquid_cooler",
        "base_url": "https://www.x-kom.pl/g-5/c/2650-chlodzenia-wodne.html",
    },
    {
        "name": "air_cooler",
        "base_url": "https://www.x-kom.pl/g-5/c/105-chlodzenia-procesorow.html",
    },
    {
        "name": "case",
        "base_url": "https://www.x-kom.pl/g-5/c/389-obudowy-do-komputera.html",
    },
]


def fetch_page_count(
    scraper: CloudScraper, url: str, pagination_class: str
) -> int:
    """Fetch the total number of pages for a category listing."""
    response: Response = scraper.get(url, timeout=10)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    elements = soup.find_all(class_=pagination_class)

    if not elements:
        raise ValueError(
            f"No pagination elements found (class='{pagination_class}') at {url}"
        )

    return max(int(el.get_text(strip=True)) for el in elements)


def generate_urls(base_url: str, page_count: int) -> list[str]:
    """Generate paginated URLs for all pages in a category."""
    return [f"{base_url}?page={i}" for i in range(1, page_count + 1)]


def chunk_list(
    urls: list[str], batch_size: int = BATCH_SIZE
) -> list[list[str]]:
    """Split a flat list of URLs into fixed-size batches."""
    return [urls[i : i + batch_size] for i in range(0, len(urls), batch_size)]


def handler(_event, _context) -> int:
    """AWS Lambda entry point."""
    logger.info("Orchestrator started")

    scraper = create_scraper(PROXIES)
    sqs_client = create_aws_client("sqs")

    for category in CATEGORIES:
        category_name = category["name"]
        base_url = category["base_url"]
        logger.info(f"Processing category: {category_name}")

        page_count = fetch_page_count(scraper, base_url, PAGINATION_CLASS)
        logger.info(f"[{category_name}] {page_count} pages found")

        urls = generate_urls(base_url, page_count)
        urls_batches = chunk_list(urls)
        logger.info(f"[{category_name}] Created {len(urls_batches)} batches")

        for index, urls_batch in enumerate(urls_batches, 1):
            sqs_message = {
                "category_name": category_name,
                "urls": urls_batch,
                "batch_index": index,
            }
            send_message_to_sqs(sqs_client, SQS_QUEUE_URL, sqs_message)
            logger.info(
                f"[{category_name}] Sent batch {index}/{len(urls_batches)} ({len(urls_batch)} URLs)"
            )

        time.sleep(CATEGORY_SLEEP_SECONDS)

    logger.info("Orchestrator completed successfully")
    return 0


if __name__ == "__main__":
    handler({}, {})
