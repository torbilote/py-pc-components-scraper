import json
import cloudscraper
import boto3
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
PROXIES = [
    'http://zkwvadnm:zdirzi2wtn11@31.59.20.176:6754/',
    # 'http://zkwvadnm:zdirzi2wtn11@23.95.150.145:6114/',
    # 'http://zkwvadnm:zdirzi2wtn11@198.23.239.134:6540/',
    # 'http://zkwvadnm:zdirzi2wtn11@45.38.107.97:6014/',
    # 'http://zkwvadnm:zdirzi2wtn11@107.172.163.27:6543/',
    # 'http://zkwvadnm:zdirzi2wtn11@198.105.121.200:6462/',
    # 'http://zkwvadnm:zdirzi2wtn11@216.10.27.159:6837/',
    # 'http://zkwvadnm:zdirzi2wtn11@142.111.67.146:5611/',
    # 'http://zkwvadnm:zdirzi2wtn11@191.96.254.138:6185/',
    # 'http://zkwvadnm:zdirzi2wtn11@31.58.9.4:6077/',
]

# sqs_client = boto3.client("sqs")
session = cloudscraper.create_scraper(
    rotating_proxies=PROXIES,
    interpreter='js2py',
    enable_stealth=True,
    browser='chrome'
    )

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


# def send_message_to_sqs(message: list[str], **kwargs) -> None:
#     """Send a message to SQS."""
#     sqs_client.send_message(
#         QueueUrl=SQS_QUEUE_URL,
#         MessageBody=json.dumps({"pages": message, **kwargs}),
#     )


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
            # send_message_to_sqs(batch, category_name=category_name)
            logger.info(f"[{category_name}] Sent batch {idx}/{len(batches)}")

    return {"statusCode": 200, "body": "OK"}


# For local testing
if __name__ == "__main__":
    handler({}, {})
