import json
import random
import time

import boto3
import cloudscraper
import requests
from bs4 import BeautifulSoup
from loguru import logger

BATCH_SIZE: int = 3
SQS_QUEUE_URL: str = "https://sqs.eu-north-1.amazonaws.com/335721753558/py-pc-components-scraper-sqs"
CATEGORIES: list[dict] = [
    {
        "category_id": "gpu",
        "base_url": "https://www.x-kom.pl/g-5/c/345-karty-graficzne.html",
        "pagination_html_class_name": "eqRsIL",
    },
    {
        "category_id": "cpu",
        "base_url": "https://www.x-kom.pl/g-5/c/11-procesory.html",
        "pagination_html_class_name": "eqRsIL",
    },
    {
        "category_id": "ssd",
        "base_url": "https://www.x-kom.pl/g-5/c/1779-dyski-ssd.html",
        "pagination_html_class_name": "eqRsIL",
    },
]

sqs_client = boto3.client("sqs")


# Step 1: Fetch page count for provided category
def fetch_page_count(
    url: str, pagination_class: str, session: cloudscraper.CloudScraper
) -> int:
    """Fetch page count for provided category."""
    try:
        logger.info(
            f"Fetching page count from {url} with class '{pagination_class}'"
        )
        response = session.get(url, timeout=15)
        response.raise_for_status()
        logger.info(
            f"Response received: status={response.status_code}, "
            f"content_length={len(response.text)}"
        )

        with open("debug_response.html", "w", encoding="utf-8") as f:
            f.write(response.text)

        soup = BeautifulSoup(response.text, "lxml")
        elements = soup.find_all(recursive=True, class_=pagination_class)
        logger.info(
            f"Found {len(elements)} elements with pagination_class '{pagination_class}'"
        )

        if not elements:
            raise ValueError(
                f"No elements found with pagination_class '{pagination_class}' on {url}"
            )

        numbers = [int(element.get_text(strip=True)) for element in elements]
        page_count = max(numbers)
        logger.info(
            f"Page count calculated: {page_count} (from elements: {numbers})"
        )

        return page_count
    except requests.RequestException as e:
        logger.error(f"HTTP request failed for {url}: {e}")
        raise
    except ValueError as e:
        logger.error(f"Failed to parse page count: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching page count: {e}")
        raise


# Step 2: Generate URLs for all pages for provided category
def generate_urls(url: str, page_count: int) -> list[str]:
    """Generate URLs for all pages for provided category."""
    logger.info(f"Generating URLs for {page_count} pages")
    urls = [f"{url}?page={i}" for i in range(1, page_count + 1)]
    logger.info(f"Generated {len(urls)} URLs")
    return urls


# Step 3: Chunk list of URLs into batches
def chunk_list(list_of_urls: list[str]) -> list[list[str]]:
    """Chunk list of URLs into batches."""
    logger.info(
        f"Creating batches of size {BATCH_SIZE} from {len(list_of_urls)} URLs"
    )
    batches = [
        list_of_urls[i : i + BATCH_SIZE]
        for i in range(0, len(list_of_urls), BATCH_SIZE)
    ]
    logger.info(f"Created {len(batches)} batches")
    return batches


# Step 4: Send a message to SQS
def send_message_to_sqs(message: list[str], sqs_client=sqs_client) -> dict:
    """Send a message to SQS."""
    try:
        response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({"pages": message}),
        )
        logger.info(
            f"Message sent to SQS. MessageId: {response.get('MessageId')}"
        )
        return response
    except Exception as e:
        logger.error(f"Failed to send message to SQS: {e}")
        raise


# Entry point for AWS Lambda
def handler(_event, _context):
    """AWS Lambda entry point."""
    logger.info("Handler started")

    session = cloudscraper.create_scraper()

    try:
        logger.info(f"Processing {len(CATEGORIES)} categories")
        for category in CATEGORIES:
            category_id = category["category_id"]
            base_url = category["base_url"]
            class_name = category["pagination_html_class_name"]

            try:
                logger.info(f"Processing category: {category_id}")
                page_count = fetch_page_count(base_url, class_name, session)
                urls = generate_urls(base_url, page_count)
                batches = chunk_list(urls)

                logger.info(
                    f"Category {category_id}: {len(urls)} URLs in "
                    f"{len(batches)} batches"
                )

                for idx, batch in enumerate(batches, 1):
                    logger.info(
                        f"Sending batch {idx}/{len(batches)} with "
                        f"{len(batch)} URLs"
                    )
                    send_message_to_sqs(batch, sqs_client)
                    logger.info(f"Batch {idx} sent successfully")
            except Exception as e:
                logger.error(f"Error processing category {category_id}: {e}")

                time.sleep(random.uniform(1.5, 3.5))
                continue

        logger.info("Handler completed successfully")
        return {"statusCode": 200, "body": "OK"}
    except Exception as e:
        logger.error(f"Handler failed with error: {e}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}


# For local testing
if __name__ == "__main__":
    handler({}, {})
