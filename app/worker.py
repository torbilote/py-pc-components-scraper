import csv
import json
import os
from datetime import UTC, datetime

import boto3
from bs4 import BeautifulSoup
from cloudscraper.session_pool import SessionPool
from dotenv import load_dotenv
from loguru import logger
from requests import Response

load_dotenv()

os.environ["AWS_REGION"] = "eu-north-1"
PROXY_URL_1: str = os.environ["PROXY_URL_1"]  # necessary
PROXY_URL_2: str = os.getenv("PROXY_URL_2")  # optional
PROXY_URL_3: str = os.getenv("PROXY_URL_3")  # optional

S3_BUCKET: str = os.environ["S3_BUCKET"]

PRODUCT_CLASS: str = "<class name>"
PRODUCTS_ATTRIBTUES_CLASSES: dict = {
    "gpu": {
        "full_name": "<class name>",
        "price": "<class name>",
        "model": "<class name>",
        "memory": "<class name>",
        "memory_type": "<class name>",
    },
    "cpu": {
        "full_name": "<class name>",
        "price": "<class name>",
        "socket": "<class name>",
        "clock_speed": "<class name>",
        "cores_count": "<class name>",
        "cache_size": "<class name>",
    },
    "ssd": {
        "full_name": "<class name>",
        "price": "<class name>",
        "size": "<class name>",
        "interface": "<class name>",
        "read_speed": "<class name>",
        "write_speed": "<class name>",
    },
}


def scrape_page(category_name: str, urls: list[str]) -> list[dict]:
    """Scrape data from provided urls, combine the results and return the data."""
    logger.info(
        f"Starting scrape_page for category={category_name} with {len(urls)} urls"
    )

    proxies = [
        proxy for proxy in [PROXY_URL_1, PROXY_URL_2, PROXY_URL_3] if proxy
    ]

    scraper_session = SessionPool(
        pool_size=3,
        rotation_strategy="least_used",
        cookie_storage_dir="/tmp/cookies",
        rotating_proxies=proxies,
        circuit_failure_threshold=3,
        circuit_timeout=60,
    )

    data: list[dict[str, str]] = []
    # data format example:
    # [
    #     {"full_name": "RTX 3080", "price": "$699.99"},
    #     {"full_name": "RTX 3090", "price": "$1499.99"}
    # ]

    for url in urls:
        logger.info(f"Fetching URL: {url}")
        response: Response = scraper_session.get(url)
        response.raise_for_status()
        logger.info(
            f"Fetched URL {url} with status_code={response.status_code}"
        )

        soup = BeautifulSoup(response.text, "lxml")
        products = soup.find_all(class_=PRODUCT_CLASS)
        logger.info(f"Found {len(products)} products on URL: {url}")

        for product_index, product in enumerate(products, start=1):
            logger.info(f"Parsing product #{product_index} from URL: {url}")
            attributes = PRODUCTS_ATTRIBTUES_CLASSES[category_name]
            product_data = {}

            for attribute_name, class_name in attributes.items():
                product_data[attribute_name] = product.find(
                    class_=class_name
                ).get_text(strip=True)

            data.append(product_data)

    logger.info(
        f"Completed scrape_page for category={category_name}, total_items={len(data)}"
    )
    return data


def upload_to_s3(category_name: str, sqs_date: str, data: list[dict]) -> None:
    """Upload data to S3 in CSV format."""
    s3_key = f"py-pc-components-scraper/scraped-data/{category_name}/{sqs_date}/data.csv"
    tmp_path = f"/tmp/files/{category_name}_{sqs_date}_data.csv"

    logger.info(
        f"Preparing upload for category={category_name}, date={sqs_date}, records={len(data)}"
    )
    os.makedirs(os.path.dirname(tmp_path), exist_ok=True)

    with open(tmp_path, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    logger.info(f"Wrote CSV file to {tmp_path}")

    client = boto3.client("s3")
    client.upload_file(tmp_path, S3_BUCKET, s3_key)
    logger.info(f"Uploaded file to s3://{S3_BUCKET}/{s3_key}")

    os.remove(tmp_path)
    logger.info(f"Removed temporary file {tmp_path}")


def handler(event, _context) -> dict:
    """AWS Lambda entry point."""
    logger.info("Starting worker")

    sqs_message: dict = json.loads(event["Records"][0]["body"])
    sqs_timestamp: str = json.loads(
        event["Records"][0]["attributes"]["SentTimestamp"]
    )
    sqs_datetime: datetime = datetime.fromtimestamp(
        int(sqs_timestamp) / 1000, tz=UTC
    )
    logger.info(
        f"Received SQS message sent at {sqs_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}: {sqs_message}"
    )

    if not sqs_message.get("data"):
        logger.info("Bad Request: No data in message")
        raise ValueError("Bad Request: No data in message")

    category_name: str = sqs_message["category_name"]
    urls: list[str] = sqs_message["data"]

    logger.info(f"Processing category={category_name} with {len(urls)} urls")
    data = scrape_page(category_name, urls)

    if not data:
        logger.info("No data scraped from the page")
        raise ValueError("No data scraped from the page")

    sqs_date: str = sqs_datetime.strftime("%Y%m%d")
    logger.info(
        f"Uploading scraped data for category={category_name} and date={sqs_date}"
    )
    upload_to_s3(category_name, sqs_date, data)

    logger.info("Lambda handler completed successfully")
    return {"completed": "true"}


if __name__ == "__main__":
    handler({}, {})
