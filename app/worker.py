import json
import os

import boto3
from cloudscraper.session_pool import SessionPool
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from loguru import logger
from datetime import datetime, timezone
import csv

from requests import Response
from worker import save_to_s3

load_dotenv()

os.environ["AWS_REGION"] = "eu-north-1"
PROXY_URL_1: str = os.environ["PROXY_URL_1"] # necessary
PROXY_URL_2: str = os.getenv("PROXY_URL_2") # optional
PROXY_URL_3: str = os.getenv("PROXY_URL_3") # optional

S3_BUCKET: str = os.environ["S3_BUCKET"]

PRODUCT_CLASS: str = "<class name>"
PRODUCTS_ATTRIBTUES_CLASSES: dict = {
    "gpu": {
        "full_name": "<class name>",
        "price": "<class name>",
        "model": "<class name>",
        "memory": "<class name>",
        "memory_type": "<class name>"
    },
    "cpu": {
        "full_name": "<class name>",
        "price": "<class name>",
        "socket": "<class name>",
        "clock_speed": "<class name>",
        "cores_count": "<class name>",
        "cache_size": "<class name>"
    },
    "ssd": {
        "full_name": "<class name>",
        "price": "<class name>",
        "size": "<class name>",
        "interface": "<class name>",
        "read_speed": "<class name>",
        "write_speed": "<class name>"
    },
}



def scrape_page(category_name: str, urls: list[str]) -> list[dict]:
    """Scrape data from provided urls, combine the results and return the data."""
    proxies = [proxy for proxy in [PROXY_URL_1, PROXY_URL_2, PROXY_URL_3] if proxy]

    scraper_session = SessionPool(
        pool_size=3,
        rotation_strategy='least_used',
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
        response: Response = scraper_session.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        products = soup.find_all(class_=PRODUCT_CLASS)

        for product in products:
            attributes = PRODUCTS_ATTRIBTUES_CLASSES[category_name]
            product_data = {}

            for attribute_name, class_name in attributes.items():
                product_data[attribute_name] = product.find(class_=class_name).get_text(strip=True)

            data.append(product_data)

    return data


def upload_to_s3(category_name: str, sqs_date: str, data: list[dict]) -> None:
    """Upload data to S3 in CSV format."""
    s3_key = f"py-pc-components-scraper/scraped-data/{category_name}/{sqs_date}/data.csv"
    tmp_path = f"/tmp/files/{category_name}_{sqs_date}_data.csv"



    with open(tmp_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    
    client = boto3.client("s3")
    client.upload_file(tmp_path, S3_BUCKET, s3_key)

    os.remove(tmp_path)


def handler(event, context) -> dict:
    """AWS Lambda entry point."""
    sqs_message: dict = json.loads(event["Records"][0]["body"])
    sqs_timestamp: str = json.loads(event["Records"][0]["attributes"]["SentTimestamp"])
    sqs_datetime: datetime = datetime.fromtimestamp(int(sqs_timestamp) / 1000, tz=timezone.utc)
    logger.info(f"Received message from SQS which was sent at {sqs_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}:\n {sqs_message}")

    if not sqs_message.get("data"):
        raise ValueError("Bad Request: No data in message")

    category_name: str = sqs_message["category_name"]
    urls: list[str] = sqs_message["data"]

    data = scrape_page(category_name, urls)

    if not data:
        raise ValueError("No data scraped from the page")
    
    sqs_date: str = sqs_datetime.strftime("%Y%m%d")
    save_to_s3(category_name, sqs_date, data)

    return {"completed": "true"}

if __name__ == "__main__":
    handler({}, {})