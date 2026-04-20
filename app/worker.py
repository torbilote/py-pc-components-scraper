import csv
import json
import os
from datetime import UTC, datetime

import boto3
from bs4 import BeautifulSoup
import cloudscraper
from dotenv import load_dotenv
from loguru import logger
from requests import Response

load_dotenv()

os.environ["AWS_REGION"] = "eu-north-1"
PROXY_URL_1: str = os.environ["PROXY_URL_1"]  # necessary
PROXY_URL_2: str = os.getenv("PROXY_URL_2")  # optional
PROXY_URL_3: str = os.getenv("PROXY_URL_3")  # optional

S3_BUCKET: str = os.environ["S3_BUCKET"]

PRODUCT_CONTAINER_CLASS: str = "gDPdFR"
PRODUCT_FULL_NAME_CLASS: str = "eyGQAu"
PRODUCT_PRICE_CLASS: str = "looiKE"
PRODUCT_SECONDARY_ATTRIBUTES_CLASS: str = "dpmgFj"

def scrape_page(category_name: str, date: str,urls: list[str]) -> list[dict]:
    """Scrape data from provided urls, combine the results and return the data."""
    logger.info(
        f"Starting scrape_page for category={category_name} with {len(urls)} urls for date={date}"
    )

    proxies = [
        proxy for proxy in [PROXY_URL_1, PROXY_URL_2, PROXY_URL_3] if proxy
    ]

    scraper = cloudscraper.create_scraper(
        cookie_storage_dir="/tmp/cookies",
        rotating_proxies=proxies,
        circuit_failure_threshold=3,
        circuit_timeout=5,
    )

    data: list[dict[str, str]] = []
    # data format example:
    # [
    #     {"full_name": "RTX 3080", "price": "$699.99"},
    #     {"full_name": "RTX 3090", "price": "$1499.99"}
    # ]

    for url in urls:
        logger.info(f"Fetching URL: {url}")
        response: Response = scraper.get(url)
        response.raise_for_status()
        logger.info(
            f"Fetched URL {url} with status_code={response.status_code}"
        )

        soup = BeautifulSoup(response.text, "lxml")
        products = soup.find_all(class_=PRODUCT_CONTAINER_CLASS)
        logger.info(f"Found {len(products)} products on URL: {url}")

        for product in products:
            product_data = {}

            product_data['category'] = category_name
            product_data['date'] = date
            product_data['full_name'] = product.find(class_=PRODUCT_FULL_NAME_CLASS).get_text(strip=True)
            product_data['price'] = product.find(class_=PRODUCT_PRICE_CLASS).get_text(strip=True)

            secondary_attributes = product.find_all(class_=PRODUCT_SECONDARY_ATTRIBUTES_CLASS)

            if category_name == "gpu":
                product_data['model'] = secondary_attributes[0].get_text(strip=True)
                product_data['memory'] = secondary_attributes[1].get_text(strip=True)
                product_data['memory_type'] = secondary_attributes[2].get_text(strip=True)

            elif category_name == "cpu":
                product_data['socket'] = secondary_attributes[0].get_text(strip=True)
                product_data['clock_speed'] = secondary_attributes[1].get_text(strip=True)
                product_data['cores'] = secondary_attributes[2].get_text(strip=True)

            elif category_name == "ssd":
                product_data['size'] = secondary_attributes[0].get_text(strip=True)
                product_data['interface'] = secondary_attributes[1].get_text(strip=True)
                product_data['read_speed'] = secondary_attributes[2].get_text(strip=True)
                product_data['write_speed'] = secondary_attributes[3].get_text(strip=True)
                
            data.append(product_data)

    logger.info(
        f"Completed scrape_page for category={category_name}, total_items={len(data)}"
    )
    return data


def upload_to_s3(category_name: str, sqs_date: str, data: list[dict]) -> None:
    """Upload data to S3 in CSV format."""
    s3_key = f"py-pc-components-scraper/scraped-data/{category_name}/{sqs_date}/{sqs_date}-{category_name}.csv"
    tmp_path = f"/tmp/files/{category_name}-{sqs_date}.csv"

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
    sqs_date: str = sqs_datetime.strftime("%Y%m%d")
    logger.info(
        f"Received SQS message sent at {sqs_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}: {sqs_message}"
    )

    if not sqs_message.get("urls"):
        logger.info("Bad Request: No data in message")
        raise ValueError("Bad Request: No data in message")

    category_name: str = sqs_message["category_name"]
    urls: list[str] = sqs_message["urls"]

    logger.info(f"Processing category={category_name} with {len(urls)} urls")
    data = scrape_page(category_name, sqs_date, urls)

    if not data:
        logger.info("No data scraped from the page")
        raise ValueError("No data scraped from the page")

    
    logger.info(
        f"Uploading scraped data for category={category_name} and date={sqs_date}"
    )
    upload_to_s3(category_name, sqs_date, data)

    logger.info("Lambda handler completed successfully")
    return {"completed": "true"}


if __name__ == "__main__":
    lambda_invocation_event_obj = {
        "Records": [
            {
                "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
                "receiptHandle": "AQEBwJnKyrHigUMZj6reyNu4...",
                "body": "{\"category_name\": \"gpu\",\"urls\": [\"https://www.x-kom.pl/g-5/c/345-karty-graficzne.html?page=1\", \"https://www.x-kom.pl/g-5/c/345-karty-graficzne.html?page=2\"],\"batch_index\": 1}",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1545082650636"
                },
                "messageAttributes": {},
                "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b0",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:eu-north-1:335721753558:py-pc-components-scraper-sqs",
                "awsRegion": "eu-north-1"
            }
        ]
    }

    handler(lambda_invocation_event_obj, None)
