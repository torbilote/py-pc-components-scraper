import csv
import json
import os


def create_temp_csv_file(path: str, data: list[dict]) -> None:
    """Create a temporary CSV file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=data[0].keys(), quoting=csv.QUOTE_ALL
        )
        writer.writeheader()
        writer.writerows(data)


lambda_invocation_event_obj: dict = {
    "Records": [
        {
            "messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
            "receiptHandle": "AQEBwJnKyrHigUMZj6reyNu4...",
            "body": json.dumps(
                {
                    "category_name": "gpu",
                    "urls": [
                        "https://www.x-kom.pl/g-5/c/345-karty-graficzne.html?page=1",
                        "https://www.x-kom.pl/g-5/c/345-karty-graficzne.html?page=2",
                    ],
                    "batch_index": 1,
                }
            ),
            "attributes": {
                "ApproximateReceiveCount": "1",
                "SentTimestamp": "1545082650636",
            },
            "messageAttributes": {},
            "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b0",
            "eventSource": "aws:sqs",
            "eventSourceARN": "arn:aws:sqs:eu-north-1:335721753558:py-pc-components-scraper-sqs",
            "awsRegion": "eu-north-1",
        }
    ]
}
