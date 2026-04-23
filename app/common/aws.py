import json
import os

import boto3
from botocore.client import BaseClient

from app.common.utils import create_temp_csv_file


def create_aws_client(service_name: str) -> BaseClient:
    return boto3.client(service_name)


def send_message_to_sqs(
    sqs_client: BaseClient, queue_url: str, message: dict
) -> None:
    """Send a message to SQS."""
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message),
    )


def upload_to_s3(
    s3_client: BaseClient,
    s3_key: str,
    s3_bucket: str,
    data: list[dict],
) -> None:
    """Upload data to S3 as CSV file."""
    tmp_path = f"/tmp/files/{s3_key}"
    create_temp_csv_file(tmp_path, data)
    s3_client.upload_file(tmp_path, s3_bucket, s3_key)
    os.remove(tmp_path)
