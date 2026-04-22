import boto3
from botocore.client import BaseClient


def create_aws_client(service_name: str) -> BaseClient:
    return boto3.client(service_name)
