from urllib.parse import urlparse

import boto3
import yaml


def load_config(config_path):
    config_path = str(config_path)

    if config_path.startswith("s3://"):
        parsed = urlparse(config_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        s3_client = boto3.client("s3")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        config = yaml.safe_load(response["Body"].read().decode("utf-8"))
        return config

    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config


def get_path(config, *keys):
    root = str(config["paths"]["root"]).rstrip("/")

    path = config["paths"]

    for key in keys:
        path = path[key]

    return f"{root}/{str(path).lstrip('/')}"
