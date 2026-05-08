import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import yaml

from config_loader import get_path, load_config


def test_get_path_joins_root_and_nested_keys():
    cfg = {
        "paths": {
            "root": "s3://bucket/lakehouse",
            "sample_data": "raw/events.jsonl",
            "bronze": {"base": "/data/bronze"},
        }
    }
    assert get_path(cfg, "sample_data") == "s3://bucket/lakehouse/raw/events.jsonl"
    assert get_path(cfg, "bronze", "base") == "s3://bucket/lakehouse/data/bronze"


def test_get_path_strips_slashes():
    cfg = {"paths": {"root": "/tmp/root/", "sample_data": "/data/x.jsonl"}}
    assert get_path(cfg, "sample_data") == "/tmp/root/data/x.jsonl"


def test_load_config_local_file(tmp_path: Path):
    data = {"spark": {"app_name": "x"}, "paths": {"root": str(tmp_path)}}
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    loaded = load_config(p)
    assert loaded["spark"]["app_name"] == "x"


@patch("config_loader.boto3.client")
def test_load_config_s3(mock_client: MagicMock):
    body = io.BytesIO(
        yaml.safe_dump({"spark": {"app_name": "from-s3"}, "paths": {"root": "s3://b"}}).encode(
            "utf-8"
        )
    )
    mock_client.return_value.get_object.return_value = {"Body": body}
    loaded = load_config("s3://my-bucket/prefix/config.yaml")
    assert loaded["spark"]["app_name"] == "from-s3"
    mock_client.return_value.get_object.assert_called_once_with(
        Bucket="my-bucket", Key="prefix/config.yaml"
    )
