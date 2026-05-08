import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_SPARK_JOBS_ROOT = Path(__file__).resolve().parents[1]
_SRC = _SPARK_JOBS_ROOT / "src"

if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))


@pytest.fixture(scope="session")
def spark_jobs_root() -> Path:
    return _SPARK_JOBS_ROOT


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    # Same interpreter for driver and workers (fixes PYTHON_VERSION_MISMATCH on Windows
    # when SPARK_HOME points at a different Python than pytest).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    session = (
        SparkSession.builder.master("local[1]")
        .appName("trading-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def tiny_pipeline_env(tmp_path: Path) -> SimpleNamespace:
    """Minimal config + paths under tmp_path for bronze/silver/gold integration tests."""
    import yaml

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    fixture = Path(__file__).resolve().parent / "fixtures" / "tiny.jsonl"
    dest = data_dir / "tiny.jsonl"
    dest.write_text(fixture.read_text(encoding="utf-8"), encoding="utf-8")

    cfg = {
        "spark": {
            "app_name": "unit-test",
            "schema_version": 1,
            "configs": {"spark.sql.caseSensitive": "true"},
        },
        "paths": {
            "root": str(tmp_path),
            "sample_data": "data/tiny.jsonl",
            "bronze": {"base": "/bronze", "checkpoint": "/bronze/_checkpoint"},
            "silver": {
                "orders": "/silver/orders",
                "account": "/silver/account",
                "checkpoint": "/silver/_checkpoint",
            },
            "gold": {"daily_metrics": "/gold/daily_metrics", "trades": "/gold/trades"},
            "quarantine": {"base": "/quarantine"},
        },
    }
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return SimpleNamespace(config=cfg, root=tmp_path, config_path=cfg_path)
