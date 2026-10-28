"""
tests/conftest.py — every test runs against a throwaway APP_HOME (lake, checkpoints,
registry, SQLite all under a temp dir), a local[2] SparkSession, and no Kafka sinks.
"""

import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_TMP = os.environ.get("ADP_TEST_HOME") or tempfile.mkdtemp(prefix="adp-tests-")   # set to keep outputs

# Must be set before any `src.*` import resolves paths
os.environ["APP_HOME"] = _TMP
os.environ["PIPELINE_CONF"] = str(ROOT / "conf" / "pipeline.yaml")
os.environ["KAFKA_SINKS"] = "0"
os.environ.pop("RUNNING_IN_DOCKER", None)
sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402


@pytest.fixture(scope="session")
def app_home() -> str:
    return _TMP


@pytest.fixture(scope="session")
def spark():
    from src.common.spark import get_spark
    s = get_spark("pytest", master="local[2]")
    s.conf.set("spark.sql.shuffle.partitions", "2")
    yield s
    s.stop()
