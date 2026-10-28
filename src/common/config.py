"""
src/common/config.py — loads conf/pipeline.yaml and resolves paths.

Every module reads settings through `cfg()` so topics, paths and tuning knobs
live in exactly one place. Relative paths resolve against APP_HOME (the project
root; /opt/app inside the Docker containers).
"""

import os
from functools import lru_cache
from pathlib import Path

import yaml

_REPO = Path(__file__).resolve().parents[2]
APP_HOME = Path(os.environ.get("APP_HOME", _REPO))            # where data/ lives
CONF_PATH = Path(os.environ.get("PIPELINE_CONF", _REPO / "conf" / "pipeline.yaml"))


@lru_cache(maxsize=1)
def cfg() -> dict:
    with open(CONF_PATH) as f:
        return yaml.safe_load(f)


def path(rel: str) -> str:
    """Resolve a config path against APP_HOME (absolute paths pass through)."""
    p = Path(rel)
    return str(p if p.is_absolute() else APP_HOME / p)


def in_docker() -> bool:
    return os.environ.get("RUNNING_IN_DOCKER") == "1"


def kafka_bootstrap() -> str:
    """Internal listener inside the compose network, external one from the host."""
    if os.environ.get("KAFKA_BOOTSTRAP"):
        return os.environ["KAFKA_BOOTSTRAP"]
    k = cfg()["kafka"]
    return k["bootstrap_internal"] if in_docker() else k["bootstrap_external"]


def replication_factor() -> int:
    return int(os.environ.get("KAFKA_RF", cfg()["kafka"]["replication_factor"]))


def topic(name: str) -> str:
    return cfg()["kafka"]["topics"][name]["name"]


def lake(name: str) -> str:
    return path(cfg()["lake"][name])


def checkpoint(query: str) -> str:
    return os.path.join(path(cfg()["lake"]["checkpoints"]), query)


def trigger() -> dict:
    """Streaming trigger: micro-batches every N seconds, or availableNow (drain the backlog, then stop)."""
    if os.environ.get("STREAM_AVAILABLE_NOW") == "1":
        return {"availableNow": True}
    return {"processingTime": f"{cfg()['streaming']['trigger_seconds']} seconds"}


def kafka_sinks_enabled() -> bool:
    """KAFKA_SINKS=0 turns off the Kafka sinks (DLQ, flows.scored, alerts), e.g. in unit tests."""
    return os.environ.get("KAFKA_SINKS", "1") != "0"
