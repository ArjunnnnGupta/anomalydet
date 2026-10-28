"""
src/ml/registry.py — a file-based model registry with versioning and hot reload.

    data/models/registry/
        v1/ features/  rf/  kmeans/  meta.json
        v2/ ...
        latest.json    {"version": 2}     ← swapped atomically (write temp file + rename)

Training registers a new version; the streaming scorer checks latest.json every
micro-batch and swaps models without restarting the stream.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass

import numpy as np
from pyspark.ml import PipelineModel
from pyspark.ml.clustering import KMeansModel

from src.common.config import cfg, path


def root() -> str:
    return path(cfg()["ml"]["registry"])


@dataclass
class Bundle:
    version: int
    features: PipelineModel
    rf: PipelineModel
    kmeans: KMeansModel
    meta: dict
    centers: np.ndarray


def latest_version(reg: str | None = None) -> int | None:
    p = os.path.join(reg or root(), "latest.json")
    try:
        with open(p) as f:
            return int(json.load(f)["version"])
    except (FileNotFoundError, KeyError, ValueError, json.JSONDecodeError):
        return None


def load(version: int, reg: str | None = None) -> Bundle:
    d = os.path.join(reg or root(), f"v{version}")
    with open(os.path.join(d, "meta.json")) as f:
        meta = json.load(f)
    km = KMeansModel.load(os.path.join(d, "kmeans"))
    return Bundle(version, PipelineModel.load(os.path.join(d, "features")),
                  PipelineModel.load(os.path.join(d, "rf")), km, meta,
                  np.array([c.tolist() for c in km.clusterCenters()]))


def register(features: PipelineModel, rf: PipelineModel, kmeans: KMeansModel, meta: dict,
             reg: str | None = None) -> int:
    reg = reg or root()
    os.makedirs(reg, exist_ok=True)
    existing = [int(m.group(1)) for n in os.listdir(reg) if (m := re.fullmatch(r"v(\d+)", n))]
    version = max(existing, default=0) + 1
    d = os.path.join(reg, f"v{version}")
    features.write().overwrite().save(os.path.join(d, "features"))
    rf.write().overwrite().save(os.path.join(d, "rf"))
    kmeans.write().overwrite().save(os.path.join(d, "kmeans"))
    with open(os.path.join(d, "meta.json"), "w") as f:
        json.dump({**meta, "version": version}, f, indent=2, default=str)
    tmp = os.path.join(reg, ".latest.json.tmp")
    with open(tmp, "w") as f:
        json.dump({"version": version}, f)
    os.replace(tmp, os.path.join(reg, "latest.json"))   # atomic pointer swap
    return version


class HotLoader:
    """Returns the newest registered Bundle, reloading only when latest.json changes."""

    def __init__(self, reg: str | None = None):
        self.reg = reg
        self.bundle: Bundle | None = None

    def get(self) -> Bundle | None:
        v = latest_version(self.reg)
        if v is None:
            return None
        if self.bundle is None or self.bundle.version != v:
            self.bundle = load(v, self.reg)
        return self.bundle
