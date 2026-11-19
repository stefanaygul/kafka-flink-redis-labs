"""Prova testleri: Flink → Redis → skor uçtan uca (make test-pipeline) ve Redis kesintisi (make test-failover)."""
from __future__ import annotations

import subprocess
import sys
import time

import pytest

from common import LAB_DIR, RISKY_CUSTOMERS, SCORES_TOPIC, redis_url
from conftest import feature_json, read_topic, send, worker_args
from feature_store import FeatureStore

T1 = 1794651300000


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=LAB_DIR, check=True, capture_output=True)


@pytest.mark.pipeline
def test_flink_redis_skor_uctan_uca(admin):
    """Canlı trafik → Flink feature job → worker → Redis + bank.risk-scores. Riskli müşteriler HIGH almalı."""
    producer = subprocess.Popen([sys.executable, "producer.py", "--rate", "10", "--duration", "200"], cwd=LAB_DIR,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    worker = subprocess.Popen([sys.executable, "feature_worker.py"], cwd=LAB_DIR,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    store = FeatureStore(redis_url())
    try:
        deadline = time.monotonic() + 150
        while time.monotonic() < deadline:
            fresh = [f for c in RISKY_CUSTOMERS if (f := store.get(c)) and f.window_end > time.time() * 1000 - 120_000]
            if len(fresh) == 3 and store.count() >= 100:
                break
            time.sleep(5)
        assert store.count() >= 100, f"Redis'te {store.count()} müşteri (Flink job çalışıyor mu? make jobs)"
        time.sleep(3)
        latest = {}
        for s in read_topic(SCORES_TOPIC, timeout=15):
            latest[s["customerId"]] = s
        assert all(latest.get(c, {}).get("level") == "HIGH" for c in RISKY_CUSTOMERS), \
            {c: latest.get(c) for c in RISKY_CUSTOMERS}
        normal = [s for c, s in latest.items() if c not in RISKY_CUSTOMERS]
        assert sum(s["level"] == "LOW" for s in normal) > len(normal) * 0.8
    finally:
        producer.terminate()
        worker.terminate()
        producer.wait(timeout=30)
        worker.wait(timeout=30)


@pytest.mark.failover
def test_redis_kesintisinde_worker_bekler_hicbir_feature_kaybolmaz(topics, store, run_id, tmp_path):
    t_features, t_scores = topics
    log_file = tmp_path / "worker.log"
    with log_file.open("w") as out:
        worker = subprocess.Popen(worker_args(t_features, t_scores, store.prefix, f"test-{run_id}"), cwd=LAB_DIR,
                                  stdout=out, stderr=subprocess.STDOUT)
        try:
            send(t_features, [("C-9101", feature_json("C-9101", T1, 100))])
            _wait(lambda: store.get("C-9101") is not None, "ilk feature Redis'e yazılmalı")

            compose("stop", "redis")
            send(t_features, [(f"C-91{i:02d}", feature_json(f"C-91{i:02d}", T1, 100 * i)) for i in range(2, 7)])
            _wait(lambda: "Redis erişilemiyor" in log_file.read_text(), "worker Redis'i beklemeli", timeout=20)
            assert worker.poll() is None, "worker Redis kesintisinde çökmemeli"

            compose("start", "redis")
            _wait(lambda: all(_safe_get(store, f"C-91{i:02d}") for i in range(1, 7)),
                  "Redis dönünce kesintideki 5 feature da yazılmalı", timeout=60)
        finally:
            worker.terminate()
            worker.wait(timeout=30)
    ids = sorted(s["scoreId"] for s in read_topic(t_scores))
    assert ids == sorted(f"C-91{i:02d}:{T1}" for i in range(1, 7)), ids  # her biri bir kez


def _safe_get(store, customer):
    try:
        return store.get(customer)
    except Exception:  # noqa: BLE001  Redis henüz açılıyor olabilir
        return None


def _wait(condition, message: str, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.5)
    pytest.fail(message)
