"""Feature job uçtan uca (make test-pipeline) ve TaskManager çökmesi (make test-failover).

İkisi de ders topic'lerini kullanır: bank.transactions'a canlı trafik gönderir, customer-features job'ını
(gerekirse) başlatır ve bank.customer-features'ı okur. Job testten sonra çalışmaya devam eder.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid

import pytest
from confluent_kafka import Consumer

from common import FEATURES_TOPIC, LAB_DIR, RISKY_CUSTOMERS, bootstrap
from conftest import flink, wait_running


@pytest.fixture
def traffic():
    """Testin süresince canlı trafik (saniyede 20 işlem)."""
    proc = subprocess.Popen([sys.executable, "producer.py", "--rate", "20", "--duration", "240"], cwd=LAB_DIR,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    yield
    proc.terminate()
    proc.wait(timeout=30)


def submit_features() -> str:
    out = subprocess.run(["bash", "scripts/submit.sh"], cwd=LAB_DIR, capture_output=True, text=True)
    assert out.returncode == 0, out.stdout + out.stderr
    return wait_running("customer-features")


def features_after(after_ms: int, timeout: float = 120) -> dict[str, dict]:
    """windowEnd'i after_ms'ten büyük feature'lar gelene kadar bekler (müşteri → son feature)."""
    c = Consumer({"bootstrap.servers": bootstrap(), "group.id": f"test-{uuid.uuid4().hex[:6]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([FEATURES_TOPIC])
    found, deadline = {}, time.monotonic() + timeout
    while time.monotonic() < deadline:
        m = c.poll(0.5)
        if m is not None and not m.error() and m.value():
            f = json.loads(m.value())
            if f["windowEnd"] > after_ms:
                found[f["customerId"]] = f
        if len(found) >= 20 and any(r in found for r in RISKY_CUSTOMERS):
            break
    c.close()
    return found


@pytest.mark.pipeline
def test_feature_job_her_dakika_musteri_ozetini_yazar(admin, traffic):
    submit_features()
    start = int(time.time() * 1000)
    found = features_after(start)
    assert len(found) >= 20, f"{len(found)} müşterinin yeni feature'ı geldi (job: make jobs, Flink UI)"
    sample = next(iter(found.values()))
    assert set(sample) == {"customerId", "windowStart", "windowEnd", "txnCount", "totalAmount", "maxAmount",
                           "distinctCategories"}
    assert sample["windowEnd"] - sample["windowStart"] == 5 * 60_000
    risky = [f for c, f in found.items() if c in RISKY_CUSTOMERS]
    normal = [f for c, f in found.items() if c not in RISKY_CUSTOMERS]
    assert risky and float(risky[0]["totalAmount"]) > max(float(f["totalAmount"]) for f in normal) / 2


@pytest.mark.failover
def test_taskmanager_cokunce_job_checkpointten_devam_eder(admin, traffic):
    jid = submit_features()
    time.sleep(25)  # en az bir checkpoint tamamlansın (10 sn'de bir)
    out = subprocess.run(["bash", "scripts/tm-restart.sh"], cwd=LAB_DIR, capture_output=True, text=True,
                         timeout=180)
    assert out.returncode == 0 and "geri yüklendi" in out.stdout, out.stdout + out.stderr
    assert wait_running("customer-features") == jid, "aynı job devam etmeli (yeni job değil)"
    restored = flink(f"/jobs/{jid}/checkpoints")["latest"]["restored"]
    assert restored is not None
    after = int(time.time() * 1000)
    assert len(features_after(after)) >= 20, "TaskManager dönünce feature üretimi devam etmeli"
