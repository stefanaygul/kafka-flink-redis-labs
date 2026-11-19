"""Test yardımcıları. Kafka veya Redis'e ulaşılamıyorsa testler tek, açık bir mesajla durur."""
from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid

import pytest
import redis
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from common import LAB_DIR, bootstrap, redis_url
from feature_store import FeatureStore


@pytest.fixture(scope="session")
def admin() -> AdminClient:
    client = AdminClient({"bootstrap.servers": bootstrap(), "log_level": 0})
    try:
        client.list_topics(timeout=8)
        redis.Redis.from_url(redis_url(), socket_connect_timeout=3).ping()
    except Exception:  # noqa: BLE001
        pytest.exit(f"\n\n  Kafka ({bootstrap()}) veya Redis ({redis_url()}) yanıt vermiyor.  Önce:  make up\n",
                    returncode=2)
    return client


@pytest.fixture
def run_id() -> str:
    return time.strftime("%H%M%S") + uuid.uuid4().hex[:4]


@pytest.fixture
def topics(admin, run_id):
    """Geçici feature ve skor topic'leri (ders topic'lerine dokunulmaz)."""
    names = [f"test.features.{run_id}", f"test.scores.{run_id}"]
    for f in admin.create_topics([NewTopic(n, 1, 1) for n in names]).values():
        f.result()
    deadline = time.monotonic() + 30  # metadata yayılsın: her topic'in leader'ı belli olsun
    while time.monotonic() < deadline:
        md = admin.list_topics(timeout=10).topics
        if all(n in md and md[n].error is None and all(p.leader >= 0 for p in md[n].partitions.values())
               for n in names):
            break
        time.sleep(0.5)
    yield names
    admin.delete_topics(names)


@pytest.fixture
def store(run_id):
    """Testlere özel Redis anahtar öneki; test bitince silinir."""
    s = FeatureStore(redis_url(), prefix=f"test-{run_id}:features:")
    yield s
    for key in s.r.scan_iter(f"{s.prefix}*"):
        s.r.delete(key)


def feature_json(customer: str, window_end: int, total: float, count: int = 3, max_amount: float = 100.0,
                 categories: int = 2) -> str:
    return json.dumps({"customerId": customer, "windowStart": window_end - 300_000, "windowEnd": window_end,
                       "txnCount": count, "totalAmount": total, "maxAmount": max_amount,
                       "distinctCategories": categories})


def send(topic: str, messages: list[tuple[str, str]]) -> None:
    p = Producer({"bootstrap.servers": bootstrap(), "acks": "all"})
    for key, value in messages:
        p.produce(topic, key=key, value=value)
    assert p.flush(10) == 0


def worker_args(features_topic: str, scores_topic: str, prefix: str, group: str, *extra: str) -> list[str]:
    return [sys.executable, "feature_worker.py", "--features-topic", features_topic, "--scores-topic", scores_topic,
            "--prefix", prefix, "--group", group, *extra]


def run_worker(*args: str) -> subprocess.CompletedProcess:
    """feature_worker.py'yi ayrı süreç olarak çalıştırır: run_worker(features, scores, prefix, group, ...)."""
    return subprocess.run(worker_args(*args), cwd=LAB_DIR, capture_output=True, text=True, timeout=120)


def read_topic(topic: str, timeout: float = 10) -> list[dict]:
    c = Consumer({"bootstrap.servers": bootstrap(), "group.id": f"test-{uuid.uuid4().hex[:6]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    rows, deadline = [], time.monotonic() + timeout
    while time.monotonic() < deadline:
        m = c.poll(0.5)
        if m is not None and not m.error() and m.value():
            rows.append(json.loads(m.value()))
    c.close()
    return rows


def tail(text: str, n: int = 20) -> str:
    return "\n".join(text.strip().splitlines()[-n:])
