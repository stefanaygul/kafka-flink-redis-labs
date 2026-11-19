"""Test yardımcıları. Kafka'ya ulaşılamıyorsa testler tek, açık bir mesajla durur."""
from __future__ import annotations

import subprocess
import sys
import time
import uuid

import pytest
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient

from common import LAB_DIR, bootstrap


@pytest.fixture(scope="session")
def admin() -> AdminClient:
    client = AdminClient({"bootstrap.servers": bootstrap(), "log_level": 0})
    try:
        md = client.list_topics(timeout=8)
    except Exception:  # noqa: BLE001
        pytest.exit(f"\n\n  Kafka'ya ulaşılamıyor ({bootstrap()}).  Önce:  make up\n", returncode=2)
    if len(md.brokers) < 3:
        pytest.exit(f"\n\n  Cluster'da {len(md.brokers)} broker çalışıyor, 3 gerekli.  Kontrol:  make brokers"
                    "  →  make broker-start N=<n>\n", returncode=2)
    return client


@pytest.fixture
def run_id() -> str:
    return time.strftime("%H%M%S") + "-" + uuid.uuid4().hex[:6]


def run_app(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    """producer.py / consumer.py'yi öğrencinin çalıştırdığı gibi ayrı bir süreç olarak çalıştırır."""
    try:
        return subprocess.run([sys.executable, *args], cwd=LAB_DIR, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        out = exc.stdout.decode() if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        pytest.fail(f"{' '.join(args)} {timeout} sn içinde bitmedi:\n{tail(out)}")


def tail(text: str, n: int = 20) -> str:
    return "\n".join(text.strip().splitlines()[-n:])


def read_all(admin: AdminClient, topic: str, timeout: float = 20) -> list:
    """Topic'teki bütün mesajları baştan sona okur."""
    parts = sorted(admin.list_topics(topic=topic, timeout=10).topics[topic].partitions)
    c = Consumer({"bootstrap.servers": bootstrap(), "group.id": f"test-reader-{uuid.uuid4().hex[:6]}",
                  "enable.auto.commit": False, "enable.partition.eof": True})
    c.assign([TopicPartition(topic, p, 0) for p in parts])
    messages, eof, deadline = [], set(), time.monotonic() + timeout
    while len(eof) < len(parts) and time.monotonic() < deadline:
        m = c.poll(0.5)
        if m is None:
            continue
        if m.error():
            if m.error().code() == KafkaError._PARTITION_EOF:
                eof.add(m.partition())
            continue
        messages.append(m)
    c.close()
    return messages
