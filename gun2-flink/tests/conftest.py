"""Test yardımcıları. Kafka veya Flink'e ulaşılamıyorsa testler tek, açık bir mesajla durur."""
from __future__ import annotations

import json
import subprocess
import time
import urllib.request
import uuid

import pytest
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient

from common import LAB_DIR, bootstrap, flink_url


def flink(path: str, method: str = "GET") -> dict:
    req = urllib.request.Request(flink_url() + path, method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        body = resp.read()
    return json.loads(body) if body else {}


@pytest.fixture(scope="session")
def admin() -> AdminClient:
    client = AdminClient({"bootstrap.servers": bootstrap(), "log_level": 0})
    try:
        client.list_topics(timeout=8)
        overview = flink("/overview")
    except Exception:  # noqa: BLE001
        pytest.exit(f"\n\n  Kafka ({bootstrap()}) veya Flink ({flink_url()}) yanıt vermiyor.  Önce:  make up\n",
                    returncode=2)
    if overview["taskmanagers"] < 1:
        pytest.exit("\n\n  Flink'te TaskManager yok.  Kontrol:  make ps\n", returncode=2)
    return client


@pytest.fixture
def run_id() -> str:
    return time.strftime("%H%M%S") + uuid.uuid4().hex[:4]


def run_sql(sql: str) -> str:
    """SQL'i tablolar yüklü bir SQL client'ta çalıştırır (INSERT INTO → arka planda job)."""
    name = f"/tmp/test-{uuid.uuid4().hex[:8]}.sql"
    subprocess.run(["docker", "compose", "exec", "-T", "jobmanager", "bash", "-c", f"cat > {name}"],
                   input=sql, text=True, cwd=LAB_DIR, check=True)
    out = subprocess.run(["docker", "compose", "exec", "-T", "jobmanager", "./bin/sql-client.sh",
                          "-i", "/opt/sql/00-tables.sql", "-f", name],
                         capture_output=True, text=True, cwd=LAB_DIR, timeout=120)
    return out.stdout + out.stderr


def job_state(name: str) -> tuple[str | None, str | None]:
    for j in flink("/jobs/overview")["jobs"]:
        if j["name"] == name and j["state"] not in ("CANCELED", "FINISHED", "FAILED"):
            return j["jid"], j["state"]
    return None, None


def wait_running(name: str, timeout: float = 60) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        jid, state = job_state(name)
        if state == "RUNNING":
            return jid
        time.sleep(1)
    pytest.fail(f"'{name}' job'ı {timeout:.0f} sn içinde RUNNING olmadı → {flink_url()}")


def cancel(jid: str) -> None:
    try:
        flink(f"/jobs/{jid}?mode=cancel", method="PATCH")
    except Exception:  # noqa: BLE001
        pass


def read_topic(topic: str, expect: int, timeout: float = 90) -> list[dict]:
    """Topic'ten en az `expect` mesaj gelene (veya süre dolana) kadar okur."""
    c = Consumer({"bootstrap.servers": bootstrap(), "group.id": f"test-{uuid.uuid4().hex[:6]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    rows, deadline = [], time.monotonic() + timeout
    while len(rows) < expect and time.monotonic() < deadline:
        m = c.poll(0.5)
        if m is not None and not m.error() and m.value():
            rows.append(json.loads(m.value()))
    c.close()
    return rows
