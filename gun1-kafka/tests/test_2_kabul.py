"""Kabul testleri: gerçek cluster. Testler kendi geçici topic'lerini kullanır; bank.transactions'a dokunmaz."""
from __future__ import annotations

import json
import re
import sqlite3
import time
from decimal import Decimal

import pytest
from confluent_kafka import ConsumerGroupTopicPartitions, Producer, TopicPartition
from confluent_kafka.admin import ConfigResource, NewTopic, OffsetSpec, ResourceType

from common import TOPIC, Transaction, bootstrap
from conftest import read_all, run_app, tail


def wait_topic_ready(admin, name: str, partitions: int, isr: int, timeout: float = 30) -> None:
    """Yeni topic'in bütün partition'larının leader'ı ve tam ISR'ı olana kadar bekler (metadata yayılması)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        md = admin.list_topics(topic=name, timeout=10).topics.get(name)
        if md and md.error is None and len(md.partitions) == partitions and all(
                p.leader >= 0 and len(p.isrs) >= isr for p in md.partitions.values()):
            return
        time.sleep(0.5)
    pytest.fail(f"{name} {timeout:.0f} sn içinde hazır olmadı → make describe")


@pytest.fixture
def temp_topic(admin, run_id):
    name = f"test.{run_id}"
    admin.create_topics([NewTopic(name, 6, 3, config={"min.insync.replicas": "2"})])[name].result()
    wait_topic_ready(admin, name, 6, 3)
    yield name
    admin.delete_topics([name])[name].result()


def test_cluster_3_broker_ve_bank_transactions_hazir(admin):
    md = admin.list_topics(timeout=10)
    assert sorted(md.brokers) == [1, 2, 3]
    topic = md.topics.get(TOPIC)
    assert topic is not None, "bank.transactions yok → make up (kafka-init topic'leri oluşturur)"
    assert len(topic.partitions) == 6
    assert all(len(p.replicas) == 3 for p in topic.partitions.values()), "replication factor 3 olmalı"
    # Cluster yeni açıldıysa takipçiler ISR'ye birkaç saniyede katılır.
    wait_topic_ready(admin, TOPIC, 6, 3)
    configs = admin.describe_configs([ConfigResource(ResourceType.TOPIC, TOPIC)])
    entries = next(iter(configs.values())).result()
    assert entries["min.insync.replicas"].value == "2"


def test_producer_tum_mesajlar_teslim_key_customer_id(admin, temp_topic):
    run = run_app("producer.py", "--topic", temp_topic, "--rate", "100", "--duration", "3", "--customers", "12",
                  "--seed", "7")
    assert run.returncode == 0, tail(run.stdout + run.stderr)
    assert "SUMMARY delivered=300 failed=0 undelivered=0" in run.stdout, tail(run.stdout)

    messages = read_all(admin, temp_topic)
    assert len(messages) == 300
    assert all(m.key().decode() == json.loads(m.value())["customerId"] for m in messages)
    partitions_by_key: dict[bytes, set[int]] = {}
    for m in messages:
        partitions_by_key.setdefault(m.key(), set()).add(m.partition())
    assert all(len(ps) == 1 for ps in partitions_by_key.values()), "bir müşterinin işlemleri tek partition'da olmalı"


def test_consumer_coker_kaldigi_yerden_devam_eder_tekrar_is_sonucunu_degistirmez(admin, temp_topic, run_id, tmp_path):
    # Bilinen 120 işlem
    p = Producer({"bootstrap.servers": bootstrap(), "acks": "all", "partitioner": "murmur2_random"})
    seeded = []
    for i in range(120):
        customer = f"C-{1001 + i % 12}"
        t = Transaction(f"TEST-{run_id}-{i:03d}", customer, f"A-{customer[2:]}-01", Decimal(10 + i), "TRY",
                        "GROCERY", "POS", int(time.time() * 1000))
        seeded.append(t)
        p.produce(temp_topic, key=customer, value=t.to_json())
    assert p.flush(30) == 0

    db, group = tmp_path / "ledger.db", f"test-{run_id}"
    common_args = ("consumer.py", "--topic", temp_topic, "--group", group, "--db", str(db))

    crash = run_app(*common_args, "--instance", "test-a", "--crash-after", "40")
    assert crash.returncode == 99, f"40. mesajdan sonra çökmesi bekleniyordu:\n{tail(crash.stdout)}"

    again = run_app(*common_args, "--instance", "test-b", "--exit-when-idle", "6")
    assert again.returncode == 0, tail(again.stdout + again.stderr)
    m = re.search(r"processed=(\d+) applied=(\d+) duplicates=(\d+)", again.stdout)
    assert m, tail(again.stdout)
    assert int(m.group(3)) >= 1, "çökmeden önce işlenen kayıtlar tekrar gelmeliydi (at-least-once)"

    with sqlite3.connect(db) as conn:
        q = lambda sql: conn.execute(sql).fetchone()[0]  # noqa: E731
        entries, distinct = q("SELECT COUNT(*) FROM ledger_entries"), q("SELECT COUNT(DISTINCT transaction_id) FROM ledger_entries")
        balance = q("SELECT SUM(balance_minor) FROM account_balances")
    assert entries == distinct, f"{entries - distinct} işlem iki kez uygulanmış (idempotency, LAB 5)"
    assert distinct == len(seeded), f"{len(seeded) - distinct} işlem kayıp (offset store sırası, LAB 4)"
    assert balance == sum(t.amount_minor for t in seeded)

    # Kapanışta lag = 0: commit edilen offset'ler topic sonunda
    tps = [TopicPartition(temp_topic, i) for i in range(6)]
    committed = {tp.partition: tp.offset for tp in admin.list_consumer_group_offsets(
        [ConsumerGroupTopicPartitions(group, tps)])[group].result().topic_partitions}
    ends = {tp.partition: f.result().offset for tp, f in admin.list_offsets({tp: OffsetSpec.latest() for tp in tps}).items()}
    lagging = {p: (committed.get(p), e) for p, e in ends.items() if e > 0 and committed.get(p) != e}
    assert not lagging, f"lag > 0 (partition: (commit, son)): {lagging}"
