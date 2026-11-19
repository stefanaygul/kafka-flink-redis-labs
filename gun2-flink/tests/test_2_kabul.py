"""Kabul testleri: gerçek Kafka + Flink. Testler kendi geçici topic'lerini kullanır; ders topic'lerine dokunmaz."""
from __future__ import annotations

import subprocess
import time
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic

from common import LAB_DIR, Transaction, bootstrap
from conftest import cancel, flink, read_topic, run_sql, wait_running

DAY = datetime(2026, 11, 14, tzinfo=timezone.utc)


def at(hms: str) -> int:
    h, m, s = (int(x) for x in hms.split(":"))
    return int(DAY.replace(hour=h, minute=m, second=s).timestamp() * 1000)


def send(topic: str, events: list[tuple[str, str, str, float]]) -> None:
    """(id, müşteri, saat, tutar) olaylarını sırayla gönderir."""
    p = Producer({"bootstrap.servers": bootstrap(), "acks": "all"})
    for txn_id, customer, hms, amount in events:
        t = Transaction(txn_id, customer, f"A-{customer[2:]}-01", Decimal(str(amount)), "TRY", "GROCERY", "POS", at(hms))
        p.produce(topic, key=customer, value=t.to_json())
    assert p.flush(10) == 0


@pytest.fixture
def topics(admin, run_id):
    names = [f"test.{kind}.{run_id}" for kind in ("in", "profiles", "out")]
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


def scenario_table(name: str, topic: str, watermark: str) -> str:
    return f"""
CREATE TABLE {name} (
  transactionId STRING, customerId STRING, accountId STRING, amount DECIMAL(12, 2), currency STRING,
  merchantCategory STRING, channel STRING, eventTime BIGINT,
  event_time AS TO_TIMESTAMP_LTZ(eventTime, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL {watermark}
) WITH ('connector' = 'kafka', 'topic' = '{topic}', 'properties.bootstrap.servers' = 'kafka:29092',
        'properties.group.id' = 'test', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json');
"""


def test_ortam_flink_kafka_connector_topicler(admin):
    overview = flink("/overview")
    assert overview["flink-version"] == "1.20.5"
    assert overview["slots-total"] >= 2
    jars = subprocess.run(["docker", "compose", "exec", "-T", "jobmanager", "ls", "/opt/flink/lib"],
                          capture_output=True, text=True, cwd=LAB_DIR).stdout
    assert "flink-sql-connector-kafka-3.4.0-1.20.jar" in jars
    topics = admin.list_topics(timeout=10).topics
    for t in ("bank.transactions", "lab.scenario-transactions", "bank.customer-profiles", "bank.customer-features"):
        assert t in topics, f"{t} yok → make up"


def test_hop_penceresi_watermark_gecince_yazilir_gec_olay_atilir(topics, run_id):
    t_in, _, t_out = topics
    sql = f"""
SET 'pipeline.name' = 'test-hop-{run_id}';
{scenario_table('t_in', t_in, "'1' MINUTE")}
CREATE TABLE t_out (customerId STRING, windowStart STRING, windowEnd STRING, islem BIGINT, toplam DECIMAL(14, 2))
WITH ('connector' = 'kafka', 'topic' = '{t_out}', 'properties.bootstrap.servers' = 'kafka:29092', 'format' = 'json');
INSERT INTO t_out
SELECT customerId, CAST(window_start AS STRING), CAST(window_end AS STRING), COUNT(*), CAST(SUM(amount) AS DECIMAL(14, 2))
FROM TABLE(HOP(TABLE t_in, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '5' MINUTES))
GROUP BY customerId, window_start, window_end;
"""
    out = run_sql(sql)
    assert "Job ID" in out, out[-2000:]
    jid = wait_running(f"test-hop-{run_id}")
    try:
        # W1..W7: her dakika bir işlem (scenarios/watermark.json ile aynı)
        send(t_in, [(f"W{i}", "C-2001", f"11:0{i - 1}:30", 10 * i) for i in range(1, 8)])
        rows = read_topic(t_out, expect=5)
        got = [(r["windowStart"][11:16], r["windowEnd"][11:16], r["islem"], float(r["toplam"])) for r in rows]
        assert got == [("10:56", "11:01", 1, 10.0), ("10:57", "11:02", 2, 30.0), ("10:58", "11:03", 3, 60.0),
                       ("10:59", "11:04", 4, 100.0), ("11:00", "11:05", 5, 150.0)], got

        # Watermark artık 11:05:30. 10:59'daki olay geç kaldı: içinde olduğu pencerelerin hepsi kapandı → atılır.
        send(t_in, [("LATE", "C-2001", "10:59:00", 1000), ("W8", "C-2001", "11:08:00", 80)])
        rows = read_topic(t_out, expect=7)
        got = [(r["windowEnd"][11:16], r["islem"], float(r["toplam"])) for r in rows[5:]]
        assert got == [("11:06", 5, 200.0), ("11:07", 5, 250.0)], f"geç olay (1000 TL) sayılmamalıydı: {got}"
    finally:
        cancel(jid)


def test_temporal_join_islem_anindaki_kategoriyi_kullanir(topics, run_id):
    t_in, t_profiles, t_out = topics
    sql = f"""
SET 'pipeline.name' = 'test-join-{run_id}';
{scenario_table('t_in', t_in, "'0' SECOND")}
CREATE TABLE t_profiles (
  customerId STRING, riskCategory STRING, segment STRING, updatedAt BIGINT,
  update_time AS TO_TIMESTAMP_LTZ(updatedAt, 3),
  PRIMARY KEY (customerId) NOT ENFORCED,
  WATERMARK FOR update_time AS update_time
) WITH ('connector' = 'upsert-kafka', 'topic' = '{t_profiles}', 'properties.bootstrap.servers' = 'kafka:29092',
        'key.format' = 'raw', 'value.format' = 'json');
CREATE TABLE t_out (transactionId STRING, riskCategory STRING)
WITH ('connector' = 'kafka', 'topic' = '{t_out}', 'properties.bootstrap.servers' = 'kafka:29092', 'format' = 'json');
INSERT INTO t_out
SELECT t.transactionId, p.riskCategory
FROM t_in AS t
LEFT JOIN t_profiles FOR SYSTEM_TIME AS OF t.event_time AS p ON t.customerId = p.customerId;
"""
    out = run_sql(sql)
    assert "Job ID" in out, out[-2000:]
    jid = wait_running(f"test-join-{run_id}")
    try:
        p = Producer({"bootstrap.servers": bootstrap(), "acks": "all"})
        for category, hms in (("LOW", "10:00:00"), ("HIGH", "10:05:00")):
            p.produce(t_profiles, key="C-3001", value=f'{{"customerId":"C-3001","riskCategory":"{category}",'
                                                       f'"segment":"RETAIL","updatedAt":{at(hms)}}}')
        assert p.flush(10) == 0
        time.sleep(2)
        send(t_in, [("J1", "C-3001", "10:02:00", 100), ("J2", "C-3001", "10:07:00", 100),
                    ("J3", "C-3001", "10:20:00", 1)])
        rows = {r["transactionId"]: r["riskCategory"] for r in read_topic(t_out, expect=2)}
        assert rows.get("J1") == "LOW", rows   # 10:02'de geçerli kategori
        assert rows.get("J2") == "HIGH", rows  # 10:05'te HIGH oldu
    finally:
        cancel(jid)
