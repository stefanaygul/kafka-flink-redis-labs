"""Birim testleri: Kafka gerekmez. LAB notlarıyla işaretli parçaların doğru çalıştığını kontrol eder."""
from __future__ import annotations

import sqlite3
from decimal import Decimal

import pytest
from confluent_kafka import KafkaError

import consumer
import producer
from common import Transaction


def txn(txn_id: str = "T1", customer: str = "C-1001", amount: str = "100.25") -> Transaction:
    return Transaction(txn_id, customer, f"A-{customer[2:]}-01", Decimal(amount), "TRY", "GROCERY", "POS",
                       1794650410000)


def test_mesaj_bicimi_flink_ile_ayni():
    raw = txn("T1", "C-1042", "350.25").to_json()
    assert raw == ('{"transactionId":"T1","customerId":"C-1042","accountId":"A-1042-01","amount":350.25,'
                   '"currency":"TRY","merchantCategory":"GROCERY","channel":"POS","eventTime":1794650410000}')
    assert Transaction.from_json(raw.encode()) == txn("T1", "C-1042", "350.25")
    with pytest.raises(ValueError):
        Transaction.from_json(b"merhaba")


def test_lab1_key_customer_id():
    key, value = producer.build_message(txn("T1", "C-1042"))
    assert key == "C-1042"
    assert '"transactionId":"T1"' in value


def test_producer_ayarlari_dayanikli():
    config = producer.create_config()
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["partitioner"] == "murmur2_random"


class FakeMessage:
    def __init__(self, partition: int = 0):
        self._partition = partition

    def key(self):
        return b"C-1001"

    def partition(self):
        return self._partition


class FakeProducer:
    """produce() ilk `full_times` çağrıda BufferError fırlatır (yerel kuyruk dolu)."""

    def __init__(self, full_times: int = 0):
        self.full_times, self.calls, self.polls, self.sent = full_times, 0, 0, 0

    def produce(self, *args, **kwargs):
        self.calls += 1
        if self.calls <= self.full_times:
            raise BufferError("Local: Queue full")
        self.sent += 1

    def poll(self, timeout=0):
        self.polls += 1

    def __len__(self):
        return 1000


def test_lab2_teslim_hatasi_sayilir_ve_dagilim_tutulur():
    p = producer.TransactionProducer("t", client=FakeProducer())
    p.on_delivery(KafkaError(KafkaError._MSG_TIMED_OUT), FakeMessage())
    p.on_delivery(None, FakeMessage(partition=3))
    assert (p.failed, p.delivered, p.per_partition[3]) == (1, 1, 1)


def test_lab3_kuyruk_dolunca_cokmez_bekleyip_yeniden_dener():
    fake = FakeProducer(full_times=3)
    p = producer.TransactionProducer("t", client=fake)
    p.send(txn())  # BufferError dışarı sızmamalı
    assert fake.sent == 1 and fake.polls == 3 and p.queue_full_waits == 3


def test_lab4_offset_islemden_sonra_store_edilir():
    config = consumer.create_config("g", "c1")
    assert config["enable.auto.offset.store"] is False
    assert config["enable.auto.commit"] is True


def test_lab5_ayni_islem_ikinci_kez_bakiyeyi_degistirmez(tmp_path):
    db = tmp_path / "ledger.db"
    c1, c2 = consumer.Ledger(db), consumer.Ledger(db)  # rebalance sonrası tekrar başka consumer'a da gelebilir
    assert c1.apply(txn("TXN-1"), "c1", 0, 10) == consumer.Ledger.APPLIED
    assert c1.apply(txn("TXN-1"), "c1", 0, 10) == consumer.Ledger.DUPLICATE
    assert c2.apply(txn("TXN-1"), "c2", 0, 10) == consumer.Ledger.DUPLICATE
    assert c1.apply(txn("TXN-2", amount="15.50"), "c1", 0, 11) == consumer.Ledger.APPLIED
    c1.close()
    c2.close()
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM ledger_entries").fetchone()[0] == 2
        assert conn.execute("SELECT balance_minor FROM account_balances").fetchone()[0] == 10025 + 1550
        assert conn.execute("SELECT COUNT(*) FROM duplicate_deliveries").fetchone()[0] == 2
