"""Birim testleri: Kafka/Flink gerekmez."""
from __future__ import annotations

import json
from collections import Counter

from common import LAB_DIR, RISKY_CUSTOMERS, TransactionGenerator


def test_senaryo_dosyalari_gecerli():
    for name in ("watermark", "late-events"):
        data = json.loads((LAB_DIR / "scenarios" / f"{name}.json").read_text(encoding="utf-8"))
        events = [s for s in data["steps"] if "id" in s]
        assert events, name
        assert all({"customer", "time", "amount"} <= set(e) for e in events)


def test_canli_trafikte_riskli_musteriler_buyuk_ve_sik():
    gen = TransactionGenerator(customers=300, seed=1)
    txns = [gen.next() for _ in range(5000)]
    counts = Counter(t.customer_id for t in txns)
    risky_avg = sum(counts[c] for c in RISKY_CUSTOMERS) / 3
    normal_avg = sum(n for c, n in counts.items() if c not in RISKY_CUSTOMERS) / (len(counts) - 3)
    assert risky_avg > 10 * normal_avg
    risky_amount = sum(float(t.amount) for t in txns if t.customer_id in RISKY_CUSTOMERS) / sum(
        counts[c] for c in RISKY_CUSTOMERS)
    assert risky_amount > 1500


def test_mesaj_bicimi_gun1_ile_ayni():
    txn = TransactionGenerator(seed=2).next()
    assert list(json.loads(txn.to_json())) == ["transactionId", "customerId", "accountId", "amount", "currency",
                                               "merchantCategory", "channel", "eventTime"]
