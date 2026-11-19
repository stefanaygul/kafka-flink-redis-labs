"""Birim testleri: Kafka/Redis gerekmez."""
from __future__ import annotations

from decimal import Decimal

from scoring import STALE_AFTER_MS, Features, is_stale, score


def features(total="1000", max_amount="500", count=3, categories=2, window_end=1794651300000) -> Features:
    return Features("C-1001", window_end - 300_000, window_end, count, Decimal(total), Decimal(max_amount), categories)


def test_flink_json_okunur():
    raw = ('{"customerId":"C-1042","windowStart":1794651000000,"windowEnd":1794651300000,"txnCount":69,'
           '"totalAmount":204658.79,"maxAmount":17979.08,"distinctCategories":8}')
    f = Features.from_json(raw)
    assert f.customer_id == "C-1042" and f.total_amount == Decimal("204658.79") and f.txn_count == 69


def test_normal_musteri_low():
    s = score(features())
    assert (s.score, s.level, s.reasons) == (0, "LOW", [])


def test_riskli_musteri_high_ve_gerekceli():
    s = score(features(total="204658.79", max_amount="17979.08", count=69, categories=8))
    assert s.level == "HIGH" and s.score == 100
    assert "5 dk toplam ≥ 20.000 TL" in s.reasons


def test_skor_kimligi_musteri_ve_pencere():
    assert score(features()).score_id == "C-1001:1794651300000"


def test_tazelik():
    f = features(window_end=1_000_000)
    assert not is_stale(f, 1_000_000 + STALE_AFTER_MS)
    assert is_stale(f, 1_000_000 + STALE_AFTER_MS + 1)
