"""Feature modeli ve risk skoru kuralları (saf Python: Kafka/Redis bilmez, kolay test edilir)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal

# Bir feature'ın penceresi bu kadar önce bittiyse "bayat" sayılır. Normalde her dakika yeni pencere gelir
# (en fazla ~75 sn yaşında); 2 dk'dan eskiyse pipeline (Flink veya worker) durmuş ya da geride kalmıştır.
STALE_AFTER_MS = 2 * 60_000


@dataclass(frozen=True)
class Features:
    """Flink'in bank.customer-features'a yazdığı, müşteri başına son 5 dakikanın özeti."""
    customer_id: str
    window_start: int        # epoch ms
    window_end: int          # epoch ms: SÜRÜM olarak kullanılır (büyük olan yenidir)
    txn_count: int
    total_amount: Decimal
    max_amount: Decimal
    distinct_categories: int

    @classmethod
    def from_json(cls, raw: bytes | str) -> "Features":
        d = json.loads(raw, parse_float=Decimal)
        return cls(str(d["customerId"]), int(d["windowStart"]), int(d["windowEnd"]), int(d["txnCount"]),
                   Decimal(str(d["totalAmount"])), Decimal(str(d["maxAmount"])), int(d["distinctCategories"]))

    def to_redis(self) -> dict[str, str]:
        return {"windowStart": str(self.window_start), "windowEnd": str(self.window_end),
                "txnCount": str(self.txn_count), "totalAmount": str(self.total_amount),
                "maxAmount": str(self.max_amount), "distinctCategories": str(self.distinct_categories)}

    @classmethod
    def from_redis(cls, customer_id: str, h: dict[str, str]) -> "Features":
        return cls(customer_id, int(h["windowStart"]), int(h["windowEnd"]), int(h["txnCount"]),
                   Decimal(h["totalAmount"]), Decimal(h["maxAmount"]), int(h["distinctCategories"]))


# (açıklama, koşul, puan)
RULES = [
    ("5 dk toplam ≥ 20.000 TL", lambda f: f.total_amount >= 20_000, 40),
    ("tek işlem ≥ 10.000 TL", lambda f: f.max_amount >= 10_000, 30),
    ("5 dk'da ≥ 10 işlem", lambda f: f.txn_count >= 10, 20),
    ("≥ 5 farklı kategori", lambda f: f.distinct_categories >= 5, 10),
]


@dataclass(frozen=True)
class Score:
    score_id: str            # müşteri:windowEnd → aynı pencere için aynı kimlik (idempotency anahtarı)
    customer_id: str
    window_end: int
    score: int
    level: str
    reasons: list[str]

    def to_json(self) -> str:
        return json.dumps({"scoreId": self.score_id, "customerId": self.customer_id, "windowEnd": self.window_end,
                           "score": self.score, "level": self.level, "reasons": self.reasons},
                          ensure_ascii=False, separators=(",", ":"))


def score(f: Features) -> Score:
    hits = [(text, points) for text, rule, points in RULES if rule(f)]
    total = sum(points for _, points in hits)
    level = "HIGH" if total >= 60 else "MEDIUM" if total >= 30 else "LOW"
    return Score(f"{f.customer_id}:{f.window_end}", f.customer_id, f.window_end, total, level,
                 [text for text, _ in hits])


def is_stale(f: Features, now_ms: int) -> bool:
    return now_ms - f.window_end > STALE_AFTER_MS
