"""Ortak kod: bağlantı adresi, mesaj modeli, işlem üretici, renkli log.

bank.transactions mesajı (JSON, Gün 1 ile aynı biçim):

    {"transactionId":"TXN-…","customerId":"C-1007","accountId":"A-1007-01","amount":350.25,
     "currency":"TRY","merchantCategory":"GROCERY","channel":"POS","eventTime":1794650410000}

Key = customerId. eventTime = işlemin gerçekleştiği an, UTC epoch milisaniye.
"""
from __future__ import annotations

import json
import math
import os
import random
import secrets
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

LAB_DIR = Path(__file__).resolve().parent
TOPIC = "bank.transactions"
SCENARIO_TOPIC = "lab.scenario-transactions"
PROFILES_TOPIC = "bank.customer-profiles"
FEATURES_TOPIC = "bank.customer-features"
RISKY_CUSTOMERS = ["C-1007", "C-1042", "C-1113"]


def env(key: str, default: str) -> str:
    if os.environ.get(key):
        return os.environ[key]
    path = LAB_DIR / ".env"
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip().startswith(f"{key}="):
                return line.split("=", 1)[1].strip()
    return default


def bootstrap() -> str:
    return os.environ.get("BOOTSTRAP") or f"localhost:{env('KAFKA_PORT', '9092')}"


def flink_url() -> str:
    return f"http://localhost:{env('FLINK_UI_PORT', '8081')}"


@dataclass(frozen=True)
class Transaction:
    transaction_id: str
    customer_id: str
    account_id: str
    amount: Decimal
    currency: str
    merchant_category: str
    channel: str
    event_time: int

    def to_json(self) -> str:
        return json.dumps({
            "transactionId": self.transaction_id, "customerId": self.customer_id,
            "accountId": self.account_id, "amount": float(self.amount), "currency": self.currency,
            "merchantCategory": self.merchant_category, "channel": self.channel, "eventTime": self.event_time,
        }, separators=(",", ":"))


CATEGORIES = ["GROCERY", "FUEL", "RESTAURANT", "ELECTRONICS", "TRAVEL", "ONLINE_RETAIL",
              "CASH_WITHDRAWAL", "UTILITIES"]


class TransactionGenerator:
    """Canlı trafik: C-1001… müşterileri. Riskli müşteriler (C-1007, C-1042, C-1113) trafiğin %15'ini,
    6 kat büyük tutarlarla yapar; Gün 3'te bunlar yüksek risk skoru alır."""

    def __init__(self, customers: int = 300, seed: int | None = None, risky: bool = True,
                 excluded: set[str] | None = None):
        excluded = excluded or set()
        self.customers = [c for c in (f"C-{1001 + i}" for i in range(customers)) if c not in excluded]
        self.risky = [c for c in RISKY_CUSTOMERS if risky and c not in excluded]
        self._rng = random.Random(seed)

    def next(self) -> Transaction:
        now_ms = int(time.time() * 1000)
        risky = bool(self.risky) and self._rng.random() < 0.15
        customer = self._rng.choice(self.risky if risky else self.customers)
        category = self._rng.choice(CATEGORIES)
        channel = "ATM" if category == "CASH_WITHDRAWAL" else self._rng.choice(["POS", "ONLINE", "MOBILE"])
        amount = 350 * math.exp(0.9 * self._rng.gauss(0, 1)) * (6 if risky else 1)
        amount = min(max(amount, 5), 60_000)
        return Transaction(f"TXN-{now_ms:012X}{secrets.token_hex(4).upper()}", customer, f"A-{customer[2:]}-01",
                           Decimal(f"{amount:.2f}"), "TRY", category, channel, now_ms)


_COLORS = {"green": "32", "yellow": "33", "red": "31", "cyan": "36"}


def log(message: str, color: str | None = None) -> None:
    if color and sys.stdout.isatty():
        message = f"\033[{_COLORS[color]}m{message}\033[0m"
    print(f"{time.strftime('%H:%M:%S')} {message}", flush=True)
