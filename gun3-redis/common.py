"""Ortak kod: adresler, topic adları, işlem üretici, log."""
from __future__ import annotations

import json
import math
import os
import random
import secrets
import sys
import time
from decimal import Decimal
from pathlib import Path

LAB_DIR = Path(__file__).resolve().parent
TRANSACTIONS_TOPIC = "bank.transactions"
FEATURES_TOPIC = "bank.customer-features"
SCORES_TOPIC = "bank.risk-scores"
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


def redis_url() -> str:
    return os.environ.get("REDIS_URL") or f"redis://localhost:{env('REDIS_PORT', '6379')}/0"


def flink_url() -> str:
    return f"http://localhost:{env('FLINK_UI_PORT', '8081')}"


CATEGORIES = ["GROCERY", "FUEL", "RESTAURANT", "ELECTRONICS", "TRAVEL", "ONLINE_RETAIL",
              "CASH_WITHDRAWAL", "UTILITIES"]


def transaction_json(customers: list[str], risky: list[str], rng: random.Random) -> tuple[str, str]:
    """Canlı işlem (key, value). Riskli müşteriler trafiğin %15'i, 6 kat tutar (Gün 2 ile aynı)."""
    now_ms = int(time.time() * 1000)
    is_risky = bool(risky) and rng.random() < 0.15
    customer = rng.choice(risky if is_risky else customers)
    category = rng.choice(CATEGORIES)
    amount = min(max(350 * math.exp(0.9 * rng.gauss(0, 1)) * (6 if is_risky else 1), 5), 60_000)
    return customer, json.dumps({
        "transactionId": f"TXN-{now_ms:012X}{secrets.token_hex(4).upper()}", "customerId": customer,
        "accountId": f"A-{customer[2:]}-01", "amount": float(Decimal(f"{amount:.2f}")), "currency": "TRY",
        "merchantCategory": category,
        "channel": "ATM" if category == "CASH_WITHDRAWAL" else rng.choice(["POS", "ONLINE", "MOBILE"]),
        "eventTime": now_ms,
    }, separators=(",", ":"))


_COLORS = {"green": "32", "yellow": "33", "red": "31", "cyan": "36"}


def log(message: str, color: str | None = None) -> None:
    if color and sys.stdout.isatty():
        message = f"\033[{_COLORS[color]}m{message}\033[0m"
    print(f"{time.strftime('%H:%M:%S')} {message}", flush=True)
