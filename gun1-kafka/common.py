"""Ortak kod: bağlantı adresi, mesaj modeli, işlem üretici, renkli log.

bank.transactions mesajı (JSON, Gün 2'de Flink de aynı biçimi okuyacak):

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
DATA_DIR = LAB_DIR / "data"
TOPIC = "bank.transactions"
GROUP = "bank-ledger"


def _dotenv() -> dict[str, str]:
    values: dict[str, str] = {}
    path = LAB_DIR / ".env"
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip()
    return values


def bootstrap() -> str:
    """Üç broker'ın üçü de listede: biri kapalıyken istemci diğerlerinden cluster'a bağlanır."""
    if os.environ.get("BOOTSTRAP"):
        return os.environ["BOOTSTRAP"]
    env = _dotenv()
    ports = [env.get(f"KAFKA_{n}_PORT", str(9091 + n)) for n in (1, 2, 3)]
    return ",".join(f"localhost:{p}" for p in ports)


# --- Mesaj modeli -------------------------------------------------------------------------------

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
            "transactionId": self.transaction_id,
            "customerId": self.customer_id,
            "accountId": self.account_id,
            "amount": float(self.amount),
            "currency": self.currency,
            "merchantCategory": self.merchant_category,
            "channel": self.channel,
            "eventTime": self.event_time,
        }, separators=(",", ":"))

    @property
    def amount_minor(self) -> int:
        """Kuruş cinsinden tutar (defterde kayan nokta hatası olmasın)."""
        return int((self.amount * 100).to_integral_value())

    @classmethod
    def from_json(cls, raw: bytes | str | None) -> "Transaction":
        """Mesajı çözer. Bozuk mesajda ValueError fırlatır."""
        if not raw:
            raise ValueError("boş mesaj")
        try:
            d = json.loads(raw, parse_float=Decimal)
            return cls(str(d["transactionId"]), str(d["customerId"]), str(d["accountId"]),
                       Decimal(str(d["amount"])), str(d["currency"]), str(d["merchantCategory"]),
                       str(d["channel"]), int(d["eventTime"]))
        except (ValueError, TypeError, KeyError, UnicodeDecodeError) as exc:
            raise ValueError(f"JSON işlem değil: {exc}") from exc


# --- İşlem üretici ------------------------------------------------------------------------------

CATEGORIES = ["GROCERY", "FUEL", "RESTAURANT", "ELECTRONICS", "TRAVEL", "ONLINE_RETAIL",
              "CASH_WITHDRAWAL", "UTILITIES"]


class TransactionGenerator:
    """C-1001, C-1002, … müşterileri için rastgele ama gerçekçi işlemler üretir."""

    def __init__(self, customers: int = 20, seed: int | None = None,
                 hot_customer: str | None = None, hot_share: float = 0.0):
        self.customers = [f"C-{1001 + i}" for i in range(customers)]
        self.hot_customer, self.hot_share = hot_customer, hot_share
        self._rng = random.Random(seed)

    def next(self) -> Transaction:
        now_ms = int(time.time() * 1000)
        if self.hot_customer and self._rng.random() < self.hot_share:
            customer = self.hot_customer
        else:
            customer = self._rng.choice(self.customers)
        category = self._rng.choice(CATEGORIES)
        channel = "ATM" if category == "CASH_WITHDRAWAL" else self._rng.choice(["POS", "ONLINE", "MOBILE"])
        # Log-normal: çoğu işlem küçük, az sayıda büyük işlem.
        amount = min(max(350 * math.exp(0.9 * self._rng.gauss(0, 1)), 5), 60_000)
        return Transaction(f"TXN-{now_ms:012X}{secrets.token_hex(4).upper()}", customer,
                           f"A-{customer[2:]}-01", Decimal(f"{amount:.2f}"), "TRY", category, channel, now_ms)


# --- Log ----------------------------------------------------------------------------------------

_COLORS = {"green": "32", "yellow": "33", "red": "31", "cyan": "36"}


def log(message: str, color: str | None = None) -> None:
    if color and sys.stdout.isatty():
        message = f"\033[{_COLORS[color]}m{message}\033[0m"
    print(f"{time.strftime('%H:%M:%S')} {message}", flush=True)
