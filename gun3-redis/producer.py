#!/usr/bin/env python3
"""Gün 3 · Canlı işlem trafiği (bank.transactions). Flink feature job'ı bunu özetler.

    uv run python producer.py                       # 300 müşteri, saniyede 10 işlem, Ctrl+C'ye kadar
    uv run python producer.py --exclude C-1007      # C-1007'ye işlem üretme
"""
from __future__ import annotations

import argparse
import random
import signal
import sys
import time

from confluent_kafka import Producer

from common import RISKY_CUSTOMERS, TRANSACTIONS_TOPIC, bootstrap, log, transaction_json


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Gün 3 canlı işlem trafiği")
    p.add_argument("--rate", type=float, default=10, help="saniyedeki işlem (varsayılan 10)")
    p.add_argument("--duration", type=float, default=0, help="süre, saniye (0 = Ctrl+C'ye kadar)")
    p.add_argument("--customers", type=int, default=300)
    p.add_argument("--exclude", default="", help="bu müşterilere işlem üretme (ör. C-1007)")
    args = p.parse_args(argv)
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    excluded = {c.strip() for c in args.exclude.split(",") if c.strip()}
    customers = [c for c in (f"C-{1001 + i}" for i in range(args.customers)) if c not in excluded]
    risky = [c for c in RISKY_CUSTOMERS if c not in excluded]
    rng = random.Random()
    producer = Producer({"bootstrap.servers": bootstrap(), "acks": "all", "enable.idempotence": True,
                         "linger.ms": 5, "partitioner": "murmur2_random"})
    log(f"Canlı trafik → {TRANSACTIONS_TOPIC}  ({args.rate:g}/sn, {len(customers)} müşteri"
        + (f", hariç: {','.join(sorted(excluded))}" if excluded else "") + ")", "cyan")
    total = int(args.rate * args.duration) if args.duration else None
    sent, start, next_report = 0, time.monotonic(), 10.0
    try:
        while total is None or sent < total:
            elapsed = time.monotonic() - start
            due = int(elapsed * args.rate) if total is None else min(int(elapsed * args.rate), total)
            while sent < due:
                key, value = transaction_json(customers, risky, rng)
                producer.produce(TRANSACTIONS_TOPIC, key=key, value=value)
                sent += 1
            producer.poll(0)
            if elapsed >= next_report:
                log(f"gönderilen={sent}")
                next_report += 10
            time.sleep(0.02)
    except KeyboardInterrupt:
        pass
    remaining = producer.flush(30)
    log(f"SUMMARY sent={sent} undelivered={remaining}", "green" if remaining == 0 else "red")
    return 0 if remaining == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
