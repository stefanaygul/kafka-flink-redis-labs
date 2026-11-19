#!/usr/bin/env python3
"""Gün 2 · İşlem üretici.

Canlı trafik (bank.transactions; event time = şimdi):
    uv run python producer.py                         # 300 müşteri, saniyede 10 işlem, Ctrl+C'ye kadar
    uv run python producer.py --rate 20 --duration 60

Deterministik senaryo (lab.scenario-transactions; sabit test günü 2026-11-14, olay saatleri dosyada):
    uv run python producer.py --scenario watermark    # duraklarda Enter bekler
    uv run python producer.py --scenario late-events
"""
from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

from confluent_kafka import Producer

from common import LAB_DIR, SCENARIO_TOPIC, TOPIC, Transaction, TransactionGenerator, bootstrap, log


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": bootstrap(), "acks": "all", "enable.idempotence": True,
                     "linger.ms": 5, "partitioner": "murmur2_random"})


def run_live(args: argparse.Namespace) -> int:
    excluded = {c.strip() for c in args.exclude.split(",") if c.strip()}
    gen = TransactionGenerator(args.customers, args.seed, risky=not args.no_risky, excluded=excluded)
    producer, failed = make_producer(), []
    on_delivery = lambda err, msg: failed.append(err) if err else None  # noqa: E731
    total = int(args.rate * args.duration) if args.duration else None
    log(f"Canlı trafik → {args.topic}  ({args.rate:g}/sn, {len(gen.customers)} müşteri"
        + (f", hariç: {','.join(sorted(excluded))}" if excluded else "")
        + (f", {args.duration:g} sn" if args.duration else ", Ctrl+C ile durdurun") + ")", "cyan")
    sent, start, next_report = 0, time.monotonic(), 5.0
    try:
        while total is None or sent < total:
            elapsed = time.monotonic() - start
            due = int(elapsed * args.rate) if total is None else min(int(elapsed * args.rate), total)
            while sent < due:
                txn = gen.next()
                producer.produce(args.topic, key=txn.customer_id, value=txn.to_json(), on_delivery=on_delivery)
                sent += 1
            producer.poll(0)
            if elapsed >= next_report:
                log(f"gönderilen={sent} hata={len(failed)}")
                next_report += 5
            time.sleep(0.02)
    except KeyboardInterrupt:
        pass
    remaining = producer.flush(30)
    ok = not failed and remaining == 0
    log(f"SUMMARY sent={sent} failed={len(failed)} undelivered={remaining}", "green" if ok else "red")
    return 0 if ok else 1


def run_scenario(args: argparse.Namespace) -> int:
    path = LAB_DIR / "scenarios" / f"{args.scenario}.json"
    if not path.is_file():
        log(f"Senaryo yok: {args.scenario}. Mevcut: {', '.join(p.stem for p in (LAB_DIR / 'scenarios').glob('*.json'))}",
            "red")
        return 2
    scenario = json.loads(path.read_text(encoding="utf-8"))
    day = datetime.strptime(scenario["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    interactive = sys.stdin.isatty() and not args.no_wait
    producer = make_producer()
    log(f"Senaryo '{args.scenario}' → {args.topic}  (test günü {scenario['date']} UTC)", "cyan")
    log(scenario["description"])
    for step in scenario["steps"]:
        if "say" in step:
            print()
            log(step["say"], "cyan")
        if "pause" in step:
            log(f"⏸  {step['pause']}", "yellow")
            if interactive:
                input("   Devam etmek için Enter...")
            else:
                time.sleep(step.get("seconds", 3))
        if "id" in step:
            h, m, s = (int(x) for x in step["time"].split(":"))
            event_ms = int(day.replace(hour=h, minute=m, second=s).timestamp() * 1000)
            customer = step["customer"]
            txn = Transaction(step["id"], customer, f"A-{customer[2:]}-01", Decimal(str(step["amount"])), "TRY",
                              "GROCERY", "POS", event_ms)
            result = {}
            producer.produce(args.topic, key=customer, value=txn.to_json(),
                             on_delivery=lambda err, msg: result.update(err=err, msg=msg))
            producer.flush(10)  # sırayla, birer birer: her olay yazıldıktan sonra sonrakine geç
            if result.get("err"):
                log(f"Teslim edilemedi: {result['err'].str()}", "red")
                return 1
            note = f"   ({step['note']})" if step.get("note") else ""
            log(f"  {step['id']:<6} {customer}  event time {step['time']}  {step['amount']:>8.2f} TL{note}")
    print()
    log("Senaryo bitti.", "green")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Gün 2 işlem üretici")
    p.add_argument("--rate", type=float, default=10, help="saniyedeki işlem (varsayılan 10)")
    p.add_argument("--duration", type=float, default=0, help="süre, saniye (0 = Ctrl+C'ye kadar)")
    p.add_argument("--customers", type=int, default=300, help="müşteri sayısı: C-1001… (varsayılan 300)")
    p.add_argument("--exclude", default="", help="bu müşterilere işlem üretme (ör. C-1007)")
    p.add_argument("--no-risky", action="store_true", help="riskli müşteri trafiği üretme")
    p.add_argument("--seed", type=int)
    p.add_argument("--scenario", help="deterministik senaryo: watermark | late-events")
    p.add_argument("--topic", help=f"hedef topic (canlı: {TOPIC}, senaryo: {SCENARIO_TOPIC})")
    p.add_argument("--no-wait", action="store_true", help="senaryoda Enter bekleme, kısa süre bekle")
    args = p.parse_args(argv)
    signal.signal(signal.SIGTERM, signal.default_int_handler)
    if args.scenario:
        args.topic = args.topic or SCENARIO_TOPIC
        return run_scenario(args)
    args.topic = args.topic or TOPIC
    return run_live(args)


if __name__ == "__main__":
    sys.exit(main())
