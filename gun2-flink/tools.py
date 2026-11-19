#!/usr/bin/env python3
"""Gün 2 yardımcı araçları.

    uv run python tools.py profile C-1001 HIGH          # müşterinin risk kategorisini değiştir (temporal join)
    uv run python tools.py features [--customer C-1007] # bank.customer-features'taki son feature'lar
    uv run python tools.py jobs                         # Flink job'ları
    uv run python tools.py cancel <job-adı veya id>     # job'ı durdur
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from common import FEATURES_TOPIC, PROFILES_TOPIC, bootstrap, flink_url, log


def utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def cmd_profile(args: argparse.Namespace) -> int:
    category = args.category.upper()
    if category not in ("LOW", "MEDIUM", "HIGH"):
        log("Kategori LOW, MEDIUM veya HIGH olmalı.", "red")
        return 2
    now = int(time.time() * 1000)
    value = json.dumps({"customerId": args.customer, "riskCategory": category, "segment": "RETAIL", "updatedAt": now})
    p = Producer({"bootstrap.servers": bootstrap(), "acks": "all"})
    p.produce(PROFILES_TOPIC, key=args.customer, value=value)
    if p.flush(10):
        log("Profil yazılamadı.", "red")
        return 1
    log(f"{args.customer} → {category}, geçerlilik başlangıcı {utc(now)} UTC", "green")
    log("Bu andan ÖNCEKİ event time'a sahip işlemler eski kategoriyle, SONRAKİLER yeni kategoriyle eşleşir.")
    return 0


def read_latest(topic: str, timeout: float = 15, newest_by: str | None = None) -> dict[str, dict]:
    """Topic'i baştan sona okur; her key'in son değerini (newest_by verilirse o alanı en büyük olanı) döndürür."""
    admin = AdminClient({"bootstrap.servers": bootstrap(), "log_level": 0})
    parts = sorted(admin.list_topics(topic=topic, timeout=10).topics[topic].partitions)
    c = Consumer({"bootstrap.servers": bootstrap(), "group.id": f"tools-{uuid.uuid4().hex[:6]}",
                  "enable.auto.commit": False, "enable.partition.eof": True})
    c.assign([TopicPartition(topic, p, 0) for p in parts])
    latest, eof, deadline = {}, set(), time.monotonic() + timeout
    while len(eof) < len(parts) and time.monotonic() < deadline:
        m = c.poll(0.5)
        if m is None:
            continue
        if m.error():
            if m.error().code() == KafkaError._PARTITION_EOF:
                eof.add(m.partition())
            continue
        if m.value():
            key, value = m.key().decode(), json.loads(m.value())
            if newest_by is None or key not in latest or value[newest_by] >= latest[key][newest_by]:
                latest[key] = value
    c.close()
    return latest


def cmd_features(args: argparse.Namespace) -> int:
    # Her müşterinin EN YENİ penceresi (en büyük windowEnd). Topic'teki son kayıt her zaman en yenisi
    # değildir: Flink çökmeden sonra son checkpoint'ten tekrar işlerken bazı pencereleri ikinci kez yazar.
    rows = read_latest(FEATURES_TOPIC, newest_by="windowEnd")
    if args.customer:
        rows = {k: v for k, v in rows.items() if k == args.customer}
    if not rows:
        log("Henüz feature yok. Feature job çalışıyor mu (make jobs) ve producer açık mı?", "yellow")
        return 1
    print(f"  {'müşteri':<8} {'pencere (UTC)':<27} {'işlem':>6} {'toplam TL':>12} {'en büyük TL':>12} {'kategori sayısı':>16}")
    top = sorted(rows.values(), key=lambda f: float(f["totalAmount"]), reverse=True)[: args.top]
    for f in top:
        window = f"{utc(f['windowStart'])[11:16]}–{utc(f['windowEnd'])[11:16]} {utc(f['windowEnd'])[:10]}"
        print(f"  {f['customerId']:<8} {window:<27} {f['txnCount']:>6} {float(f['totalAmount']):>12,.2f} "
              f"{float(f['maxAmount']):>12,.2f} {f['distinctCategories']:>16}")
    log(f"{len(rows)} müşterinin son feature'ı (toplam tutara göre ilk {min(args.top, len(rows))}).")
    return 0


def flink(path: str, method: str = "GET") -> dict:
    req = urllib.request.Request(flink_url() + path, method=method)
    with urllib.request.urlopen(req, timeout=10) as resp:
        body = resp.read()
    return json.loads(body) if body else {}


def cmd_jobs(_args: argparse.Namespace) -> int:
    jobs = flink("/jobs/overview")["jobs"]
    running = [j for j in jobs if j["state"] in ("RUNNING", "RESTARTING", "CREATED", "INITIALIZING")]
    if not running:
        log("Çalışan job yok.")
    for j in running:
        print(f"  {j['state']:<10} {j['jid']}  {j['name'][:70]}")
    return 0


def cmd_cancel(args: argparse.Namespace) -> int:
    jobs = [j for j in flink("/jobs/overview")["jobs"] if j["state"] == "RUNNING"
            and (args.job in ("all", j["jid"]) or j["name"] == args.job
                 or (args.job == "select" and j["name"].startswith(("SELECT", "collect"))))]
    for j in jobs:
        flink(f"/jobs/{j['jid']}?mode=cancel", method="PATCH")
        log(f"durduruldu: {j['name'][:60]}", "yellow")
    if not jobs:
        log("Eşleşen çalışan job yok.")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Gün 2 araçları")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("profile", help="müşterinin risk kategorisini değiştir")
    s.add_argument("customer")
    s.add_argument("category")
    s.set_defaults(func=cmd_profile)
    s = sub.add_parser("features", help="son feature'lar")
    s.add_argument("--customer")
    s.add_argument("--top", type=int, default=10)
    s.set_defaults(func=cmd_features)
    sub.add_parser("jobs", help="çalışan Flink job'ları").set_defaults(func=cmd_jobs)
    s = sub.add_parser("cancel", help="job durdur (ad, id, select = açık sorgular, all = hepsi)")
    s.add_argument("job")
    s.set_defaults(func=cmd_cancel)
    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
