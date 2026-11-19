#!/usr/bin/env python3
"""Gün 3 araçları.

    uv run python tools.py feature C-1007        # Redis'ten online skor (ve feature ne kadar taze?)
    uv run python tools.py scores [--customer C-1007]
    uv run python tools.py stats                 # Redis'te kaç müşteri var
    uv run python tools.py rebuild               # Redis'i compacted bank.customer-features'tan yeniden kur
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient

from common import FEATURES_TOPIC, SCORES_TOPIC, bootstrap, log, redis_url
from feature_store import FeatureStore
from scoring import Features, is_stale, score


def utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M:%S")


def read_all(topic: str, timeout: float = 30):
    """Topic'i baştan sona okur (mesajları sırayla verir)."""
    admin = AdminClient({"bootstrap.servers": bootstrap(), "log_level": 0})
    parts = sorted(admin.list_topics(topic=topic, timeout=10).topics[topic].partitions)
    c = Consumer({"bootstrap.servers": bootstrap(), "group.id": f"tools-{uuid.uuid4().hex[:6]}",
                  "enable.auto.commit": False, "enable.partition.eof": True})
    c.assign([TopicPartition(topic, p, 0) for p in parts])
    eof, deadline = set(), time.monotonic() + timeout
    try:
        while len(eof) < len(parts) and time.monotonic() < deadline:
            m = c.poll(0.5)
            if m is None:
                continue
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    eof.add(m.partition())
                continue
            if m.value():
                yield m
    finally:
        c.close()


def cmd_feature(args: argparse.Namespace) -> int:
    store = FeatureStore(redis_url(), args.prefix)
    t0 = time.perf_counter()
    f = store.get(args.customer)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    if f is None:
        log(f"{args.customer}: Redis'te feature yok (hiç işlem yapmadı, TTL doldu veya Redis boşaltıldı).", "yellow")
        return 0
    s, now = score(f), int(time.time() * 1000)
    age_s = (now - f.window_end) / 1000
    print(f"  {args.customer}  pencere {utc(f.window_start)}–{utc(f.window_end)} UTC   (Redis'ten {elapsed_ms:.1f} ms)")
    print(f"  işlem={f.txn_count}  toplam={f.total_amount:,.2f} TL  en büyük={f.max_amount:,.2f} TL  "
          f"farklı kategori={f.distinct_categories}")
    print(f"  skor={s.score}  seviye={s.level}  {', '.join(s.reasons) or '-'}")
    # >>> LAB 4 · tazelik kontrolü
    #     Redis "en son bildiğimiz" değeri verir; ne kadar eski olduğunu söylemez. Pencere 2 dk'dan eskiyse
    #     pipeline (Flink veya worker) durmuş ya da geride kalmıştır. Karar verirken bunu bilmeliyiz.
    #     Olmasaydı: 20 dk önceki "LOW" skoru şimdiki durummuş gibi kullanılırdı.
    if is_stale(f, now):
        log(f"BAYAT: feature {age_s / 60:.1f} dk önce bitmiş bir pencereye ait. Bu skora güvenmeyin.", "yellow")
    else:
        log(f"Taze: pencere {age_s:.0f} sn önce bitti.", "green")
    # <<< LAB 4
    return 0


def cmd_scores(args: argparse.Namespace) -> int:
    latest, ids = {}, Counter()
    for m in read_all(args.topic):
        s = json.loads(m.value())
        ids[s["scoreId"]] += 1
        if args.customer and s["customerId"] != args.customer:
            continue
        if s["customerId"] not in latest or s["windowEnd"] >= latest[s["customerId"]]["windowEnd"]:
            latest[s["customerId"]] = s
    levels = Counter(s["level"] for s in latest.values())
    dupes = sum(1 for n in ids.values() if n > 1)
    for s in sorted(latest.values(), key=lambda s: -s["score"])[: args.top]:
        print(f"  {s['customerId']:<8} pencere sonu {utc(s['windowEnd'])}  {s['level']:<6} {s['score']:>3}  "
              f"{', '.join(s['reasons'])}")
    log(f"{len(ids)} skor, {len(latest)} müşteri · son seviyeler: "
        + ", ".join(f"{k}={levels[k]}" for k in ("HIGH", "MEDIUM", "LOW")) + f" · aynı pencere için tekrar: {dupes}")
    return 0


def cmd_stats(args: argparse.Namespace) -> int:
    log(f"Redis'te {FeatureStore(redis_url(), args.prefix).count()} müşterinin feature'ı var.")
    return 0


def cmd_rebuild(args: argparse.Namespace) -> int:
    """>>> LAB 5 · Redis'i Kafka'dan yeniden kur.
    bank.customer-features compacted: her müşterinin (en az) son penceresi topic'te durur. Baştan okuyup
    versiyonlu yazımla Redis'e basarız: eski kayıtlar yenilerini ezemez, sonuç en güncel hâldir.
    Skorlar zaten yayımlandığı için "yayımlandı" işaretiyle yazılır; tekrar skor üretilmez.
    Olmasaydı: Redis verisi kaybolunca her müşteri bir sonraki işlemine kadar (veya hiç) feature'sız kalırdı.
    <<< LAB 5"""
    store, written, skipped, t0 = FeatureStore(redis_url(), args.prefix), 0, 0, time.monotonic()
    for m in read_all(args.topic):
        if store.upsert(Features.from_json(m.value()), published=True):
            written += 1
        else:
            skipped += 1
    log(f"Yeniden kuruldu: {written} yazım, {skipped} eski/tekrar kayıt atlandı ({time.monotonic() - t0:.1f} sn). "
        f"Redis'te {store.count()} müşteri.", "green")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Gün 3 araçları")
    p.add_argument("--prefix", default="features:", help=argparse.SUPPRESS)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("feature", help="Redis'ten online skor ve tazelik")
    s.add_argument("customer")
    s.set_defaults(func=cmd_feature)
    s = sub.add_parser("scores", help="bank.risk-scores özeti")
    s.add_argument("--customer")
    s.add_argument("--top", type=int, default=8)
    s.add_argument("--topic", default=SCORES_TOPIC)
    s.set_defaults(func=cmd_scores)
    sub.add_parser("stats", help="Redis'teki müşteri sayısı").set_defaults(func=cmd_stats)
    s = sub.add_parser("rebuild", help="Redis'i compacted feature topic'inden yeniden kur")
    s.add_argument("--topic", default=FEATURES_TOPIC)
    s.set_defaults(func=cmd_rebuild)
    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
