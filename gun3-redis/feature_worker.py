#!/usr/bin/env python3
"""Gün 3 · Feature worker: bank.customer-features → Redis (online store) → risk skoru → bank.risk-scores.

    uv run python feature_worker.py

LAB NOTU: Güvenilirlik için eklenen parçalar "# >>> LAB" ile başlar, "# <<< LAB" ile biter:
  LAB 1  feature_store.py: versiyonlu yazım (eski veya tekrar gelen feature yenisini ezmez)
  LAB 2  her pencere için skor yalnızca BİR kez yayımlanır (çökme sonrası tekrar da dahil)
  LAB 3  Redis kesintisinde bekle; offset commit edilmez, hiçbir feature kaybolmaz
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
import time

import redis
from confluent_kafka import Consumer, Producer

from common import FEATURES_TOPIC, SCORES_TOPIC, bootstrap, log, redis_url
from feature_store import FeatureStore
from scoring import Features, score

CRASH_EXIT_CODE = 99


class RedisWaiter:
    """>>> LAB 3 · Redis'e erişilemiyorsa aynı işlemi tekrar dene; bu sırada offset store EDİLMEZ.
    Olmasaydı: hata yutulup sonraki mesaja geçilirse o feature hiç yazılmaz (kayıp); program çökerse
    Redis dönene kadar kimse işlemez. Burada worker bekler, Redis dönünce kaldığı mesajdan devam eder.
    <<< LAB 3"""

    def __init__(self) -> None:
        self.down_since: float | None = None
        self.last_log = 0.0

    def __call__(self, fn):
        while True:
            try:
                result = fn()
                if self.down_since is not None:
                    log(f"Redis geri geldi ({time.monotonic() - self.down_since:.0f} sn kesinti) → kaldığı yerden devam",
                        "green")
                    self.down_since = None
                return result
            except (redis.ConnectionError, redis.TimeoutError):
                now = time.monotonic()
                self.down_since = self.down_since or now
                if now - self.last_log >= 5:
                    log("Redis erişilemiyor → bekleniyor. Offset commit edilmiyor, feature kaybolmaz.", "yellow")
                    self.last_log = now
                time.sleep(1)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="feature'ları Redis'e yazar, risk skorunu yayımlar")
    p.add_argument("--group", default="feature-worker")
    p.add_argument("--features-topic", default=FEATURES_TOPIC)
    p.add_argument("--scores-topic", default=SCORES_TOPIC)
    p.add_argument("--prefix", default="features:", help="Redis anahtar öneki")
    p.add_argument("--exit-when-idle", type=int, default=0, help="bu kadar sn mesaj gelmezse kapan")
    p.add_argument("--crash-after-redis", type=int, default=0,
                   help="n. feature Redis'e yazıldıktan sonra, skor yayımlanmadan çök (çıkış 99)")
    p.add_argument("--verbose", action="store_true", help="her skoru yazdır (varsayılan: yalnızca HIGH)")
    args = p.parse_args(argv)
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    store, wait_redis = FeatureStore(redis_url(), args.prefix), RedisWaiter()
    consumer = Consumer({
        "bootstrap.servers": bootstrap(), "group.id": args.group, "client.id": f"worker-{os.getpid()}",
        # İlk açılışta compacted topic'in başından: her müşterinin son feature'ı Redis'e gelir.
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True, "auto.commit.interval.ms": 5000,
        "enable.auto.offset.store": False,  # Gün 1 LAB 4: iş bittikten sonra store
        "partition.assignment.strategy": "cooperative-sticky",
        # Redis kesintisinde aynı mesajda beklerken group'tan atılmayalım (en fazla 10 dk).
        "max.poll.interval.ms": 600000,
        # Çöken worker'ın partition'ları ~10 sn içinde yenisine geçsin.
        "session.timeout.ms": 10000, "heartbeat.interval.ms": 3000,
    })
    producer = Producer({"bootstrap.servers": bootstrap(), "acks": "all", "enable.idempotence": True})
    assigned, last_msg = [], time.monotonic()

    def on_assign(_consumer, partitions) -> None:
        nonlocal last_msg
        assigned.extend(partitions)
        last_msg = time.monotonic()  # boşta kalma süresi partition alındıktan sonra sayılır

    consumer.subscribe([args.features_topic], on_assign=on_assign)
    log(f"Feature worker: {args.features_topic} → Redis ({redis_url()}) → {args.scores_topic}", "cyan")

    n = {"işlenen": 0, "yazılan": 0, "eski/tekrar": 0, "skor": 0, "HIGH": 0}
    next_report = time.monotonic() + 5
    try:
        while True:
            msg = consumer.poll(0.5)
            if time.monotonic() >= next_report:
                log(" ".join(f"{k}={v}" for k, v in n.items()))
                next_report = time.monotonic() + 5
            if msg is None:
                if args.exit_when_idle and assigned and time.monotonic() - last_msg > args.exit_when_idle:
                    break
                continue
            if msg.error():
                continue
            last_msg = time.monotonic()
            try:
                f = Features.from_json(msg.value())
            except (ValueError, KeyError, TypeError) as exc:
                log(f"okunamadı, atlanıyor: {exc}", "red")
                consumer.store_offsets(message=msg)
                continue

            n["işlenen"] += 1
            if wait_redis(lambda: store.upsert(f)):
                n["yazılan"] += 1
            else:
                n["eski/tekrar"] += 1

            if args.crash_after_redis and n["işlenen"] == args.crash_after_redis:
                log(f"💥 SİMÜLE ÇÖKME: {f.customer_id} Redis'e yazıldı, skor yayımlanmadı.", "red")
                os._exit(CRASH_EXIT_CODE)

            # >>> LAB 2 · her pencere için skor BİR kez
            #     Skor yalnızca Redis'teki en güncel pencere içinse ve henüz yayımlanmadıysa hesaplanır.
            #     Yayımlandıktan sonra Redis'e işaretlenir (publishedWindowEnd). Çökme Redis'e yazdıktan sonra,
            #     skordan önce olursa: tekrar gelen mesajda feature "eski/tekrar" sayılır ama skor eksik olduğu
            #     için yine yayımlanır. Olmasaydı: o pencerenin skoru hiç yayımlanmazdı (veya her tekrarda yeniden).
            if wait_redis(lambda: store.needs_score(f)):
                s = score(f)
                errors = []
                producer.produce(args.scores_topic, key=f.customer_id, value=s.to_json(),
                                 on_delivery=lambda err, _m: errors.append(err) if err else None)
                if producer.flush(10) or errors:
                    raise RuntimeError(f"skor Kafka'ya yazılamadı: {errors}")
                wait_redis(lambda: store.mark_published(f))
                n["skor"] += 1
                if s.level == "HIGH":
                    n["HIGH"] += 1
                if s.level == "HIGH" or args.verbose:
                    log(f"SKOR {s.customer_id} {s.level:<6} {s.score:>3}  {', '.join(s.reasons)}",
                        "red" if s.level == "HIGH" else None)
            # <<< LAB 2

            consumer.store_offsets(message=msg)
    except KeyboardInterrupt:
        log("Kapatılıyor...", "yellow")
    finally:
        consumer.close()
    log("SUMMARY " + " ".join(f"{k}={v}" for k, v in n.items()), "green")
    return 0


if __name__ == "__main__":
    sys.exit(main())
