#!/usr/bin/env python3
"""Gün 1 · İşlem producer'ı: bank.transactions topic'ine işlem gönderir.

    uv run python producer.py --rate 50 --duration 20
    uv run python producer.py --rate 50 --duration 20 --hot-customer C-1001      # sıcak partition
    uv run python producer.py --rate 100 --duration 900 --queue-max 1000         # Görev 3
    uv run python producer.py --rate 5 --duration 2 --no-retry                   # broker'ın hatasını hemen gör

LAB NOTU: Güvenilirlik için eklenen parçalar kodda "# >>> LAB" ile başlar, "# <<< LAB" ile biter
(editörde ">>> LAB" diye arayın). Yalnızca bunlar eklendi; her birinin altında "olmasaydı" notu var:
  LAB 1  build_message  → key = customerId
  LAB 2  on_delivery    → teslim sonucu kontrol edilir (hata sayılır, partition dağılımı tutulur)
  LAB 3  send           → yerel kuyruk doluysa bekle, yeniden dene (backpressure)
"""
from __future__ import annotations

import argparse
import signal
import sys
import time
from collections import Counter

from confluent_kafka import KafkaError, KafkaException, Message, Producer

from common import TOPIC, Transaction, TransactionGenerator, bootstrap, log


def create_config(*, acks: str = "all", queue_max: int = 100_000, message_timeout_ms: int = 300_000,
                  retry: bool = True) -> dict:
    """Producer ayarları. Her satırın neden orada olduğu yorumunda."""
    config = {
        "bootstrap.servers": bootstrap(),
        "client.id": "lab-producer",
        # Leader + bütün in-sync replica'lar yazmadan "başarılı" sayma.
        # Topic'te min.insync.replicas=2: 2 kopya yazılamıyorsa broker yazmayı REDDEDER.
        "acks": acks,
        # Yeniden denemeler aynı kaydı iki kez yazmaz, partition içi sıra bozulmaz (acks=all ister).
        "enable.idempotence": acks == "all" and retry,
        # Java istemcileriyle (Gün 2'de Flink) aynı key → aynı partition.
        "partitioner": "murmur2_random",
        # Batch: mesajlar partition başına biriktirilip toplu gönderilir. Hangisi önce dolarsa:
        #   linger.ms  → en fazla bu kadar bekle            (librdkafka varsayılanı 5 ms)
        #   batch.size → bir batch en fazla bu kadar byte   (librdkafka varsayılanı 1.000.000; Java'da 16 KB)
        "linger.ms": 5,
        "batch.size": 1_000_000,
        "compression.type": "lz4",
        # Yerel kuyruk: henüz teslim edilmemiş mesajlar producer'ın belleğinde bekler.
        # Dolarsa produce() BufferError fırlatır (Görev 3).
        "queue.buffering.max.messages": queue_max,
        # Producer bir mesajı bu süre boyunca yeniden dener; sonra vazgeçer ve hatayı delivery report'ta bildirir.
        # (librdkafka varsayılanı: 300000 ms = 5 dk)
        "message.timeout.ms": message_timeout_ms,
    }
    if not retry:
        # --no-retry: broker'ın verdiği hata (ör. NotEnoughReplicas) hemen delivery report'a düşer.
        config["retries"] = 0
    return config


def build_message(txn: Transaction) -> tuple[str, str]:
    """İşlem → Kafka mesajı (key, value)."""
    # >>> LAB 1 · key = customerId
    #     Aynı müşterinin bütün işlemleri aynı partition'a düşer ve sırası korunur.
    #     Olmasaydı (key=None): mesajlar partition'lara rastgele dağılır, müşterinin işlem sırası bozulur.
    return txn.customer_id, txn.to_json()
    # <<< LAB 1


class TransactionProducer:
    def __init__(self, topic: str, client: Producer | None = None, **config):
        self.topic = topic
        self.delivered = 0
        self.failed = 0
        self.per_partition: Counter[int] = Counter()
        self.queue_full_waits = 0
        self._producer = client or Producer(create_config(**config))

    def send(self, txn: Transaction) -> None:
        """Asenkron gönderim. Kesin sonuç on_delivery içinde gelir."""
        key, value = build_message(txn)
        # >>> LAB 3 · yerel kuyruk doluysa bekle ve aynı mesajı yeniden dene (backpressure)
        #     Broker yetişemiyorsa (ya da yazmayı reddediyorsa) üretimi yavaşlat, veriyi atma.
        #     Olmasaydı: produce() BufferError fırlatır, producer ÇÖKER ve kuyrukta bekleyen,
        #     henüz teslim edilmemiş bütün mesajlar kaybolur.
        while True:
            try:
                self._producer.produce(self.topic, key=key, value=value, on_delivery=self.on_delivery)
                return
            except BufferError:
                self.queue_full_waits += 1
                if self.queue_full_waits == 1 or self.queue_full_waits % 20 == 0:
                    log(f"Yerel kuyruk dolu ({self.in_queue} mesaj) → yeni mesaj bekletiliyor (backpressure)",
                        "yellow")
                self._producer.poll(0.5)  # delivery report'lar işlenir, kuyrukta yer açılır
        # <<< LAB 3

    def on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        """Her mesaj için Kafka'nın verdiği kesin sonuç (delivery report)."""
        # >>> LAB 2 · teslim sonucunu kontrol et
        #     produce() hata vermeden dönse bile mesaj henüz yalnızca yerel kuyruktadır.
        #     Olmasaydı: teslim edilemeyen mesaj da "gönderildi" sanılır → sessiz veri kaybı.
        if err is not None:
            self.failed += 1
            if self.failed <= 5 or self.failed % 100 == 0:
                key = msg.key().decode() if msg.key() else None
                log(f"Teslim hatası #{self.failed}: key={key} → {err.str()}", "red")
            return
        self.delivered += 1
        self.per_partition[msg.partition()] += 1
        # <<< LAB 2

    @property
    def in_queue(self) -> int:
        """Teslim sonucu henüz gelmemiş mesaj sayısı (yerel kuyruk + yolda olanlar)."""
        return len(self._producer)

    def poll(self, timeout: float = 0) -> None:
        self._producer.poll(timeout)

    def flush(self, timeout: float) -> int:
        return self._producer.flush(timeout)

    def distribution(self) -> str:
        return " ".join(f"p{p}={n}" for p, n in sorted(self.per_partition.items())) or "-"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="bank.transactions producer'ı")
    p.add_argument("--rate", type=float, default=10, help="saniyedeki işlem (varsayılan 10)")
    p.add_argument("--duration", type=float, default=60, help="süre, saniye (varsayılan 60)")
    p.add_argument("--customers", type=int, default=20, help="müşteri sayısı: C-1001… (varsayılan 20)")
    p.add_argument("--hot-customer", help="trafiğin büyük kısmı bu müşteriden (ör. C-1001)")
    p.add_argument("--hot-share", type=float, default=0.7, help="sıcak müşterinin payı (varsayılan 0.7)")
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--seed", type=int, help="tekrarlanabilir veri")
    p.add_argument("--acks", choices=["all", "1"], default="all")
    p.add_argument("--queue-max", type=int, default=100_000, help="yerel kuyruk sınırı (mesaj)")
    p.add_argument("--message-timeout-ms", type=int, default=300_000)
    p.add_argument("--no-retry", action="store_true", help="yeniden deneme yok: broker hatasını hemen gör")
    args = p.parse_args(argv)
    # kill / docker stop (SIGTERM) da Ctrl+C gibi düzgün kapansın.
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    gen = TransactionGenerator(args.customers, args.seed, args.hot_customer, args.hot_share)
    producer = TransactionProducer(args.topic, acks=args.acks, queue_max=args.queue_max,
                                   message_timeout_ms=args.message_timeout_ms, retry=not args.no_retry)
    extra = ", yeniden deneme yok" if args.no_retry else ""
    log(f"Producer → {args.topic}  ({args.rate:g}/sn, {args.duration:g} sn, acks={args.acks}{extra})", "cyan")

    total, sent, fatal = int(args.rate * args.duration), 0, None
    start, next_report = time.monotonic(), 2.0
    try:
        while sent < total:
            elapsed = time.monotonic() - start
            due = min(int(elapsed * args.rate), total)  # hedef hıza göre şimdiye kadar gönderilmesi gereken
            while sent < due:
                producer.send(gen.next())
                sent += 1
            producer.poll(0)  # delivery report'ları işle
            if elapsed >= next_report:
                log(f"gönderilen={sent} teslim={producer.delivered} hata={producer.failed} "
                    f"kuyrukta={producer.in_queue} │ {producer.distribution()}")
                next_report += 2
            time.sleep(0.02)
    except KeyboardInterrupt:
        log("Durduruluyor: kuyruktaki mesajlar teslim ediliyor (en fazla 30 sn)...", "yellow")
    except KafkaException as exc:
        # Kurtarılamaz (fatal) hata: bu producer nesnesi artık kullanılamaz.
        fatal = exc.args[0]
        log(f"Producer kurtarılamaz bir hataya düştü: {fatal.str()}", "red")

    try:
        remaining = producer.in_queue if fatal else producer.flush(30)
    except KeyboardInterrupt:
        remaining = producer.in_queue

    ok = producer.failed == 0 and remaining == 0
    print()
    log(f"SUMMARY delivered={producer.delivered} failed={producer.failed} undelivered={remaining}",
        "green" if ok else "red")
    log(f"Partition dağılımı: {producer.distribution()}")
    if remaining:
        log(f"{remaining} mesaj hâlâ kuyruktaydı ve program kapanınca KAYBOLDU.", "red")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
