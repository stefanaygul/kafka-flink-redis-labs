#!/usr/bin/env python3
"""Gün 1 · Defter consumer'ı: bank.transactions → müşteri defteri (SQLite, data/ledger.db).

Her işlemi deftere yazar ve müşterinin bakiyesini günceller.

    uv run python consumer.py --instance c1
    uv run python consumer.py --instance c1 --group lag-demo --start latest --delay-ms 50   # yavaş consumer (lag)
    uv run python consumer.py --instance c1 --group crash-1 --crash-after 25  # 25. mesajdan sonra çök

LAB NOTU: Güvenilirlik için eklenen parçalar kodda "# >>> LAB" ile başlar, "# <<< LAB" ile biter
(editörde ">>> LAB" diye arayın). Yalnızca bunlar eklendi; her birinin altında "olmasaydı" notu var:
  LAB 4  offset, iş sonucu kalıcı olduktan SONRA store edilir (3 yer)
  LAB 5  Ledger.apply: idempotency, aynı işlem iki kez gelirse bakiye değişmez
"""
from __future__ import annotations

import argparse
import os
import signal
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

from common import DATA_DIR, GROUP, TOPIC, Transaction, bootstrap, log

CRASH_EXIT_CODE = 99  # --crash-after ile bilinçli çökme


# --- Defter (SQLite) ----------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT, transaction_id TEXT NOT NULL, customer_id TEXT NOT NULL,
    amount_minor INTEGER NOT NULL, kafka_partition INTEGER NOT NULL, kafka_offset INTEGER NOT NULL,
    consumer TEXT NOT NULL, applied_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ix_ledger_txn ON ledger_entries(transaction_id);
CREATE TABLE IF NOT EXISTS account_balances (
    customer_id TEXT PRIMARY KEY, balance_minor INTEGER NOT NULL, txn_count INTEGER NOT NULL);
-- Idempotency: daha önce uygulanmış işlemlerin kimlikleri.
CREATE TABLE IF NOT EXISTS processed_transactions (
    transaction_id TEXT PRIMARY KEY, processed_at TEXT NOT NULL, consumer TEXT NOT NULL);
-- Gözlem: yakalanan tekrar teslimler (iş sonucunu etkilemez).
CREATE TABLE IF NOT EXISTS duplicate_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT, transaction_id TEXT NOT NULL, kafka_partition INTEGER NOT NULL,
    kafka_offset INTEGER NOT NULL, consumer TEXT NOT NULL, seen_at TEXT NOT NULL);
"""


class Ledger:
    APPLIED, DUPLICATE = "Applied", "Duplicate"

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        # isolation_level=None: transaction'ları BEGIN/COMMIT ile kendimiz yönetiyoruz.
        # timeout=30: aynı dosyaya yazan diğer consumer'ları bekle.
        self._db = sqlite3.connect(path, timeout=30, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript(SCHEMA)

    def apply(self, txn: Transaction, consumer: str, partition: int, offset: int) -> str:
        """İşlemi deftere uygular. Tutarlar kuruş cinsinden tam sayı."""
        now = datetime.now(timezone.utc).isoformat()
        db = self._db
        db.execute("BEGIN IMMEDIATE")
        try:
            # >>> LAB 5 · idempotency: bu işlem daha önce uygulandıysa yan etkiyi ATLA
            #     Kafka at-least-once teslim eder: çökme, rebalance veya replay sonrası aynı kayıt tekrar gelir.
            #     İşaret (processed_transactions) ile yan etki (defter + bakiye) AYNI transaction'da.
            #     Olmasaydı: tekrar gelen işlem deftere yeniden yazılır, bakiye iki kez artar.
            marked = db.execute(
                "INSERT OR IGNORE INTO processed_transactions(transaction_id, processed_at, consumer) VALUES (?,?,?)",
                (txn.transaction_id, now, consumer)).rowcount
            if marked == 0:
                db.execute(
                    "INSERT INTO duplicate_deliveries(transaction_id, kafka_partition, kafka_offset, consumer, seen_at)"
                    " VALUES (?,?,?,?,?)", (txn.transaction_id, partition, offset, consumer, now))
                db.execute("COMMIT")
                return self.DUPLICATE
            # <<< LAB 5
            db.execute(
                "INSERT INTO ledger_entries(transaction_id, customer_id, amount_minor, kafka_partition, kafka_offset,"
                " consumer, applied_at) VALUES (?,?,?,?,?,?,?)",
                (txn.transaction_id, txn.customer_id, txn.amount_minor, partition, offset, consumer, now))
            db.execute(
                "INSERT INTO account_balances(customer_id, balance_minor, txn_count) VALUES (?,?,1)"
                " ON CONFLICT(customer_id) DO UPDATE SET balance_minor = balance_minor + excluded.balance_minor,"
                " txn_count = txn_count + 1", (txn.customer_id, txn.amount_minor))
            db.execute("COMMIT")
            return self.APPLIED
        except BaseException:
            db.execute("ROLLBACK")
            raise

    def close(self) -> None:
        self._db.close()


# --- Consumer -----------------------------------------------------------------------------------

def create_config(group: str, instance: str, start: str = "earliest") -> dict:
    return {
        "bootstrap.servers": bootstrap(),
        "group.id": group,
        "client.id": instance,
        # Group için commit edilmiş offset yoksa nereden başlansın: earliest = topic'in başı,
        # latest = yalnızca bundan sonra gelenler (Görev 6'da lag'i sıfırdan izlemek için).
        "auto.offset.reset": start,
        # Arka planda 5 sn'de bir commit: commit edilen değer "store" edilmiş son offset'tir.
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 5000,
        # >>> LAB 4 (1/3) · offset'i poll() anında değil, iş bitince biz store ederiz
        #     Olmasaydı (True): offset mesaj alındığı an store edilir; işlem bitmeden çökersek auto-commit
        #     işlenmemiş mesajın offset'ini commit edebilir → mesaj KAYBOLUR.
        "enable.auto.offset.store": False,
        # <<< LAB 4
        # Rebalance'ta yalnızca taşınan partition'lar durur, diğerleri işlenmeye devam eder.
        "partition.assignment.strategy": "cooperative-sticky",
        # Lab için kısa: çöken consumer'ın partition'ları ~10 sn içinde başkasına geçer.
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
    }


@dataclass
class Summary:
    processed: int = 0
    applied: int = 0
    duplicates: int = 0
    skipped: int = 0


def run(args: argparse.Namespace) -> Summary:
    name, assigned = args.instance, set()
    last_activity = time.monotonic()

    def on_assign(_consumer, partitions: list[TopicPartition]) -> None:
        nonlocal last_activity
        added = sorted(tp.partition for tp in partitions)  # cooperative: yalnızca bu turda EKLENENLER
        assigned.update(added)
        last_activity = time.monotonic()
        if added:
            log(f"[{name}] REBALANCE + eklendi: {_join(added)} │ şu an: {_join(sorted(assigned))}", "green")

    def on_revoke(_consumer, partitions: list[TopicPartition], reason: str = "geri alındı") -> None:
        removed = sorted(tp.partition for tp in partitions)
        assigned.difference_update(removed)
        if removed:
            log(f"[{name}] REBALANCE − {reason}: {_join(removed)} │ şu an: {_join(sorted(assigned))}", "yellow")

    ledger = Ledger(args.db)
    consumer = Consumer(create_config(args.group, name, args.start))
    consumer.subscribe([args.topic], on_assign=on_assign, on_revoke=on_revoke,
                       on_lost=lambda c, p: on_revoke(c, p, "KAYBEDİLDİ"))
    log(f"[{name}] group={args.group} topic={args.topic} defter={args.db.name}", "cyan")

    s, next_report = Summary(), time.monotonic() + 2
    try:
        while True:
            msg = consumer.poll(0.5)
            if time.monotonic() >= next_report:
                log(f"[{name}] işlenen={s.processed} uygulanan={s.applied} duplicate={s.duplicates} "
                    f"│ partition'lar: {_join(sorted(assigned))}")
                next_report = time.monotonic() + 2
            if msg is None:
                if args.exit_when_idle and assigned and time.monotonic() - last_activity > args.exit_when_idle:
                    log(f"[{name}] {args.exit_when_idle} sn boyunca yeni mesaj yok, çıkılıyor.")
                    break
                continue
            if msg.error():
                log(f"[{name}] {msg.error().str()}", "yellow")
                continue

            last_activity = time.monotonic()
            where = f"p{msg.partition()}@{msg.offset()}"
            try:
                txn = Transaction.from_json(msg.value())
            except ValueError as exc:
                log(f"[{name}] {where} okunamadı, atlanıyor: {exc}", "red")
                s.skipped += 1
                # >>> LAB 4 (2/3) · bozuk mesajın offset'i de store edilir
                #     Olmasaydı: consumer her yeniden başlatmada aynı bozuk mesaja takılır.
                consumer.store_offsets(message=msg)
                # <<< LAB 4
                continue

            if args.delay_ms:
                time.sleep(args.delay_ms / 1000)

            outcome = ledger.apply(txn, name, msg.partition(), msg.offset())  # iş sonucu (yan etki)
            s.processed += 1
            if outcome == Ledger.APPLIED:
                s.applied += 1
            else:
                s.duplicates += 1
                if s.duplicates <= 30 or s.duplicates % 1000 == 0:  # replay'de binlerce satır basmasın
                    log(f"[{name}] {where} {txn.transaction_id} {txn.customer_id} {txn.amount:.2f} TL → Duplicate "
                        f"#{s.duplicates} (tekrar teslim, iş sonucu değişmedi)", "yellow")
            if args.verbose and outcome == Ledger.APPLIED:
                log(f"[{name}] {where} {txn.transaction_id} {txn.customer_id} {txn.amount:.2f} TL → Applied")

            # Çökme simülasyonu: iş sonucu yazıldı, offset henüz store/commit EDİLMEDİ.
            if args.crash_after and s.processed == args.crash_after:
                log(f"[{name}] 💥 SİMÜLE ÇÖKME: {where} işlendi ama offset commit edilmedi.", "red")
                os._exit(CRASH_EXIT_CODE)  # close() çalışmaz: gerçek bir çökme gibi

            # >>> LAB 4 (3/3) · iş sonucu kalıcı → offset'i ŞİMDİ store et (auto-commit 5 sn'de bir gönderir)
            #     Çökme bundan önce olursa işlenmiş ama commit edilmemiş kayıt tekrar gelir (at-least-once);
            #     LAB 5 bu tekrarı zararsız kılar.
            consumer.store_offsets(message=msg)
            # <<< LAB 4
    except KeyboardInterrupt:
        log(f"[{name}] Kapatılıyor...", "yellow")
    finally:
        # close(): store edilmiş offset'leri commit eder ve group'tan hemen ayrılır (hızlı rebalance).
        consumer.close()
        ledger.close()
        log(f"[{name}] consumer kapatıldı, offset'ler commit edildi.")
    return s


def _join(numbers: list[int]) -> str:
    return ",".join(map(str, numbers)) or "(yok)"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="bank.transactions → defter (SQLite) consumer'ı")
    p.add_argument("--instance", default=f"consumer-{os.getpid()}", help="log'daki ad (c1, c2, c3)")
    p.add_argument("--group", default=GROUP, help="consumer group (varsayılan bank-ledger)")
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--start", choices=["earliest", "latest"], default="earliest",
                   help="group'un offset'i yoksa nereden başlasın (varsayılan earliest)")
    p.add_argument("--delay-ms", type=int, default=0, help="mesaj başına yapay işlem süresi (lag için)")
    p.add_argument("--crash-after", type=int, default=0, help="n. mesajdan sonra commit etmeden çök (çıkış 99)")
    p.add_argument("--exit-when-idle", type=int, default=0, help="bu kadar sn mesaj gelmezse kapan")
    p.add_argument("--db", type=Path, default=DATA_DIR / "ledger.db")
    p.add_argument("--verbose", action="store_true", help="her mesajı yazdır")
    args = p.parse_args(argv)
    signal.signal(signal.SIGTERM, signal.default_int_handler)  # kill da Ctrl+C gibi düzgün kapansın

    s = run(args)
    log(f"SUMMARY instance={args.instance} processed={s.processed} applied={s.applied} "
        f"duplicates={s.duplicates} skipped={s.skipped}", "green")
    return 0


if __name__ == "__main__":
    sys.exit(main())
