#!/usr/bin/env python3
"""Gün 1 gözlem araçları.

    uv run python tools.py lag [--group bank-ledger] [--watch]   # partition başına lag ve sahibi
    uv run python tools.py ledger-report                          # defterde iki kez uygulanan işlem var mı?
    uv run python tools.py ledger-reset                           # defteri sil
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from pathlib import Path

from confluent_kafka import ConsumerGroupTopicPartitions, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from common import DATA_DIR, GROUP, TOPIC, bootstrap, log


def lag_table(admin: AdminClient, group: str, topic: str) -> list[str]:
    md = admin.list_topics(topic=topic, timeout=10).topics.get(topic)
    if md is None or md.error is not None:
        return [f"Topic bulunamadı: {topic}"]
    tps = [TopicPartition(topic, p) for p in sorted(md.partitions)]
    ends = {tp.partition: f.result().offset
            for tp, f in admin.list_offsets({tp: OffsetSpec.latest() for tp in tps}, request_timeout=10).items()}
    result = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(group, tps)], request_timeout=10)
    committed = {tp.partition: tp.offset for tp in result[group].result().topic_partitions}

    owners, state, members = {}, "(yok)", 0
    try:
        desc = admin.describe_consumer_groups([group], request_timeout=10)[group].result()
        state, members = desc.state.name, len(desc.members)
        for m in desc.members:
            for tp in m.assignment.topic_partitions:
                owners[tp.partition] = m.client_id
    except KafkaException:
        pass

    lines = [f"{time.strftime('%H:%M:%S')}  group={group}  durum={state}  üye={members}  topic={topic}",
             f"  {'partition':<10}{'son offset':>12}{'commit':>10}{'lag':>8}   consumer"]
    total = 0
    for tp in tps:
        end, c = ends[tp.partition], committed.get(tp.partition, -1)
        lag = end - c if c >= 0 else end
        total += lag
        lines.append(f"  {tp.partition:<10}{end:>12}{(c if c >= 0 else '-'):>10}{lag:>8}   "
                     f"{owners.get(tp.partition, '-')}")
    lines.append(f"  {'TOPLAM':<10}{'':>12}{'':>10}{total:>8}")
    return lines


def cmd_lag(args: argparse.Namespace) -> int:
    admin = AdminClient({"bootstrap.servers": bootstrap()})
    while True:
        try:
            lines = lag_table(admin, args.group, args.topic)
        except KafkaException as exc:
            lines = [f"{time.strftime('%H:%M:%S')}  Offset'ler okunamadı: {exc.args[0].str()}"]
        if args.watch and sys.stdout.isatty():
            print("\033[2J\033[H", end="")
        print("\n".join(lines), flush=True)
        if not args.watch:
            return 0
        try:
            time.sleep(2)
        except KeyboardInterrupt:
            return 0


def cmd_ledger_report(args: argparse.Namespace) -> int:
    if not args.db.is_file():
        log(f"Defter yok: {args.db} (consumer henüz çalışmamış)", "yellow")
        return 1
    db = sqlite3.connect(args.db, timeout=30)
    q = lambda sql: int(db.execute(sql).fetchone()[0] or 0)  # noqa: E731
    entries = q("SELECT COUNT(*) FROM ledger_entries")
    distinct = q("SELECT COUNT(DISTINCT transaction_id) FROM ledger_entries")
    caught = q("SELECT COUNT(*) FROM duplicate_deliveries")
    balance = q("SELECT COALESCE(SUM(balance_minor),0) FROM account_balances")
    expected = q("SELECT COALESCE(SUM(e.amount_minor),0) FROM (SELECT MIN(id) id FROM ledger_entries "
                 "GROUP BY transaction_id) f JOIN ledger_entries e ON e.id = f.id")
    print(f"  Deftere yazılan kayıt   : {entries}")
    print(f"  Farklı işlem            : {distinct}")
    print(f"  Toplam bakiye           : {tl(balance)} TL")
    print(f"  Olması gereken          : {tl(expected)} TL")
    print(f"  Yakalanan tekrar teslim : {caught}")
    if entries > distinct:
        log(f"✗ {entries - distinct} işlem BİRDEN FAZLA KEZ uygulanmış. Bakiye farkı: {tl(balance - expected)} TL",
            "red")
        return 1
    log("✓ Her işlem tam olarak bir kez uygulanmış.", "green")
    return 0


def cmd_ledger_reset(args: argparse.Namespace) -> int:
    for suffix in ("", "-wal", "-shm"):
        Path(f"{args.db}{suffix}").unlink(missing_ok=True)
    log("Defter silindi. Not: group offset'leri Kafka'da duruyor; baştan okumak için yeni --group verin.", "green")
    return 0


def cmd_topic_metadata(args: argparse.Namespace) -> int:
    """kafka-topics --describe biçiminde tablo, yalnızca broker metadata'sından (controller gerekmez).
    scripts/describe.sh controller yanıt vermediğinde kullanır."""
    admin = AdminClient({"bootstrap.servers": bootstrap(), "log_level": 0})
    md = admin.list_topics(topic=args.topic, timeout=10).topics[args.topic]
    rf = len(next(iter(md.partitions.values())).replicas)
    # min.insync.replicas cluster ve topic ayarı: 2 (docker-compose.yml, kafka-init)
    print(f"Topic: {args.topic}\tPartitionCount: {len(md.partitions)}\tReplicationFactor: {rf}"
          "\tConfigs: min.insync.replicas=2")
    for p, info in sorted(md.partitions.items()):
        print(f"\tTopic: {args.topic}\tPartition: {p}\tLeader: {info.leader}\t"
              f"Replicas: {','.join(map(str, info.replicas))}\tIsr: {','.join(map(str, info.isrs))}")
    return 0


def tl(minor: int) -> str:
    """Kuruş → 1.234,56"""
    s = f"{abs(minor) / 100:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"-{s}" if minor < 0 else s


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Gün 1 gözlem araçları")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("lag", help="consumer group lag'i")
    s.add_argument("--group", default=GROUP)
    s.add_argument("--topic", default=TOPIC)
    s.add_argument("--watch", action="store_true", help="2 sn'de bir yenile (Ctrl+C ile çık)")
    s.set_defaults(func=cmd_lag)
    s = sub.add_parser("topic-metadata", help="partition tablosu (broker metadata'sından)")
    s.add_argument("--topic", default=TOPIC)
    s.set_defaults(func=cmd_topic_metadata)
    for name, func in (("ledger-report", cmd_ledger_report), ("ledger-reset", cmd_ledger_reset)):
        s = sub.add_parser(name)
        s.add_argument("--db", type=Path, default=DATA_DIR / "ledger.db")
        s.set_defaults(func=func)
    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
