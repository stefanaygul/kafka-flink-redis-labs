#!/usr/bin/env bash
# Leader'lığı "preferred" broker'lara (Replicas listesindeki ilk broker) geri verir.
# Broker geri geldiğinde leader'lık kendiliğinden hemen dönmez; Kafka bunu 5 dk'da bir kendisi de yapar
# (auto.leader.rebalance.enable). Burada beklemeden tetikliyoruz.
source "$(dirname "$0")/lib.sh"
load_brokers
step "Preferred leader election"
out=$(ktool kafka-leader-election --bootstrap-server "$BOOT" --election-type PREFERRED --all-topic-partitions 2>&1 || true)
if grep -q 'Successfully completed' <<<"$out"; then
  ok "$(grep 'Successfully completed' <<<"$out" | tr ',' '\n' | wc -l | tr -d ' ') partition'ın leader'ı preferred broker'a geri verildi"
else
  ok "Leader'lar zaten preferred broker'larda."
fi
bash "$(dirname "$0")/describe.sh" bank.transactions
