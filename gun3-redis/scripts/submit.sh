#!/usr/bin/env bash
# Feature job'ını (sql/06-features.sql) gönderir; zaten çalışıyorsa tekrar göndermez. make up bunu kendisi çağırır.
source "$(dirname "$0")/lib.sh"
jid=$(running_job customer-features)
if [[ -n $jid ]]; then
  ok "customer-features zaten çalışıyor ($jid) → $FLINK/#/job/running/$jid/overview"
  exit 0
fi
out=$(sql_file 06-features.sql)
jid=$(grep -o 'Job ID: [0-9a-f]*' <<<"$out" | awk '{print $3}')
if [[ -z $jid ]]; then
  echo "$out" | tail -20; err "Job gönderilemedi"; exit 1
fi
for _ in $(seq 1 60); do   # makine yüklüyse job'ın RUNNING olması birkaç saniye sürebilir
  state=$(curl -sf "$FLINK/jobs/$jid" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"])' 2>/dev/null || echo "?")
  [[ $state == RUNNING ]] && break
  sleep 2
done
if [[ $state != RUNNING ]]; then err "job 120 sn içinde RUNNING olmadı (durum: $state) → $FLINK"; exit 1; fi
ok "customer-features gönderildi ve çalışıyor ($jid) → $FLINK/#/job/running/$jid/overview"
echo "  Feature'lar her dakika, pencere bittikten ~10 sn sonra gelir:  make features"
