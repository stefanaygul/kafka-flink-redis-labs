#!/usr/bin/env bash
# make tm-restart: TaskManager'ı yeniden başlatır (çökme). Job son checkpoint'ten devam eder.
source "$(dirname "$0")/lib.sh"
jid=$(running_job customer-features)
[[ -n $jid ]] || { err "customer-features çalışmıyor → make submit"; exit 1; }
before=$(curl -sf "$FLINK/jobs/$jid/checkpoints" | python3 -c 'import json,sys; c=json.load(sys.stdin)["latest"]["completed"]; print(c["id"] if c else "-")')
ok "son tamamlanan checkpoint: #$before"
step "TaskManager yeniden başlatılıyor (job'ın çalıştığı süreç ölüyor)"
since=$(python3 -c 'import time; print(int(time.time() * 1000))')
docker compose restart -t 0 taskmanager > /dev/null 2>&1
for _ in $(seq 1 60); do
  info=$(curl -sf "$FLINK/jobs/$jid" 2>/dev/null | python3 -c 'import json,sys; j=json.load(sys.stdin); print(j["state"])' 2>/dev/null || echo "?")
  printf "\r  job durumu: %-12s" "$info"
  # Yalnızca yeniden başlatmadan SONRAKİ geri yükleme sayılır (önceki denemelerinki değil).
  restored=$(curl -sf "$FLINK/jobs/$jid/checkpoints" 2>/dev/null | python3 -c '
import json, sys
r = json.load(sys.stdin)["latest"]["restored"]
print(r["id"] if r and r["restore_timestamp"] >= int(sys.argv[1]) else "")' "$since" 2>/dev/null || true)
  [[ $info == RUNNING && -n $restored ]] && break
  sleep 2
done
echo
if [[ -n ${restored:-} ]]; then
  ok "job yeniden RUNNING, checkpoint #$restored'dan geri yüklendi (state kaybolmadı)"
  echo "  Flink UI → $FLINK/#/job/running/$jid/checkpoints  (Latest Restore)"
else
  err "job 120 sn içinde toparlanmadı → $FLINK"; exit 1
fi
