#!/usr/bin/env bash
# Topic anatomisi: broker'lar, controller ve her partition için Leader / Replicas / Isr.
#   make describe            (bank.transactions)
#   make describe TOPIC=lab.segments
source "$(dirname "$0")/lib.sh"
topic=${1:-bank.transactions}
load_brokers

step "Broker'lar"
for i in 1 2 3; do
  if [[ " ${BROKERS[*]} " == *" kafka-$i "* ]]; then ok "kafka-$i çalışıyor"; else warn "kafka-$i DURDURULDU"; fi
done

step "Controller (KRaft)"
# Ayaktaki broker'lara kısa zaman aşımlı ayar dosyası (controller yokken araçlar 60 sn beklemesin)
docker compose exec -T "$LIVE" bash -c "printf 'default.api.timeout.ms=8000\nrequest.timeout.ms=6000\n' > /tmp/q.properties"
leader=
attempts=3; [[ ${#BROKERS[@]} -lt 2 ]] && attempts=0   # tek node ayaktaysa çoğunluk zaten yok
for ((attempt = 1; attempt <= attempts; attempt++)); do   # aktif controller yeni durdurulduysa seçim birkaç saniye sürer
  leader=$(ktool kafka-metadata-quorum --bootstrap-server "$BOOT" --command-config /tmp/q.properties describe --status 2>/dev/null \
    | awk -F': *' '/^LeaderId/ {print $2}' || true)
  [[ -n $leader && $leader != -1 ]] && break
  leader=; if [[ $attempt -lt 3 ]]; then sleep 2; fi
done
if [[ -n $leader ]]; then
  ok "aktif controller: node $leader  (3 oy veren node'dan çoğunluk = 2)"
else
  warn "aktif controller YOK: çoğunluk (2 node) ayakta değil. Veri ve mevcut leader'lar yerinde,"
  echo "    ama metadata değişemez (leader seçimi, ISR değişikliği, topic oluşturma)."
fi

step "Topic: $topic"
# kafka-topics controller'a da sorar; controller yokken zaman aşımına düşer. O zaman tabloyu
# broker'ın metadata'sından (Python istemcisi) aynı biçimde üretiriz (Elr sütunu olmadan).
if [[ -z $leader ]] || ! desc=$(ktool kafka-topics --bootstrap-server "$BOOT" --command-config /tmp/q.properties --describe --topic "$topic" 2>/dev/null); then
  desc=$(uv run --quiet python tools.py topic-metadata --topic "$topic" 2>/dev/null) \
    || { err "topic okunamadı → docker compose logs kafka-1"; exit 1; }
  echo "  (controller yanıt vermiyor: tablo broker metadata'sından, Elr sütunu yok)"
fi
echo "$desc" | awk -F'\t' '
  /PartitionCount/ { for (i = 1; i <= NF; i++) if ($i ~ /^(PartitionCount|ReplicationFactor)/) printf "  %s", $i; print ""; next }
  { line = ""; for (i = 1; i <= NF; i++) if ($i ~ /^(Partition|Leader|Replicas|Isr|Elr):/) line = line sprintf("%-16s", $i); print "  " line }'
echo "$desc" | grep -o 'min.insync.replicas=[0-9]*' | sed 's/^/  /' || true

min=$(echo "$desc" | grep -o 'min.insync.replicas=[0-9]*' | cut -d= -f2 || true)
echo "$desc" | awk -F'\t' -v min="${min:-2}" -v G="$G" -v Y="$Y" -v R="$R" -v N="$N" '
  /Partition: / { total++; for (i = 1; i <= NF; i++) if ($i ~ /^Isr: /) { s = $i; sub(/^Isr: /, "", s) }
                  n = split(s, a, ","); if (n < 3) under++; if (n < min) below++ }
  END { print ""
        if (!under) print "  " G "✓" N " bütün partition\047larda ISR = 3"
        else print "  " Y "!" N " " under "/" total " partition eksik kopyalı (ISR < 3)"
        if (below) print "  " R "✗" N " " below " partition min.insync.replicas=" min " altında → acks=all yazım REDDEDİLİR" }'
