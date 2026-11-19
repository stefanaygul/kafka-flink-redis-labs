#!/usr/bin/env bash
# Görev 4: bir partition'ın diskteki hâli.
#   make segments            (bank.transactions, partition 0)
#   make segments P=3
#   make segment-roll        (lab.segments'e ~3,5 MB yazar, segment değişimini gösterir)
source "$(dirname "$0")/lib.sh"
load_brokers

if [[ ${1:-} == roll ]]; then
  step "lab.segments'e 12.000 × 300 byte yazılıyor (segment.bytes = 1 MB)"
  ktool kafka-producer-perf-test --topic lab.segments --num-records 12000 --record-size 300 \
    --throughput -1 --producer-props bootstrap.servers="$BOOT" acks=all 2>/dev/null | tail -1 | sed 's/^/  /'
  step "$LIVE:/var/lib/kafka/data/lab.segments-0"
  docker compose exec -T "$LIVE" ls -l /var/lib/kafka/data/lab.segments-0 | awk 'NR > 1 {printf "  %10s  %s\n", $5, $NF}'
  echo
  echo "  Dosya adı = segment'teki ilk offset. En sondaki aktif segment: yazılar yalnızca ona eklenir."
  exit 0
fi

dir=/var/lib/kafka/data/bank.transactions-${1:-0}
step "Partition dizini her replica'da (RF=3)"
for b in "${BROKERS[@]}"; do
  echo "  ### $b:$dir"
  docker compose exec -T "$b" ls -l "$dir" 2>/dev/null | awk 'NR > 1 {printf "  %10s  %s\n", $5, $NF}' || echo "    (yok)"
done

step "İlk segmentin içi: kafka-dump-log (ilk 2 batch)"
docker compose exec -T "$LIVE" bash -c "
  /opt/kafka/bin/kafka-dump-log.sh --files \$(ls $dir/*.log | head -1) --print-data-log 2>/dev/null \
    | awk '/^baseOffset/ {b++} b > 2 {exit} {print}' \
    | sed -E 's/ (isTransactional|isControl|deleteHorizonMs|position|CreateTime|size|magic|crc|isvalid|headerKeys|valueSize|keySize): [^ ]*//g; s/(payload: .{0,110}).*/\1 …/'"

step "Index: offset → .log içindeki byte konumu (seyrek)"
docker compose exec -T "$LIVE" bash -c "/opt/kafka/bin/kafka-dump-log.sh --files \$(ls $dir/*.index | head -1) 2>/dev/null | sed -n '2,5p'"
