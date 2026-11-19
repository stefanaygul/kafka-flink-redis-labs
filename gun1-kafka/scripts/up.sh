#!/usr/bin/env bash
# make up: ön koşulları kontrol eder, cluster'ı başlatır, hazır olana kadar bekler, Python ortamını kurar.
source "$(dirname "$0")/lib.sh"

step "Ön koşullar"
docker info > /dev/null 2>&1 || { err "Docker çalışmıyor → Docker Desktop'ı başlatın"; exit 1; }
ok "docker $(docker version --format '{{.Server.Version}}')"
command -v uv > /dev/null || { err "uv yok → https://docs.astral.sh/uv/getting-started/installation/"; exit 1; }
ok "$(uv --version)"

# Başka bir uygulamanın tuttuğu port: compose'un anlaşılmaz hatası yerine açık mesaj.
for var in KAFKA_1_PORT KAFKA_2_PORT KAFKA_3_PORT KAFKA_UI_PORT; do
  default=9092; [[ $var == KAFKA_2_PORT ]] && default=9093; [[ $var == KAFKA_3_PORT ]] && default=9094
  [[ $var == KAFKA_UI_PORT ]] && default=8080
  port=${!var:-$default}
  owner=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | awk 'NR == 2 {print $1}' || true)
  if [[ -n $owner ]] && ! docker compose ps --format '{{.Ports}}' 2>/dev/null | grep -q ":$port->"; then
    err "Port $port dolu ($owner). → cp .env.example .env ve $var değerini değiştirin."
    exit 1
  fi
done
ok "portlar boş"

step "Cluster başlatılıyor (ilk seferde imajlar indirilir)"
docker compose up -d 2>&1 | grep -vE '^\s*$' | sed 's/^/  /' | tail -12

step "Hazır olması bekleniyor"
deadline=$((SECONDS + 180))
while :; do
  states="$(health kafka-1) $(health kafka-2) $(health kafka-3)"
  init=$(docker compose ps -a --format '{{.State}} {{.ExitCode}}' kafka-init 2>/dev/null | head -1)
  [[ $states == "healthy healthy healthy" && $init == "exited 0" ]] && break
  if [[ $init == exited* && $init != "exited 0" ]]; then
    err "Topic oluşturma başarısız → docker compose logs kafka-init"; exit 1
  fi
  if (( SECONDS > deadline )); then
    err "180 sn içinde hazır olmadı (broker'lar: $states, kafka-init: ${init:-yok}) → docker compose logs"; exit 1
  fi
  sleep 2
done
for _ in $(seq 1 30); do   # yeniden başlatmada takipçiler ISR'ye birkaç saniyede katılır
  short=$(docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:29092 \
    --describe --under-replicated-partitions --topic bank.transactions 2>/dev/null | grep -c Partition || true)
  [[ $short == 0 ]] && break
  sleep 2
done
ok "3 broker sağlıklı, topic'ler hazır, bütün kopyalar senkron (ISR = 3)"
for _ in $(seq 1 30); do
  curl -sf "http://localhost:$KAFKA_UI_PORT/actuator/health" > /dev/null 2>&1 && break
  sleep 2
done
curl -sf "http://localhost:$KAFKA_UI_PORT/actuator/health" > /dev/null 2>&1 \
  && ok "Kafka UI → http://localhost:$KAFKA_UI_PORT" \
  || warn "Kafka UI henüz açılmadı → http://localhost:$KAFKA_UI_PORT (birkaç saniye sonra deneyin)"

step "Python ortamı (uv sync)"
uv sync --quiet && ok "$(uv run --quiet python -c 'import sys, importlib.metadata as m; print("Python", sys.version.split()[0], "· confluent-kafka", m.version("confluent-kafka"))')"

echo
echo "${G}${B}✓ Ortam hazır.${N}  Kontrol: make test"
