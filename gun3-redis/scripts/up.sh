#!/usr/bin/env bash
# make up: ön koşullar, başlatma, hazır olana kadar bekleme, Python ortamı, feature job'ını gönderme.
source "$(dirname "$0")/lib.sh"

step "Ön koşullar"
docker info > /dev/null 2>&1 || { err "Docker çalışmıyor → Docker Desktop'ı başlatın"; exit 1; }
ok "docker $(docker version --format '{{.Server.Version}}')"
command -v uv > /dev/null || { err "uv yok → https://docs.astral.sh/uv/getting-started/installation/"; exit 1; }
ok "$(uv --version)"
for pair in "KAFKA_PORT:$KAFKA_PORT" "KAFKA_UI_PORT:$KAFKA_UI_PORT" "FLINK_UI_PORT:$FLINK_UI_PORT" "REDIS_PORT:$REDIS_PORT"; do
  var=${pair%%:*}; port=${pair#*:}
  owner=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | awk 'NR == 2 {print $1}' || true)
  if [[ -n $owner ]] && ! docker compose ps --format '{{.Ports}}' 2>/dev/null | grep -q ":$port->"; then
    err "Port $port dolu ($owner). Başka bir günün ortamı açık olabilir → o gün klasöründe: make down"
    echo "    Değilse: cp .env.example .env ve $var değerini değiştirin."
    exit 1
  fi
done
ok "portlar boş"

step "Başlatılıyor (ilk seferde Flink imajı derlenir, ~1 dk)"
docker compose up -d --build 2>&1 | grep -E 'Built|Error|error' | sed 's/^/  /' || true

step "Hazır olması bekleniyor"
deadline=$((SECONDS + 240))
while :; do
  init=$(docker compose ps -a --format '{{.State}} {{.ExitCode}}' kafka-init 2>/dev/null | head -1)
  tms=$(curl -sf "$FLINK/overview" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["taskmanagers"])' 2>/dev/null || echo 0)
  [[ $(health kafka) == healthy && $init == "exited 0" && $(health jobmanager) == healthy && $tms -ge 1 \
     && $(health redis) == healthy ]] && break
  if [[ $init == exited* && $init != "exited 0" ]]; then err "Topic oluşturma başarısız → docker compose logs kafka-init"; exit 1; fi
  if (( SECONDS > deadline )); then err "240 sn içinde hazır olmadı → docker compose ps ; docker compose logs"; exit 1; fi
  sleep 3
done
ok "Kafka hazır, topic'ler oluşturuldu"
ok "Redis → localhost:$REDIS_PORT"
ok "Flink 1.20.5 → $FLINK (TaskManager bağlı)"
for _ in $(seq 1 30); do curl -sf "http://localhost:$KAFKA_UI_PORT/actuator/health" > /dev/null 2>&1 && break; sleep 2; done
curl -sf "http://localhost:$KAFKA_UI_PORT/actuator/health" > /dev/null 2>&1 \
  && ok "Kafka UI → http://localhost:$KAFKA_UI_PORT" || warn "Kafka UI henüz açılmadı → http://localhost:$KAFKA_UI_PORT"

step "Python ortamı (uv sync)"
uv sync --quiet && ok "$(uv run --quiet python -c 'import sys, importlib.metadata as m; print("Python", sys.version.split()[0], "· confluent-kafka", m.version("confluent-kafka"), "· redis-py", m.version("redis"))')"
step "Flink feature job'ı"
bash scripts/submit.sh | sed '/Feature.lar her dakika/d'
echo
echo "${G}${B}✓ Ortam hazır.${N}  Kontrol: make test"
echo "  Sonra: make produce (trafik) ve make worker (Redis + skor)"
