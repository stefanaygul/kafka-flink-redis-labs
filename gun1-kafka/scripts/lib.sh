#!/usr/bin/env bash
# Ortak yardımcılar (diğer script'ler "source" eder). macOS bash 3.2 uyumlu.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ -t 1 ]]; then G=$'\e[32m'; R=$'\e[31m'; Y=$'\e[33m'; B=$'\e[1m'; N=$'\e[0m'; else G=; R=; Y=; B=; N=; fi
ok()   { echo "  ${G}✓${N} $*"; }
warn() { echo "  ${Y}!${N} $*"; }
err()  { echo "  ${R}✗${N} $*"; }
step() { echo; echo "${B}$*${N}"; }

if [[ -f .env ]]; then
  while IFS='=' read -r key value; do
    [[ $key =~ ^[A-Z0-9_]+$ ]] && export "$key=${value%$'\r'}"
  done < .env
fi
KAFKA_UI_PORT=${KAFKA_UI_PORT:-8080}

# Çalışan broker'lar: kafka-1 kafka-2 kafka-3 içinden
running_brokers() {
  local running n
  running=$(docker compose ps --status running --services 2>/dev/null || true)
  for n in 1 2 3; do
    if grep -qx "kafka-$n" <<<"$running"; then echo "kafka-$n"; fi
  done
  return 0
}

# LIVE: komutun çalışacağı broker, BOOT: yalnızca çalışan broker'lar (kapalıyı listeye koymak uyarı üretir)
load_brokers() {
  BROKERS=($(running_brokers))
  if [[ ${#BROKERS[@]} -eq 0 ]]; then
    err "Çalışan broker yok → make up"
    exit 1
  fi
  LIVE=${BROKERS[0]}
  BOOT=$(printf '%s:29092,' "${BROKERS[@]}")
  BOOT=${BOOT%,}
}

# Çalışan bir broker'ın içinde Kafka aracı çalıştırır:  ktool kafka-topics --list ...
ktool() {
  local tool=$1
  shift
  docker compose exec -T "$LIVE" "/opt/kafka/bin/$tool.sh" "$@"
}

health() { docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$(docker compose ps -aq "$1" 2>/dev/null | head -1)" 2>/dev/null || echo yok; }
