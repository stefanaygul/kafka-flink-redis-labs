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
KAFKA_PORT=${KAFKA_PORT:-9092}; KAFKA_UI_PORT=${KAFKA_UI_PORT:-8080}; FLINK_UI_PORT=${FLINK_UI_PORT:-8081}
REDIS_PORT=${REDIS_PORT:-6379}
FLINK="http://localhost:$FLINK_UI_PORT"

health() { docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$(docker compose ps -aq "$1" 2>/dev/null | head -1)" 2>/dev/null || echo yok; }

# Çalışan job'ın id'si (ada göre); yoksa boş
running_job() {
  curl -sf "$FLINK/jobs/overview" 2>/dev/null | python3 -c "
import json, sys
for j in json.load(sys.stdin)['jobs']:
    if j['name'] == sys.argv[1] and j['state'] in ('RUNNING', 'RESTARTING', 'CREATED', 'INITIALIZING'):
        print(j['jid']); break" "$1" || true
}

# SQL client: tablolar yüklü olarak bir SQL dosyası çalıştırır. Java uyarı satırları süzülür.
sql_file() {
  docker compose exec -T jobmanager ./bin/sql-client.sh -i /opt/sql/00-tables.sql -f "/opt/sql/$1" 2>&1 \
    | grep --line-buffered -vE '^WARNING: |^\s*$|org\.jline|^Command history file|^Successfully initialized|Executing SQL from file'
}
