#!/usr/bin/env bash
# make query Q=03-hop: sql/<Q>.sql'i çalıştırır, sonuçlar terminale akar. Ctrl+C ile durur;
# çıkarken bu sorgunun Flink'teki job'ı da kapatılır (slot boşa gitmesin).
source "$(dirname "$0")/lib.sh"
q=${1:-01-izle}
if [[ ! -f sql/$q.sql ]]; then
  err "sql/$q.sql yok. Mevcut sorgular:"; ls sql | grep -v 00-tables | sed 's/\.sql$//; s/^/    /'; exit 2
fi
cleanup() { uv run --quiet python tools.py cancel select > /dev/null 2>&1 || true; }
trap cleanup EXIT
echo "  sql/$q.sql çalışıyor. Sonuçlar aşağıda akar (birkaç saniye gecikebilir). Durdurmak için Ctrl+C."
sql_file "$q.sql"
