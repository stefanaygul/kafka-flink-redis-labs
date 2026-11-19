SHELL := /bin/bash
.DEFAULT_GOAL := help
C ?= C-1007

.PHONY: help up down clean reset ps logs test test-pipeline test-failover produce worker \
        feature scores stats redis-cli redis-stop redis-start redis-flush rebuild jobs submit

help: ## Bu listeyi göster
	@awk 'BEGIN {FS = ":.*## "} /^[a-z-]+:.*## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# --- Ortam ---------------------------------------------------------------------------------------
up: ## Kafka + Flink (feature job dahil) + Redis'i başlat, hazır olana kadar bekle
	@bash scripts/up.sh

down: ## Durdur (veri kalır)
	docker compose down

clean: ## Durdur ve BÜTÜN veriyi sil (topic'ler, checkpoint'ler, Redis)
	docker compose down -v --remove-orphans

reset: clean up ## Sıfırdan başlat

ps: ## Container durumları
	@docker compose ps -a --format 'table {{.Service}}\t{{.State}}\t{{.Status}}\t{{.Ports}}'

logs: ## Log'ları izle (make logs L=redis)
	docker compose logs -f --tail 50 $(L)

test: ## Kabul testleri: Redis versiyonlu yazım, tek skor, çökme, yeniden kurma (~40 sn)
	@uv run --quiet pytest -v --tb=short --no-header

test-pipeline: ## Flink → Redis → skor uçtan uca (~1,5 dk)
	@uv run --quiet pytest -v --tb=short --no-header -m pipeline

test-failover: ## Redis kesintisi: worker bekler, veri kaybolmaz (~15 sn)
	@uv run --quiet pytest -v --tb=short --no-header -m failover

# --- Uygulamalar --------------------------------------------------------------------------------
produce: ## Canlı işlem trafiği (Ctrl+C)
	@uv run --quiet python producer.py

worker: ## Feature worker: Kafka → Redis → skor (Ctrl+C)
	@uv run --quiet python feature_worker.py

# --- Gözlem ve arıza ----------------------------------------------------------------------------
feature: ## Redis'ten online skor ve tazelik (make feature C=C-1007)
	@uv run --quiet python tools.py feature $(C)

scores: ## bank.risk-scores özeti
	@uv run --quiet python tools.py scores

stats: ## Redis'te kaç müşteri var
	@uv run --quiet python tools.py stats

redis-cli: ## Redis komut satırı (ör. HGETALL features:C-1007)
	docker compose exec redis redis-cli

redis-stop: ## Redis'i durdur (kesinti)
	@docker compose stop redis > /dev/null 2>&1 && echo "  Redis durdu. Worker terminaline bakın."

redis-start: ## Redis'i başlat
	@docker compose start redis > /dev/null 2>&1 && echo "  Redis başladı."

redis-flush: ## Redis'teki BÜTÜN veriyi sil (veri kaybı senaryosu)
	@docker compose exec -T redis redis-cli FLUSHALL > /dev/null && echo "  Redis boşaltıldı."

rebuild: ## Redis'i compacted bank.customer-features'tan yeniden kur
	@uv run --quiet python tools.py rebuild

jobs: ## Flink job'ları
	@curl -s localhost:$${FLINK_UI_PORT:-8081}/jobs/overview | python3 -c 'import json,sys; [print(" ", j["state"], j["name"][:60]) for j in json.load(sys.stdin)["jobs"] if j["state"] == "RUNNING"]'

submit: ## Feature job'ını gönder (make up bunu zaten yapar)
	@bash scripts/submit.sh
