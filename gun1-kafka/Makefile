SHELL := /bin/bash
.DEFAULT_GOAL := help
N ?= 2
P ?= 0
TOPIC ?= bank.transactions
G ?= bank-ledger

.PHONY: help up down clean reset ps logs test test-failover describe brokers broker-stop broker-start \
        leaders segments segment-roll lag ledger-report ledger-reset

help: ## Bu listeyi göster
	@awk 'BEGIN {FS = ":.*## "} /^[a-z-]+:.*## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# --- Ortam ---------------------------------------------------------------------------------------
up: ## Cluster'ı başlat, hazır olana kadar bekle, Python ortamını kur
	@bash scripts/up.sh

down: ## Durdur (veri kalır)
	docker compose down

clean: ## Durdur ve BÜTÜN veriyi sil (topic'ler, defter)
	docker compose down -v --remove-orphans
	rm -rf data

reset: clean up ## Sıfırdan başlat

ps: ## Container durumları
	@docker compose ps -a --format 'table {{.Service}}\t{{.State}}\t{{.Status}}\t{{.Ports}}'

logs: ## Log'ları izle (make logs S=kafka-2)
	docker compose logs -f --tail 50 $(S)

test: ## Kabul testleri (~30 sn)
	@uv run --quiet pytest -v --tb=short --no-header

test-failover: ## Broker durdurma provası: NotEnoughReplicas, backpressure (~1 dk)
	@uv run --quiet pytest -v --tb=short --no-header -m failover

# --- Cluster ------------------------------------------------------------------------------------
describe: ## Broker'lar, controller, Leader / Replicas / ISR (make describe TOPIC=...)
	@bash scripts/describe.sh $(TOPIC)

brokers: ## Hangi broker çalışıyor?
	@bash scripts/broker.sh status

broker-stop: ## Broker durdur (make broker-stop N=2)
	@bash scripts/broker.sh stop $(N)

broker-start: ## Broker başlat (make broker-start N=2)
	@bash scripts/broker.sh start $(N)

leaders: ## Leader'ları preferred broker'lara geri ver
	@bash scripts/leaders.sh

segments: ## Partition'ın diskteki dosyaları (make segments P=0)
	@bash scripts/segments.sh $(P)

segment-roll: ## Segment değişimini göster (lab.segments'e ~3,5 MB yazar)
	@bash scripts/segments.sh roll

# --- Gözlem -------------------------------------------------------------------------------------
lag: ## Group'un lag'i, 2 sn'de bir (make lag G=lag-demo; varsayılan bank-ledger; Ctrl+C)
	@uv run --quiet python tools.py lag --group $(G) --watch

ledger-report: ## Defterde iki kez uygulanan işlem var mı?
	@uv run --quiet python tools.py ledger-report

ledger-reset: ## Defteri sil
	@uv run --quiet python tools.py ledger-reset
