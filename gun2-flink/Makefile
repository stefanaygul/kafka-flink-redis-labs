SHELL := /bin/bash
.DEFAULT_GOAL := help
Q ?= 01-izle
S ?= watermark
C ?= C-1001
R ?= HIGH
J ?= all

.PHONY: help up down clean reset ps logs test test-pipeline test-failover sql query submit jobs cancel \
        tm-restart produce scenario profile features

help: ## Bu listeyi göster
	@awk 'BEGIN {FS = ":.*## "} /^[a-z-]+:.*## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# --- Ortam ---------------------------------------------------------------------------------------
up: ## Kafka + Flink'i başlat, hazır olana kadar bekle, Python ortamını kur
	@bash scripts/up.sh

down: ## Durdur (veri kalır)
	docker compose down

clean: ## Durdur ve BÜTÜN veriyi sil (topic'ler, checkpoint'ler)
	docker compose down -v --remove-orphans

reset: clean up ## Sıfırdan başlat

ps: ## Container durumları
	@docker compose ps -a --format 'table {{.Service}}\t{{.State}}\t{{.Status}}\t{{.Ports}}'

logs: ## Log'ları izle (make logs L=taskmanager)
	docker compose logs -f --tail 50 $(L)

test: ## Kabul testleri: ortam + pencere/watermark doğruluğu (~1 dk)
	@uv run --quiet pytest -v --tb=short --no-header

test-pipeline: ## Feature job uçtan uca (~1,5 dk)
	@uv run --quiet pytest -v --tb=short --no-header -m pipeline

test-failover: ## TaskManager çökmesi → checkpoint'ten devam (~1,5 dk)
	@uv run --quiet pytest -v --tb=short --no-header -m failover

# --- Flink SQL ----------------------------------------------------------------------------------
sql: ## Etkileşimli SQL client (tablolar yüklü). Çıkış: QUIT;
	docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/00-tables.sql

query: ## Bir sorgu dosyasını çalıştır, sonuçlar akar; Ctrl+C ile dur (make query Q=03-hop)
	@bash scripts/query.sh $(Q)

submit: ## Feature job'ını gönder (sql/06-features.sql)
	@bash scripts/submit.sh

jobs: ## Çalışan Flink job'ları
	@uv run --quiet python tools.py jobs

cancel: ## Job durdur (make cancel J=customer-features; J=all hepsi)
	@uv run --quiet python tools.py cancel $(J)

tm-restart: ## TaskManager'ı çökert ve yeniden başlat: job checkpoint'ten devam eder
	@bash scripts/tm-restart.sh

# --- Veri ---------------------------------------------------------------------------------------
produce: ## Canlı trafik: 300 müşteri, saniyede 10 işlem (Ctrl+C)
	@uv run --quiet python producer.py

scenario: ## Deterministik senaryo (make scenario S=watermark | S=late-events)
	@uv run --quiet python producer.py --scenario $(S)

profile: ## Müşterinin risk kategorisini değiştir (make profile C=C-1001 R=HIGH)
	@uv run --quiet python tools.py profile $(C) $(R)

features: ## bank.customer-features'taki son feature'lar
	@uv run --quiet python tools.py features
