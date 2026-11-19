# Kafka, Flink ve Redis · Uygulamalı Lab'lar

Veri ekibi için uygulamalı Kafka, Flink ve Redis eğitimi. Örnekler Python ve Flink SQL. Her gün kendi klasöründe:
tek komutla ayağa kalkar, kendi testlerini taşır, kod hazırdır (önemli satırlar `LAB` notlarıyla işaretli).

| Gün | Lab | Süre | Konular |
| --- | --- | --- | --- |
| 1 | [gun1-kafka](gun1-kafka/) | 60–90 dk | 3 broker'lı cluster, broker kaybı ve NotEnoughReplicas, producer'da biriken veri, partition'ın içi, consumer group, lag, idempotent consumer, Kafka UI |
| 2 | [gun2-flink](gun2-flink/) | 90 dk | Flink SQL: event time ve watermark, TUMBLE/HOP pencereleri, geç gelen olaylar, temporal join, feature job, checkpoint ile çökme |
| 3 | [gun3-redis](gun3-redis/) | 90 dk | Redis online feature store, versiyonlu yazım, tek seferlik skor, Redis kesintisi, Kafka'dan yeniden kurma, tazelik |

Günler birbirinin çıktısını kullanır (Gün 1'in işlem formatı → Gün 2'nin feature'ları → Gün 3'ün skorları), ama her
gün kendi ortamını kurar: bir gün bitmeden öbürüne başlamak gerekmez.

## Ön koşullar

| Gereksinim | Kontrol |
| --- | --- |
| Docker Desktop (veya Engine + Compose v2), en az **6 GB** bellek (Gün 2–3'te Flink çalışır) | `docker info` |
| [uv](https://docs.astral.sh/uv/getting-started/installation/): Python 3.12'yi ve paketleri kendisi kurar | `uv --version` |
| make, bash (macOS/Linux; Windows'ta WSL 2) | `make --version` |

## Her gün aynı

```bash
git clone https://github.com/stefanaygul/kafka-flink-redis-labs.git
cd kafka-flink-redis-labs/gun1-kafka        # veya gun2-flink, gun3-redis
make up              # ortamı başlatır, hazır olana kadar bekler, Python ortamını kurar
make test            # her şey çalışıyor mu
make help            # bütün komutlar
make down            # gün bitince (veri kalır)
```

Günler aynı portları kullanır (Kafka 9092, Kafka UI 8080, Flink 8081, Redis 6379): **aynı anda tek gün açık olsun.**
Başka bir gün açıksa `make up` bunu söyler. Port başka bir uygulamadaysa: `cp .env.example .env`, değeri değiştirin.

Her günün klasöründe: `README.md` (kılavuz, gerçek çıktılarla) ve `KOMUTLAR.md` (kopyala-yapıştır komutlar, eğitim
öncesi prova dahil).

## Sabitlenmiş sürümler

| Bileşen | Sürüm |
| --- | --- |
| Apache Kafka (KRaft) | 4.3.1 (Gün 1: 3 node, Gün 2–3: 1 node) |
| Kafka UI | kafbat v1.5.0 |
| Apache Flink | 1.20.5 (Java 17) + Kafka SQL connector 3.4.0-1.20 (sha256 doğrulamalı) |
| Redis | 7.4.12 (AOF açık) |
| Python | 3.12 · confluent-kafka 2.16.1 · redis-py 8.1.0 (`uv.lock` ile sabit) |
