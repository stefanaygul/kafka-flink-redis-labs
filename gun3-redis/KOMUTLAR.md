# Gün 3 · Komutlar

Komutlar görev sırasıyla, her birinin altında ne olduğu ve ne göreceğiniz. Ayrıntılı açıklama ve sorular:
[README.md](README.md).

| İşaret | Anlamı |
| --- | --- |
| **T1, T2, T3** | Ayrı terminal pencereleri. Her yeni terminalde önce: `cd kafka-flink-redis-labs/gun3-redis` (repoyu klonladığınız yerde) |
| **biter** | Komut işini yapıp kendiliğinden kapanır. Bitmesini bekleyin, sonra sıradaki adım. |
| **açık kalır** | Sürekli çalışır. O terminali bırakın, sıradaki adımı başka terminalde yapın. **Ctrl+C** ile durur. |
| → | Ne göreceksiniz |

Bugünün düzeni: **T1 = trafik** (producer), **T2 = worker** (Redis'e yazar, skor üretir), **T3 = diğer komutlar**.
T1 ve T2 Görev 1'de açılır ve gün boyunca açık kalır (Görev 6'da T2'yi bilerek durduracağız).

---

## Eğitimden önce (prova, ~4 dk)

**0. Gün 2 açıksa · biter**
```bash
cd ../gun2-flink && make down && cd ../gun3-redis
```
→ Aynı portlar: iki gün aynı anda açık olamaz.

**1. T1 · biter (~40 sn)**
```bash
make reset
```
→ Kafka + Flink + Redis başlar, Flink feature job'ı gönderilir:
`✓ customer-features gönderildi ve çalışıyor` ve `✓ Ortam hazır.`

**2. T1 · biter**
```bash
make test               # ~45 sn  → 10 passed
make test-failover      # ~15 sn  → 1 passed (Redis kesintisi)
make test-pipeline      # ~1,5 dk → 1 passed (trafik → Flink → Redis → skor)
make reset              # provanın verisini sil
```

## Sınıf başı

**T1 · biter (~1,5 dk)**
```bash
make up && make test
```
→ `✓ Ortam hazır.` ve `10 passed`. Flink UI <http://localhost:8081>'de `customer-features` RUNNING olmalı.

---

## Görev 1 · Pipeline'ı çalıştır

**1. T1 · açık kalır**
```bash
make produce
```
→ Saniyede 10 işlem, 300 müşteri. 10 sn'de bir `gönderilen=…`.

**2. T2 · açık kalır**
```bash
make worker
```
→ Hemen: `Feature worker: bank.customer-features → Redis … → bank.risk-scores`. ~1 dk sonra (Flink ilk dakikalık
pencereyi kapatınca) `SKOR C-1007 HIGH 100 …` satırları ve 5 sn'de bir
`işlenen=… yazılan=… eski/tekrar=0 skor=… HIGH=…`. Yalnızca HIGH skorlar ekrana basılır.

## Görev 2 · Online feature store

Hepsi **T3**, hepsi **biter** (anında):

```bash
make stats
```
→ `Redis'te 294 müşterinin feature'ı var.`

```bash
make feature C=C-1007
```
→ Riskli müşteri: `işlem=60 toplam=174,131.82 TL … skor=100 seviye=HIGH`, `(Redis'ten 8.3 ms)` ve
`Taze: pencere 41 sn önce bitti.`

```bash
make feature C=C-1100
```
→ Normal müşteri: `işlem=4 toplam=1,096.78 TL … skor=0 seviye=LOW`.

```bash
make redis-cli
```
→ Redis komut satırı açılır (etkileşimli). Yazın: `HGETALL features:C-1007` (müşterinin bütün alanları),
`TTL features:C-1007` (kalan süre, ≤ 3600 sn), çıkmak için `QUIT`.

```bash
make scores
```
→ En yüksek skorlu müşteriler ve özet: `798 skor, 294 müşteri · son seviyeler: HIGH=3, MEDIUM=2, LOW=289 ·
aynı pencere için tekrar: 0`.

## Görev 3 · Versiyonlu yazım ve tek skor

**T3 · biter (~20 sn)** (T1 ve T2 açık kalabilir: testler ayrı topic ve Redis anahtarı kullanır)
```bash
uv run pytest -v -k "lab1 or lab2"
```
→ 3 test `PASSED`: tekrar veya eski feature yenisini ezmez; her pencere için tek skor; worker Redis'e yazıp skoru
yayımlamadan çökse bile skor bir kez yayımlanır. Kod: `feature_store.py` → `>>> LAB 1`, `feature_worker.py` →
`>>> LAB 2`.

## Görev 4 · Redis kesintisi

**1. T3 · biter**
```bash
make redis-stop
```
→ `Redis durdu.` T2'de 5 sn'de bir:
`Redis erişilemiyor → bekleniyor. Offset commit edilmiyor, feature kaybolmaz.` Worker çökmedi, bekliyor; Flink yeni
pencereleri Kafka'ya yazmaya devam ediyor.

**2. ~1 dk bekleyin, sonra T3 · biter**
```bash
make redis-start
```
→ T2'de `Redis geri geldi (49 sn kesinti) → kaldığı yerden devam`. `işlenen` sayısı biriken feature'larla birlikte
artar.

## Görev 5 · Redis verisi kaybolursa

Hepsi **T3**, hepsi **biter**, sırayla:

```bash
make redis-flush            # → Redis boşaltıldı.
make stats                  # → Redis'te 0 müşterinin feature'ı var.
make feature C=C-1007       # → Redis'te feature yok
make rebuild                # → Yeniden kuruldu: 658 yazım, 739 eski/tekrar kayıt atlandı (0.6 sn). Redis'te 300 müşteri.
make stats                  # → Redis'te 300 müşterinin feature'ı var.
make feature C=C-1007       # → skor=100 seviye=HIGH, Taze
```

`rebuild`, compacted `bank.customer-features` topic'ini baştan okur ve aynı versiyonlu yazımla Redis'e basar;
eski kayıtlar atlanır.

## Görev 6 · Tazelik: worker durursa

**1. T2 · Ctrl+C** (worker durur). T1 açık kalsın; Flink feature üretmeye devam eder ama Redis'e yazan yok.

**2. 2 dakikadan fazla bekleyin, sonra T3 · biter**
```bash
make feature C=C-1007
```
→ `BAYAT: feature 2.6 dk önce bitmiş bir pencereye ait. Bu skora güvenmeyin.`

**3. T2 · açık kalır**
```bash
make worker
```
→ Kafka'da biriken feature'ları birkaç saniyede işler: `işlenen=600 yazılan=600 …`.

**4. T3 · biter**
```bash
make feature C=C-1007
```
→ `Taze: pencere 54 sn önce bitti.`

---

## Kapanış ve kurtarma

```bash
make test           # biter: 10 passed (T1 ve T2 açıkken de çalışır)
make down           # biter: durdurur, veri kalır
```

T1 ve T2'yi Ctrl+C ile kapatmayı unutmayın.

| Durum | Komut |
| --- | --- |
| Flink feature job'ı çalışıyor mu? | `make jobs` → yoksa `make submit` |
| Redis'te ne var? | `make stats`, `make redis-cli` |
| Worker hiçbir şey işlemiyor | T1 açık mı? İlk feature'lar ~1 dk sonra gelir |
| Her şey karıştı | `make reset` |
