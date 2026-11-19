# Gün 3 · Redis ile online feature store ve risk skoru (90 dk)

Gün 2'de Flink her müşteri için son 5 dakikanın özetini (feature) Kafka'ya yazıyordu. Bugün bu feature'ları
**Redis**'e taşıyıp milisaniyede okunabilir hâle getireceğiz ve her pencere için bir **risk skoru** üreteceğiz.
Gün sonunda şunları görmüş olacaksınız:

- Online feature store: Flink'in sonucu Redis'te, müşteri başına tek kayıt, ~10 ms'de okunur.
- **Versiyonlu yazım**: tekrar gelen veya sırası karışmış eski bir pencere yenisini ezmez.
- Her pencere için skor **bir kez** yayımlanır; worker çökse bile.
- **Redis kesintisi**: worker bekler, hiçbir feature kaybolmaz.
- **Redis verisi kaybolursa**: compacted Kafka topic'inden saniyeler içinde yeniden kurulur.
- **Tazelik**: Redis'teki değer ne kadar eski, karar verirken güvenilir mi?

```
 producer.py ─▶ bank.transactions ─▶ Flink (feature job) ─▶ bank.customer-features ─▶ feature_worker.py ─▶ Redis
                                                            (compacted)                    │            features:C-1007
                                                                                           └──▶ bank.risk-scores
```

**Kod hazır, kod yazmıyoruz.** Önemli parçalar `# >>> LAB n` … `# <<< LAB n` arasında; her birinde "olmasaydı ne
olurdu" notu var.

| İşaret | Dosya | Ne yapıyor |
| --- | --- | --- |
| LAB 1 | `feature_store.py` | Versiyonlu yazım (Lua): `windowEnd` daha yeni değilse yazma |
| LAB 2 | `feature_worker.py` | Her pencere için skor bir kez yayımlanır (çökme sonrası da) |
| LAB 3 | `feature_worker.py` | Redis kesintisinde bekle; offset commit edilmez, feature kaybolmaz |
| LAB 4 | `tools.py` | Tazelik kontrolü: 2 dk'dan eski feature "bayat" |
| LAB 5 | `tools.py` | Redis'i compacted topic'ten yeniden kurma |

Skor kuralları `scoring.py`'de, dört basit kural: 5 dk toplam ≥ 20.000 TL (+40), tek işlem ≥ 10.000 TL (+30),
≥ 10 işlem (+20), ≥ 5 farklı kategori (+10). 60+ HIGH, 30+ MEDIUM, altı LOW.

---

## Kurulum (5 dk)

Önceki günün ortamı açıksa kapatın (aynı portlar): `cd ../gun2-flink && make down`

```bash
cd gun3-redis
make up      # Kafka + Flink + Redis + Kafka UI; Flink feature job'ı da gönderilir (~30 sn)
make test    # 10 test, ~40 sn: hepsi PASSED olmalı
```

| | Adres |
| --- | --- |
| Flink UI | <http://localhost:8081> (`customer-features` job'ı çalışıyor olmalı) |
| Kafka UI | <http://localhost:8080> (port `.env` ile değişir) |
| Redis | `localhost:6379` · `make redis-cli` |

---

## Görev 1 · Pipeline'ı çalıştır (10 dk)

```bash
make produce        # T1: canlı trafik (açık kalsın)
make worker         # T2: feature worker (açık kalsın)
```

İlk feature'lar ~1 dk sonra gelir (Flink dakikalık pencere kapanınca). Worker:

```
Feature worker: bank.customer-features → Redis (redis://localhost:6379/0) → bank.risk-scores
SKOR C-1007 HIGH   100  5 dk toplam ≥ 20.000 TL, tek işlem ≥ 10.000 TL, 5 dk'da ≥ 10 işlem, ≥ 5 farklı kategori
işlenen=567 yazılan=567 eski/tekrar=0 skor=567 HIGH=6
```

Her dakika her müşteri için bir feature gelir; worker Redis'e yazar ve skoru `bank.risk-scores`'a gönderir. Yalnızca
HIGH skorlar ekrana basılır.

## Görev 2 · Online feature store (15 dk)

```bash
make stats                  # Redis'te 294 müşterinin feature'ı var.
make feature C=C-1007       # riskli müşteri
make feature C=C-1100       # normal müşteri
```

```
  C-1007  pencere 18:08:00–18:13:00 UTC   (Redis'ten 8.3 ms)
  işlem=60  toplam=174,131.82 TL  en büyük=18,596.97 TL  farklı kategori=8
  skor=100  seviye=HIGH  5 dk toplam ≥ 20.000 TL, tek işlem ≥ 10.000 TL, 5 dk'da ≥ 10 işlem, ≥ 5 farklı kategori
Taze: pencere 41 sn önce bitti.

  C-1100  pencere 18:08:00–18:13:00 UTC   (Redis'ten 14.2 ms)
  işlem=4  toplam=1,096.78 TL  en büyük=331.12 TL  farklı kategori=3
  skor=0  seviye=LOW  -
```

Redis'te nasıl duruyor?

```bash
make redis-cli
> HGETALL features:C-1007
> TTL features:C-1007
> QUIT
```

Müşteri başına bir HASH: `windowStart`, `windowEnd`, `txnCount`, `totalAmount`, `maxAmount`,
`distinctCategories`, `publishedWindowEnd`. 1 saat TTL: işlem yapmayan müşterinin kaydı kendiliğinden silinir.

```bash
make scores                 # bank.risk-scores özeti
```

```
798 skor, 294 müşteri · son seviyeler: HIGH=3, MEDIUM=2, LOW=289 · aynı pencere için tekrar: 0
```

<details><summary>Sorular ve cevaplar</summary>

- **Neden Redis, neden doğrudan Kafka'dan okumuyoruz?** Bir ödeme anında "bu müşterinin son 5 dakikası" sorusuna
  milisaniyede cevap gerekir. Kafka bir log'dur, key'e göre arama yapmaz. Redis key'e göre anında okur.
- **Neden müşteri başına tek kayıt?** Online soru hep "şu an"dır. Geçmiş Kafka'da (ve Flink'te) duruyor.
- **`publishedWindowEnd` ne?** Bu pencerenin skoru yayımlandı mı işareti (LAB 2, Görev 3).
</details>

## Görev 3 · Tekrar ve eski veri: versiyonlu yazım (15 dk)

Kafka at-least-once teslim eder: worker çöküp yeniden başlarsa, Flink checkpoint'ten devam ederse veya biri topic'i
baştan okutursa aynı feature tekrar gelir. Sırası karışmış eski bir pencere de gelebilir.

`feature_store.py` → `>>> LAB 1`: yazım Redis içinde Lua script'iyle yapılır. Gelen `windowEnd`, Redis'tekinden büyük
değilse **yazılmaz**. Kontrol ve yazma tek adımda (atomik): araya başka bir yazım giremez.

`feature_worker.py` → `>>> LAB 2`: skor yalnızca Redis'teki en güncel pencere içinse ve henüz yayımlanmadıysa
hesaplanır, yayımlandıktan sonra işaretlenir.

Bu davranışları testler gerçek Kafka ve Redis'le kanıtlıyor:

```bash
uv run pytest -v -k "lab1 or lab2"
```

| Test | Ne gönderiliyor | Sonuç |
| --- | --- | --- |
| `test_lab1_…` | Yeni, aynısı, daha eski pencere | Yalnızca yeni yazılır; Redis'te hep en güncel |
| `test_lab2_worker_tekrar_…` | 5 feature: biri tekrar, biri eski | `yazılan=3 eski/tekrar=2 skor=3`: her pencere bir skor |
| `test_lab2_…_cokme_…` | Worker Redis'e yazdıktan sonra, skordan önce çöker | Yeniden başlayınca feature "tekrar" ama skor eksik → **bir kez** yayımlanır |

<details><summary>Sorular ve cevaplar</summary>

- **Neden "önce oku, sonra yaz" iki ayrı komutla olmaz?** İki worker (veya bir worker ve yeniden kurma aracı) aynı
  anda yazarsa araya girer: ikisi de "benimki yeni" der, eskisi sonra yazılır. Lua script Redis'te bölünmeden çalışır.
- **Skor bir kez yayımlanıyor ama Kafka'ya iki kez yazılabilir mi?** Skor gönderilip işaret konmadan çökerse evet:
  aynı `scoreId` (müşteri:pencere) ile ikinci kez. İçerik birebir aynıdır; okuyan taraf `scoreId` ile ayıklar.
  `make scores` bunu "aynı pencere için tekrar" diye sayar.
</details>

## Görev 4 · Redis kesintisi (10 dk)

T1 ve T2 açıkken:

```bash
make redis-stop     # T3
```

Worker (T2):

```
Redis erişilemiyor → bekleniyor. Offset commit edilmiyor, feature kaybolmaz.
Redis erişilemiyor → bekleniyor. Offset commit edilmiyor, feature kaybolmaz.
```

~1 dk bekleyin (Flink yeni pencereler yazmaya devam ediyor, Kafka'da birikiyor), sonra:

```bash
make redis-start
```

```
Redis geri geldi (49 sn kesinti) → kaldığı yerden devam
işlenen=866 yazılan=866 eski/tekrar=0 skor=866 HIGH=9
```

`feature_worker.py` → `>>> LAB 3`. Worker hatayı yutup sonraki mesaja geçmedi (kayıp olurdu), çökmedi de. Aynı
mesajda bekledi; offset commit edilmediği için worker o sırada çökseydi bile mesaj tekrar gelirdi. Kesinti sırasında
Flink'in yazdıkları Kafka'da bekledi.

## Görev 5 · Redis verisi kaybolursa (10 dk)

```bash
make redis-flush            # Redis'teki her şey silinir
make stats                  # Redis'te 0 müşterinin feature'ı var.
make feature C=C-1007       # Redis'te feature yok
make rebuild
make stats
```

```
Yeniden kuruldu: 658 yazım, 739 eski/tekrar kayıt atlandı (0.6 sn). Redis'te 300 müşteri.
```

`tools.py` → `>>> LAB 5`. `bank.customer-features` compacted: her müşterinin son pencereleri topic'te durur. Baştan
okuyup **aynı versiyonlu yazımla** Redis'e basıyoruz: eski kayıtlar atlanır (739), sonuç en güncel hâl. Skorlar zaten
yayımlandığı için "yayımlandı" işaretiyle yazılır; tekrar skor üretilmez.

<details><summary>Sorular ve cevaplar</summary>

- **Redis'te AOF açık, neden bu gerekli?** AOF yeniden başlatmaya karşı korur (`make redis-stop/start` veriyi kaybetmedi).
  Disk bozulması, yanlış `FLUSHALL`, yeni bir Redis kurulumu veya yeni bir bölge için "gerçeğin kaynağı" Kafka'dır.
- **Rebuild sırasında worker da yazıyor; çakışmaz mı?** İkisi de versiyonlu yazar: hangisi daha yeni pencereyi
  getirirse o kalır.
</details>

## Görev 6 · Tazelik: worker durursa (10 dk)

T2'de worker'ı **Ctrl+C** ile durdurun. 2 dakikadan fazla bekleyin (trafik ve Flink devam ediyor):

```bash
make feature C=C-1007
```

```
BAYAT: feature 2.6 dk önce bitmiş bir pencereye ait. Bu skora güvenmeyin.
```

Worker'ı tekrar başlatın (`make worker`): Kafka'da biriken feature'ları işler ve veri tazelenir.

```
işlenen=600 yazılan=600 eski/tekrar=0 skor=600 HIGH=6
Taze: pencere 54 sn önce bitti.
```

`tools.py` → `>>> LAB 4`. Redis "en son bildiğimiz" değeri verir, ne kadar eski olduğunu söylemez. Normalde her dakika
yeni pencere gelir; 2 dk'dan eskiyse pipeline (Flink veya worker) durmuş veya geride kalmıştır. Online karar veren
servis bunu kontrol etmeli: bayat veride ya ek kontrol ya da güvenli varsayılan.

---

## Kapanış

```bash
make test         # her şey yerinde mi
make down         # durdur (veri kalır)
```

Prova testleri (eğitmen): `make test-pipeline` (canlı trafik → Flink → Redis → skor, ~1,5 dk), `make test-failover`
(Redis kesintisi, ~15 sn).

## Sık karşılaşılan sorunlar

| Belirti | Çözüm |
| --- | --- |
| `make up`: `Port 9092 dolu` | Başka bir günün ortamı açık: o gün klasöründe `make down` |
| `make test`: `Kafka … veya Redis … yanıt vermiyor` | `make up` |
| `make test`: `Flink feature job'ı çalışmıyor` | `make submit` (Flink yeniden başladıysa job kaybolur) |
| Worker hiçbir şey işlemiyor | Producer açık mı? İlk feature'lar ~1 dk sonra gelir. Flink UI'da job RUNNING mi? |
| `make feature`: hep `BAYAT` | Worker çalışıyor mu (`make worker`)? Flink job'ı çalışıyor mu (`make jobs`)? |
| Her şey karıştı | `make reset` |
