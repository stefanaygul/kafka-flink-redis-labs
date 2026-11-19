# Gün 2 · Flink SQL ile akış işleme (90 dk)

Gün 1'de Kafka'ya yazdığımız işlemleri bugün **Flink SQL** ile gerçek zamanlı işleyeceğiz. Gün sonunda şunları
görmüş olacaksınız:

- Event time ve **watermark**: Flink "zamanı" neye göre ilerletir, bir pencere ne zaman kapanır.
- **TUMBLE** ve **HOP** pencereleri ile dakikalık ve kayan 5 dakikalık özetler.
- **Geç gelen olaylar**: hangisi hâlâ sayılır, hangisi atılır.
- **Temporal join**: her işlemi, işlemin olduğu andaki müşteri risk kategorisiyle eşleştirmek.
- Sürekli çalışan bir **feature job'ı** (Gün 3'ün girdisi) ve TaskManager çökünce **checkpoint**'ten devam etmesi.

```
 producer.py ──▶ bank.transactions ──▶ Flink SQL ─────────────────────────────▶ bank.customer-features
                                       event time + watermark                    (müşteri başına son 5 dk,
 make profile ──▶ bank.customer-profiles ──▶ temporal join                        her dakika; Gün 3'te Redis'e)
```

**Kod hazır.** SQL'ler `sql/` klasöründe, her biri tek bir konu. Önemli satırlar `-- >>> LAB` ile işaretli.
Sorguyu kopyalamanıza gerek yok: `make query Q=<dosya>` çalıştırır, sonuçlar terminale akar, **Ctrl+C** ile durur.

| Dosya | Konu |
| --- | --- |
| `00-tables.sql` | Tablolar: Kafka topic'lerinin üzerine konmuş şemalar. Event time ve watermark burada (LAB 1) |
| `01-izle.sql` | Canlı akış ve anlık watermark |
| `02-tumble.sql` · `03-hop.sql` | Dakikalık ve kayan 5 dakikalık pencereler |
| `04a-…` · `04b-…` | Watermark ve geç gelen olaylar (deterministik senaryo) |
| `05-temporal-join.sql` | İşlem anındaki risk kategorisi (LAB 2) |
| `06-features.sql` | Sürekli çalışan feature job'ı |

---

## Kurulum (5 dk)

Gün 1 ortamı açıksa önce kapatın (aynı portları kullanıyor): `cd ../gun1-kafka && make down`

```bash
cd gun2-flink
make up      # Kafka + Flink + Kafka UI. İlk seferde Flink imajı derlenir (~1 dk)
make test    # 6 test, ~30 sn: hepsi PASSED olmalı
```

| | Adres |
| --- | --- |
| Flink UI | <http://localhost:8081> |
| Kafka UI | <http://localhost:8080> (port `.env` ile değişir) |

Bütün komutlar: `make help`. Birden fazla terminal açacaksınız; hepsinde önce `cd gun2-flink`.

---

## Görev 1 · Flink'i tanı (5 dk)

Flink UI → *Overview*: 1 TaskManager, 8 slot. JobManager işi planlar, TaskManager çalıştırır. Bir sorgu
(SELECT veya INSERT) Flink'te bir **job** olur; `parallelism = 2` ile her job 2 slot kullanır.

`sql/00-tables.sql`'i açın: `transactions` tablosu veriyi **tutmaz**, `bank.transactions` topic'inin üzerine
konmuş bir şema ve okuma ayarıdır. `>>> LAB 1`: `event_time` ve `WATERMARK` satırları.

## Görev 2 · Canlı akış ve watermark (10 dk)

```bash
make produce               # T1: 300 müşteri, saniyede 10 işlem (açık kalsın)
make query Q=01-izle       # T2
```

```
| op | customerId |  amount | merchantCategory |              event_time |               watermark |
| +I |     C-1238 |  359.61 |           TRAVEL | 2026-11-17 17:49:01.086 |                  <NULL> |
| +I |     C-1158 |  349.31 |          GROCERY | 2026-11-17 17:49:00.986 |                  <NULL> |
  …
| +I |     C-1007 | 4467.05 |      ELECTRONICS | 2026-11-17 17:49:02.578 | 2026-11-17 17:48:51.621 |
| +I |     C-1277 |   87.60 |        UTILITIES | 2026-11-17 17:49:02.627 | 2026-11-17 17:48:51.621 |
```

- `+I` = insert: yeni satır.
- İlk satırlarda watermark `<NULL>`: henüz üretilmedi. Sonra event time'ın ~10 sn gerisinden gelir
  (en büyük event time − 10 sn; 6 partition'ın en gerideki belirler).
- Satırlar event time'a göre sıralı değil (17:49:01.086 → 00.986): olaylar farklı partition'lardan karışık sırayla
  gelir. Watermark bu karışıklığa 10 sn tolerans tanır.

Ctrl+C ile durdurun (job da kapanır).

## Görev 3 · Pencereler (15 dk)

```bash
make query Q=02-tumble     # dakikalık özet: her dakika bir satır
```

```
| op |            window_start |              window_end | islem |  toplam_tl | musteri |
| +I | 2026-11-17 17:36:00.000 | 2026-11-17 17:37:00.000 |  1200 | 1039803.39 |     293 |
```

Satır, dakika bittikten ~10 sn sonra gelir: watermark pencerenin sonunu geçince.

```bash
make query Q=03-hop        # müşteri başına son 5 dk, her dakika güncellenir
```

Aynı işlem 5 ayrı pencerede sayılır (5 dk boyunca her dakika). Riskli müşteriler (C-1007, C-1042) normal
müşterilerden çok daha sık ve büyük tutarlarla işlem yapar; Gün 3'ün skorlaması bunun üzerine kurulu.

<details><summary>Sorular ve cevaplar</summary>

- **TUMBLE ile HOP farkı?** TUMBLE pencereleri üst üste binmez; her işlem tek pencerede. HOP (5 dk, 1 dk kayma)
  pencereleri üst üste biner; her işlem 5 pencerede. "Son 5 dakikada" gibi sorular için HOP.
- **Neden pencere bitince hemen değil de ~10 sn sonra?** Flink, o dakikaya ait geç kalmış olay gelebilir diye
  watermark'ı bekler. Daha kısa gecikme = daha erken sonuç ama daha çok atılan geç olay.
</details>

## Görev 4 · Watermark ve geç gelen olaylar (20 dk)

Canlı trafikte zaman çok hızlı akar. Burada sabit saatli, ağır çekim bir senaryo kullanıyoruz (ayrı topic:
`lab.scenario-transactions`, watermark 1 dk geride).

**4a · Olay olay watermark:**

```bash
make query Q=04a-watermark-olaylar     # T2
make scenario S=watermark              # T3: duraklarda Enter
```

C-2001 her dakika bir işlem yapar (W1 11:00:30, W2 11:01:30, …). Her olaydan sonra watermark = en büyük event time −
1 dk.

**4b · Pencere tam olarak ne zaman yazılır?** T2'de Ctrl+C, sonra:

```bash
make query Q=04b-watermark-pencere     # T2
make scenario S=watermark              # T3
```

```
| +I | C-2001 | 2026-11-14 10:56:00.000 | 2026-11-14 11:01:00.000 | 1 |  10.00 |   ← W3 ile (watermark 11:01:30)
| +I | C-2001 | 2026-11-14 10:57:00.000 | 2026-11-14 11:02:00.000 | 2 |  30.00 |   ← W4 ile
| +I | C-2001 | 2026-11-14 10:58:00.000 | 2026-11-14 11:03:00.000 | 3 |  60.00 |
| +I | C-2001 | 2026-11-14 10:59:00.000 | 2026-11-14 11:04:00.000 | 4 | 100.00 |
| +I | C-2001 | 2026-11-14 11:00:00.000 | 2026-11-14 11:05:00.000 | 5 | 150.00 |
```

Satırlar SQL client'ta birkaç saniye gecikmeyle görünebilir; bu, client'ın sonuçları toplu çekmesinden kaynaklanır.

**4c · Geç gelen olaylar:** aynı iki sorguyla `make scenario S=late-events`. Senaryo her adımda ne olması gerektiğini
yazar:

- O4, O3'ten sonra gelir ama event time'ı daha erkendir: watermark'ın gerisinde değil, **sayılır**.
- L2 (2 dk geç): kapanmış pencereler için atılır, hâlâ açık pencerelere **girer**.
- L10 (10 dk geç): içinde olduğu pencerelerin hepsi kapanmış, **hiçbir sonuçta yok**.

<details><summary>Sorular ve cevaplar</summary>

- **Watermark gecikmesini 1 dk'dan 10 dk'ya çıkarsak?** L10 da sayılırdı, ama her pencere 10 dk geç yazılırdı.
  Gecikme ↔ eksiksizlik takası.
- **Geç olayları atmak yerine ne yapılabilir?** Ayrı bir çıktıya yönlendirip sonradan düzeltmek (side output) veya
  pencereyi güncelleyerek yeniden yazmak. Bunlar SQL'de değil DataStream API'de yapılır.
- **Neden canlı tabloda 10 sn, senaryoda 1 dk?** Canlı trafik saniyelik; senaryo dakikalık. Gecikme verinin ne kadar
  karışık geldiğine göre seçilir.
</details>

## Görev 5 · Temporal join (15 dk)

```bash
make produce                           # T1 (açık değilse)
make query Q=05-temporal-join          # T2: C-1001 ve C-1007'nin işlemleri ve kategorisi
make profile C=C-1001 R=HIGH           # T3: C-1001'in kategorisini değiştir
```

```
C-1001   2026-11-17 17:35:47.619   LOW
C-1001   2026-11-17 17:35:47.876   LOW
C-1001   2026-11-17 17:36:22.817   HIGH      ← 17:36:16'da HIGH oldu
```

`sql/05-temporal-join.sql` → `>>> LAB 2`: `FOR SYSTEM_TIME AS OF t.event_time`. Her işlem, **gerçekleştiği andaki**
profil sürümüyle eşleşir. Profil değişince geçmiş işlemlerin sonucu değişmez.

<details><summary>Sorular ve cevaplar</summary>

- **Normal JOIN ile farkı?** Normal join profil tablosunun **güncel** hâlini kullanır; kategori değişince eski
  işlemler de yeni kategoriyle eşleşmiş gibi olur. Denetim ve skor açıklanabilirliği için yanlış.
- **Profil topic'i neden compacted?** Her müşterinin yalnızca son kategorisi kalır; topic sonsuza kadar büyümez ama
  Flink açılışta her müşterinin geçerli kategorisini yine okuyabilir.
</details>

## Görev 6 · Feature job (10 dk)

```bash
make submit                # sql/06-features.sql: INSERT INTO → sürekli çalışan job
make features              # bank.customer-features'taki son pencereler (~1 dk sonra)
```

```
  müşteri  pencere (UTC)                işlem    toplam TL  en büyük TL  kategori sayısı
  C-1042   17:38–17:43 2026-11-17         237   691,234.13    26,672.57                8
  C-1007   17:38–17:43 2026-11-17         240   667,643.54    18,619.51                8
  C-1012   17:38–17:43 2026-11-17          23    20,867.65     4,494.30                8
```

- SELECT'ten farkı: INSERT INTO bir **job** olur ve siz SQL client'ı kapatsanız da çalışır. Flink UI → *Running Jobs*
  → `customer-features`: kutucuklar (source → window → sink) ve saniyedeki kayıt sayıları.
- Kafka UI → *Topics* → `bank.customer-features`: key = müşteri, `cleanup.policy = compact`. Her müşterinin son
  penceresi saklanır; Gün 3'te Redis bu topic'ten yeniden kurulabilecek.

## Görev 7 · Checkpoint ve çökme (10 dk)

```bash
make tm-restart
```

```
  ✓ son tamamlanan checkpoint: #21
TaskManager yeniden başlatılıyor (job'ın çalıştığı süreç ölüyor)
  ✓ job yeniden RUNNING, checkpoint #21'dan geri yüklendi (state kaybolmadı)
```

Flink UI → job → *Checkpoints*: her 10 sn'de bir checkpoint; *Latest Restore*. Job'ın penceredeki ara sonuçları
(state) ve Kafka'da nereye kadar okuduğu (offset) birlikte checkpoint'e yazılır. Çökmeden sonra ikisi birlikte geri
yüklenir; sayımlar ne eksik ne fazla olur.

<details><summary>Sorular ve cevaplar</summary>

- **Checkpoint ile son çöküş arasındaki olaylar ne oldu?** Offset de checkpoint'teki yere döndüğü için Kafka'dan
  **tekrar okundu**. State ile offset birlikte geri döndüğü için iç sayımlar tutarlı kalır (exactly-once state).
- **Dışarıya yazılan sonuçlar da mı tek sefer?** Kafka'ya yazan sink varsayılan olarak at-least-once'tır: checkpoint ile
  çöküş arasında yazılmış bir sonuç tekrar yazılabilir. Bu yüzden Gün 3'te Redis'e yazarken pencere sonunu
  (`windowEnd`) sürüm olarak kullanacağız: eski veya tekrar gelen sonuç yenisini ezmez.
</details>

---

## Kapanış

```bash
make test             # her şey yerinde mi
make down             # durdur (feature job da durur; Gün 3 kendi job'ını başlatır)
```

Prova testleri (eğitmen): `make test-pipeline` (feature job uçtan uca, ~1,5 dk), `make test-failover`
(TaskManager çökmesi, ~1 dk).

## Sık karşılaşılan sorunlar

| Belirti | Çözüm |
| --- | --- |
| `make up`: `Port 9092 dolu` | Başka bir günün ortamı açık: o gün klasöründe `make down` |
| `make test`: `Kafka … veya Flink … yanıt vermiyor` | `make up` |
| Sorguda hiç satır yok | Producer açık mı (`make produce`)? Pencere sorgularında ilk satır ~1 dk sonra gelir |
| Watermark sütunu `<NULL>` | Henüz watermark üretilmedi; birkaç olay sonra gelir |
| SQL client açılırken `WARNING: Unknown module …` | Flink'in Java ayarı, zararsız |
| `make submit`: zaten çalışıyor | Doğru: job bir kez gönderilir. Durdurmak için `make cancel J=customer-features` |
| Slotlar doldu, yeni sorgu başlamıyor | Açık kalan sorgular: `make jobs` → `make cancel J=select` |
| Her şey karıştı | `make reset` |
