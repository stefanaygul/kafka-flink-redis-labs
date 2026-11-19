# Gün 1 · Python ile Kafka (60–90 dk)

Bankacılık işlemlerini Python ile **3 broker'lı** bir Kafka cluster'ına gönderip tüketeceğiz. Gün sonunda şunları
kendi terminalinizde görmüş olacaksınız:

- Broker kapanınca cluster ne yapar, ne zaman yazmayı **reddeder** (`NotEnoughReplicas`).
- Broker'a yazılamayan mesajlar producer'da nasıl **birikir**, kuyruk dolunca ne olur.
- Partition'ın diskte nasıl durduğu (segment, index).
- Consumer group, rebalance ve **lag**.
- Consumer çöküp yeniden başlayınca aynı kaydın tekrar gelmesi ve bunun zararsız hâle getirilmesi.
- Kafka UI ile topic, mesaj, consumer group ve offset işlemleri.

```
                     3 broker · RF=3 · min.insync.replicas=2
 producer.py ──key=customerId──▶ bank.transactions (6 partition) ──▶ consumer.py ──▶ defter (SQLite)
                                 kafka-1 · kafka-2 · kafka-3          group bank-ledger    data/ledger.db
```

**Kod hazır, kod yazmıyoruz.** Güvenilirlik için eklenen birkaç parça kodda `# >>> LAB n` … `# <<< LAB n`
arasında; her birinin altında "olmasaydı ne olurdu" notu var (editörde `>>> LAB` diye arayın).

| İşaret | Dosya | Ne yapıyor |
| --- | --- | --- |
| LAB 1 | `producer.py` | Key = customerId: bir müşterinin işlemleri aynı partition'da, sıralı |
| LAB 2 | `producer.py` | Delivery report: teslim edilemeyen mesaj sayılır ve gösterilir |
| LAB 3 | `producer.py` | Yerel kuyruk doluysa bekle (backpressure): producer çökmez, mesaj kaybolmaz |
| LAB 4 | `consumer.py` | Offset, iş sonucu kalıcı olduktan **sonra** store edilir: çökmede mesaj kaybolmaz |
| LAB 5 | `consumer.py` | Idempotency: aynı işlem iki kez gelirse bakiye değişmez |

---

## Kurulum (5 dk)

| Gereksinim | Kontrol |
| --- | --- |
| Docker Desktop, en az 4 GB bellek | `docker info` |
| [uv](https://docs.astral.sh/uv/getting-started/installation/) (Python'u ve paketleri kendisi kurar) | `uv --version` |
| make | `make --version` |

```bash
cd gun1-kafka
make up        # 3 broker + Kafka UI başlar, hazır olana kadar bekler, Python ortamını kurar (~20 sn)
make test      # 10 test, ~25 sn: hepsi PASSED olmalı
```

| | Adres |
| --- | --- |
| Kafka UI | <http://localhost:8080> (port `.env` ile değişir) |
| Kafka | `localhost:9092`, `localhost:9093`, `localhost:9094` |

Bütün `make` hedefleri: `make help`. Birden fazla terminal açacaksınız; hepsinde önce `cd gun1-kafka`.

---

## Görev 1 · Cluster'ı tanı (10 dk)

```bash
make describe
```

```
Controller (KRaft)
  ✓ aktif controller: node 1  (3 oy veren node'dan çoğunluk = 2)

Topic: bank.transactions
  PartitionCount: 6  ReplicationFactor: 3
  Partition: 0    Leader: 2       Replicas: 2,3,1 Isr: 1,2,3
  Partition: 1    Leader: 3       Replicas: 3,1,2 Isr: 1,2,3
  ...
  min.insync.replicas=2
  ✓ bütün partition'larda ISR = 3
```

| Sütun | Anlamı |
| --- | --- |
| Leader | Bu partition'a yazma ve okuma trafiğini karşılayan broker |
| Replicas | Kopyaların durduğu broker'lar; ilki "tercih edilen" (preferred) leader |
| Isr | In-sync replicas: leader'a yetişmiş kopyalar. `acks=all` yazım bunları bekler |

**Kafka UI** → *Brokers*: 3 broker, URP (under-replicated partitions) = 0. *Topics* → `bank.transactions` →
*Overview* (partition'lar, leader'lar) ve *Settings* (`min.insync.replicas`).

<details><summary>Sorular ve cevaplar</summary>

1. **Leader'lar neden hep aynı broker'da değil?** Yazma ve okuma leader'a gider; leader'lar dağıtılarak yük
   paylaşılır.
2. **RF=3 ile kaç broker kaybı tolere edilir?** Veri kaybı olmadan 2. Ama `min.insync.replicas=2` ile **yazma**
   yalnızca 1 broker kaybına kadar sürer; 2. kayıpta durur (Görev 3).
3. **min.insync.replicas=2 ne demek?** `acks=all` ile yazan producer'a "en az 2 kopya" garantisi. `acks=1` bu ayara
   bakmaz.
</details>

---

## Görev 2 · Producer (10 dk)

```bash
uv run python producer.py --rate 50 --duration 20
```

```
SUMMARY delivered=1000 failed=0 undelivered=0
Partition dağılımı: p0=347 p1=192 p2=279 p3=45 p4=45 p5=92
```

- Kafka UI → *Topics* → `bank.transactions` → *Messages*: her mesajın **Key**'i `C-10xx`. Arama kutusuna bir müşteri
  yazın (ör. `C-1007`): mesajların hepsi **tek partition**'da.
- `producer.py` → `>>> LAB 1` (key) ve `>>> LAB 2` (delivery report).
- `create_config` içinde her ayarın yorumu var: `acks`, `enable.idempotence`, `linger.ms` / `batch.size`,
  `queue.buffering.max.messages`, `message.timeout.ms`.

**Sıcak müşteri:** trafiğin %70'i tek müşteriden gelirse?

```bash
uv run python producer.py --rate 50 --duration 10 --hot-customer C-1001
#   Partition dağılımı: p0=411 p1=31 p2=34 p3=4 p4=6 p5=14
```

<details><summary>Sorular ve cevaplar</summary>

- **Neden key?** Kafka sırayı yalnızca partition içinde garanti eder. Key = customerId → bir müşterinin bütün
  işlemleri aynı partition'da, sırayla.
- **Dağılım neden eşit değil?** 20 key'in hash'i 6 partition'a eşit düşmek zorunda değil. Az key = dengesiz dağılım.
- **Neden delivery report?** `produce()` hata vermeden dönse bile mesaj henüz yalnızca producer'ın yerel
  kuyruğundadır. Kesin sonuç `on_delivery`'de gelir; bakmayan producer veri kaybını fark etmez.
- **Sıcak partition'ı consumer ekleyerek hızlandırabilir miyiz?** Hayır. Bir partition'ı group'ta tek consumer okur.
</details>

---

## Görev 3 · Broker kaybı (20 dk)

**T1 (açık kalır):** sürekli trafik. Yerel kuyruğu bilerek küçük tutuyoruz (1000 mesaj).

```bash
uv run python producer.py --rate 100 --duration 900 --queue-max 1000
```

Her 2 sn'de bir: `gönderilen=… teslim=… hata=… kuyrukta=…`. **kuyrukta** = producer'ın belleğinde, teslim sonucu
henüz gelmemiş mesaj sayısı.

### 3.1 Bir broker durur → cluster devam eder

**T2:**

```bash
make broker-stop N=2
make describe
```

```
  Partition: 0    Leader: 3       Replicas: 2,3,1 Isr: 1,3
  ! 6/6 partition eksik kopyalı (ISR < 3)
```

Leader'lık kafka-2'den diğerlerine geçti, ISR 2 kopyaya indi. T1'de `teslim` artmaya devam ediyor,
`hata=0`. Kafka UI → *Brokers*: 2 broker, URP = 6.

### 3.2 İki broker durur → Kafka yazmayı reddeder

```bash
make broker-stop N=3
make describe
```

```
Controller (KRaft)
  ! aktif controller YOK: çoğunluk (2 node) ayakta değil. ...
  Partition: 0    Leader: 1       Replicas: 2,3,1 Isr: 1
  ✗ 6 partition min.insync.replicas=2 altında → acks=all yazım REDDEDİLİR
```

Broker'ın cevabını görmek için yeniden denemesiz kısa bir üretim yapın:

```bash
uv run python producer.py --rate 5 --duration 2 --no-retry
```

```
Teslim hatası #1: key=C-1016 → Broker: Not enough in-sync replicas
SUMMARY delivered=0 failed=10 undelivered=0
```

> **Labın en önemli cümlesi:** Kafka mesajı tek kopyaya yazıp "tamam" diyebilirdi; demedi. Söz verdiği dayanıklılığı
> (en az 2 kopya) sağlayamayacağı anda **yazmayı reddetti**.

### 3.3 Producer'da biriken veri

T1'e bakın. Producer hatayı yeniden dener; mesajlar teslim edilemedikçe **yerel kuyrukta birikir**:

```
gönderilen=2200 teslim=1846 hata=0 kuyrukta=354
gönderilen=2400 teslim=1846 hata=0 kuyrukta=554
gönderilen=2801 teslim=1846 hata=0 kuyrukta=955
Yerel kuyruk dolu (1000 mesaj) → yeni mesaj bekletiliyor (backpressure)
```

`teslim` durdu, kuyruk saniyede ~100 büyüdü, 1000'de **backpressure**: producer yeni mesaj üretmeyi bekletiyor ama
çökmüyor. `producer.py` → `>>> LAB 3`. Bu kod olmasaydı producer `BufferError: Local: Queue full` ile çöker,
kuyruktaki 1000 mesaj kaybolurdu.

### 3.4 Broker'lar geri gelir

```bash
make broker-start N=3
```

T1: kuyruk birkaç saniyede boşalır, `teslim` hızla artar, `hata=0`. **Hiçbir mesaj kaybolmadı.**

```bash
make broker-start N=2
make describe      # Isr: 1,2,3 ama leader'ların hepsi kafka-1'de
make leaders       # leader'lığı preferred broker'lara geri ver
```

T1'i **Ctrl+C** ile kapatın: `SUMMARY … failed=0 undelivered=0`.

<details><summary>Sorular ve cevaplar</summary>

- **`make broker-stop` ani çökme mi?** Hayır, kontrollü kapanış: broker leader'lığını devreder ve ISR'den hemen
  çıkarılır. Ani çökmede ISR'den çıkarılması `replica.lag.time.max.ms` (30 sn) sürer.
- **"aktif controller YOK" ne demek?** Her node aynı zamanda controller. 3 oy verenden 1'i ayakta, çoğunluk yok:
  mevcut leader'lar ve veri yerinde, ama leader seçimi, ISR değişikliği, topic oluşturma gibi metadata değişiklikleri
  yapılamaz. Production'da controller'lar ayrı node'larda çalışır.
- **Producer kuyruğu sonsuza kadar bekler mi?** Hayır. Her mesaj `message.timeout.ms` (varsayılan 5 dk) boyunca
  denenir; süre dolarsa `Local: Message timed out` ile delivery report'a düşer. Bu, veri kaybıdır ve bunu yalnızca
  LAB 2'deki kontrol görür. Gerçek sistemde alarm, yerel diske yazma veya upstream'i durdurma gerekir.
- **acks=1 ile yazsaydık?** İki broker kapalıyken `--acks 1` ile deneyin: yazım **başarılı** olur, çünkü yalnızca
  leader'ı bekler. Mesaj tek diskte; o broker da bozulursa kaybolur. Kafka 4.x'te bu mesajlar ISR toparlanana kadar
  consumer'a görünmez de (high watermark ilerlemez).
- **Leader'lar neden geri dönmedi?** Kafka bunu 5 dk'da bir kendisi yapar (`auto.leader.rebalance.enable`);
  `make leaders` beklemeden tetikler.
</details>

---

## Görev 4 · Partition'ın içi (10 dk, isteğe bağlı)

```bash
make segments
```

```
  ### kafka-1:/var/lib/kafka/data/bank.transactions-0
    10485760  00000000000000000000.index
      753252  00000000000000000000.log
    10485756  00000000000000000000.timeindex
  ### kafka-2: … aynı .log, aynı boyut

baseOffset: 0 lastOffset: 0 count: 1 baseSequence: 0 lastSequence: 0 producerId: 1 producerEpoch: 0 …
| offset: 0 sequence: 0 key: C-1011 payload: {"transactionId":"TXN-…","customerId":"C-1011",…

offset: 17 position: 4268
offset: 49 position: 8392
```

| Dosya | İçinde ne var |
| --- | --- |
| `.log` | Kayıtlar, batch'ler hâlinde. Dosya adı = içindeki ilk offset |
| `.index` | offset → `.log` içindeki byte konumu. Seyrek: ~4 KB'ta bir giriş |
| `.timeindex` | zaman → offset ("dün 14:00'ten itibaren oku") |
| `.snapshot` | idempotent producer durumu: hangi `producerId` hangi `sequence`'a kadar yazdı |

Kafka içeriği yorumlamaz: `payload` düz JSON, byte olarak saklanır. `producerId` + `sequence`, broker'ın producer'ın
yeniden gönderdiği batch'i tanıyıp ikinci kez yazmamasını sağlar (`enable.idempotence`).

```bash
make segment-roll
```

```
       512  00000000000000000000.index
   1048385  00000000000000000000.log
   1048385  00000000000000003380.log
   1048385  00000000000000006760.log
    576936  00000000000000010140.log      ← aktif segment
```

`lab.segments` topic'inde `segment.bytes` = 1 MB. Segment dolunca yenisi açılır; dosya adı = ilk offset. Yazılar
yalnızca aktif segmente eklenir; retention (eski verinin silinmesi) segment bazında çalışır. Kapanan segmentin
`.index`'i gerçek boyutuna (512 byte) küçülür; aktif segmentinki 10 MB önceden ayrılmıştır.

---

## Görev 5 · Consumer group ve rebalance (10 dk)

```bash
uv run python consumer.py --instance c1                              # T1
uv run python producer.py --rate 20 --duration 600                   # T2
uv run python consumer.py --instance c2                              # T3
uv run python consumer.py --instance c3                              # T4
make lag                                                             # T5
```

c1'in log'unda:

```
[c1] REBALANCE + eklendi: 0,1,2,3,4,5 │ şu an: 0,1,2,3,4,5
[c1] REBALANCE − geri alındı: 0,1,2,3 │ şu an: 4,5          ← c2 ve c3 katıldı
[c1] REBALANCE + eklendi: 3 │ şu an: 3,4,5                   ← c2 kapatıldı
```

T3'te **Ctrl+C** ile c2'yi kapatın: partition'ları birkaç saniyede diğerlerine geçer. `make lag` her
partition'ın sahibini gösterir; Kafka UI → *Consumers* → `bank-ledger` de öyle.

<details><summary>Sorular ve cevaplar</summary>

- **7. consumer eklesek?** Boşta kalır: bir group'ta paralellik tavanı = partition sayısı (6).
- **cooperative-sticky ne kazandırdı?** Rebalance'ta yalnızca taşınan partition'lar durdu; c1 4 ve 5'i işlemeye
  devam etti. Eski "eager" stratejide bütün partition'lar önce geri alınırdı.
- **Çöken consumer'ın partition'ları ne zaman taşınır?** `session.timeout.ms` (10 sn) dolunca. Ctrl+C ile kapanan
  consumer group'tan hemen ayrılır.
</details>

Hepsini kapatın.

---

## Görev 6 · Lag (10 dk)

Ayrı bir group (`lag-demo`) yalnızca bundan sonra gelen mesajları okusun (`--start latest`); 200 müşteri trafiği
partition'lara eşit dağıtsın.

```bash
uv run python producer.py --rate 40 --duration 600 --customers 200                       # T2
uv run python consumer.py --instance c1 --group lag-demo --start latest --delay-ms 50    # T1
make lag G=lag-demo                                                                     # T5
```

Mesaj başına 50 ms iş: tek consumer ~18 mesaj/sn işler, 40 gelir → lag ~**22/sn artar**:

```
10 sn   TOPLAM   215
20 sn   TOPLAM   440
30 sn   TOPLAM   659
```

İki consumer daha ekleyin (T3 ve T4, aynı komut, `--instance c2` / `c3`). Kapasite ~54/sn > 40 → lag erir:

```
+10 sn  TOPLAM   915
+30 sn  TOPLAM   624
+50 sn  TOPLAM   336
+70 sn  TOPLAM   135
```

Kafka UI → *Consumers* → `lag-demo`: partition başına lag.

<details><summary>Sorular ve cevaplar</summary>

- **200 değil 20 müşteriyle (varsayılan) denesek?** 3 consumer'la da lag **artmaya devam eder**: dağılım çarpık
  (Görev 2: p0 trafiğin ~%35'i), p0'ın tek sahibi yetişemez, başka bir consumer boşta kalır. Consumer sayısı
  kapasiteyi ancak yük partition'lara eşit dağılıyorsa artırır.
- **Partition sayısını artırmak çözüm mü?** Key → partition eşlemesi değişir: aynı müşterinin eski ve yeni işlemleri
  farklı partition'larda kalır, geçiş anında sıra garantisi bozulur. Önce işlemeyi hızlandırın (batch, paralel I/O).
</details>

Hepsini kapatın.

---

## Görev 7 · Çökme ve idempotency (15 dk)

```bash
make ledger-reset
uv run python producer.py --rate 50 --duration 4
uv run python consumer.py --instance c1 --group crash-1 --crash-after 25      # 25. mesajdan sonra çöker
uv run python consumer.py --instance c1 --group crash-1 --exit-when-idle 10   # yeniden başlat
make ledger-report
```

```
[c1] 💥 SİMÜLE ÇÖKME: p4@24 işlendi ama offset commit edilmedi.
...
[c1] p4@0 TXN-… C-1013 658.18 TL → Duplicate #1 (tekrar teslim, iş sonucu değişmedi)
[c1] p4@24 TXN-… C-1013 310.75 TL → Duplicate #25 (tekrar teslim, iş sonucu değişmedi)
SUMMARY instance=c1 processed=… applied=… duplicates=25 skipped=0

  Yakalanan tekrar teslim : 25
✓ Her işlem tam olarak bir kez uygulanmış.
```

Yeni group topic'i baştan okur (önceki görevlerin mesajları dahil, birkaç saniye sürer).

Çökmeden önce işlenen 25 işlem **tekrar geldi**: Kafka at-least-once teslim eder, kaybı önler ama tekrarı önlemez.
Tekrar gelen kayıtlar bakiyeyi değiştirmedi:

- `consumer.py` → `>>> LAB 4`: offset, iş sonucu kalıcı olduktan **sonra** store edilir. Olmasaydı offset mesaj
  alındığı an store edilir; işlem bitmeden çökersek mesaj **kaybolabilirdi**.
- `consumer.py` → `>>> LAB 5`: işlem kimliği `processed_transactions` tablosuna yazılır, ikinci gelişte yan etki
  atlanır. Olmasaydı: `✗ 25 işlem BİRDEN FAZLA KEZ uygulanmış`.

### Kafka UI: bozuk mesaj ve baştan okuma

1. `uv run python consumer.py --instance c1 --group crash-1` (açık kalsın)
2. Kafka UI → *Topics* → `bank.transactions` → **Produce Message**: Partition `0`, Key `C-1001`, Value `merhaba`.
   Consumer: `p0@… okunamadı, atlanıyor: JSON işlem değil: …`. Akış durmadı.
3. Consumer'ı **Ctrl+C** ile kapatın. Kafka UI → *Consumers* → `crash-1` → **Reset offset** → *Earliest*
   (group'ta çalışan consumer yokken yapılabilir).
4. `uv run python consumer.py --instance c1 --group crash-1 --exit-when-idle 10` → `applied=0 duplicates=<hepsi>`,
   `make ledger-report` → `✓`. Bütün işlemler yeniden geldi, bakiye değişmedi.

<details><summary>Sorular ve cevaplar</summary>

- **25 işlemin neden hepsi tekrar geldi, yalnızca sonuncusu değil?** Offset'ler 5 sn'de bir commit edilir
  (`auto.commit.interval.ms`). Consumer 25 mesajı ilk saniyede işleyip çöktü; henüz hiç commit olmamıştı. Commit
  aralığı, çökmede tekrar işlenecek işin üst sınırıdır.
- **Neden offset değil de transactionId?** Aynı iş olayı farklı offset'lerle gelebilir: producer retry'ı,
  upstream'in tekrar göndermesi, replay. İş kimliği her durumda aynıdır.
- **Neden işaret ile defter kaydı aynı veritabanı transaction'ında?** İşaret konup kayıt yazılmadan çökersek işlem
  kaybolur; kayıt yazılıp işaret konmadan çökersek iki kez uygulanır. Tek transaction: ya ikisi birden ya hiçbiri.
- **Bozuk mesajın offset'i neden store edildi?** Yoksa consumer her yeniden başlatmada aynı mesaja takılırdı
  ("poison message"). Gerçek sistemde bu mesajlar ayrı bir topic'e (DLQ) yazılır.
</details>

---

## Kapanış

```bash
make test       # her şey hâlâ yerinde mi
make down       # durdur (veri kalır); her şeyi silmek için: make clean
```

## Sık karşılaşılan sorunlar

| Belirti | Çözüm |
| --- | --- |
| `make up`: `Port 8080 dolu` | `cp .env.example .env`, `KAFKA_UI_PORT=8085` yapın, tekrar `make up` |
| `make test`: `Kafka'ya ulaşılamıyor … Önce: make up` | Cluster kapalı (bilgisayar yeniden başladı vb.): `make up` |
| `make test`: `Cluster'da 2 broker çalışıyor` | Görev 3'te durdurulan broker: `make brokers` → `make broker-start N=2` |
| `%3\|…\|FAIL\|… Connection refused` satırları | Kafka istemci kütüphanesinin log'u: durdurduğunuz broker'a bağlanamıyor. Normal. |
| `make lag`: `Offset'ler okunamadı: … high watermark is not caught up` | İki broker kapalıyken olur; broker'ları geri getirin |
| Consumer hiç mesaj almıyor | Group'un offset'i zaten sonda. Yeni `--group` adı verin veya producer çalıştırın |
| Yeniden başlatılan consumer ~10 sn bekliyor | Çöken üyenin session'ı doluyor (`session.timeout.ms`). Normal |
| Her şey karıştı | `make reset` (bütün veri silinir, ~30 sn) |
