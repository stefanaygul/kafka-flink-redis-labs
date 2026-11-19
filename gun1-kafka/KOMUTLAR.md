# Gün 1 · Komutlar

Komutlar görev sırasıyla, her birinin altında ne olduğu ve ne göreceğiniz. Ayrıntılı açıklama ve sorular:
[README.md](README.md).

| İşaret | Anlamı |
| --- | --- |
| **T1, T2, T3…** | Ayrı terminal pencereleri. Her yeni terminalde önce: `cd kafka-flink-redis-labs/gun1-kafka` (repoyu klonladığınız yerde) |
| **biter** | Komut işini yapıp kendiliğinden kapanır. Bitmesini bekleyin, sonra sıradaki adım. |
| **açık kalır** | Sürekli çalışır. O terminali bırakın, sıradaki adımı başka terminalde yapın. **Ctrl+C** ile durur. |
| → | Ne göreceksiniz |

---

## Eğitimden önce (prova, ~3 dk)

**1. T1 · biter (~30 sn)**
```bash
make reset
```
→ Eski veriyi siler, 3 broker + Kafka UI'ı başlatır, hazır olana kadar bekler. Son satır: `✓ Ortam hazır.`

**2. T1 · biter (~25 sn)**
```bash
make test
```
→ `10 passed`

**3. T1 · biter (~1 dk)**
```bash
make test-failover
```
→ `1 passed`. Broker 2 ve 3'ü gerçekten durdurup geri getirir; bu sırada başka komut çalıştırmayın.

**4. T1 · biter (~30 sn)**
```bash
make reset
```
→ Provanın mesajlarını siler; sınıfa temiz başlarsınız.

## Sınıf başı

**T1 · biter (~45 sn)**
```bash
make up && make test
```
→ `✓ Ortam hazır.` ve `10 passed`. Kafka UI: <http://localhost:8080>.

---

## Görev 1 · Cluster'ı tanı

**1. T1 · biter (anında)**
```bash
make describe
```
→ Üç broker çalışıyor, aktif controller (ör. `node 1`), `bank.transactions` için 6 partition satırı
(Leader / Replicas / Isr) ve `✓ bütün partition'larda ISR = 3`.

**2. Tarayıcı:** Kafka UI → *Brokers* (3 broker, URP 0) → *Topics* → `bank.transactions` → *Settings*
(`min.insync.replicas = 2`).

## Görev 2 · Producer

**1. T1 · biter (20 sn)**
```bash
uv run python producer.py --rate 50 --duration 20
```
→ Saniyede 50 işlem × 20 sn = 1000 işlem. 2 sn'de bir durum satırı, sonunda
`SUMMARY delivered=1000 failed=0 undelivered=0` ve `Partition dağılımı: p0=347 p1=192 …` (eşit değil: 20 müşteri, hash).

**2. Tarayıcı:** *Topics* → `bank.transactions` → *Messages*: Key sütunu `C-10xx`. Arama kutusuna `C-1007`
yazın → hepsi tek partition'da.

**3. T1 · biter (10 sn)**
```bash
uv run python producer.py --rate 50 --duration 10 --hot-customer C-1001
```
→ Trafiğin %70'i C-1001'den: onun partition'ı şişer (`p0=411 p1=31 p2=34 …`). Bu partition'ı consumer eklemek
hızlandırmaz.

## Görev 3 · Broker kaybı (sıra önemli)

**1. T1 · açık kalır (15 dk; Ctrl+C ile durur)**
```bash
uv run python producer.py --rate 100 --duration 900 --queue-max 1000
```
→ 2 sn'de bir: `gönderilen=… teslim=… hata=0 kuyrukta=2`. **Bu terminali görev boyunca izleyin.**

**2. T2 · biter (birkaç sn)**
```bash
make broker-stop N=2
```
→ `✓ kafka-2 durdu.` T1'de hiçbir şey bozulmaz: `teslim` artmaya devam eder, `hata=0`.

**3. T2 · biter**
```bash
make describe
```
→ `Isr: 1,3` (2 kopya); kafka-2'nin leader'lığı diğerlerine geçti. `! 6/6 partition eksik kopyalı`.

**4. T2 · biter**
```bash
make broker-stop N=3
```
→ `✓ kafka-3 durdu.` Artık yalnızca kafka-1 ayakta.

**5. T2 · biter**
```bash
make describe
```
→ Her partition'da `Isr: 1` ve `✗ 6 partition min.insync.replicas=2 altında → acks=all yazım REDDEDİLİR`.
Birkaç saniye sonra tekrar çalıştırırsanız: `! aktif controller YOK` (3 oy verenden 1'i ayakta).

**6. T2 · biter (2 sn)**
```bash
uv run python producer.py --rate 5 --duration 2 --no-retry
```
→ `Teslim hatası #1: key=C-10xx → Broker: Not enough in-sync replicas` ve `SUMMARY delivered=0 failed=10`.
Kafka yazmayı **reddetti**.

**7. T1'e bakın (komut yok)**
→ `teslim` durdu, `kuyrukta` saniyede ~100 artıyor. ~10 sn sonra
`Yerel kuyruk dolu (1000 mesaj) → yeni mesaj bekletiliyor (backpressure)`. Producer çökmedi, bekliyor.

**8. T2 · biter (~10–20 sn)**
```bash
make broker-start N=3
```
→ `✓ kafka-3 çalışıyor.` T1'de kuyruk birkaç saniyede boşalır, `teslim` hızla artar, `hata=0`: hiçbir mesaj
kaybolmadı.

**9. T2 · biter (~10–20 sn)**
```bash
make broker-start N=2
```

**10. T2 · biter**
```bash
make describe
```
→ `Isr: 1,2,3` geri geldi, ama leader'ların hepsi kafka-1'de (yük dengesiz).

**11. T2 · biter**
```bash
make leaders
```
→ `✓ … partition'ın leader'ı preferred broker'a geri verildi`; tablo yine dağınık leader'lar gösterir.

**12. T1 · Ctrl+C**
→ `SUMMARY delivered=… failed=0 undelivered=0`

**Ek deney: acks=1 (isteğe bağlı), T2'de sırayla:**
```bash
make broker-stop N=2 && make broker-stop N=3
uv run python producer.py --rate 5 --duration 3 --acks 1
make broker-start N=2 && make broker-start N=3
```
→ İkinci komut iki broker kapalıyken bile `delivered=15 failed=0` der: `acks=1` yalnızca leader'ı bekler. Mesaj
tek diskte.

## Görev 4 · Partition'ın içi (isteğe bağlı)

**1. T1 · biter**
```bash
make segments
```
→ Partition 0'ın dizini üç broker'da da var. `.log` dosyalarının boyutu aynı. Altında `kafka-dump-log` çıktısı:
`producerId`, `baseSequence`, `key: C-10xx`, `payload: {…}`. En altta index: `offset: 17 position: 4268`.

**2. T1 · biter (~5 sn)**
```bash
make segment-roll
```
→ 3,5 MB yazar. Dosyalar: `…000.log`, `…3380.log`, `…6760.log`, `…10140.log`. Dosya adı = segment'teki ilk
offset; sonuncusu aktif segment.

## Görev 5 · Consumer group ve rebalance

**1. T1 · açık kalır**
```bash
uv run python consumer.py --instance c1
```
→ `REBALANCE + eklendi: 0,1,2,3,4,5`: tek consumer bütün partition'ları alır. Önceki görevlerin binlerce mesajını
birkaç saniyede işler.

**2. T2 · açık kalır**
```bash
uv run python producer.py --rate 20 --duration 600
```
→ Sürekli trafik (10 dk).

**3. T3 · açık kalır**
```bash
uv run python consumer.py --instance c2
```
→ T1'de `REBALANCE − geri alındı: 0,1,2 │ şu an: 3,4,5`; T3'te `+ eklendi: 0,1,2`.

**4. T4 · açık kalır**
```bash
uv run python consumer.py --instance c3
```
→ Her consumer'da 2 partition.

**5. T5 · açık kalır**
```bash
make lag
```
→ 2 sn'de bir tablo: her partition'ın son offset'i, commit'i, lag'i ve sahibi (c1/c2/c3).

**6. T3 · Ctrl+C** (c2'yi kapatır)
→ Birkaç saniye içinde T1 ve T4'te `+ eklendi`: c2'nin partition'ları diğerlerine geçti. T5'te sahip sütunu değişir.

**7. Hepsini kapatın:** T1, T2, T4, T5'te Ctrl+C.

## Görev 6 · Lag

**1. T2 · açık kalır**
```bash
uv run python producer.py --rate 40 --duration 600 --customers 200
```
→ Saniyede 40 işlem, 200 müşteri (partition'lara eşit dağılır).

**2. T1 · açık kalır**
```bash
uv run python consumer.py --instance c1 --group lag-demo --start latest --delay-ms 50
```
→ Yeni bir group, yalnızca bundan sonra gelenleri okur. Mesaj başına 50 ms: ~18 mesaj/sn işler, yetişemez.

**3. T5 · açık kalır**
```bash
make lag G=lag-demo
```
→ `TOPLAM` her 10 sn'de ~220 artar (215 → 440 → 659).

**4. ~30 sn sonra T3 ve T4 · açık kalır**
```bash
uv run python consumer.py --instance c2 --group lag-demo --start latest --delay-ms 50    # T3
uv run python consumer.py --instance c3 --group lag-demo --start latest --delay-ms 50    # T4
```
→ T5'te `TOPLAM` düşmeye başlar (~140 / 10 sn): 915 → 624 → 336 → 135.

**5. Hepsini kapatın:** T1–T5'te Ctrl+C.

## Görev 7 · Çökme ve idempotency

**1. T1 · biter**
```bash
make ledger-reset
```
→ Defter (data/ledger.db) silinir.

**2. T1 · biter (4 sn)**
```bash
uv run python producer.py --rate 50 --duration 4
```
→ 200 yeni işlem.

**3. T1 · kendiliğinden çöker (1–2 sn)**
```bash
uv run python consumer.py --instance c1 --group crash-1 --crash-after 25
```
→ `💥 SİMÜLE ÇÖKME: p…@24 işlendi ama offset commit edilmedi.` (Bilerek: 25 işlem deftere yazıldı, Kafka'ya
"işledim" denmedi.)

**4. T1 · biter (~20–30 sn)**
```bash
uv run python consumer.py --instance c1 --group crash-1 --exit-when-idle 10
```
→ Önce ~10 sn bekler (çöken üyenin süresi dolsun), sonra işler: `→ Duplicate #1 … #25 (tekrar teslim, iş sonucu
değişmedi)`. 10 sn yeni mesaj gelmeyince kapanır: `SUMMARY … duplicates=25`.

**5. T1 · biter**
```bash
make ledger-report
```
→ `Yakalanan tekrar teslim : 25` ve `✓ Her işlem tam olarak bir kez uygulanmış.`

**Kafka UI ile bozuk mesaj ve baştan okuma:**

**6. T1 · açık kalır**
```bash
uv run python consumer.py --instance c1 --group crash-1
```

**7. Tarayıcı:** *Topics* → `bank.transactions` → **Produce Message**: Partition `0`, Key `C-1001`, Value `merhaba`
→ *Produce*. T1'de `p0@… okunamadı, atlanıyor: JSON işlem değil`. Consumer durmadı.

**8. T1 · Ctrl+C**

**9. Tarayıcı:** *Consumers* → `crash-1` → **Reset offset** → *Earliest* (group'ta çalışan consumer yokken yapılabilir).

**10. T1 · biter (~30 sn)**
```bash
uv run python consumer.py --instance c1 --group crash-1 --exit-when-idle 10
```
→ Bütün işlemler baştan geldi: `SUMMARY … applied=0 duplicates=<hepsi>`. Bakiye değişmedi.

**11. T1 · biter**
```bash
make ledger-report
```
→ `✓ Her işlem tam olarak bir kez uygulanmış.`

---

## Kapanış ve kurtarma

```bash
make test           # biter: her şey yerinde mi (10 passed)
make down           # biter: durdurur, veri kalır. Gün 2'ye geçmeden ŞART (aynı portlar)
```

| Durum | Komut |
| --- | --- |
| Bir broker kapalı mı kaldı? | `make brokers` → `make broker-start N=2` |
| Consumer/producer açık kaldı mı? | O terminalde Ctrl+C |
| Her şey karıştı | `make reset` (bütün veri silinir, ~30 sn) |
