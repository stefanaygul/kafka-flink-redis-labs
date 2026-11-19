# Gün 2 · Komutlar

Komutlar görev sırasıyla, her birinin altında ne olduğu ve ne göreceğiniz. Ayrıntılı açıklama ve sorular:
[README.md](README.md).

| İşaret | Anlamı |
| --- | --- |
| **T1, T2, T3** | Ayrı terminal pencereleri. Her yeni terminalde önce: `cd kafka-flink-redis-labs/gun2-flink` (repoyu klonladığınız yerde) |
| **biter** | Komut işini yapıp kendiliğinden kapanır. Bitmesini bekleyin, sonra sıradaki adım. |
| **açık kalır** | Sürekli çalışır. O terminali bırakın, sıradaki adımı başka terminalde yapın. **Ctrl+C** ile durur. |
| → | Ne göreceksiniz |

Bugünün düzeni: **T1 = trafik** (producer), **T2 = sorgu** (sonuçlar burada akar), **T3 = diğer komutlar**.

`make query` ile açılan her sorgu Flink'te bir job olur. Sonuçlar T2'ye akar; ilk satırların gelmesi 10–20 sn
sürebilir (SQL client açılıyor). **Ctrl+C** sorguyu ve Flink'teki job'ını birlikte kapatır.

---

## Eğitimden önce (prova, ~5 dk)

**0. Gün 1 açıksa · biter**
```bash
cd ../gun1-kafka && make down && cd ../gun2-flink
```
→ Gün 1 ile aynı portlar: ikisi aynı anda açık olamaz.

**1. T1 · biter (~40 sn; ilk seferde Flink imajı derlenir, ~1 dk)**
```bash
make reset
```
→ Son satır: `✓ Ortam hazır.`

**2. T1 · biter**
```bash
make test               # ~30 sn  → 6 passed
make test-pipeline      # ~1,5 dk → 1 passed (feature job uçtan uca)
make test-failover      # ~1 dk   → 1 passed (TaskManager çökmesi, checkpoint'ten devam)
make reset              # provanın verisini sil
```

## Sınıf başı

**T1 · biter (~1 dk)**
```bash
make up && make test
```
→ `✓ Ortam hazır.` ve `6 passed`. Flink UI: <http://localhost:8081>. Kafka UI: <http://localhost:8080>.

---

## Görev 1 · Flink'i tanı

**Tarayıcı:** Flink UI → *Overview*: 1 TaskManager, 8 slot, çalışan job yok. Editörde `sql/00-tables.sql`:
`>>> LAB 1` (event time ve watermark).

## Görev 2 · Canlı akış ve watermark

**1. T1 · açık kalır (bütün gün açık kalabilir)**
```bash
make produce
```
→ Saniyede 10 işlem, 300 müşteri. 5 sn'de bir `gönderilen=…`.

**2. T2 · açık kalır**
```bash
make query Q=01-izle
```
→ Her işlem bir satır: müşteri, tutar, `event_time`, `watermark`. İlk satırlarda watermark `<NULL>`, sonra hep
event time'ın ~10 sn gerisinde. Satırlar zamana göre sıralı değil (farklı partition'lar).

**3. T2 · Ctrl+C**

## Görev 3 · Pencereler

**1. T2 · açık kalır**
```bash
make query Q=02-tumble
```
→ Dakikada bir satır: `window_start | window_end | islem | toplam_tl | musteri`. İlk satır bir sonraki dakika
bitince ~10 sn sonra gelir (en fazla ~70 sn bekleyin). Örnek: `17:36:00 | 17:37:00 | 1200 | 1039803.39 | 293`.

**2. T2 · Ctrl+C, sonra açık kalır**
```bash
make query Q=03-hop
```
→ C-1001, C-1007, C-1042 için her dakika "son 5 dakika" satırı. C-1007 ve C-1042 (riskli) çok daha sık ve büyük.

**3. T2 · Ctrl+C**

## Görev 4 · Watermark ve geç gelen olaylar

Bu görev ayrı bir topic kullanır (sabit saatli senaryo); T1'deki trafik açık kalabilir, etkilemez.

**1. T2 · açık kalır**
```bash
make query Q=04a-watermark-olaylar
```
→ Henüz satır yok: senaryo gönderilmedi. Açık bırakın.

**2. T3 · Enter ile ilerler, sonunda biter**
```bash
make scenario S=watermark
```
→ T3 her olayı gönderir ve durur (`⏸ … Devam etmek için Enter`). Her durakta T2'ye bakın: olay satırı ve watermark
(= en büyük event time − 1 dk). Sonra Enter.

**3. T2 · Ctrl+C, sonra açık kalır**
```bash
make query Q=04b-watermark-pencere
```

**4. T3 · Enter ile ilerler**
```bash
make scenario S=watermark
```
→ T2'de 5 dakikalık pencereler watermark pencerenin sonunu geçtikçe tek tek gelir:
`[10:56–11:01) 1 işlem 10 TL`, `[10:57–11:02) 2 / 30`, `[10:58–11:03) 3 / 60`, `[10:59–11:04) 4 / 100`,
`[11:00–11:05) 5 / 150`. Satırlar birkaç saniye gecikebilir.

**5. T3 · Enter ile ilerler** (T2'de 04b açık kalsın; isterseniz 04a ile de tekrarlayın)
```bash
make scenario S=late-events
```
→ T3 her adımda ne olması gerektiğini yazar. O4 (sıra dışı ama geç değil) sayılır; L2 (2 dk geç) açık pencerelere
girer; L10 (10 dk geç) hiçbir pencerede görünmez. 04a ile bakarsanız L2 ve L10 `GEÇ KALDI` yazar.

**6. T2 · Ctrl+C**

## Görev 5 · Temporal join

**1. T1** · `make produce` açık olmalı (değilse başlatın, açık kalır).

**2. T2 · açık kalır**
```bash
make query Q=05-temporal-join
```
→ C-1001 ve C-1007'nin işlemleri ve o andaki risk kategorisi: C-1001 `LOW`, C-1007 `HIGH`. C-1001'in satırları
seyrek gelir (300 müşteriden biri); 30–60 sn bekleyin.

**3. T3 · biter**
```bash
make profile C=C-1001 R=HIGH
```
→ `C-1001 → HIGH, geçerlilik başlangıcı 17:36:16 UTC`. T2'de C-1001'in **bundan sonraki** işlemleri `HIGH`,
öncekiler `LOW` kalır.

**4. T2 · Ctrl+C**

## Görev 6 · Feature job

**1. T3 · biter (~20 sn)**
```bash
make submit
```
→ `✓ customer-features gönderildi ve çalışıyor (…)`. Job arka planda, Flink'te çalışır; terminali kapatsanız da
durmaz. Flink UI → *Running Jobs* → `customer-features`.

**2. T3 · biter**
```bash
make jobs
```
→ `RUNNING … customer-features`

**3. ~1 dk sonra T3 · biter**
```bash
make features
```
→ Müşteri başına son 5 dakika, en büyük tutarlılar üstte. Riskli müşteriler (C-1042, C-1007, C-1113) yüz binlerce TL ile en üstte.

**4. Tarayıcı:** Kafka UI → *Topics* → `bank.customer-features`: key = müşteri, `cleanup.policy = compact`.

## Görev 7 · Checkpoint ve çökme

**T3 · biter (~30 sn)** (T1 trafik açık, feature job çalışıyor olmalı)
```bash
make tm-restart
```
→ `✓ son tamamlanan checkpoint: #21`, TaskManager yeniden başlar, `job durumu: RESTARTING … RUNNING`, sonunda
`✓ job yeniden RUNNING, checkpoint #21'dan geri yüklendi (state kaybolmadı)`. Flink UI → job → *Checkpoints* →
*Latest Restore*.

---

## Kapanış ve kurtarma

```bash
make test           # biter: 6 passed
make down           # biter: durdurur (feature job da durur). Gün 3'e geçmeden ŞART (aynı portlar)
```

T1'deki producer'ı Ctrl+C ile kapatmayı unutmayın.

| Durum | Komut |
| --- | --- |
| Açık kalan sorgu job'ları slotları doldurdu | `make jobs` → `make cancel J=select` |
| Feature job'ını durdur | `make cancel J=customer-features` |
| Sorguda hiç satır yok | T1'de `make produce` açık mı? Pencere sorgularında ilk satır ~1 dk sürer |
| Her şey karıştı | `make reset` |
