-- Gün 2 · Tablolar. "make sql" bu dosyayı SQL client açılırken çalıştırır.
-- Bir Flink tablosu veriyi tutmaz: Kafka topic'inin üzerine konmuş bir "şema + okuma ayarı"dır.

-- Sonuçlar terminale satır satır aksın:
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'table.local-time-zone' = 'UTC';
-- Bir partition'a bir süre veri gelmezse "boşta" say; yoksa watermark o partition yüzünden ilerlemez.
SET 'table.exec.source.idle-timeout' = '10 s';

-- Canlı işlemler (producer.py). Sorgu açıldıktan SONRA gelen mesajları okur.
CREATE TABLE transactions (
  transactionId     STRING,
  customerId        STRING,
  accountId         STRING,
  amount            DECIMAL(12, 2),
  currency          STRING,
  merchantCategory  STRING,
  channel           STRING,
  eventTime         BIGINT,                                   -- UTC epoch milisaniye
  -- >>> LAB 1 · event time ve watermark
  --     event_time: işlemin GERÇEKLEŞTİĞİ an (Kafka'ya ulaştığı an değil). Pencereler buna göre açılır.
  --     Watermark: "bundan daha eski olay artık gelmeyecek" tahmini. En büyük event time − 10 sn.
  --     Pencere, watermark pencerenin sonunu geçince kapanır ve sonucu yazılır.
  event_time AS TO_TIMESTAMP_LTZ(eventTime, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
  -- <<< LAB 1
) WITH (
  'connector' = 'kafka',
  'topic' = 'bank.transactions',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-lab',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- Deterministik senaryolar (producer.py --scenario watermark | late-events). Sabit test günü 2026-11-14.
-- Watermark burada 1 dk geride: geç gelen olayları ağır çekimde görmek için.
CREATE TABLE scenario_transactions (
  transactionId     STRING,
  customerId        STRING,
  accountId         STRING,
  amount            DECIMAL(12, 2),
  currency          STRING,
  merchantCategory  STRING,
  channel           STRING,
  eventTime         BIGINT,
  event_time AS TO_TIMESTAMP_LTZ(eventTime, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'lab.scenario-transactions',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-lab',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

-- Müşteri risk kategorileri: her müşterinin GEÇMİŞİYLE birlikte tutulan (versiyonlu) tablo.
-- Kafka'daki compacted topic, key = customerId. updatedAt = bu kategorinin geçerli olduğu an.
CREATE TABLE customer_profiles (
  customerId    STRING,
  riskCategory  STRING,
  segment       STRING,
  updatedAt     BIGINT,
  update_time AS TO_TIMESTAMP_LTZ(updatedAt, 3),
  PRIMARY KEY (customerId) NOT ENFORCED,
  WATERMARK FOR update_time AS update_time
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'bank.customer-profiles',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'raw',
  'value.format' = 'json'
);

-- Flink'in ürettiği müşteri feature'ları (Görev 6). Her müşteri için son 5 dakikanın özeti.
CREATE TABLE customer_features (
  customerId          STRING,
  windowStart         BIGINT,          -- epoch ms
  windowEnd           BIGINT,          -- epoch ms
  txnCount            BIGINT,
  totalAmount         DECIMAL(14, 2),
  maxAmount           DECIMAL(12, 2),
  distinctCategories  BIGINT,
  PRIMARY KEY (customerId) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'bank.customer-features',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'raw',
  'value.format' = 'json'
);
