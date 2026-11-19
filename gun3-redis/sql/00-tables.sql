-- Gün 3 · Feature job'ının tabloları (Gün 2'deki 00-tables.sql'in ilgili kısmı).

SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.source.idle-timeout' = '10 s';

CREATE TABLE transactions (
  transactionId     STRING,
  customerId        STRING,
  accountId         STRING,
  amount            DECIMAL(12, 2),
  currency          STRING,
  merchantCategory  STRING,
  channel           STRING,
  eventTime         BIGINT,
  event_time AS TO_TIMESTAMP_LTZ(eventTime, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'bank.transactions',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-features',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE customer_features (
  customerId          STRING,
  windowStart         BIGINT,
  windowEnd           BIGINT,
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
