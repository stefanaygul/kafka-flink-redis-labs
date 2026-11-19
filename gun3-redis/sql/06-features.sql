-- Görev 6 · Feature job: her müşteri için son 5 dakikanın özeti, her dakika bank.customer-features'a yazılır.
-- "make submit" bu dosyayı gönderir. SELECT'ten farkı: INSERT INTO arka planda sürekli çalışan bir JOB olur;
-- SQL client'ı kapatsanız da çalışmaya devam eder (Flink UI → Running Jobs).
SET 'pipeline.name' = 'customer-features';

INSERT INTO customer_features
SELECT
  customerId,
  UNIX_TIMESTAMP(DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS windowStart,
  UNIX_TIMESTAMP(DATE_FORMAT(window_end,   'yyyy-MM-dd HH:mm:ss')) * 1000 AS windowEnd,
  COUNT(*)                                      AS txnCount,
  CAST(SUM(amount) AS DECIMAL(14, 2))           AS totalAmount,
  MAX(amount)                                   AS maxAmount,
  COUNT(DISTINCT merchantCategory)              AS distinctCategories
FROM TABLE(HOP(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '5' MINUTES))
GROUP BY customerId, window_start, window_end;
