-- Görev 3 · Dakikalık özet. TUMBLE: üst üste binmeyen 1 dakikalık pencereler.
-- Bir satır, pencere bittikten ~10 sn sonra (watermark pencerenin sonunu geçince) gelir.
SELECT window_start, window_end,
       COUNT(*)                   AS islem,
       SUM(amount)                AS toplam_tl,
       COUNT(DISTINCT customerId) AS musteri
FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;
