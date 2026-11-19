-- Görev 3 · Müşteri başına son 5 dakika, her dakika güncellenir. HOP: kayan (sliding) pencere.
-- Riskli müşteriler (C-1007, C-1042, C-1113) daha sık ve daha büyük tutarlarla işlem yapar.
SELECT customerId, window_start, window_end,
       COUNT(*)    AS islem,
       SUM(amount) AS toplam_tl,
       MAX(amount) AS en_buyuk_tl
FROM TABLE(HOP(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '5' MINUTES))
WHERE customerId IN ('C-1001', 'C-1007', 'C-1042')
GROUP BY customerId, window_start, window_end;
