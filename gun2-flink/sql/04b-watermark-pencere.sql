-- Görev 4b · Aynı senaryo, 5 dakikalık kayan pencere: pencere TAM OLARAK ne zaman yazılıyor?
-- Sorgu açıkken başka terminalde:  uv run python producer.py --scenario watermark   (veya late-events)
SELECT customerId, window_start, window_end,
       COUNT(*)    AS islem,
       SUM(amount) AS toplam_tl
FROM TABLE(HOP(TABLE scenario_transactions, DESCRIPTOR(event_time), INTERVAL '1' MINUTE, INTERVAL '5' MINUTES))
GROUP BY customerId, window_start, window_end;
