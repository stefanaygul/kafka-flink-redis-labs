-- Görev 4a · Senaryo olayları ve watermark, satır satır.
-- Sorgu açıkken başka terminalde:  uv run python producer.py --scenario watermark   (veya late-events)
SELECT transactionId, customerId, amount, event_time,
       CURRENT_WATERMARK(event_time) AS `watermark`,
       CASE WHEN event_time <= CURRENT_WATERMARK(event_time) THEN 'GEÇ KALDI' ELSE '' END AS durum
FROM scenario_transactions;
