-- Görev 2 · Canlı akışı izle: her işlem ve o anki watermark.
-- Önce başka bir terminalde:  uv run python producer.py
SELECT customerId, amount, merchantCategory, event_time,
       CURRENT_WATERMARK(event_time) AS `watermark`
FROM transactions;
