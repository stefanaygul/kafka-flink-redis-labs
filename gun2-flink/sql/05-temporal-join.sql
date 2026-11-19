-- Görev 5 · Her işlemi, işlemin GERÇEKLEŞTİĞİ ANDAKİ müşteri risk kategorisiyle eşleştir.
-- >>> LAB 2 · temporal join
--     FOR SYSTEM_TIME AS OF t.event_time: profil tablosunun o andaki sürümü kullanılır.
--     Olmasaydı (normal JOIN): kategori sonradan değişince geçmiş işlemlerin sonucu da "değişirdi".
-- <<< LAB 2
-- Sorgu açıkken:  make profile C=C-1001 R=HIGH   → C-1001'in bundan sonraki işlemleri HIGH görünür.
SELECT t.customerId, t.amount, t.event_time, p.riskCategory, p.update_time AS kategori_gecerlilik
FROM transactions AS t
LEFT JOIN customer_profiles FOR SYSTEM_TIME AS OF t.event_time AS p
  ON t.customerId = p.customerId
WHERE t.customerId IN ('C-1001', 'C-1007');
