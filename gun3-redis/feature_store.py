"""Redis online feature store: müşteri başına en güncel feature, milisaniyede okunur.

Anahtar: features:<customerId>  (HASH)  alanlar: windowStart, windowEnd, txnCount, totalAmount, maxAmount,
distinctCategories, publishedWindowEnd (bu pencerenin skoru yayımlandı mı?). Süre: 1 saat (TTL).
"""
from __future__ import annotations

import redis

from scoring import Features

TTL_SECONDS = 3600

# >>> LAB 1 · versiyonlu yazım (Lua: Redis'te tek adımda, atomik çalışır)
#     windowEnd sürümdür. Gelen feature, Redis'tekinden YENİ değilse yazılmaz (0 döner).
#     Olmasaydı: tekrar gelen (at-least-once) veya sırası karışmış eski bir pencere, yenisinin üstüne yazılır;
#     skor eski veriyle hesaplanır. "Önce oku, sonra yaz" iki ayrı komutla yapılsaydı araya başka yazım girebilirdi.
UPSERT_LUA = """
local current = tonumber(redis.call('HGET', KEYS[1], 'windowEnd') or '-1')
if tonumber(ARGV[1]) <= current then
  return 0
end
redis.call('HSET', KEYS[1], unpack(ARGV, 3))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
"""
# <<< LAB 1


class FeatureStore:
    def __init__(self, url: str, prefix: str = "features:"):
        self.r = redis.Redis.from_url(url, decode_responses=True, socket_timeout=3, socket_connect_timeout=3)
        self.prefix = prefix
        self._upsert = self.r.register_script(UPSERT_LUA)

    def key(self, customer_id: str) -> str:
        return f"{self.prefix}{customer_id}"

    def upsert(self, f: Features, published: bool = False) -> bool:
        """Feature'ı yalnızca daha yeniyse yazar. True = yazıldı, False = eski veya tekrar."""
        fields = f.to_redis()
        if published:  # yeniden kurulumda (rebuild) bu pencerenin skoru zaten yayımlanmıştı
            fields["publishedWindowEnd"] = str(f.window_end)
        args = [f.window_end, TTL_SECONDS] + [x for kv in fields.items() for x in kv]
        return self._upsert(keys=[self.key(f.customer_id)], args=args) == 1

    def get(self, customer_id: str) -> Features | None:
        h = self.r.hgetall(self.key(customer_id))
        return Features.from_redis(customer_id, h) if h else None

    def needs_score(self, f: Features) -> bool:
        """Redis'teki en güncel pencere bu mu ve skoru henüz yayımlanmadı mı?"""
        stored, published = self.r.hmget(self.key(f.customer_id), "windowEnd", "publishedWindowEnd")
        return stored is not None and int(stored) == f.window_end and int(published or -1) < f.window_end

    def mark_published(self, f: Features) -> None:
        self.r.hset(self.key(f.customer_id), "publishedWindowEnd", str(f.window_end))

    def count(self) -> int:
        return sum(1 for _ in self.r.scan_iter(f"{self.prefix}*", count=500))
