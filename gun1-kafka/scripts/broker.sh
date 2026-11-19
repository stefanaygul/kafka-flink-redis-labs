#!/usr/bin/env bash
# Broker durdur / başlat (Görev 3).
#   make broker-stop N=2     kontrollü kapanış: leader'lık devredilir, broker ISR'den çıkarılır
#   make broker-start N=2    geri getir, sağlıklı olana kadar bekle
#   make brokers             durum
source "$(dirname "$0")/lib.sh"
cmd=${1:-status}; n=${2:-}

case $cmd in
  stop|start)
    [[ $n =~ ^[123]$ ]] || { err "Broker numarası 1, 2 veya 3 olmalı: make broker-$cmd N=2"; exit 2; }
    if [[ $cmd == stop ]]; then
      docker compose stop -t 30 "kafka-$n" > /dev/null 2>&1
      ok "kafka-$n durdu.  Şimdi: make describe"
    else
      docker compose start "kafka-$n" > /dev/null 2>&1
      for _ in $(seq 1 45); do [[ $(health "kafka-$n") == healthy ]] && break; sleep 2; done
      if [[ $(health "kafka-$n") == healthy ]]; then
        ok "kafka-$n çalışıyor. Eksik kayıtları kopyalayıp ISR'ye katılması birkaç saniye sürer.  Şimdi: make describe"
      else
        err "kafka-$n 90 sn içinde sağlıklı olmadı → docker compose logs kafka-$n"; exit 1
      fi
    fi
    ;;
  status)
    running=$(running_brokers)
    for i in 1 2 3; do
      if grep -qx "kafka-$i" <<<"$running"; then ok "kafka-$i çalışıyor"; else warn "kafka-$i DURDURULDU → make broker-start N=$i"; fi
    done
    ;;
  *) err "Kullanım: broker.sh stop|start|status [1|2|3]"; exit 2 ;;
esac
