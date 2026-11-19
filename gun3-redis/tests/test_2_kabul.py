"""Kabul testleri: gerçek Kafka + Redis. Geçici topic'ler ve Redis önekleri kullanılır."""
from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.request

from common import LAB_DIR, flink_url
from conftest import feature_json, read_topic, run_worker, send, tail
from scoring import Features

T0, T1, T2 = 1794651240000, 1794651300000, 1794651360000  # ardışık üç pencere sonu (1 dk arayla)


def test_ortam_kafka_redis_ve_feature_job(admin, store):
    topics = admin.list_topics(timeout=10).topics
    for t in ("bank.transactions", "bank.customer-features", "bank.risk-scores"):
        assert t in topics, f"{t} yok → make up"
    assert store.r.ping()
    deadline = time.monotonic() + 60  # yeni gönderildiyse RUNNING olması birkaç saniye sürebilir
    while True:
        with urllib.request.urlopen(flink_url() + "/jobs/overview", timeout=10) as resp:
            jobs = json.load(resp)["jobs"]
        if any(j["name"] == "customer-features" and j["state"] == "RUNNING" for j in jobs):
            break
        assert time.monotonic() < deadline, "Flink feature job'ı çalışmıyor → make submit"
        time.sleep(2)


def test_lab1_versiyonlu_yazim_eski_ve_tekrar_yeniyi_ezmez(store):
    new = Features.from_json(feature_json("C-1001", T1, 500))
    assert store.upsert(new) is True
    assert store.upsert(new) is False                                             # tekrar
    assert store.upsert(Features.from_json(feature_json("C-1001", T0, 999))) is False  # eski pencere
    got = store.get("C-1001")
    assert got.window_end == T1 and float(got.total_amount) == 500
    assert store.upsert(Features.from_json(feature_json("C-1001", T2, 700))) is True  # yeni pencere
    assert store.get("C-1001").window_end == T2
    assert 0 < store.r.ttl(store.key("C-1001")) <= 3600


def test_lab2_worker_tekrar_ve_eski_featurelarda_tek_skor(topics, store, run_id):
    t_features, t_scores = topics
    send(t_features, [
        ("C-9001", feature_json("C-9001", T1, 50_000, count=40, max_amount=15_000, categories=6)),  # HIGH
        ("C-9001", feature_json("C-9001", T1, 50_000, count=40, max_amount=15_000, categories=6)),  # tekrar
        ("C-9001", feature_json("C-9001", T0, 1_000)),                                              # eski
        ("C-9001", feature_json("C-9001", T2, 800)),                                                # yeni, LOW
        ("C-9002", feature_json("C-9002", T1, 300)),
    ])
    run = run_worker(t_features, t_scores, store.prefix, f"test-{run_id}", "--exit-when-idle", "5")
    assert run.returncode == 0, tail(run.stdout + run.stderr)
    assert "işlenen=5 yazılan=3 eski/tekrar=2 skor=3" in run.stdout, tail(run.stdout)
    assert store.get("C-9001").window_end == T2

    scores = read_topic(t_scores)
    ids = [s["scoreId"] for s in scores]
    assert sorted(ids) == sorted([f"C-9001:{T1}", f"C-9001:{T2}", f"C-9002:{T1}"]), ids
    by_id = {s["scoreId"]: s for s in scores}
    assert by_id[f"C-9001:{T1}"]["level"] == "HIGH" and by_id[f"C-9001:{T2}"]["level"] == "LOW"


def test_lab2_redise_yazildiktan_sonra_cokme_skor_bir_kez_yayimlanir(topics, store, run_id):
    t_features, t_scores = topics
    send(t_features, [("C-9003", feature_json("C-9003", T1, 25_000, count=12))])
    group = f"test-{run_id}"
    crash = run_worker(t_features, t_scores, store.prefix, group, "--crash-after-redis", "1")
    assert crash.returncode == 99, tail(crash.stdout + crash.stderr)
    assert store.get("C-9003") is not None and read_topic(t_scores, timeout=3) == []  # yazıldı, skor yok

    again = run_worker(t_features, t_scores, store.prefix, group, "--exit-when-idle", "5")
    assert again.returncode == 0, tail(again.stdout + again.stderr)
    assert "eski/tekrar=1 skor=1" in again.stdout, tail(again.stdout)  # feature tekrar ama skor eksikti
    scores = read_topic(t_scores)
    assert [s["scoreId"] for s in scores] == [f"C-9003:{T1}"]


def test_lab5_redis_compacted_topicten_yeniden_kurulur(topics, store):
    t_features, _ = topics
    send(t_features, [
        ("C-9004", feature_json("C-9004", T1, 100)),
        ("C-9004", feature_json("C-9004", T2, 200)),
        ("C-9004", feature_json("C-9004", T0, 999)),   # sırası karışmış eski kayıt
        ("C-9005", feature_json("C-9005", T1, 300)),
    ])
    out = subprocess.run([sys.executable, "tools.py", "--prefix", store.prefix, "rebuild", "--topic", t_features],
                         cwd=LAB_DIR, capture_output=True, text=True, timeout=60)
    assert out.returncode == 0, out.stdout + out.stderr
    assert store.get("C-9004").window_end == T2 and float(store.get("C-9004").total_amount) == 200
    assert store.get("C-9005") is not None
    # Yeniden kurulan pencerelerin skoru zaten yayımlanmıştı: worker tekrar skor üretmemeli.
    assert store.needs_score(store.get("C-9004")) is False
