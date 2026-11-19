"""Görev 3 provası: broker'ları gerçekten durdurur ve geri getirir (~30 sn).  make test-failover

1 broker kapalı → yazma devam eder. 2 broker kapalı → NotEnoughReplicas; producer'da mesajlar birikir,
kuyruk dolunca backpressure; broker dönünce kuyruk boşalır, hiç mesaj kaybolmaz.
Test bittiğinde (başarısız olsa bile) broker'lar geri getirilir ve leader'lar dengelenir.
"""
from __future__ import annotations

import subprocess
import sys
import time

import pytest

from common import LAB_DIR, TOPIC
from conftest import run_app, tail

pytestmark = pytest.mark.failover


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=LAB_DIR, check=True, capture_output=True)


def wait_isr(admin, size: int, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        parts = admin.list_topics(topic=TOPIC, timeout=10).topics[TOPIC].partitions.values()
        if all(len(p.isrs) == size for p in parts):
            return
        time.sleep(1)
    pytest.fail(f"{timeout:.0f} sn içinde bütün partition'larda ISR = {size} olmadı")


def test_broker_kaybi_notenoughreplicas_ve_backpressure(admin, tmp_path):
    try:
        compose("stop", "-t", "30", "kafka-2")
        wait_isr(admin, 2)
        one_down = run_app("producer.py", "--rate", "50", "--duration", "2")
        assert "failed=0 undelivered=0" in one_down.stdout, f"1 broker kapalıyken yazma sürmeliydi:\n{tail(one_down.stdout)}"

        compose("stop", "-t", "30", "kafka-3")
        wait_isr(admin, 1)
        rejected = run_app("producer.py", "--rate", "5", "--duration", "2", "--no-retry")
        assert "Not enough in-sync replicas" in rejected.stdout, tail(rejected.stdout + rejected.stderr)

        # Yerel kuyruk 200 mesajda dolar → backpressure; broker dönünce hepsi teslim edilir.
        out_file = tmp_path / "producer.log"
        with out_file.open("w") as out:
            proc = subprocess.Popen([sys.executable, "producer.py", "--rate", "100", "--duration", "6",
                                     "--queue-max", "200"], cwd=LAB_DIR, stdout=out, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 20
            while "Yerel kuyruk dolu" not in out_file.read_text() and time.monotonic() < deadline:
                time.sleep(0.5)
            assert "Yerel kuyruk dolu" in out_file.read_text(), tail(out_file.read_text())
            assert proc.poll() is None, "kuyruk dolunca producer çökmemeli"

            compose("start", "kafka-3")
            proc.wait(timeout=120)
        log = out_file.read_text()
        assert "SUMMARY delivered=600 failed=0 undelivered=0" in log, tail(log)
    finally:
        compose("start", "kafka-2", "kafka-3")
        wait_isr(admin, 3, timeout=120)
        subprocess.run(["bash", "scripts/leaders.sh"], cwd=LAB_DIR, capture_output=True)
