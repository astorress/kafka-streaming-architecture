"""Measure producer throughput under different batching/compression settings."""
import json
import time

from lab.config import producer
from lab.kafka import ensure_topic
from lab.show import step, table, title

title("08 - Throughput, batching y compresión")
topic = "throughput-lab"
ensure_topic(topic, 6, reset=True)
rows = []
for label, config in (("sin linger", {"linger.ms": 0, "compression.type": "none"}),
                      ("batch + lz4", {"linger.ms": 20, "batch.size": 131072, "compression.type": "lz4"})):
    errors = []
    writer = producer(**{"acks": "all", "enable.idempotence": True, **config})
    started = time.perf_counter()
    for i in range(20_000):
        writer.produce(topic, key=f"M{i % 1000:05d}",
                       value=json.dumps({"seq": i, "value": "synthetic-meter-reading" * 4}),
                       on_delivery=lambda error, _message: errors.append(error) if error else None)
        writer.poll(0)
    remaining = writer.flush(60)
    elapsed = time.perf_counter() - started
    if remaining or errors:
        raise RuntimeError(f"delivery failed: remaining={remaining}, errors={len(errors)}")
    rows.append({"profile": label, "messages": 20_000, "seconds": round(elapsed, 2),
                 "messages_per_second": round(20_000 / elapsed)})
step("Benchmark local: el resultado depende de CPU, disco, red, carga y número de particiones")
table(rows)
print("Optimiza midiendo latencia p95/p99, bytes, CPU y skew; el mayor throughput no siempre es el objetivo.")
print("Siguiente: python lessons/09_broker_resilience.py")
