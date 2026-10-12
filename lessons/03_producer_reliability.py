"""acks, min.insync.replicas, retries, idempotence, batching and delivery callbacks."""
import json
import time

from lab.config import producer
from lab.kafka import ensure_topic, topic_rows
from lab.show import step, table, title

title("03 - Productor confiable")
topic = "producer-reliability"
ensure_topic(topic, 3, reset=True, config={"min.insync.replicas": "2"})

step("Productor con acks=all + idempotencia: confirma la escritura cuando todas las ISR responden")
delivered = []
writer = producer(**{"acks": "all", "enable.idempotence": True, "linger.ms": 10, "batch.size": 65536,
                     "compression.type": "lz4", "delivery.timeout.ms": 30000})
started = time.perf_counter()
for i in range(10_000):
    writer.produce(topic, key=f"M{i % 1000:05d}", value=json.dumps({"seq": i, "kwh": i / 1000}),
                   on_delivery=lambda error, message: delivered.append((error, message.partition(), message.offset())))
    writer.poll(0)
remaining = writer.flush(60)
elapsed = time.perf_counter() - started
if remaining or any(error for error, *_ in delivered):
    raise RuntimeError(f"delivery errors: remaining={remaining}")
print(f"sent={len(delivered):,} in {elapsed:.2f}s; {len(delivered) / elapsed:,.0f} msg/s")
table(topic_rows(topic))

step("Semántica de entrega")
print("acks=0: no espera confirmación; acks=1: espera al leader; acks=all: espera a todas las ISR.")
print("Idempotence deduplica retries del productor en el broker; no hace exactamente-una-vez entre sistemas.")
print("Con min.insync.replicas=2 + acks=all se rechazan escrituras si queda solo una ISR.")
print("Siguiente: python lessons/04_consumer_groups.py")
