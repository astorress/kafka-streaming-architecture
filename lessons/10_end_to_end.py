"""End-to-end keyed stream: publish readings, aggregate them, and inspect output."""
import json
from collections import defaultdict

from lab.config import ALERTS, READINGS, consumer, producer
from lab.kafka import ensure_topic
from lab.show import step, table, title

title("10 - Flujo end-to-end")
ensure_topic(READINGS, 6, reset=True)
ensure_topic(ALERTS, 6, reset=True)

step("Productor: publicar un evento por medidor; la key define afinidad y orden")
writer = producer(**{"acks": "all", "enable.idempotence": True})
for i in range(1, 501):
    meter = f"M{(i % 25) + 1:05d}"
    reading = {"event_id": i, "meter_id": meter, "ts": f"2026-09-01T00:{i % 60:02d}:00-05:00",
               "kwh": round((i % 40) / 10, 2)}
    writer.produce(READINGS, key=meter, value=json.dumps(reading))
if writer.flush(30):
    raise RuntimeError("producer still has undelivered records")

step("Consumer: procesar el lote, calcular acumulados por medidor y publicar alertas")
reader = consumer("end-to-end-lab", **{"enable.auto.commit": False})
reader.subscribe([READINGS])
totals = defaultdict(float)
processed = 0
while processed < 500:
    message = reader.poll(10)
    if message is None:
        continue
    if message.error():
        raise RuntimeError(message.error())
    record = json.loads(message.value())
    totals[record["meter_id"]] += record["kwh"]
    if record["kwh"] >= 3.5:
        writer.produce(ALERTS, key=record["meter_id"], value=json.dumps({"event_id": record["event_id"],
                       "meter_id": record["meter_id"], "kind": "high-consumption", "kwh": record["kwh"]}))
    processed += 1
reader.commit(asynchronous=False)
reader.close()
if writer.flush(30):
    raise RuntimeError("alert producer still has undelivered records")

table([{"meter_id": meter, "total_kwh": round(total, 2)} for meter, total in sorted(totals.items())[:10]])
print(f"processed={processed}; meters={len(totals)}")
print("Este ejemplo ilustra at-least-once: el commit va después del procesamiento; para atomicidad Kafka usa transacciones.")
