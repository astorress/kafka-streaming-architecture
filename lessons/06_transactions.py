"""Kafka transactions and read_committed isolation for atomic consume-transform-produce."""
import json

from lab.config import consumer, producer
from lab.kafka import ensure_topic
from lab.show import step, table, title
from confluent_kafka import TopicPartition

title("06 - Transacciones")
source, target = "tx-source", "tx-results"
ensure_topic(source, 3, reset=True)
ensure_topic(target, 3, reset=True)

step("Escribir un lote y abortarlo: read_committed no expone mensajes de transacciones abortadas")
tx = producer(**{"transactional.id": "lab-transformer-1", "enable.idempotence": True, "acks": "all"})
tx.init_transactions()
tx.begin_transaction()
tx.produce(target, key="M00001", value=json.dumps({"kind": "aborted", "kwh": 99}))
tx.abort_transaction()

source_writer = producer(**{"acks": "all", "enable.idempotence": True})
for i in range(5):
    source_writer.produce(source, key=f"M{i:05d}", value=json.dumps({"kind": "input", "kwh": i / 10}))
if source_writer.flush(30):
    raise RuntimeError("source records were not delivered")

reader = consumer("transactional-transformer", **{"enable.auto.commit": False,
                                                   "isolation.level": "read_committed"})
reader.subscribe([source])
inputs = []
while len(inputs) < 5:
    message = reader.poll(10)
    if message is None:
        continue
    if message.error():
        raise RuntimeError(message.error())
    inputs.append(message)

step("Consume-transform-produce: resultado y offsets de entrada se confirman como una transacción")
tx.begin_transaction()
for message in inputs:
    record = json.loads(message.value())
    tx.produce(target, key=message.key(), value=json.dumps({**record, "kind": "committed"}))
offsets = [TopicPartition(message.topic(), message.partition(), message.offset() + 1) for message in inputs]
tx.send_offsets_to_transaction(offsets, reader.consumer_group_metadata(), 30)
tx.commit_transaction()
reader.close()

reader = consumer("verify-read-committed", **{"isolation.level": "read_committed", "auto.offset.reset": "earliest"})
reader.subscribe([target])
messages = []
idle = 0
while idle < 4:
    message = reader.poll(0.5)
    if message is None:
        idle += 1
    elif message.error() is None:
        messages.append(json.loads(message.value()))
        idle = 0
reader.close()
table([{"records visible": len(messages), "aborted visible": sum(m["kind"] == "aborted" for m in messages),
        "committed visible": sum(m["kind"] == "committed" for m in messages)}])
print("Kafka transactions are exactly-once within Kafka topics/partitions, with read_committed consumers.")
print("For an external database, coordinate commits with an outbox/CDC or idempotent sink.")
print("Siguiente: python lessons/07_compaction_retention.py")
