"""Offsets, commits, at-least-once, auto-commit pitfalls and lag."""
from confluent_kafka import TopicPartition

from lab.config import consumer, producer
from lab.kafka import ensure_topic, watermarks
from lab.show import step, table, title

title("05 - Offsets y lag")
topic = "offsets-lab"
ensure_topic(topic, 3, reset=True)
writer = producer(**{"acks": "all"})
for i in range(90):
    writer.produce(topic, key=f"m-{i % 9}", value=str(i))
writer.flush()

step("Low/high watermark: el offset high es el siguiente mensaje a escribir")
table([{"partition": partition, "low": bounds[0], "high": bounds[1], "log_end": bounds[1]}
       for partition, bounds in watermarks(topic).items()])

step("Commit explícito: confirmar después del efecto da at-least-once; antes del efecto puede perder trabajo")
reader = consumer("billing-lab", **{"enable.auto.commit": False, "auto.offset.reset": "earliest"})
reader.subscribe([topic])
records = []
while len(records) < 20:
    message = reader.poll(5)
    if message is not None and message.error() is None:
        records.append(message)
reader.commit(asynchronous=False)
committed = reader.committed([TopicPartition(topic, p) for p in range(3)], timeout=10)
reader.close()
table([{"partition": tp.partition, "committed_next_offset": tp.offset} for tp in committed])

step("Lag = log end offset - committed offset, por partición del grupo")
latest = watermarks(topic)
commits = {tp.partition: tp.offset for tp in committed}
table([{"partition": p, "committed": commits.get(p), "latest": bounds[1],
        "lag": bounds[1] - max(commits.get(p, 0), 0)} for p, bounds in sorted(latest.items())])
print("El commit guarda el siguiente offset a consumir. Tras procesar y caer antes del commit, el mensaje se repite.")
print("Siguiente: python lessons/06_transactions.py")
