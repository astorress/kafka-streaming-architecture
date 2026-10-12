"""Consumer groups, partition assignment, rebalances and group coordination."""
import threading
import time
from collections import Counter

from lab.config import consumer, producer
from lab.kafka import ensure_topic
from lab.show import step, table, title

title("04 - Consumer groups")
topic = "consumer-group-lab"
ensure_topic(topic, 6, reset=True)
writer = producer(**{"acks": "all"})
for i in range(600):
    writer.produce(topic, key=f"key-{i % 20}", value=str(i))
writer.flush()

step("Tres consumidores, un group.id: cada partición se asigna a un solo miembro del grupo")
barrier = threading.Barrier(3)
results = []


def run_member(index):
    reader = consumer("analytics-lab", **{"auto.offset.reset": "earliest", "enable.auto.commit": False})
    reader.subscribe([topic])
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline and not reader.assignment():
        reader.poll(0.2)
    assignment = reader.assignment()
    barrier.wait()
    count = 0
    idle = 0
    while time.monotonic() < deadline and idle < 6:
        message = reader.poll(0.3)
        if message is None:
            idle += 1
        elif message.error() is None:
            count += 1
            idle = 0
    results.append({"member": index, "assignment": [p.partition for p in assignment], "records": count})
    reader.close()


threads = [threading.Thread(target=run_member, args=(i,)) for i in range(3)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
table(sorted(results, key=lambda row: row["member"]))
print("No más de un consumidor del mismo grupo procesa una partición al tiempo. Consumidores extra quedan idle.")
print("Cierra uno y el coordinator redistribuye sus particiones: rebalance.")
print("Siguiente: python lessons/05_offsets_lag.py")
