"""Inspect replica placement and reason about broker loss without stopping containers."""
from lab.kafka import ensure_topic, topic_rows, wait_for_cluster
from lab.show import step, table, title

title("09 - Replicación y resiliencia")
wait_for_cluster()
topic = "resilience-lab"
ensure_topic(topic, 6, replication=3, reset=True,
                     config={"min.insync.replicas": "2"})

step("Cada partición tiene tres réplicas distribuidas entre los tres brokers")
rows = topic_rows(topic)
table(rows)
if any(len(row["replicas"]) != 3 or len(row["isr"]) < 2 for row in rows):
    raise RuntimeError("replica placement or ISR does not meet the lab policy")

step("Semántica ante pérdida de un broker")
print("Con RF=3, min.insync.replicas=2 y acks=all, una caída permite seguir escribiendo si quedan 2 ISR.")
print("Con una sola ISR, las escrituras fallan de forma segura en lugar de confirmar datos con poca redundancia.")
print("En un entorno controlado, detén un solo broker manualmente y observa leader/ISR; no uses down -v.")
print("Siguiente: python lessons/10_end_to_end.py")
