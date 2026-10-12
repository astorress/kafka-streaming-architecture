"""Crea los temas base; las lecciones también saben resetear los suyos de forma idempotente."""
from confluent_kafka.admin import NewTopic

from lab.config import ALERTS, READINGS, admin

client = admin()
existing = client.list_topics(timeout=10).topics
topics = [NewTopic(name, num_partitions=count, replication_factor=3) for name, count in ((READINGS, 6), (ALERTS, 3))
          if name not in existing]
for future in client.create_topics(topics).values():
    future.result()
print("topics ready:", READINGS, ALERTS)
