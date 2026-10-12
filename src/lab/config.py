import os

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
READINGS = "meter-readings"
ALERTS = "meter-alerts"


def admin(**configs):
    return AdminClient({"bootstrap.servers": BOOTSTRAP, **configs})


def producer(**configs):
    return Producer({"bootstrap.servers": BOOTSTRAP, **configs})


def consumer(group_id, **configs):
    return Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": group_id, "auto.offset.reset": "earliest",
                     **configs})
