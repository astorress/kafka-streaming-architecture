import time

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka.admin import NewTopic

from lab.config import BOOTSTRAP, admin


def wait_for_cluster(timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            metadata = admin().list_topics(timeout=5)
            if len(metadata.brokers) == 3:
                return metadata
        except KafkaException:
            pass
        time.sleep(1)
    raise TimeoutError("Kafka did not expose all 3 brokers")


def ensure_topic(name, partitions, replication=3, config=None, reset=False):
    client = admin()
    metadata = client.list_topics(timeout=10).topics
    if name in metadata and reset:
        client.delete_topics([name])[name].result()
        deadline = time.monotonic() + 60
        while name in client.list_topics(timeout=5).topics:
            if time.monotonic() > deadline:
                raise TimeoutError(f"topic deletion timed out: {name}")
            time.sleep(0.5)
    if name not in client.list_topics(timeout=10).topics:
        client.create_topics([NewTopic(name, partitions, replication, config=config or {})])[name].result()
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        topic = client.list_topics(name, timeout=5).topics.get(name)
        if topic and topic.partitions and all(p.leader >= 0 and len(p.isrs) == len(p.replicas)
                                              for p in topic.partitions.values()):
            return topic
        time.sleep(0.5)
    raise TimeoutError(f"topic did not reach full ISR: {name}")


def topic_rows(name):
    topic = admin().list_topics(name, timeout=10).topics[name]
    return [{"partition": p.id, "leader": p.leader, "replicas": list(p.replicas), "isr": list(p.isrs)}
            for p in sorted(topic.partitions.values(), key=lambda p: p.id)]


def watermarks(name):
    client = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"inspect-{name}", "enable.auto.commit": False})
    try:
        topic = admin().list_topics(name, timeout=10).topics[name]
        return {p: client.get_watermark_offsets(TopicPartition(name, p), timeout=10)
                for p in sorted(topic.partitions)}
    finally:
        client.close()


def poll_one(consumer, timeout=1):
    message = consumer.poll(timeout)
    if message is None:
        return None
    if message.error():
        if message.error().code() == KafkaError._PARTITION_EOF:
            return None
        raise KafkaException(message.error())
    return message
