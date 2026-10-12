"""Compare delete retention with compacted topics and inspect topic-level policies."""
from confluent_kafka.admin import ConfigResource, ResourceType

from lab.config import admin
from lab.kafka import ensure_topic
from lab.show import step, table, title

title("07 - Retención y compactación")

step("Un topic de eventos conserva el historial por tiempo/tamaño y luego elimina segmentos")
events = "retention-lab"
ensure_topic(events, 3, reset=True, config={"cleanup.policy": "delete", "retention.ms": "604800000",
                                           "retention.bytes": "104857600"})

step("Un topic compactado conserva el último valor por key; tombstones habilitan borrados lógicos")
state = "compaction-lab"
ensure_topic(state, 3, reset=True, config={"cleanup.policy": "compact", "min.compaction.lag.ms": "60000",
                                          "delete.retention.ms": "86400000"})
resources = [ConfigResource(ResourceType.TOPIC, name) for name in (events, state)]
client = admin()
configs = client.describe_configs(resources)
policies = []
for resource, future in configs.items():
    settings = future.result()
    policies.append({"topic": resource.name, "cleanup.policy": settings["cleanup.policy"].value})
table(policies)
print("La compactación corre en segundo plano; no garantiza que los duplicados desaparezcan inmediatamente.")
print("Usa delete para eventos acotados por retención; compact para changelog/estado por key.")
print("Siguiente: python lessons/08_producer_throughput.py")
