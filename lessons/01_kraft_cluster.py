"""KRaft metadata quorum, brokers, controller and cluster config."""
from confluent_kafka.admin import ConfigResource, ResourceType

from lab.config import BOOTSTRAP
from lab.kafka import admin, wait_for_cluster
from lab.show import step, table, title

title("01 - Brokers y KRaft")
step("Metadatos del clúster: 3 brokers con rol combinado broker + controller")
metadata = wait_for_cluster()
table([{"node_id": broker_id, "host": broker.host, "port": broker.port}
       for broker_id, broker in sorted(metadata.brokers.items())])
print("cluster id:", metadata.cluster_id)
print("En KRaft el metadata log vive en un quórum Raft; ZooKeeper ya no coordina brokers.")

step("Configuración dinámica del broker: defaults y valores efectivos")
client = admin()
resources = [ConfigResource(ResourceType.BROKER, str(broker_id)) for broker_id in sorted(metadata.brokers)]
for resource in resources:
    values = client.describe_configs([resource])[resource].result()
    table([{"broker": resource.name, "config": name, "value": entry.value, "source": entry.source}
           for name, entry in sorted(values.items()) if name in ("num.partitions", "default.replication.factor",
                                                                 "min.insync.replicas", "log.retention.ms")])
print("bootstrap.servers:", BOOTSTRAP)
step("Controladores")
describe = client.describe_cluster().result()
print("controller id:", describe.controller.id, "host:", describe.controller.host)
print("Siguiente: python lessons/02_partitions_replication.py")
