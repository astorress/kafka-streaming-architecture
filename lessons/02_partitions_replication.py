"""Topics, partition leaders, replica assignment, ISR, keys and ordering guarantees."""
import json

from confluent_kafka.admin import NewTopic

from lab.config import READINGS
from lab.kafka import admin, ensure_topic, topic_rows
from lab.show import step, table, title

title("02 - Particiones y replicación")
step("Crear un topic de laboratorio, resettable, con 6 particiones y 3 réplicas")
topic = ensure_topic("partition-lab", 6, reset=True, config={"min.insync.replicas": "2"})
table(topic_rows("partition-lab"))

step("La key enruta al mismo hash/partición: conserva orden por medidor, no entre particiones")
from lab.config import producer

writer = producer(**{"acks": "all", "enable.idempotence": True})
deliveries = []
for meter in ("M00001", "M00002", "M00003"):
    for sequence in range(20):
        writer.produce("partition-lab", key=meter, value=json.dumps({"meter_id": meter, "sequence": sequence}),
                       on_delivery=lambda error, message: deliveries.append(
                           (error, message.key().decode(), message.partition())))
writer.flush(30)
if any(error for error, _, _ in deliveries):
    raise RuntimeError("delivery failure")
table([{"meter_id": meter, "partitions": sorted({partition for _, key, partition in deliveries if key == meter})}
       for meter in ("M00001", "M00002", "M00003")])
print("El orden solo es total dentro de una partición. Agregar particiones puede cambiar el hash -> partición de una key.")

step("Una réplica ISR está suficientemente sincronizada para convertirse en leader")
table(topic_rows("partition-lab"))
print("Siguiente: python lessons/03_producer_reliability.py")
