"""Genera el dataset AMI sintético y crea el topic principal de lecturas."""
import csv
from pathlib import Path

from lab import dataset
from lab.config import READINGS
from lab.kafka import ensure_topic, wait_for_cluster
from lab.show import step, title

root = Path("/app/data/raw")
title("00 - Dataset y preflight")
step("Esperar los tres brokers y validar el quórum KRaft")
metadata = wait_for_cluster()
print("cluster id:", metadata.cluster_id, "brokers:", sorted(metadata.brokers))

step("Catálogo local y lecturas CSV; la producción a Kafka se hace en la lección 10")
root.mkdir(parents=True, exist_ok=True)
with (root / "meters.csv").open("w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=dataset.METER_COLUMNS)
    writer.writeheader()
    writer.writerows(dataset.meters())
readings = root / "readings"
readings.mkdir(exist_ok=True)
for day in dataset.days():
    (readings / f"readings_{day}.csv").write_text(dataset.readings_csv(day))
print("meters:", len(dataset.METER_LIST), "days:", len(dataset.days()))

step("Crear el topic principal con 6 particiones, factor 3 y min ISR 2")
topic = ensure_topic(READINGS, 6, config={"min.insync.replicas": "2", "retention.ms": str(7 * 86_400_000)})
print(READINGS, "partitions:", len(topic.partitions))
print("Próxima: python lessons/01_kraft_cluster.py")
