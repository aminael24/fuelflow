"""
Consumer FuelFlow
Lit les événements Kafka et les écrit par lots en fichiers Parquet
partitionnés par date :  data/prix/date=YYYY-MM-DD/xxxx.parquet
Lancer depuis la racine du projet :  python consumer/consumer.py
"""
import json
import os
import uuid
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaConsumer

TOPIC = os.getenv("KAFKA_TOPIC", "prix_carburants")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
OUT_DIR = os.getenv("OUT_DIR", "data/prix")
BATCH_MAX = int(os.getenv("BATCH_MAX", "5000"))


def ecrire(batch: list[dict]) -> None:
    jour = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dossier = os.path.join(OUT_DIR, f"date={jour}")
    os.makedirs(dossier, exist_ok=True)
    chemin = os.path.join(dossier, f"{uuid.uuid4().hex}.parquet")
    pd.DataFrame(batch).to_parquet(chemin, index=False)
    print(f"{len(batch)} événements -> {chemin}")


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="fuelflow-writer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # on valide seulement après écriture du fichier
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    print(f"Consumer démarré <- topic '{TOPIC}' sur {BOOTSTRAP}")
    batch: list[dict] = []

    while True:
        records = consumer.poll(timeout_ms=5000, max_records=BATCH_MAX)
        for messages in records.values():
            batch.extend(m.value for m in messages)

        # écrit quand le lot est plein, ou quand plus rien n'arrive
        if batch and (len(batch) >= BATCH_MAX or not records):
            ecrire(batch)
            consumer.commit()  # Kafka : ces messages sont traités
            batch = []


if __name__ == "__main__":
    main()
