"""
Producer FuelFlow
Télécharge toutes les 10 minutes les prix des carburants (open data gouv.fr)
et publie dans Kafka uniquement les CHANGEMENTS de prix.
Lancer depuis la racine du projet :  python producer/producer.py
"""
import io
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from kafka import KafkaProducer

URL = (
    "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
    "prix-des-carburants-en-france-flux-instantane-v2/exports/csv"
)
TOPIC = os.getenv("KAFKA_TOPIC", "prix_carburants")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
INTERVAL = int(os.getenv("INTERVAL_SECONDS", "600"))

# carburant -> colonne de prix dans le CSV
CARBURANTS = {
    "Gazole": "gazole_prix",
    "SP95": "sp95_prix",
    "SP98": "sp98_prix",
    "E10": "e10_prix",
    "E85": "e85_prix",
    "GPLc": "gplc_prix",
}


def telecharger() -> pd.DataFrame:
    r = requests.get(URL, timeout=120)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), sep=";", low_memory=False)
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def valeur(row: dict, *cles):
    """Renvoie la première colonne présente et non vide parmi `cles`."""
    for c in cles:
        v = row.get(c)
        if v is not None and not (isinstance(v, float) and pd.isna(v)):
            return v
    return None


def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=5,
    )
    derniers_prix: dict[tuple[str, str], float] = {}
    print(f"Producer démarré -> topic '{TOPIC}' sur {BOOTSTRAP}")

    while True:
        try:
            df = telecharger()
        except Exception as e:  # réseau, API indisponible...
            print(f"[ERREUR] téléchargement : {e} — nouvel essai dans 60 s")
            time.sleep(60)
            continue

        maintenant = datetime.now(timezone.utc).isoformat()
        envoyes = 0
        colonnes_ok = {k: v for k, v in CARBURANTS.items() if v in df.columns}
        if not colonnes_ok:
            print(f"[ERREUR] colonnes de prix introuvables. Colonnes : {list(df.columns)}")
            time.sleep(INTERVAL)
            continue

        for row in df.to_dict(orient="records"):
            station_id = str(row.get("id"))
            for carburant, col in colonnes_ok.items():
                prix = row.get(col)
                if prix is None or pd.isna(prix):
                    continue
                prix = float(str(prix).replace(",", "."))
                cle = (station_id, carburant)
                ancien = derniers_prix.get(cle)
                if ancien == prix:
                    continue  # pas de changement -> rien à envoyer

                evenement = {
                    "station_id": station_id,
                    "adresse": valeur(row, "adresse"),
                    "ville": valeur(row, "ville"),
                    "code_postal": str(valeur(row, "cp") or ""),
                    "departement": valeur(row, "departement", "dep_name"),
                    "code_departement": str(valeur(row, "code_departement", "dep_code") or ""),
                    "latitude": valeur(row, "latitude"),
                    "longitude": valeur(row, "longitude"),
                    "carburant": carburant,
                    "prix": prix,
                    "ancien_prix": ancien,
                    "ts": maintenant,
                }
                producer.send(TOPIC, key=station_id, value=evenement)
                derniers_prix[cle] = prix
                envoyes += 1

        producer.flush()
        print(f"{maintenant} | {len(df)} stations lues | {envoyes} changements publiés")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
