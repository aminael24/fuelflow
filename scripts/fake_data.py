"""Génère de faux événements Parquet pour tester dbt sans Kafka (utilisé par la CI)."""
import os
import random
from datetime import datetime, timedelta, timezone

import pandas as pd

random.seed(42)
os.makedirs("data/prix/date=2026-01-01", exist_ok=True)
deps = [("Paris", "75"), ("Rhône", "69"), ("Gironde", "33")]
carbs = ["Gazole", "SP95", "E10"]
t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
rows = []
for i in range(60):
    dep, code = deps[i % 3]
    for c in carbs:
        p1 = round(random.uniform(1.6, 2.0), 3)
        p2 = round(p1 * random.choice([1.0, 1.02, 1.08]), 3)
        for k, (prix, ancien) in enumerate([(p1, None), (p2, p1)]):
            rows.append(dict(station_id=str(1000 + i), adresse=f"{i} rue Test", ville="Ville",
                             code_postal=code + "000", departement=dep, code_departement=code,
                             latitude=45.0, longitude=2.0, carburant=c, prix=prix,
                             ancien_prix=ancien, ts=(t0 + timedelta(hours=k)).isoformat()))
pd.DataFrame(rows).to_parquet("data/prix/date=2026-01-01/fake.parquet", index=False)
print(len(rows), "événements de test générés")
