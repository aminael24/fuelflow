# ⛽ FuelFlow — prix des carburants en temps réel

**Problème :** le prix d'un plein peut varier de plusieurs euros entre deux stations proches.
**Solution :** FuelFlow surveille en continu les prix officiels de toutes les stations-service de France,
détecte chaque changement de prix et affiche les stations les moins chères par département ainsi que les hausses anormales.

## Architecture

```
API open data (gouv.fr)
        │  toutes les 10 min
        ▼
producer.py ── changements de prix ──► Kafka (topic: prix_carburants)
                                              │
                                              ▼
                                        consumer.py
                                              │  lots
                                              ▼
                              data/prix/date=YYYY-MM-DD/*.parquet
                                              │
                                              ▼
                                dbt + DuckDB (modèles + tests)
                                              │
                                              ▼
                                   Dashboard Streamlit
```

## Stack
| Outil | Rôle |
|---|---|
| Kafka (Docker) | Transport des événements de changement de prix |
| Python | Producer / consumer |
| Parquet | Stockage brut partitionné par date |
| dbt + DuckDB | Nettoyage, modélisation, tests de qualité |
| Streamlit | Visualisation |
| GitHub Actions | CI : lint + dbt run/test sur données de test |

## Modèles dbt
| Modèle | Contenu |
|---|---|
| `stg_prix` | Événements nettoyés et typés |
| `prix_actuels` | Dernier prix par station et carburant |
| `top_stations` | 10 stations les moins chères par département |
| `hausses_recentes` | Hausses > 5 % |
| `stats_departement` | Prix moyen, min, max, écart |

## Lancer le projet
```powershell
.\setup.ps1                      # venv + dépendances + Kafka

python producer\producer.py      # terminal 1
python consumer\consumer.py      # terminal 2

dbt run  --project-dir dbt_project --profiles-dir dbt_project   # terminal 3
dbt test --project-dir dbt_project --profiles-dir dbt_project
streamlit run dashboard\app.py
```
Interface Kafka : http://localhost:8080 — Dashboard : http://localhost:8501

> Toutes les commandes se lancent depuis la racine `fuelflow/`.

## Choix techniques
- **Seuls les changements de prix sont publiés** : Kafka transporte de vrais événements, pas des copies complètes.
- **Commit manuel des offsets** : le consumer valide les messages seulement après l'écriture du fichier Parquet → aucune perte si le script plante.
- **Parquet plutôt qu'écriture directe en base** : DuckDB n'autorise qu'un seul processus en écriture ; les fichiers découplent ingestion et transformation.
- **Tests dbt** : valeurs acceptées, non-nullité, unicité station/carburant.

## Source des données
[Prix des carburants en France — flux instantané v2](https://data.economie.gouv.fr/explore/dataset/prix-des-carburants-en-france-flux-instantane-v2/) (Licence Ouverte).
