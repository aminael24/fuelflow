"""
Dashboard FuelFlow
Lancer depuis la racine du projet :  streamlit run dashboard/app.py
"""
import os

import duckdb
import streamlit as st

DB = os.getenv("DUCKDB_PATH", "data/fuelflow.duckdb")

st.set_page_config(page_title="FuelFlow", page_icon="⛽", layout="wide")
st.title("⛽ FuelFlow — carburant le moins cher en France")

if not os.path.exists(DB):
    st.error("Base introuvable. Lance d'abord le producer, le consumer puis `dbt run`.")
    st.stop()


@st.cache_data(ttl=60)
def requete(sql: str, params: list | None = None):
    with duckdb.connect(DB, read_only=True) as con:
        return con.execute(sql, params or []).df()


# ---- Filtres ----
deps = requete("select distinct departement from top_stations order by 1")["departement"].tolist()
carbs = requete("select distinct carburant from top_stations order by 1")["carburant"].tolist()

col1, col2 = st.columns(2)
dep = col1.selectbox("Département", deps)
carb = col2.selectbox("Carburant", carbs, index=carbs.index("Gazole") if "Gazole" in carbs else 0)

# ---- Indicateurs ----
stats = requete(
    "select * from stats_departement where departement = ? and carburant = ?", [dep, carb]
)
if not stats.empty:
    s = stats.iloc[0]
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Prix moyen", f"{s.prix_moyen:.3f} €")
    k2.metric("Moins cher", f"{s.prix_min:.3f} €")
    k3.metric("Plus cher", f"{s.prix_max:.3f} €")
    k4.metric("Économie possible / 50 L", f"{s.ecart * 50:.2f} €")

# ---- Top 10 ----
st.subheader(f"Top 10 — {carb} — {dep}")
top = requete(
    """select rang, ville, adresse, prix, maj, latitude, longitude
       from top_stations where departement = ? and carburant = ?
       order by rang""",
    [dep, carb],
)
st.dataframe(top.drop(columns=["latitude", "longitude"]), hide_index=True, width="stretch")

carte = top.dropna(subset=["latitude", "longitude"])
if not carte.empty:
    # certaines sources donnent les coordonnées x 100 000
    if carte["latitude"].abs().max() > 90:
        carte = carte.assign(latitude=carte.latitude / 100000, longitude=carte.longitude / 100000)
    st.map(carte, latitude="latitude", longitude="longitude")

# ---- Hausses ----
st.subheader("🚨 Hausses de prix récentes (> 5 %)")
hausses = requete(
    """select ts, ville, adresse, departement, carburant, ancien_prix, prix, hausse_pct
       from hausses_recentes order by ts desc limit 50"""
)
if hausses.empty:
    st.info("Aucune hausse anormale détectée pour l'instant.")
else:
    st.dataframe(hausses, hide_index=True, width="stretch")
