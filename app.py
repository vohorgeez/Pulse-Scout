import sqlite3
import pandas as pd
import streamlit as st
from pathlib import Path

DB_PATH = Path("data/db/pulse_scout.sqlite")

st.set_page_config(page_title="Pulse-Scout", layout="wide")
st.title("Pulse-Scout - Dashboard v1")

@st.cache_data
def load_data():
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql_query(
            "SELECT ts, price, volume, symbol, currency, source FROM ticks ORDER BY ts ASC",
            conn
        )
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    return df.dropna(subset=["ts", "price"])

df = load_data()

sources = sorted(df["source"].unique().tolist())
source = st.selectbox("Source", sources)

symbol = st.selectbox("Symbol", sorted(df["symbol"].unique().tolist()))

view = df[(df["source"] == source) & (df["symbol"] == symbol)].copy()

st.subheader("Alerte simple (règle)")

col1, col2 = st.columns(2)
with col1:
    window_days = st.number_input("Fenêtre (jours)", min_value=1, max_value=365, value=7, step=1)
with col2:
    threshold_pct = st.number_input("Seuil (%)", min_value=0.1, max_value=200.0, value=5.0, step=0.5)

# Sécuriser l'ordre temporel
view = view.sort_values("ts").reset_index(drop=True)

# Variation en % sur N jours (approx: N périodes si données quotidiennes)
# Pour rester simple, on utilise un shift basé sur le nombre de lignes.
shift_n = int(window_days)

view["price_prev"] = view["price"].shift(shift_n)
view["change_pct"] = (view["price"] / view["price_prev"] - 1.0) * 100

alerts = view.dropna(subset=["change_pct"]).copy()
alerts = alerts[alerts["change_pct"].abs() >= threshold_pct]

alerts = alerts.sort_values("ts")

last = alerts.iloc[-1]
direction = "📈" if last["change_pct"] > 0 else "📉"
st.metric(
    label=f"Dernière alerte ({window_days}j)",
    value=f'{last["change_pct"]:.2f}%',
    delta=f'{(last["price"] - last["price_prev"]):.2f}'
)
st.write(f"{direction} Date: {last['ts'].date()} | Prix: {last['price']:.2f}")

# Affichage des alertes
if len(alerts) == 0:
    st.info("Aucune alerte sur la période avec cette règle.")
else:
    st.warning(f"{len(alerts)} alerte(s) détectée(s).")
    st.dataframe(
        alerts[["ts", "price_prev", "price", "change_pct"]].tail(50),
        use_container_width=True
    )

st.subheader("Aperçu")
st.dataframe(view.tail(50), use_container_width=True)

st.subheader("Prix (line charts)")
st.line_chart(view.set_index("ts")["price"])

st.caption(f"Lignes: {len(view)} | DB: {DB_PATH}")