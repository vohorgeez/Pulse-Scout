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

st.subheader("Aperçu")
st.dataframe(view.tail(50), use_container_width=True)

st.subheader("Prix (line charts)")
st.line_chart(view.set_index("ts")["price"])

st.caption(f"Lignes: {len(view)} | DB: {DB_PATH}")