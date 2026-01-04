from pathlib import Path
import requests
import pandas as pd
import sqlite3
from config import CSV_SOURCE, RAW_DIR, DB_PATH
from config import API_SOURCES

"""
Pipeline:
- ingest CSV sources
- ingest API sources (future)
- normalize to standard schema
- write to SQLite (idempotent)
"""

def download_csv(url: str, out_path: Path) -> None:
    if out_path.exists() and out_path.stat().st_size > 0:
        print(f"[download] Using cached file: {out_path}")
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)

    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Download failed: HTTP {resp.status_code}")
    
    out_path.write_bytes(resp.content)

def load_raw_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # Mapping coinmetrics -> format standard
    df = df.rename(columns={
        "time": "ts",
        "PriceUSD": "price",
        "volume_reported_spot_usd_1d": "volume",
    })

    # On ne garde que ce qui nous intéresse
    expected = {"ts", "price", "volume"}
    missing = expected - set(df.columns)
    if missing:
        raise KeyError(f"Missing columns after rename: {missing}. Available: {df.columns.tolist()}")
    df = df[["ts", "price", "volume"]].copy()

    # Types
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    # Ajout colonnes standard
    df["symbol"] = "BTC"
    df["currency"] = "USD"
    df["source"] = "coinmetrics_csv"

    # On jette les lignes cassées
    df = df.dropna(subset=["ts", "price"])

    return df

CREATE_TICKS_SQL = """
CREATE TABLE IF NOT EXISTS ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    ts TEXT NOT NULL,
    price REAL NOT NULL,
    volume REAL,
    currency TEXT,
    UNIQUE(source, symbol, ts)
);
"""

def ensure_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(CREATE_TICKS_SQL)
        conn.commit()

def write_ticks(df: pd.DataFrame, db_path: Path) -> int:
    """
    Insère df dans ticks en évitant les doublons.
    Retourne le nombre de lignes effectivement insérées.
    """
    # SQLite aime bien les dates en texte ISO
    df = df.copy()
    df["ts"] = df["ts"].dt.strftime("%Y-%m-%d")

    with sqlite3.connect(db_path) as conn:
        # Table staging temporaire (sans contraintes)
        df.to_sql("ticks_staging", conn, if_exists="replace", index=False)

        before = conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]

        conn.execute("""
            INSERT OR IGNORE INTO ticks (ts, price, volume, symbol, currency, source)
            SELECT ts, price, volume, symbol, currency, source
            FROM ticks_staging
        """)
        conn.commit()

        after = conn.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]

    return after - before

def fetch_coingecko_market_chart(base_url: str, coin_id: str, vs_currency: str, days: str) -> dict:
    url = f"{base_url}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days}
    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"CoinGecko API error: HTTP {resp.status_code} - {resp.text[:200]}")
    if resp.status_code in (401, 403):
        raise RuntimeError(
            f"CoinGecko blocked the request (HTTP {resp.status_code})."
            f"Try reducing 'days' (e.g., 365 or 90). Response: {resp.text[:200]}"
        )
    return resp.json()

def normalize_coingecko_market_chart(payload: dict, symbol: str, currency: str, source: str) -> pd.DataFrame:
    prices = pd.DataFrame(payload["prices"], columns=["ts_ms", "price"])
    vols = pd.DataFrame(payload["total_volumes"], columns=["ts_ms", "volume"])

    df = prices.merge(vols, on="ts_ms", how="left")
    df["ts"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True).dt.tz_convert(None)

    df = df[["ts", "price", "volume"]].copy()
    df["symbol"] = symbol
    df["currency"] = currency
    df["source"] = source

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.dropna(subset=["ts", "price"])

    return df

def main():
    all_norm = []

    # CSV sources (déjà existant)
    download_csv(CSV_SOURCE[0]["url"], RAW_DIR / "btc.csv")
    raw_df = load_raw_csv(RAW_DIR / "btc.csv")
    all_norm.append(normalize(raw_df))

    # API sources (nouveau)
    api_cfg = API_SOURCES[0]
    payload = fetch_coingecko_market_chart(
        api_cfg["base_url"],
        api_cfg["coin_id"],
        api_cfg["vs_currency"],
        api_cfg["days"],
    )
    all_norm.append(
        normalize_coingecko_market_chart(payload, api_cfg["symbol"], api_cfg["currency"], api_cfg["source"])
    )
    norm_df = pd.concat(all_norm, ignore_index=True)

    ensure_db(DB_PATH)
    inserted = write_ticks(norm_df, DB_PATH)

    print(f"[raw] rows={len(raw_df)} cols={len(raw_df.columns)}")
    print(f"[norm] rows={len(norm_df)} cols={len(norm_df.columns)}")
    print(f"[db] inserted={inserted} db_path={DB_PATH}")
    print(norm_df.head(3))

if __name__ == "__main__":
    main()