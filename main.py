from pathlib import Path
import requests
import pandas as pd

CSV_URL = "https://raw.githubusercontent.com/coinmetrics/data/master/csv/btc.csv"
RAW_PATH = Path("data/raw/btc.csv")

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

def main():
    download_csv(CSV_URL, RAW_PATH)
    raw_df = load_raw_csv(RAW_PATH)
    norm_df = normalize(raw_df)

    print(f"[raw] rows={len(raw_df)} cols={len(raw_df.columns)}")
    print(f"[norm] rows={len(norm_df)} cols={len(norm_df.columns)}")
    print(norm_df.head(3))

if __name__ == "__main__":
    main()