from pathlib import Path

CSV_SOURCE = [
    {
        "name": "coinmetrics_btc",
        "url": "https://raw.githubusercontent.com/coinmetrics/data/master/csv/btc.csv",
        "symbol": "BTC",
        "currency": "USD",
        "source": "coinmetrics_csv",
    }
]

RAW_DIR = Path("data/raw")
DB_PATH = Path("data/db/pulse_scout.sqlite")