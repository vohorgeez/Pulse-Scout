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

API_SOURCES = [
    {
        "name": "coingecko_btc_market_chart",
        "base_url": "https://api.coingecko.com/api/v3",
        "coin_id": "bitcoin",
        "vs_currency": "usd",
        "days": "365",
        "symbol": "BTC",
        "currency": "USD",
        "source": "coingecko_api",
    }
]