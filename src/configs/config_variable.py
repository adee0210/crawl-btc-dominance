import os
from dotenv import load_dotenv

load_dotenv()

# Read and normalize MONGO port: ensure it's an int when provided, otherwise None
_mongo_port_raw = os.getenv("MONGO_PORT")
try:
    MONGO_PORT = int(_mongo_port_raw) if _mongo_port_raw not in (None, "") else None
except ValueError:
    # If conversion fails, leave as None so MongoClient will use defaults or raise a clear error later
    MONGO_PORT = None

MONGO_CONFIG = {
    "host": os.getenv("MONGO_HOST"),
    "port": MONGO_PORT,
    "user": os.getenv("MONGO_USER"),
    "pass": os.getenv("MONGO_PASS"),
    "auth": os.getenv("MONGO_AUTH"),
}


DATA_CRAWL_CONFIG = {
    # socket/url field removed (not used for BTC.D dominance-only pipeline)
    "url": None,
    "db": "btc_dominance",
    "collection": "raw_btc_dominance",
    # Preferred symbol used for historical/realtime fetches
    "symbol": "BTC.D",
    # Relative path to historical CSV exported from test (if available)
    "historical_csv": "btcd_daily_data.csv",
}

EXTRACT_CONFIG = {
    # Can enable/disable modes or both: realtime or historical
    "realtime_enabled": True,
    "historical_enabled": True,
    # configure data days here - "all" will download complete historical data
    "historical_days": "all",
    "run_parallel": True,
    # expected realtime insertion cadence in seconds (default daily)
    "realtime_poll_seconds": 24 * 60 * 60,
    # wait this many seconds after midnight UTC before inserting daily data
    "realtime_post_midnight_delay_seconds": 60,
}

TELEGRAM_CONFIG = {
    "bot_token": os.getenv("TELEGRAM_BOT_TOKEN"),
    "chat_id": os.getenv("TELEGRAM_CHAT_ID"),
    "check_interval": 30,
    "data_timeout": 60,
    "monitor_enabled": True,
}
