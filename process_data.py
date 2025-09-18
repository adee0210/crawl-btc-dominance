import os
import sys
import pandas as pd
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))
from src.configs.config_mongo import MongoDBConfig
from src.configs.config_variable import DATA_CRAWL_CONFIG
from tvDatafeed import TvDatafeed, Interval

CSV_PATH = DATA_CRAWL_CONFIG.get("historical_csv") or "btcd_daily_data.csv"


def row_to_doc(dt: datetime, row) -> dict:
    return {
        "datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
        "timestamp_ms": int(dt.timestamp() * 1000),
        "open": (
            float(row.get("open", row.get("Open", None)))
            if pd.notnull(row.get("open", row.get("Open", None)))
            else None
        ),
        "high": (
            float(row.get("high", row.get("High", None)))
            if pd.notnull(row.get("high", row.get("High", None)))
            else None
        ),
        "low": (
            float(row.get("low", row.get("Low", None)))
            if pd.notnull(row.get("low", row.get("Low", None)))
            else None
        ),
        "close": (
            float(row.get("close", row.get("Close", None)))
            if pd.notnull(row.get("close", row.get("Close", None)))
            else None
        ),
    }


def upsert_dataframe_to_mongo(df: pd.DataFrame, collection):
    inserted = 0
    df = df.reset_index()
    for _, row in df.iterrows():
        try:
            idx = row.get("datetime") if "datetime" in row else row.name
            # ensure datetime object
            if not isinstance(idx, str):
                dt = pd.to_datetime(idx)
            else:
                dt = (
                    pd.to_datetime(row["datetime"])
                    if "datetime" in row
                    else pd.to_datetime(idx)
                )

            doc = row_to_doc(dt, row)
            # upsert and remove any 'symbol' field
            collection.update_one(
                {"timestamp_ms": doc["timestamp_ms"]},
                {"$set": doc, "$unset": {"symbol": ""}},
                upsert=True,
            )
            inserted += 1
        except Exception as e:
            print(f"Failed to upsert row: {e}")
    print(f"Upserted/updated {inserted} documents into Mongo collection")


def main():
    # Setup mongo
    mongo_client = MongoDBConfig.get_client()
    db = mongo_client.get_database(DATA_CRAWL_CONFIG.get("db"))
    coll = db.get_collection(DATA_CRAWL_CONFIG.get("collection"))

    # If CSV exists, import it first
    if os.path.exists(CSV_PATH):
        try:
            existing = pd.read_csv(CSV_PATH, parse_dates=[0], index_col=0)
            print(f"Loaded {len(existing)} rows from {CSV_PATH}")
            upsert_dataframe_to_mongo(existing, coll)
        except Exception as e:
            print(f"Failed to import CSV {CSV_PATH}: {e}")

    # Fetch historical using tvDatafeed and upsert
    symbol = DATA_CRAWL_CONFIG.get("symbol", "BTC.D")
    try:
        tv = TvDatafeed()
        df = tv.get_hist(
            symbol=symbol,
            exchange="CRYPTOCAP",
            interval=Interval.in_daily,
            n_bars=10000,
        )
        if df is None or df.empty:
            # try alternative symbol
            df = tv.get_hist(
                symbol="BTCD",
                exchange="CRYPTOCAP",
                interval=Interval.in_daily,
                n_bars=10000,
            )

        if df is not None and not df.empty:
            upsert_dataframe_to_mongo(df, coll)
        else:
            print("No historical data fetched from tvDatafeed.")

    except Exception as e:
        print(f"Failed to fetch or upsert historical from tvDatafeed: {e}")


if __name__ == "__main__":
    main()
    existing = pd.DataFrame()
