import os
import sys
import time
from datetime import datetime
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.configs.config_mongo import MongoDBConfig
from src.configs.config_variable import DATA_CRAWL_CONFIG
from src.log.logger_setup import LoggerSetup


class ExtractBTCDominanceHistorical:
    def __init__(self, csv_path: str = None):
        self.logger = LoggerSetup.logger_setup("ExtractBTCDominanceHistorical")
        self.mongo_client = MongoDBConfig.get_client()
        self.db_name = DATA_CRAWL_CONFIG.get("db")
        self.collection_name = DATA_CRAWL_CONFIG.get("collection")
        self.collection = self.mongo_client.get_database(self.db_name).get_collection(
            self.collection_name
        )
        # CSV is not used in this extractor; we always write to Mongo
        self.csv_path = None
        self.symbol = DATA_CRAWL_CONFIG.get("symbol", "BTC.D")

    # No CSV reading: historical extractor writes only to Mongo

    def _insert_daily_docs(self, df: pd.DataFrame):
        # Expect df.index as datetime-like
        inserted = 0
        for idx, row in df.iterrows():
            try:
                # normalize timestamp
                if not hasattr(idx, "to_pydatetime"):
                    dt = pd.to_datetime(idx)
                else:
                    dt = idx.to_pydatetime()

                doc = {
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
                    "volume": (
                        float(row.get("volume", row.get("Volume", None)))
                        if pd.notnull(row.get("volume", row.get("Volume", None)))
                        else None
                    ),
                }

                # upsert by timestamp_ms to avoid duplicates, and remove any leftover 'symbol' field
                self.collection.update_one(
                    {"timestamp_ms": doc["timestamp_ms"]},
                    {"$set": doc, "$unset": {"symbol": ""}},
                    upsert=True,
                )
                inserted += 1
            except Exception as e:
                self.logger.error(f"Failed to insert row {idx}: {e}")

        self.logger.info(
            f"Inserted/updated {inserted} daily documents into {self.collection_name}"
        )
        return inserted

    def get_all_historical_data(self):
        # Always fetch from TradingView and upsert into Mongo
        try:
            from tvDatafeed import TvDatafeed, Interval

            tv = TvDatafeed()
            self.logger.info(
                f"Fetching historical data from TradingView via tvDatafeed for symbol {self.symbol}"
            )

            attempts = 3
            df = None
            for attempt in range(1, attempts + 1):
                try:
                    df = tv.get_hist(
                        symbol=self.symbol,
                        exchange="CRYPTOCAP",
                        interval=Interval.in_daily,
                        n_bars=10000,
                    )
                    if df is not None and len(df) > 0:
                        break
                except Exception as e:
                    self.logger.warning(f"Attempt {attempt} failed: {e}")
                time.sleep(2)

            if df is None or len(df) == 0:
                self.logger.warning(
                    "No historical bars returned by tvDatafeed after retries"
                )
                return 0

            return self._insert_daily_docs(df)

        except Exception as e:
            self.logger.error(f"Failed to fetch history via tvDatafeed: {e}")
            raise

    def get_recent_historical_data(self, days: int = 30):
        try:
            from tvDatafeed import TvDatafeed, Interval
        except Exception as e:
            self.logger.error(f"Error fetching recent historical data: {e}")
            raise


if __name__ == "__main__":
    h = ExtractBTCDominanceHistorical()
    print("Starting full historical fetch...")
    h.get_all_historical_data()
