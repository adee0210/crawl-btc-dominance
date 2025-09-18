import os
import sys
import threading
import time
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pandas as pd

from src.configs.config_mongo import MongoDBConfig
from src.configs.config_variable import DATA_CRAWL_CONFIG, EXTRACT_CONFIG
from src.log.logger_setup import LoggerSetup


class ExtractBTCDominanceRealtime:
    def __init__(self, poll_interval_seconds: int = 24 * 60 * 60):
        # Default: run daily checks
        self.logger = LoggerSetup.logger_setup("ExtractBTCDominanceRealtime")
        self.mongo_client = MongoDBConfig.get_client()
        self.db_name = DATA_CRAWL_CONFIG.get("db")
        self.collection_name = DATA_CRAWL_CONFIG.get("collection")
        self.collection = self.mongo_client.get_database(self.db_name).get_collection(
            self.collection_name
        )

        self.symbol = DATA_CRAWL_CONFIG.get("symbol", "BTC.D")

        # allow config-driven poll interval
        cfg_poll = None
        try:
            cfg_poll = int(
                __import__(
                    "src.configs.config_variable", fromlist=["EXTRACT_CONFIG"]
                ).EXTRACT_CONFIG.get("realtime_poll_seconds")
            )
        except Exception:
            cfg_poll = None

        self.poll_interval_seconds = poll_interval_seconds or cfg_poll or 24 * 60 * 60
        self.running = False
        self.thread = None

    def _fetch_latest_hour(self):
        # Try tvDatafeed first
        try:
            from tvDatafeed import TvDatafeed, Interval

            tv = TvDatafeed()
            # fetch daily bars; keep last 2 in case of shifts
            df = tv.get_hist(
                symbol=self.symbol,
                exchange="CRYPTOCAP",
                interval=Interval.in_daily,
                n_bars=2,
            )

            if df is None or len(df) == 0:
                return None

            # take the last row (most recent daily bar)
            last_idx = df.index[-1]
            last_row = df.iloc[-1]

            if hasattr(last_idx, "to_pydatetime"):
                dt = last_idx.to_pydatetime()
            else:
                dt = pd.to_datetime(last_idx).to_pydatetime()

            doc = {
                "datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp_ms": int(dt.timestamp() * 1000),
                "open": (
                    float(last_row.get("open", last_row.get("Open", None)))
                    if pd.notnull(last_row.get("open", last_row.get("Open", None)))
                    else None
                ),
                "high": (
                    float(last_row.get("high", last_row.get("High", None)))
                    if pd.notnull(last_row.get("high", last_row.get("High", None)))
                    else None
                ),
                "low": (
                    float(last_row.get("low", last_row.get("Low", None)))
                    if pd.notnull(last_row.get("low", last_row.get("Low", None)))
                    else None
                ),
                "close": (
                    float(last_row.get("close", last_row.get("Close", None)))
                    if pd.notnull(last_row.get("close", last_row.get("Close", None)))
                    else None
                ),
            }

            return doc

        except Exception as e:
            self.logger.error(f"Error fetching realtime data via tvDatafeed: {e}")
            return None

    def _insert_doc(self, doc: dict):
        if not doc:
            return False
        try:
            # Upsert and remove any existing 'symbol' field
            self.collection.update_one(
                {"timestamp_ms": doc["timestamp_ms"]},
                {"$set": doc, "$unset": {"symbol": ""}},
                upsert=True,
            )
            self.logger.info(f"Upserted realtime doc ts={doc['timestamp_ms']}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert realtime doc: {e}")
            return False

    def _run_loop(self):
        self.logger.info("Realtime extractor loop started")
        # initial immediate run
        try:
            doc = self._fetch_latest_hour()
            if doc:
                self._insert_doc(doc)
            else:
                self.logger.debug("No realtime doc fetched on initial run")
        except Exception as e:
            self.logger.error(f"Error on initial realtime fetch: {e}")

        while self.running:
            try:
                # If running daily, align next run to next UTC midnight
                if int(self.poll_interval_seconds) == 24 * 60 * 60:
                    now = datetime.utcnow()
                    # seconds until next UTC midnight
                    tomorrow = datetime(
                        year=now.year, month=now.month, day=now.day
                    ) + timedelta(days=1)
                    seconds_to_sleep = (tomorrow - now).total_seconds()
                    self.logger.info(
                        f"Sleeping until next UTC midnight ({seconds_to_sleep:.0f}s)"
                    )
                    time.sleep(max(0, seconds_to_sleep))

                    # after midnight, wait a small buffer to allow TradingView to finalize the daily bar
                    post_midnight_delay = int(
                        EXTRACT_CONFIG.get("realtime_post_midnight_delay_seconds", 60)
                    )
                    if post_midnight_delay > 0:
                        self.logger.info(
                            f"Post-midnight buffer: waiting {post_midnight_delay}s before fetching daily bar"
                        )
                        time.sleep(post_midnight_delay)
                else:
                    time.sleep(self.poll_interval_seconds)

                if not self.running:
                    break

                doc = self._fetch_latest_hour()
                if doc:
                    self._insert_doc(doc)
                else:
                    self.logger.debug("No realtime doc fetched this cycle")
            except Exception as e:
                self.logger.error(f"Error in realtime loop: {e}")

        self.logger.info("Realtime extractor loop stopped")

    def start(self):
        if self.running:
            return True
        self.running = True
        self.thread = threading.Thread(target=self._run_loop)
        self.thread.daemon = True
        self.thread.start()
        self.logger.info("Realtime extractor started")
        return True

    def stop(self):
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)
        self.logger.info("Realtime extractor stopped")


if __name__ == "__main__":
    r = ExtractBTCDominanceRealtime(poll_interval_seconds=60)
    r.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        r.stop()
