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
from src.tele_bot.tele_message import TelegramMonitor


class ExtractBTCDominanceRealtime:
    def __init__(self, poll_interval_seconds: int = 30):
        # Default: run every 30 seconds for realtime data
        self.logger = LoggerSetup.logger_setup("ExtractBTCDominanceRealtime")
        self.mongo_client = MongoDBConfig.get_client()
        self.db_name = DATA_CRAWL_CONFIG.get("db")
        
        # Tạo collection riêng cho realtime data
        self.collection_name = "realtime_btc_dominance"
        self.collection = self.mongo_client.get_database(self.db_name).get_collection(
            self.collection_name
        )

        self.symbol = DATA_CRAWL_CONFIG.get("symbol", "BTC.D")

        # Sử dụng interval 30 giây cho realtime
        self.poll_interval_seconds = poll_interval_seconds
        self.running = False
        self.thread = None
        
        # Initialize Telegram Monitor for data checking
        self.telegram_monitor = TelegramMonitor()

    def _fetch_realtime_data(self):
        # Lấy dữ liệu realtime (có thể là 1 phút hoặc 5 phút)
        try:
            from tvDatafeed import TvDatafeed, Interval

            tv = TvDatafeed()
            # Lấy dữ liệu 5 phút gần nhất cho realtime
            df = tv.get_hist(
                symbol=self.symbol,
                exchange="CRYPTOCAP",
                interval=Interval.in_5_minute,
                n_bars=1,
            )

            if df is None or len(df) == 0:
                self.logger.debug("No realtime data available")
                return None

            # Lấy data point mới nhất
            last_idx = df.index[-1]
            last_row = df.iloc[-1]

            if hasattr(last_idx, "to_pydatetime"):
                dt = last_idx.to_pydatetime()
            else:
                dt = pd.to_datetime(last_idx).to_pydatetime()

            # Tạo document với timestamp hiện tại để đánh dấu realtime
            current_time = datetime.utcnow()
            doc = {
                "datetime": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp_ms": int(current_time.timestamp() * 1000),
                "data_datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "data_timestamp_ms": int(dt.timestamp() * 1000),
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
                "volume": (
                    float(last_row.get("volume", last_row.get("Volume", None)))
                    if pd.notnull(last_row.get("volume", last_row.get("Volume", None)))
                    else None
                ),
                "type": "realtime"
            }

            return doc

        except Exception as e:
            self.logger.error(f"Error fetching realtime data via tvDatafeed: {e}")
            return None

    def _insert_doc(self, doc: dict):
        if not doc:
            return False
        try:
            # Để chỉ có 1 document realtime duy nhất, dùng type làm unique key
            self.collection.update_one(
                {"type": "realtime"},  # Chỉ có 1 document với type="realtime"
                {"$set": doc, "$unset": {"symbol": ""}},
                upsert=True,
            )
            self.logger.info(f"Updated realtime doc ts={doc['timestamp_ms']}")
            
            # Kiểm tra dữ liệu sau khi insert thành công
            self.telegram_monitor.check_data_after_realtime_extract()
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert realtime doc: {e}")
            return False

    def _run_loop(self):
        self.logger.info("Realtime extractor loop started (30s interval)")
        
        # Initial run
        try:
            doc = self._fetch_realtime_data()
            if doc:
                self._insert_doc(doc)
            else:
                self.logger.debug("No realtime doc fetched on initial run")
        except Exception as e:
            self.logger.error(f"Error on initial realtime fetch: {e}")

        while self.running:
            try:
                # Sleep for 30 seconds between each fetch
                time.sleep(self.poll_interval_seconds)

                if not self.running:
                    break

                doc = self._fetch_realtime_data()
                if doc:
                    self._insert_doc(doc)
                else:
                    self.logger.debug("No realtime doc fetched this cycle")
            except Exception as e:
                self.logger.error(f"Error in realtime loop: {e}")
                # Sleep a bit on error to avoid rapid retries
                time.sleep(5)

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
