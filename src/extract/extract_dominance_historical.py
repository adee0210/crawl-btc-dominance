import os
import sys
import time
import threading
from datetime import datetime, timedelta
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.configs.config_mongo import MongoDBConfig
from src.configs.config_variable import DATA_CRAWL_CONFIG, EXTRACT_CONFIG
from src.log.logger_setup import LoggerSetup


class ExtractBTCDominanceHistorical:
    def __init__(self, csv_path: str = None, poll_interval_seconds: int = 24 * 60 * 60):
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
        
        # Thêm logic chạy định kỳ như realtime extractor cũ
        self.poll_interval_seconds = poll_interval_seconds
        self.running = False
        self.thread = None

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
                    "datetime": dt.strftime("%Y-%m-%d"),  # Chỉ ngày, không có giờ
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

                # Check if document already exists to avoid overwriting realtime fields
                existing_doc = self.collection.find_one({"timestamp_ms": doc["timestamp_ms"]})
                
                if existing_doc:
                    # Document exists, only update historical fields
                    historical_fields = {
                        "datetime": doc["datetime"],
                        "timestamp_ms": doc["timestamp_ms"],
                        "open": doc["open"],
                        "high": doc["high"],
                        "low": doc["low"],
                        "close": doc["close"],
                        "volume": doc["volume"],
                        "type": doc["type"]
                    }
                    self.collection.update_one(
                        {"timestamp_ms": doc["timestamp_ms"]},
                        {"$set": historical_fields, "$unset": {"symbol": ""}},
                    )
                else:
                    # New document, upsert completely
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

    def _fetch_daily_data(self):
        """Lấy data daily mới nhất (logic cũ của realtime extractor)"""
        try:
            from tvDatafeed import TvDatafeed, Interval

            tv = TvDatafeed()
            # Lấy 2 ngày gần nhất để đảm bảo có data mới
            df = tv.get_hist(
                symbol=self.symbol,
                exchange="CRYPTOCAP",
                interval=Interval.in_daily,
                n_bars=2,
            )

            if df is None or len(df) == 0:
                return None

            # Lấy ngày mới nhất
            last_idx = df.index[-1]
            last_row = df.iloc[-1]

            if hasattr(last_idx, "to_pydatetime"):
                dt = last_idx.to_pydatetime()
            else:
                dt = pd.to_datetime(last_idx).to_pydatetime()

            doc = {
                "datetime": dt.strftime("%Y-%m-%d"),  # Chỉ ngày, không có giờ
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
                "volume": (
                    float(last_row.get("volume", last_row.get("Volume", None)))
                    if pd.notnull(last_row.get("volume", last_row.get("Volume", None)))
                    else None
                ),
                "type": "historical_daily"
            }

            return doc

        except Exception as e:
            self.logger.error(f"Error fetching daily data via tvDatafeed: {e}")
            return None

    def _insert_daily_doc(self, doc: dict):
        """Insert daily document"""
        if not doc:
            return False
        try:
            # Kiểm tra xem document đã tồn tại chưa
            existing_doc = self.collection.find_one({"timestamp_ms": doc["timestamp_ms"]})
            
            if existing_doc:
                # Document đã tồn tại, chỉ update các field historical, giữ nguyên realtime fields
                historical_fields = {
                    "datetime": doc["datetime"],
                    "timestamp_ms": doc["timestamp_ms"],
                    "open": doc["open"],
                    "high": doc["high"],
                    "low": doc["low"],
                    "close": doc["close"],
                    "volume": doc["volume"],
                    "type": doc["type"]
                }
                self.collection.update_one(
                    {"timestamp_ms": doc["timestamp_ms"]},
                    {"$set": historical_fields, "$unset": {"symbol": ""}},
                )
                self.logger.info(f"Updated historical fields for existing doc ts={doc['timestamp_ms']}")
            else:
                # Document mới, insert toàn bộ
                self.collection.update_one(
                    {"timestamp_ms": doc["timestamp_ms"]},
                    {"$set": doc, "$unset": {"symbol": ""}},
                    upsert=True,
                )
                self.logger.info(f"Upserted new daily doc ts={doc['timestamp_ms']}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to insert daily doc: {e}")
            return False

    def _run_daily_loop(self):
        """Logic chạy định kỳ 24h như realtime extractor cũ"""
        self.logger.info("Historical daily extractor loop started")
        
        # Chạy ngay lần đầu
        try:
            doc = self._fetch_daily_data()
            if doc:
                self._insert_daily_doc(doc)
            else:
                self.logger.debug("No daily doc fetched on initial run")
        except Exception as e:
            self.logger.error(f"Error on initial daily fetch: {e}")

        while self.running:
            try:
                # Chạy mỗi ngày vào 7h sáng UTC
                now = datetime.utcnow()
                
                # Tính thời gian đến 7h sáng ngày mai
                tomorrow_7am = datetime(
                    year=now.year, 
                    month=now.month, 
                    day=now.day,
                    hour=7,
                    minute=0,
                    second=0
                ) + timedelta(days=1)
                
                # Nếu hiện tại chưa qua 7h sáng hôm nay thì chạy hôm nay
                today_7am = datetime(
                    year=now.year, 
                    month=now.month, 
                    day=now.day,
                    hour=7,
                    minute=0,
                    second=0
                )
                
                if now < today_7am:
                    next_run = today_7am
                else:
                    next_run = tomorrow_7am
                
                seconds_to_sleep = (next_run - now).total_seconds()
                self.logger.info(f"Sleeping until next 7AM UTC ({seconds_to_sleep:.0f}s)")
                time.sleep(max(0, seconds_to_sleep))

                if not self.running:
                    break

                doc = self._fetch_daily_data()
                if doc:
                    self._insert_daily_doc(doc)
                else:
                    self.logger.debug("No daily doc fetched this cycle")
                    
            except Exception as e:
                self.logger.error(f"Error in daily loop: {e}")

        self.logger.info("Historical daily extractor loop stopped")

    def start_daily_monitoring(self):
        """Bắt đầu chạy daily monitoring"""
        if self.running:
            return True
        self.running = True
        self.thread = threading.Thread(target=self._run_daily_loop)
        self.thread.daemon = True
        self.thread.start()
        self.logger.info("Historical daily extractor started")
        return True

    def stop_daily_monitoring(self):
        """Dừng daily monitoring"""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)
        self.logger.info("Historical daily extractor stopped")


if __name__ == "__main__":
    h = ExtractBTCDominanceHistorical()
    print("Starting full historical fetch...")
    h.get_all_historical_data()
