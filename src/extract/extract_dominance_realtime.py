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
        
        # Sử dụng cùng collection với historical data
        self.collection_name = DATA_CRAWL_CONFIG.get("collection")  # raw_btc_dominance
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
        # Lấy dữ liệu realtime để update vào ngày hiện tại
        try:
            from tvDatafeed import TvDatafeed, Interval

            tv = TvDatafeed()
            # Lấy dữ liệu 1 phút gần nhất cho realtime update
            df = tv.get_hist(
                symbol=self.symbol,
                exchange="CRYPTOCAP",
                interval=Interval.in_1_minute,
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

            # Tạo datetime cho ngày hiện tại (bỏ giờ phút giây)
            today = datetime.utcnow().date()
            today_datetime = datetime.combine(today, datetime.min.time())
            
            realtime_data = {
                "current_open": (
                    float(last_row.get("open", last_row.get("Open", None)))
                    if pd.notnull(last_row.get("open", last_row.get("Open", None)))
                    else None
                ),
                "current_high": (
                    float(last_row.get("high", last_row.get("High", None)))
                    if pd.notnull(last_row.get("high", last_row.get("High", None)))
                    else None
                ),
                "current_low": (
                    float(last_row.get("low", last_row.get("Low", None)))
                    if pd.notnull(last_row.get("low", last_row.get("Low", None)))
                    else None
                ),
                "current_close": (
                    float(last_row.get("close", last_row.get("Close", None)))
                    if pd.notnull(last_row.get("close", last_row.get("Close", None)))
                    else None
                ),
                "current_volume": (
                    float(last_row.get("volume", last_row.get("Volume", None)))
                    if pd.notnull(last_row.get("volume", last_row.get("Volume", None)))
                    else None
                ),
                "last_update": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "today_date": today_datetime.strftime("%Y-%m-%d"),  # Chỉ ngày, không có giờ
                "today_timestamp_ms": int(today_datetime.timestamp() * 1000)
            }

            return realtime_data

        except Exception as e:
            self.logger.error(f"Error fetching realtime data via tvDatafeed: {e}")
            return None

    def _update_today_document(self, realtime_data: dict):
        """Update document của ngày hôm nay với dữ liệu realtime"""
        if not realtime_data:
            return False
            
        try:
            today_date = realtime_data["today_date"]  # Format: "2025-09-23"
            today_timestamp_ms = realtime_data["today_timestamp_ms"]
            
            # Tìm document của ngày hôm nay theo datetime field
            today_doc = self.collection.find_one({"datetime": today_date})
            
            if today_doc:
                # Document ngày hôm nay đã tồn tại, update realtime data
                update_fields = {
                    "current_open": realtime_data["current_open"],
                    "current_high": realtime_data["current_high"], 
                    "current_low": realtime_data["current_low"],
                    "current_close": realtime_data["current_close"],
                    "current_volume": realtime_data["current_volume"],
                    "last_update": realtime_data["last_update"]
                }
                
                # Update high nếu cao hơn
                if today_doc.get("high") and realtime_data["current_high"]:
                    if realtime_data["current_high"] > today_doc.get("high"):
                        update_fields["high"] = realtime_data["current_high"]
                
                # Update low nếu thấp hơn        
                if today_doc.get("low") and realtime_data["current_low"]:
                    if realtime_data["current_low"] < today_doc.get("low"):
                        update_fields["low"] = realtime_data["current_low"]
                
                # Update close với giá hiện tại
                if realtime_data["current_close"]:
                    update_fields["close"] = realtime_data["current_close"]
                
                # KHÔNG update volume field - giữ nguyên historical volume
                # Chỉ update current_volume field cho realtime tracking
                
                self.collection.update_one(
                    {"datetime": today_date},  # Tìm theo datetime thay vì timestamp_ms
                    {"$set": update_fields}
                )
                
                self.logger.info(f"Updated today's document with realtime data: C={realtime_data['current_close']:.4f}")
                return True
                
            else:
                # Chưa có document ngày hôm nay, tạo mới với dữ liệu realtime
                new_doc = {
                    "datetime": realtime_data["today_date"],  # Chỉ ngày: 2025-09-23
                    "timestamp_ms": today_timestamp_ms,
                    "open": realtime_data["current_open"],
                    "high": realtime_data["current_high"],
                    "low": realtime_data["current_low"], 
                    "close": realtime_data["current_close"],
                    "volume": realtime_data["current_volume"],
                    "current_open": realtime_data["current_open"],
                    "current_high": realtime_data["current_high"],
                    "current_low": realtime_data["current_low"],
                    "current_close": realtime_data["current_close"],
                    "current_volume": realtime_data["current_volume"],
                    "last_update": realtime_data["last_update"],
                    "type": "historical_daily"
                }
                
                self.collection.insert_one(new_doc)
                self.logger.info(f"Created today's document with realtime data: C={realtime_data['current_close']:.4f}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to update today's document: {e}")
            return False

    def _run_loop(self):
        self.logger.info("Realtime extractor loop started (30s interval)")
        
        # Initial run
        try:
            realtime_data = self._fetch_realtime_data()
            if realtime_data:
                success = self._update_today_document(realtime_data)
                if success:
                    self.telegram_monitor.check_data_after_realtime_extract()
            else:
                self.logger.debug("No realtime data fetched on initial run")
        except Exception as e:
            self.logger.error(f"Error on initial realtime fetch: {e}")

        while self.running:
            try:
                # Sleep for 30 seconds between each fetch
                time.sleep(self.poll_interval_seconds)

                if not self.running:
                    break

                realtime_data = self._fetch_realtime_data()
                if realtime_data:
                    success = self._update_today_document(realtime_data)
                    if success:
                        self.telegram_monitor.check_data_after_realtime_extract()
                else:
                    self.logger.debug("No realtime data fetched this cycle")
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
