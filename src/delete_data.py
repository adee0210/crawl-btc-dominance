#!/usr/bin/env python3
"""
Script để xóa toàn bộ dữ liệu BTC Dominance từ MongoDB.
Chạy script này để xóa tất cả documents trong collection raw_btc_dominance.
"""

import os
import sys

# Thêm đường dẫn để import các module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.configs.config_mongo import MongoDBConfig
from src.configs.config_variable import DATA_CRAWL_CONFIG
from src.log.logger_setup import LoggerSetup

def delete_all_data():
    """Xóa toàn bộ dữ liệu trong collection raw_btc_dominance"""
    logger = LoggerSetup.logger_setup("DeleteDataScript")

    try:
        # Kết nối MongoDB
        mongo_client = MongoDBConfig.get_client()
        db_name = DATA_CRAWL_CONFIG.get("db")
        collection_name = DATA_CRAWL_CONFIG.get("collection")

        # Lấy database và collection
        db = mongo_client.get_database(db_name)
        collection = db.get_collection(collection_name)

        # Đếm số documents trước khi xóa
        count_before = collection.count_documents({})
        logger.info(f"Số documents trước khi xóa: {count_before}")

        # Xóa tất cả documents
        result = collection.delete_many({})

        # Đếm số documents sau khi xóa
        count_after = collection.count_documents({})
        logger.info(f"Số documents đã xóa: {result.deleted_count}")
        logger.info(f"Số documents sau khi xóa: {count_after}")

        print(f"✅ Đã xóa thành công {result.deleted_count} documents từ collection '{collection_name}' trong database '{db_name}'")

    except Exception as e:
        logger.error(f"Lỗi khi xóa dữ liệu: {str(e)}")
        print(f"❌ Lỗi: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    print("🚀 Bắt đầu xóa toàn bộ dữ liệu BTC Dominance...")
    delete_all_data()
    print("🎉 Hoàn thành!")