#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Incremental sync MongoDB → DuckDB.
Only append new documents based on _id (ObjectId auto-increasing).
"""

import os
import logging
import pandas as pd  # type: ignore
import duckdb
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
from datetime import datetime

# ====== CONFIGURATION ======
load_dotenv()

MONGO_URI = "mongodb://admin:admin@mongodb:27017/?replicaSet=rs0&authSource=admin"
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

DUCKDB_PATH = "/data/mongo_to_duckdb.duckdb"

DUCKDB_TABLE = "patient_stream"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
# ====================================

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def flatten_dict(d, parent_key='', sep='.'):
    """Flatten nested dictionaries (1 level deep)."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def convert_types(doc):
    """Convert MongoDB-specific types for DuckDB compatibility."""
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            doc[k] = str(v)
        elif isinstance(v, datetime):
            doc[k] = v.isoformat()
        elif isinstance(v, list):
            doc[k] = str(v)
    return doc

def get_last_id(con):
    """Lấy ObjectId lớn nhất đã lưu trong DuckDB."""
    try:
        query = f"SELECT MAX(_id) FROM {DUCKDB_TABLE}"
        result = con.execute(query).fetchone()
        return result[0] if result and result[0] else None
    except Exception:
        return None

def fetch_from_mongo(last_id=None):
    """Truy xuất dữ liệu mới từ MongoDB."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COLLECTION]

    query = {}
    if last_id:
        try:
            query = {"_id": {"$gt": ObjectId(last_id)}}
        except Exception:
            logging.warning(f"Invalid last_id format: {last_id}")
            query = {}

    docs = list(col.find(query))
    logging.info(f"Fetched {len(docs)} new documents from MongoDB '{MONGO_COLLECTION}'")
    if not docs:
        return pd.DataFrame()

    docs = [convert_types(flatten_dict(doc)) for doc in docs]
    df = pd.DataFrame(docs)
    logging.info(f"DataFrame shape: {df.shape}")
    return df

def sync_to_duckdb(df):
    """Append dữ liệu mới vào DuckDB."""
    if df.empty:
        logging.info("No new data to append.")
        return

    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)

    # Tạo bảng nếu chưa có
    con.execute(f"CREATE TABLE IF NOT EXISTS {DUCKDB_TABLE} AS SELECT * FROM df LIMIT 0")

    # Đăng ký DataFrame và append dữ liệu mới
    con.register("df", df)
    con.execute(f"INSERT INTO {DUCKDB_TABLE} SELECT * FROM df")
    con.close()
    logging.info(f"Appended {len(df)} rows → {DUCKDB_PATH} [{DUCKDB_TABLE}]")

def main():
    try:
        os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
        con = duckdb.connect(DUCKDB_PATH)
        last_id = get_last_id(con)
        con.close()

        if last_id:
            logging.info(f"Last synced _id: {last_id}")
        else:
            logging.info("No existing data found — doing full sync.")

        df = fetch_from_mongo(last_id)
        sync_to_duckdb(df)

    except Exception as e:
        logging.error(f"Error during incremental sync: {e}", exc_info=True)

if __name__ == "__main__":
    main()
