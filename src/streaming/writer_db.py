from pymongo import MongoClient
import logging
from dotenv import load_dotenv
import os
import time

load_dotenv()

class WriterDB:
    _client = None
    _collection = None
    
    def __init__(self):
        load_dotenv()

        self.MONGO_URI = os.getenv("MONGO_URI")
        self.MONGO_DB = os.getenv("MONGO_DB")
        self.MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | writer_db | %(message)s")
        
        # Initialize connection once
        self._ensure_connection()

    def _ensure_connection(self):
        """Create or reuse a MongoDB connection"""
        if WriterDB._client is None:
            WriterDB._client = MongoClient(
                self.MONGO_URI,
                serverSelectionTimeoutMS=2000,  # Decrease timeout
                connectTimeoutMS=2000,
                socketTimeoutMS=5000,
                maxPoolSize=10,
                minPoolSize=1
            )
            db = WriterDB._client[self.MONGO_DB]
            WriterDB._collection = db[self.MONGO_COLLECTION]
            logging.info("MongoDB connection established (reused across batches)")

    def write_to_mongodb(self, df, epoch_id):
        start_time = time.time()
        
        if df.isEmpty():
            return
        
        try:
            self._ensure_connection()
            
            # Optimize: convert DataFrame to list faster
            # Use toPandas() if pandas is available, otherwise use collect() but optimized
            try:
                # Try to use toPandas() - faster for large batches
                pandas_df = df.toPandas()
                records = pandas_df.to_dict('records')
            except Exception:
                # Fallback to collect() if toPandas() is not available
                records = [row.asDict() for row in df.collect()]
            
            collect_time = time.time() - start_time
            insert_start = time.time()
            
            if records:
                # Insert with ordered=False to speed up (do not wait for each document error)
                WriterDB._collection.insert_many(
                    records,
                    ordered=False,  # Speed up insert
                    bypass_document_validation=True  # Skip validation if any
                )
                insert_time = time.time() - insert_start
                total_time = time.time() - start_time
                
                logging.info(
                    f"Inserted {len(records)} records (epoch {epoch_id}) | "
                    f"Total: {total_time:.3f}s (collect: {collect_time:.3f}s, insert: {insert_time:.3f}s)"
                )
            else:
                logging.warning(f"No records to insert (epoch {epoch_id})")

        except Exception as e:
            logging.error(f"Error writing to MongoDB: {e}", exc_info=True)
            # Reset connection if there is an error
            if WriterDB._client:
                try:
                    WriterDB._client.close()
                except:
                    pass
            WriterDB._client = None
            WriterDB._collection = None
    
    def __del__(self):
        """Cleanup connection when object is destroyed"""
        if WriterDB._client:
            try:
                WriterDB._client.close()
                logging.info("MongoDB connection closed")
            except:
                pass