# type: ignore
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import sys

# Load file .env (if exists)
load_dotenv()

# Get configuration from .env or use default
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

print("Connecting to MongoDB...")
print(f"URI: {MONGO_URI}")
print(f"DB: {MONGO_DB} | Collection: {MONGO_COLLECTION}")
print("-" * 60)

try:
    # Create client
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    
    # Test connection by ping
    client.admin.command("ping")
    print("Connected to MongoDB successfully!")

    # Access database and collection
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    # Insert 1 test document
    test_doc = {
        "test": "simple_connection",
        "message": "Hello from test_mongo_simple.py!",
        "status": "success"
    }
    result = collection.insert_one(test_doc)
    print(f"Inserted document with ID: {result.inserted_id}")

    # Read the document just inserted
    found = collection.find_one({"test": "simple_connection"})
    if found:
        print("Found document:")
        print(f"   → {found}")
    else:
        print("Document not found!")

    # Delete test document (cleanup)
    collection.delete_one({"test": "simple_connection"})
    print("Cleaned up test document.")

except Exception as e:
    print("ERROR CONNECTING TO MONGODB:")
    print(e)
    sys.exit(1)

finally:
    client.close()
    print("Closed connection.")