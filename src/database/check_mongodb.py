"""
Check MongoDB Atlas for stored Kafka messages
"""
from pymongo import MongoClient
from datetime import datetime
import os
import sys
from dotenv import load_dotenv

# Ensure project root is on sys.path to allow importing `src` modules
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.core.logger import Logger

# Load environment variables
load_dotenv()

log = Logger()

# MongoDB Atlas configuration
MONGO_URI = os.getenv('MONGO_ATLAS_URI')
if not MONGO_URI:
    message = "MONGO_ATLAS_URI not found in environment or .env file. Please set your MongoDB Atlas connection string."
    log.critical(message)
    raise ValueError(message)

print("Connecting to MongoDB Atlas...\n")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
try:
    db = client['kafka_data']
    collection = db['probando_messages']

    # Get statistics
    total_docs = collection.count_documents({})
    print("=" * 60)
    print("MongoDB Statistics")
    print("=" * 60)
    print(f"Database: kafka_data")
    print(f"Collection: probando_messages")
    print(f"Total documents: {total_docs:,}")
    print()

    if total_docs > 0:
        # Get first and last document
        first_doc = collection.find_one(sort=[('inserted_at', 1)])
        last_doc = collection.find_one(sort=[('inserted_at', -1)])
        
        print(f"First document inserted: {first_doc['inserted_at']}")
        print(f"Last document inserted: {last_doc['inserted_at']}")
        print()
        
        # Show 3 sample documents
        print("Sample Documents (latest 3):")
        print("-" * 60)
        for i, doc in enumerate(collection.find().sort('inserted_at', -1).limit(3), 1):
            print(f"\n[{i}] Document ID: {doc['_id']}")
            print(f"    Kafka Offset: {doc['kafka_metadata']['offset']}")
            print(f"    Data: {doc['data']}")
            print(f"    Inserted: {doc['inserted_at']}")
    else:
        print("No documents found yet. Make sure the consumer is running!")
finally:
    log.debug("Closing MongoDB connection.")
    client.close()
