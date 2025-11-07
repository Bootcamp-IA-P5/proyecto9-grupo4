"""
MongoDB writer helper functions.

Provides functional helpers to manage MongoDB connections, retries, document inserts, and cleanup.
Expects MONGO_ATLAS_URI to be provided via environment or a .env file.
"""
import os
import time
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from src.core.logger import Logger

load_dotenv()

log = Logger()


def connect_to_mongodb(database: str = 'kafka_data', collection_name: str = 'probando_messages', uri: str | None = None, max_retries: int = 5, retry_delay: int = 5):
    """Connect to MongoDB and return (client, collection).

    Usage:
        client, collection = connect_to_mongodb(database='kafka_data', collection_name='probando_messages')
    """
    uri = uri or os.getenv('MONGO_ATLAS_URI')
    if not uri:
        message = "MONGO_ATLAS_URI not found in environment. Please set it in a .env file or environment variables."
        log.critical(message)
        raise ValueError(message)

    log.info("Connecting to MongoDB...")
    for attempt in range(max_retries):
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            db = client[database]
            collection = db[collection_name]
            log.info("✓ Connected to MongoDB successfully!")
            log.debug(f"  Database: {database}")
            log.debug(f"  Collection: {collection_name}")
            return client, collection
        except ConnectionFailure as e:
            if attempt < max_retries - 1:
                log.warning(f"MongoDB not ready yet, retrying in {retry_delay}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_delay)
            else:
                message = f"Failed to connect to MongoDB after multiple attempts: {e}"
                log.error(message)
                raise ConnectionFailure(message)


def insert_document(collection, document: dict):
    """Insert a single document into the provided collection."""
    if collection is None:
        raise RuntimeError("No collection provided to insert_document")
    # log.debug(f"Inserting document with keys: {list(document.keys())}")
    collection.insert_one(document)


def close_connection(client):
    """Close the provided MongoClient connection."""
    if client:
        client.close()
        log.info("✓ MongoDB connection closed")
