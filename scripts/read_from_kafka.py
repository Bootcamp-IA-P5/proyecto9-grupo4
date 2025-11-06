"""
Read from Kafka and delegate writes to MongoDB writer functions.
This script only reads from Kafka.
"""
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import sys

# Load environment variables from .env file
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092']
KAFKA_TOPIC = 'probando'
KAFKA_API_VERSION = (2, 5, 0)
KAFKA_GROUP_ID = f'mongodb-consumer-{datetime.now().strftime("%Y%m%d%H%M%S")}'  # Unique group ID

# MongoDB configuration (database/collection names)
MONGO_DATABASE = 'kafka_data'
MONGO_COLLECTION = 'probando_messages'

# Ensure project root is on sys.path so we can import src.database.write_to_mongodb
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Import functional writer API
from src.database.write_to_mongodb import connect_to_mongodb, insert_document, close_connection

MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds


def connect_to_kafka():
    """Connect to Kafka with retry logic"""
    print("Connecting to Kafka...")
    for attempt in range(MAX_RETRIES):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                api_version=KAFKA_API_VERSION,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            print("✓ Connected to Kafka successfully!")
            return consumer
        except NoBrokersAvailable:
            if attempt < MAX_RETRIES - 1:
                print(f"⚠ Kafka not ready yet, retrying in {RETRY_DELAY}s... (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
            else:
                print("✗ Failed to connect to Kafka after multiple attempts")
                raise


def process_messages(consumer, collection):
    """Read messages from Kafka and store via the writer functions"""
    print("Starting to consume messages... (Press Ctrl+C to stop)\n")
    message_count = 0
    try:
        for message in consumer:
            if message_count == 0:
                print("DEBUG: First message received!")

            # Add metadata to the message
            document = {
                'kafka_metadata': {
                    'topic': message.topic,
                    'partition': message.partition,
                    'offset': message.offset,
                    'timestamp': message.timestamp,
                    'key': message.key
                },
                'data': message.value,
                'inserted_at': datetime.utcnow()
            }

            # Insert into MongoDB via functional writer
            result = insert_document(collection, document)
            message_count += 1

            print(f"[{message_count}] Inserted document ID: {result.inserted_id}")
            print(f"    Data: {message.value}")
            print("-" * 60)
    except StopIteration:
        print("DEBUG: StopIteration - no more messages")
    except KeyboardInterrupt:
        print(f"\n\n✓ Stopping consumer...")
        print(f"✓ Total messages processed: {message_count}")
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print(f"✓ Total messages processed: {message_count}")
        consumer.close()
        print("✓ Kafka consumer closed")


def main():
    print("=" * 60)
    print("Read from Kafka and write to MongoDB")
    print("=" * 60)

    # Connect to Kafka
    consumer = connect_to_kafka()

    # Connect to MongoDB (functional API)
    client, collection = connect_to_mongodb(database=MONGO_DATABASE, collection_name=MONGO_COLLECTION)

    try:
        process_messages(consumer, collection)
    finally:
        close_connection(client)
        print("\nGoodbye! 👋")


if __name__ == "__main__":
    main()
