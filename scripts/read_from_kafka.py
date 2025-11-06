"""
Read from Kafka and delegate writes to MongoDB writer functions.
This script only reads from Kafka.
"""
from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import sys

# Load environment variables from .env file
load_dotenv()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'  # Confluent Consumer uses string, not list
KAFKA_TOPIC = 'probando'
# Stable group ID - allows consumer to remember its position and resume from last committed offset
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'mongodb-consumer-stable')

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
    """Connect to Kafka with retry logic using Confluent Consumer"""
    print("Connecting to Kafka...")
    
    # Confluent Kafka consumer configuration
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'earliest',         # Read from beginning on first run
        'enable.auto.commit': True,              # Auto-commit offsets
        'auto.commit.interval.ms': 5000,         # Commit every 5 seconds
        'max.poll.interval.ms': 300000,          # Max time between polls
        'session.timeout.ms': 30000,             # Session timeout
        'fetch.min.bytes': 1,                    # Don't wait to accumulate data
        'fetch.wait.max.ms': 500                 # Reduce fetch wait time
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            consumer = Consumer(conf)
            consumer.subscribe([KAFKA_TOPIC])
            print("✓ Connected to Kafka successfully!")
            return consumer
        except KafkaException as e:
            if attempt < MAX_RETRIES - 1:
                print(f"⚠ Kafka not ready yet, retrying in {RETRY_DELAY}s... (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
            else:
                print(f"✗ Failed to connect to Kafka after multiple attempts: {e}")
                raise


def process_messages(consumer, collection):
    """Read messages from Kafka and store via the writer functions"""
    print("Starting to consume messages... (Press Ctrl+C to stop)\n")
    message_count = 0
    batch_start_time = None
    batch_size = 100  # Measure every 100 messages
    
    try:
        while True:
            # Poll for messages (timeout in seconds)
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue  # No message available, keep polling
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition - not an error
                    continue
                else:
                    # Real error
                    print(f"Consumer error: {msg.error()}")
                    break
            
            # Successfully received a message
            if message_count == 0:
                print("DEBUG: First message received!")
                batch_start_time = time.time()  # Start first batch timer
            
            # Deserialize JSON value
            try:
                document = json.loads(msg.value().decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"⚠ Failed to decode message at offset {msg.offset()}: {e}")
                continue

            # Insert into MongoDB via functional writer
            insert_document(collection, document)
            message_count += 1
            
            # Report batch statistics every N messages
            if message_count % batch_size == 0:
                batch_time = time.time() - batch_start_time
                batch_rate = batch_size / batch_time
                print(f"[{message_count}] Batch of {batch_size}: {batch_time:.2f}s, {batch_rate:.2f} msg/s")
                batch_start_time = time.time()  # Reset batch timer
                
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
