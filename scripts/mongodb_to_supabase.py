"""
MongoDB to Supabase ETL Pipeline

This script orchestrates the complete data pipeline:
1. Reads consolidated golden records from MongoDB
2. Transforms and loads them into Supabase (PostgreSQL)

This should be run AFTER consolidate_mongodb_records.py has created the golden_records collection.
"""

import os
import sys
from dotenv import load_dotenv

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.database.write_to_mongodb import connect_to_mongodb, close_connection
from src.database.sql_alchemy import connect as connect_postgres, read_from_mongo_collection
from src.core.logger import Logger

load_dotenv()

log = Logger()

# Configuration
MONGO_DATABASE = 'kafka_data'
MONGO_GOLDEN_COLLECTION = 'golden_records'


def main():
    """Main ETL pipeline from MongoDB to Supabase"""
    log.info("=" * 70)
    log.info("🚀 MongoDB to Supabase ETL Pipeline")
    log.info("=" * 70)
    
    mongo_client = None
    postgres_session = None
    
    try:
        # Step 1: Connect to MongoDB golden records collection
        log.info("\n📡 Step 1: Connecting to MongoDB golden records...")
        mongo_client, golden_collection = connect_to_mongodb(
            database=MONGO_DATABASE,
            collection_name=MONGO_GOLDEN_COLLECTION
        )
        
        # Check if we have any golden records
        record_count = golden_collection.count_documents({})
        if record_count == 0:
            log.warning("⚠ No golden records found in MongoDB!")
            log.warning("Please run consolidate_mongodb_records.py first to create golden records.")
            return 1
        
        log.info(f"✓ Found {record_count} golden records to process")
        
        # Step 2: Connect to PostgreSQL/Supabase
        log.info("\n📡 Step 2: Connecting to Supabase (PostgreSQL)...")
        postgres_session = connect_postgres()
        
        if not postgres_session:
            log.error("❌ Failed to connect to Supabase")
            return 1
        
        log.info("✓ Connected to Supabase successfully")
        
        # Step 3: Run the ETL process
        log.info("\n🔄 Step 3: Running ETL process (MongoDB → Supabase)...")
        log.info("This may take a while for large datasets...")
        read_from_mongo_collection(postgres_session, golden_collection)
        
        log.info("\n✅ ETL Pipeline completed successfully!")
        return 0
        
    except Exception as e:
        log.error(f"\n❌ Error during ETL pipeline: {e}")
        import traceback
        log.error(traceback.format_exc())
        return 1
        
    finally:
        # Clean up connections
        if mongo_client:
            close_connection(mongo_client)
        
        if postgres_session:
            try:
                postgres_session.close()
                log.info("✓ PostgreSQL session closed")
            except Exception as e:
                log.warning(f"Error closing PostgreSQL session: {e}")


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
