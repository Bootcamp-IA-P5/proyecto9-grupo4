"""
Quick test script to verify the consolidation and deletion process
"""
import sys
import os

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.database.write_to_mongodb import connect_to_mongodb, close_connection
from src.core.logger import Logger

log = Logger()

def check_collections():
    """Check the count of records in raw and golden collections"""
    DATABASE = 'kafka_data'
    RAW_COLLECTION = 'probando_messages'
    GOLDEN_COLLECTION = 'golden_records'
    
    log.info("=" * 70)
    log.info("MongoDB Collections Status")
    log.info("=" * 70)
    
    try:
        # Connect to both collections
        raw_client, raw_collection = connect_to_mongodb(DATABASE, RAW_COLLECTION)
        golden_client, golden_collection = connect_to_mongodb(DATABASE, GOLDEN_COLLECTION)
        
        # Count documents
        raw_count = raw_collection.count_documents({})
        golden_count = golden_collection.count_documents({})
        
        log.info(f"\n📊 Collection Statistics:")
        log.info(f"  - Raw Collection ({RAW_COLLECTION}): {raw_count} records")
        log.info(f"  - Golden Collection ({GOLDEN_COLLECTION}): {golden_count} records")
        
        if raw_count > 0:
            log.info(f"\n💡 Tip: Run 'python scripts/mongo_consolidate.py' to consolidate and clean up")
        else:
            log.info(f"\n✅ Raw collection is empty - all records have been consolidated!")
        
        # Close connections
        close_connection(raw_client)
        close_connection(golden_client)
        
    except Exception as e:
        log.error(f"Error checking collections: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = check_collections()
    sys.exit(exit_code)
