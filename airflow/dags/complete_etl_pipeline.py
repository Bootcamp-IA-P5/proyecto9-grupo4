"""
Complete ETL Pipeline DAG

This DAG orchestrates the complete data pipeline:
1. Check if new raw Kafka messages exist in MongoDB
2. Run consolidation to create/update golden records
3. Load golden records into Supabase (PostgreSQL)

Schedule: Runs every hour to process new data
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Ensure project root is on sys.path
PROJECT_ROOT = os.getenv('PROJECT_ROOT', '/opt/airflow')
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# DAG configuration
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='complete_etl_pipeline',
    default_args=default_args,
    description='Complete ETL: Kafka → MongoDB (raw) → MongoDB (golden) → Supabase',
    schedule='0 * * * *',  # Every hour at minute 0
    start_date=datetime(2025, 11, 16),
    catchup=False,
    tags=['etl', 'kafka', 'mongodb', 'supabase', 'production'],
)


# ============================================================================
# TASK 1: Check for New Raw Data
# ============================================================================
def check_for_new_data(**context):
    """
    Check if there are new raw Kafka messages in MongoDB that need processing.
    Compares counts between raw and golden collections.
    """
    from pymongo import MongoClient
    
    logging.info("🔍 Checking for new raw data...")
    
    mongo_uri = os.getenv('MONGO_ATLAS_URI')
    client = MongoClient(mongo_uri)
    
    try:
        db = client['kafka_data']
        raw_collection = db['probando_messages']
        golden_collection = db['golden_records']
        
        raw_count = raw_collection.count_documents({})
        golden_count = golden_collection.count_documents({})
        
        logging.info(f"📊 Raw messages: {raw_count}")
        logging.info(f"📊 Golden records: {golden_count}")
        
        # Check if we have new data to process
        if raw_count == 0:
            logging.warning("⚠ No raw messages found. Kafka consumer may not be running.")
            return {
                'has_new_data': False,
                'raw_count': 0,
                'golden_count': 0,
                'message': 'No raw data'
            }
        
        # Always process if there's raw data (consolidation is idempotent)
        result = {
            'has_new_data': True,
            'raw_count': raw_count,
            'golden_count': golden_count,
            'estimated_new': raw_count  # We'll let consolidation handle deduplication
        }
        
        logging.info(f"✓ Found {raw_count} raw messages to process")
        
        # Push to XCom for next tasks
        context['task_instance'].xcom_push(key='data_check', value=result)
        
        return result
        
    finally:
        client.close()


# ============================================================================
# TASK 2: Consolidate Records (Raw → Golden)
# ============================================================================
def run_consolidation(**context):
    """
    Run the consolidation script to create golden records.
    """
    # Import here to avoid issues with Airflow imports
    sys.path.insert(0, PROJECT_ROOT)
    from scripts.consolidate_mongodb_records import main as consolidate_main
    
    logging.info("🔄 Running record consolidation...")
    
    try:
        # Run consolidation (returns 0 on success, 1 on error)
        exit_code = consolidate_main()
        
        if exit_code == 0:
            logging.info("✓ Consolidation completed successfully")
            return {'status': 'success'}
        else:
            logging.error("❌ Consolidation failed")
            raise Exception("Consolidation script returned non-zero exit code")
            
    except Exception as e:
        logging.error(f"❌ Error during consolidation: {e}")
        raise


# ============================================================================
# TASK 3: Load to Supabase (Golden → PostgreSQL)
# ============================================================================
def load_to_supabase(**context):
    """
    Load golden records from MongoDB to Supabase.
    """
    sys.path.insert(0, PROJECT_ROOT)
    from scripts.mongodb_to_supabase import main as etl_main
    
    logging.info("🔄 Loading golden records to Supabase...")
    
    try:
        # Run ETL (returns 0 on success, 1 on error)
        exit_code = etl_main()
        
        if exit_code == 0:
            logging.info("✓ Data loaded to Supabase successfully")
            return {'status': 'success'}
        else:
            logging.error("❌ Supabase load failed")
            raise Exception("ETL script returned non-zero exit code")
            
    except Exception as e:
        logging.error(f"❌ Error loading to Supabase: {e}")
        raise


# ============================================================================
# TASK 4: Validate Data in Supabase
# ============================================================================
def validate_supabase_data(**context):
    """
    Validate that data was successfully loaded to Supabase.
    """
    from sqlalchemy import create_engine, text
    from dotenv import load_dotenv
    
    load_dotenv()
    
    logging.info("✅ Validating Supabase data...")
    
    # Build connection URL
    user = os.getenv("SQL_USER")
    password = os.getenv("SQL_PASSWORD")
    host = os.getenv("SQL_HOST")
    port = os.getenv("SQL_PORT")
    dbname = os.getenv("SQL_DBNAME")
    
    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?sslmode=require"
    
    try:
        engine = create_engine(db_url)
        
        with engine.connect() as conn:
            # Count records in each table
            person_count = conn.execute(text("SELECT COUNT(*) FROM person")).scalar()
            bank_count = conn.execute(text("SELECT COUNT(*) FROM bank")).scalar()
            work_count = conn.execute(text("SELECT COUNT(*) FROM work")).scalar()
            address_count = conn.execute(text("SELECT COUNT(*) FROM address")).scalar()
            
            logging.info(f"📊 Supabase record counts:")
            logging.info(f"  - Persons: {person_count}")
            logging.info(f"  - Banks: {bank_count}")
            logging.info(f"  - Work: {work_count}")
            logging.info(f"  - Addresses: {address_count}")
            
            result = {
                'person_count': person_count,
                'bank_count': bank_count,
                'work_count': work_count,
                'address_count': address_count,
            }
            
            # Push to XCom
            context['task_instance'].xcom_push(key='validation_results', value=result)
            
            return result
            
    except Exception as e:
        logging.error(f"❌ Validation failed: {e}")
        raise


# ============================================================================
# Define Task Dependencies
# ============================================================================

task_check_data = PythonOperator(
    task_id='check_for_new_data',
    python_callable=check_for_new_data,
    dag=dag,
)

task_consolidate = PythonOperator(
    task_id='consolidate_records',
    python_callable=run_consolidation,
    dag=dag,
)

task_load_supabase = PythonOperator(
    task_id='load_to_supabase',
    python_callable=load_to_supabase,
    dag=dag,
)

task_validate = PythonOperator(
    task_id='validate_supabase_data',
    python_callable=validate_supabase_data,
    dag=dag,
)

# Set up the pipeline flow
task_check_data >> task_consolidate >> task_load_supabase >> task_validate
