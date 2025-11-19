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
# TASK 1: Start Kafka Consumer
# ============================================================================
def start_kafka_consumer(**context):
    """
    Start the Kafka consumer for a limited time to consume messages.
    Runs for 30 seconds to allow messages to be consumed into MongoDB.
    """
    import subprocess
    import time
    
    logging.info("🚀 Starting Kafka consumer...")
    
    consumer_script = os.path.join(PROJECT_ROOT, 'scripts', 'read_from_kafka.py')
    
    try:
        # Start consumer as subprocess with timeout
        process = subprocess.Popen(
            ['python', consumer_script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        logging.info("⏱ Running consumer for 30 seconds...")
        
        # Let it run for 30 seconds
        time.sleep(30)
        
        # Terminate the consumer
        process.terminate()
        
        try:
            # Wait for graceful shutdown (max 5 seconds)
            stdout, stderr = process.communicate(timeout=5)
            logging.info(f"Consumer output: {stdout}")
            if stderr:
                logging.warning(f"Consumer stderr: {stderr}")
        except subprocess.TimeoutExpired:
            # Force kill if it doesn't terminate gracefully
            process.kill()
            stdout, stderr = process.communicate()
            
        logging.info("✓ Kafka consumer stopped")
        return {'status': 'success', 'duration_seconds': 30}
        
    except Exception as e:
        logging.error(f"❌ Error running Kafka consumer: {e}")
        raise


# ============================================================================
# TASK 2: Check for New Raw Data
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
        golden_collection = db['golden']
        
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
# TASK 3: Consolidate Records (Raw → Golden)
# ============================================================================
def run_consolidation(**context):
    """
    Run the consolidation script to create golden records.
    """
    # Import here to avoid issues with Airflow imports
    sys.path.insert(0, PROJECT_ROOT)
    from scripts.mongo_consolidate import main as mongo_consolidate
    
    logging.info("🔄 Running record consolidation...")
    
    try:
        mongo_consolidate()
        logging.info("✓ Consolidation completed successfully")
        return {'status': 'success'}
            
    except Exception as e:
        logging.error(f"❌ Error during consolidation: {e}")
        raise


# ============================================================================
# TASK 4: Load to Supabase (Golden → PostgreSQL)
# ============================================================================
def load_to_supabase(**context):
    """
    Load golden records from MongoDB to Supabase.
    """
    sys.path.insert(0, PROJECT_ROOT)
    from scripts.sql_load_db import main as load_main
    
    logging.info("🔄 Loading golden records to Supabase...")
    
    try:
        # Run ETL (script raises on failure)
        load_main(data_file=None, batch=False)
        logging.info("✓ Data loaded to Supabase successfully")
        return {'status': 'success'}
            
    except Exception as e:
        logging.error(f"❌ Error loading to Supabase: {e}")
        raise


# ============================================================================
# Define Task Dependencies
# ============================================================================

task_start_consumer = PythonOperator(
    task_id='start_kafka_consumer',
    python_callable=start_kafka_consumer,
    dag=dag,
)

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

# Set up the pipeline flow
task_start_consumer >> task_check_data >> task_consolidate >> task_load_supabase
