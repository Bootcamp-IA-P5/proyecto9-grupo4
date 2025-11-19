import os
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Ensure project root is on sys.path
PROJECT_ROOT = os.getenv('PROJECT_ROOT', '/opt/airflow')
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

consumer_script = os.path.join(PROJECT_ROOT, 'scripts', 'read_from_kafka.py')

with DAG(
    dag_id="kafka_consumer_service_dag",
    schedule=None,
    start_date=datetime(2025, 11, 16),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["service", "bash"],
) as dag:
    launch_indefinite_python_script = BashOperator(
        task_id="launch_indefinite_service",
        bash_command=f"nohup python {consumer_script} &",
        # Set a short timeout for the launch process itself
        # execution_timeout=timedelta(minutes=5),
        # Note: Airflow will NOT track this process after it succeeds.
    )