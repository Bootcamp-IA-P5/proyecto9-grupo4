# Apache Airflow Pipeline Setup Guide

This guide explains how to set up and launch the complete ETL pipeline using Apache Airflow.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Architecture Overview](#architecture-overview)
- [Initial Setup](#initial-setup)
- [Starting Airflow](#starting-airflow)
- [Connecting to Kafka Network](#connecting-to-kafka-network)
- [Accessing the Airflow UI](#accessing-the-airflow-ui)
- [Triggering the Pipeline](#triggering-the-pipeline)
- [Monitoring Pipeline Execution](#monitoring-pipeline-execution)
- [Stopping Airflow](#stopping-airflow)
- [Troubleshooting](#troubleshooting)

## Prerequisites

Before starting, ensure you have:

1. **Docker and Docker Compose** installed
2. **Kafka containers running** (zookeeper and kafka)
3. **MongoDB Atlas credentials** configured in `.env`
4. **Supabase/PostgreSQL credentials** configured in `.env`

## Architecture Overview

The pipeline consists of 4 tasks:

```
┌─────────────────────────────────────────────────────────────┐
│                    Airflow DAG Pipeline                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. start_kafka_consumer (30s)                             │
│     └─> Consumes messages from Kafka → MongoDB             │
│                          ↓                                  │
│  2. check_for_new_data                                     │
│     └─> Counts raw messages in MongoDB                     │
│                          ↓                                  │
│  3. consolidate_records                                    │
│     └─> Deduplicates & consolidates → Golden records       │
│                          ↓                                  │
│  4. load_to_supabase                                       │
│     └─> Loads golden records → PostgreSQL/Supabase         │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Initial Setup

### 1. Environment Configuration

Ensure your `.env` file contains all required credentials:

```env
# MongoDB Atlas
MONGO_ATLAS_URI=mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority

# Supabase/PostgreSQL
SQL_USER=your_user
SQL_PASSWORD=your_password
SQL_HOST=your_host
SQL_PORT=5432
SQL_DBNAME=postgres

# Airflow
AIRFLOW_FERNET_KEY=your_fernet_key
AIRFLOW_POSTGRES_USER=airflow
AIRFLOW_POSTGRES_PASSWORD=airflow
AIRFLOW_POSTGRES_DB=airflow

# Airflow Admin Credentials (persisted in Docker volume)
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=<your_secure_password>  # Change this to a strong password!
AIRFLOW_POSTGRES_DB=airflow
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=your_secure_password
```

### 2. Docker Compose Configuration

The `docker-compose-airflow.yml` file defines two services:

- **postgres**: Airflow metadata database
- **airflow-webserver**: Runs in standalone mode (includes webserver, scheduler, and triggerer)

## Starting Airflow

### Using Docker Compose

```bash
# Start Airflow services
docker-compose -f docker-compose-airflow.yml up -d
```

This will:
- Start the PostgreSQL metadata database
- Start Airflow in standalone mode
- Initialize the Airflow database
- Create the admin user automatically
- Mount your DAGs, scripts, and source code

**Wait for initialization**: First startup may take 1-2 minutes while Airflow:
- Initializes the metadata database
- Creates the admin user
- Loads the DAG files

### Verify Services are Running

```bash
docker ps
```

You should see:
- `airflow-webserver` - Running
- `airflow-postgres` - Running

## Connecting to Kafka Network

**Important**: For the pipeline to consume from Kafka, Airflow must be connected to Kafka's Docker network.

```bash
# Connect Airflow to Kafka's network
docker network connect data-engineering-educational-project_default airflow-webserver
```

This allows the Kafka consumer in the DAG to connect to `kafka:9092`.

**Why is this needed?** See [DOCKER_NETWORKING.md](./DOCKER_NETWORKING.md) for detailed explanation.

## Accessing the Airflow UI

### 1. Open the Web Interface

Navigate to: **http://localhost:8080**

### 2. Login Credentials

Use the credentials from your `.env` file:
- **Username**: `admin`
- **Password**: Value from `AIRFLOW_ADMIN_PASSWORD` in `.env` (e.g., `<YOUR_PASSWORD>`)

> **Note:** The password is stored in the `airflow-data` Docker volume at `/opt/airflow/simple_auth_manager_passwords.json.generated` and persists across container restarts. **Set a strong, unique password in your `.env` file and never use weak or default credentials in production!**

### 3. Locate the DAG

Once logged in:
1. You'll see the main DAGs page
2. Look for **`complete_etl_pipeline`**
3. The DAG may be **paused** by default (toggle switch on the left)

## Triggering the Pipeline

### Method 1: Via Airflow UI (Recommended)

1. **Unpause the DAG** (if paused):
   - Click the toggle switch next to `complete_etl_pipeline`
   - It should turn blue/green when active

2. **Trigger a manual run**:
   - Click the "Play" button (▶️) on the right side of the DAG row
   - Select "Trigger DAG"
   - Optionally add configuration (or leave empty)
   - Click "Trigger"

3. **View the DAG execution**:
   - Click on the DAG name to open the detail view
   - You'll see the graph view with all 4 tasks
   - Tasks will change colors as they execute:
     - ⚪ None/Queued
     - 🟡 Running
     - 🟢 Success
     - 🔴 Failed

### Method 2: Via Command Line

```bash
# Trigger the DAG manually
docker exec airflow-webserver airflow dags trigger complete_etl_pipeline
```

### Method 3: Automatic Scheduling

The DAG is configured to run automatically every hour:
- Schedule: `0 * * * *` (at minute 0 of every hour)
- This can be changed in `airflow/dags/complete_etl_pipeline.py`

## Monitoring Pipeline Execution

### View Task States

```bash
# Get the run_id from the trigger output or UI, then:
docker exec airflow-webserver airflow tasks states-for-dag-run complete_etl_pipeline <RUN_ID>
```

Example output:
```
dag_id                | logical_date | task_id              | state   | start_date           | end_date
======================+==============+======================+=========+======================+=====================
complete_etl_pipeline |              | start_kafka_consumer | success | 2025-11-18T21:28:42  | 2025-11-18T21:29:13
complete_etl_pipeline |              | check_for_new_data   | success | 2025-11-18T21:29:14  | 2025-11-18T21:29:15
complete_etl_pipeline |              | consolidate_records  | success | 2025-11-18T21:29:15  | 2025-11-18T21:29:17
complete_etl_pipeline |              | load_to_supabase     | success | 2025-11-18T21:29:18  | 2025-11-18T21:29:20
```

### View Task Logs in UI

1. Click on the DAG run
2. Click on any task (colored box)
3. Click "Log" to see detailed execution logs

### View Task Logs via CLI

```bash
# View logs for a specific task
docker exec airflow-webserver cat /opt/airflow/logs/dag_id=complete_etl_pipeline/run_id=<RUN_ID>/task_id=<TASK_ID>/attempt=1.log
```

### Expected Results

A successful pipeline run will process:
- **~700-900 raw messages** from Kafka
- **~200-250 golden records** after consolidation
- **All records loaded** to Supabase

## Stopping Airflow

### Graceful Shutdown

```bash
docker-compose -f docker-compose-airflow.yml down
```

This stops and removes containers but **preserves volumes** (DAG runs, logs, metadata).

### Complete Cleanup (Including Data)

```bash
# Stop and remove everything including volumes
docker-compose -f docker-compose-airflow.yml down -v
```

⚠️ **Warning**: This will delete all Airflow metadata, DAG run history, and logs!

## Troubleshooting

### Issue: DAG Not Appearing in UI

**Solution**:
```bash
# Check if DAG file has syntax errors
docker exec airflow-webserver airflow dags list-import-errors

# Manually parse the DAG file
docker exec airflow-webserver python /opt/airflow/dags/complete_etl_pipeline.py
```

### Issue: Kafka Connection Refused

**Symptoms**: Task logs show `Connection refused` for Kafka

**Solution**:
```bash
# Ensure Airflow is connected to Kafka's network
docker network connect data-engineering-educational-project_default airflow-webserver

# Restart Airflow to reload the consumer script
docker restart airflow-webserver
```

### Issue: No Raw Messages Found

**Symptoms**: `check_for_new_data` reports 0 raw messages

**Causes**:
1. Kafka is not running
2. Kafka consumer didn't run long enough (runs for 30 seconds)
3. No messages in Kafka topic

**Solution**:
```bash
# Verify Kafka is running
docker ps | grep kafka

# Check if messages exist in MongoDB
docker exec airflow-webserver python -c "
from pymongo import MongoClient
import os
client = MongoClient(os.getenv('MONGO_ATLAS_URI'))
count = client['kafka_data']['probando_messages'].count_documents({})
print(f'Raw messages: {count}')
"
```

### Issue: Task Failed

**Solution**:
1. Click on the failed task in the UI
2. Click "Log" to view error details
3. Check the specific error message
4. Common fixes:
   - MongoDB connection: Check `MONGO_ATLAS_URI` in `.env`
   - Supabase connection: Check SQL credentials in `.env`
   - Python dependencies: May need to add to `_PIP_ADDITIONAL_REQUIREMENTS`

### Issue: Can't Login to Airflow UI

**Solution**:
```bash
# Check the admin password in your .env file
cat .env | grep AIRFLOW_ADMIN_PASSWORD

# Or reset the password
docker exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password your_new_password
```

## Advanced Configuration

### Changing DAG Schedule

Edit `airflow/dags/complete_etl_pipeline.py`:

```python
dag = DAG(
    dag_id='complete_etl_pipeline',
    schedule='0 * * * *',  # Change this cron expression
    # Examples:
    # '*/15 * * * *'  - Every 15 minutes
    # '0 */6 * * *'   - Every 6 hours
    # '0 0 * * *'     - Daily at midnight
    # None            - Manual trigger only
)
```

### Modifying Consumer Duration

Edit `airflow/dags/complete_etl_pipeline.py`, in the `start_kafka_consumer` function:

```python
# Let it run for 30 seconds
time.sleep(30)  # Change this value (in seconds)
```

### Adding More Tasks

See the existing task definitions in `complete_etl_pipeline.py` for examples of:
- `PythonOperator`: Run Python functions
- `BashOperator`: Run shell commands
- Task dependencies: Use `>>` to chain tasks

## Pipeline Metrics

After a successful run, you can query the results:

### MongoDB Golden Records
```python
from pymongo import MongoClient
client = MongoClient(MONGO_ATLAS_URI)
golden_count = client['kafka_data']['golden_records'].count_documents({})
print(f"Golden records: {golden_count}")
```

### Supabase Records
```sql
-- Connect to your Supabase/PostgreSQL database
SELECT 
    (SELECT COUNT(*) FROM person) as persons,
    (SELECT COUNT(*) FROM bank) as banks,
    (SELECT COUNT(*) FROM work) as works,
    (SELECT COUNT(*) FROM address) as addresses;
```

## Next Steps

- Monitor pipeline runs over time
- Set up alerts for failed tasks
- Configure email notifications in Airflow
- Create additional DAGs for other workflows
- Set up Airflow connections for external systems

## Resources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Networking Guide](./DOCKER_NETWORKING.md)
- [Project Implementation Summary](./IMPLEMENTATION_SUMMARY.md)
