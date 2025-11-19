# Quick Start Guide

This is a quick reference for launching the complete ETL pipeline.

## Prerequisites

✅ Docker and Docker Compose installed  
✅ `.env` file configured with all credentials  
✅ Kafka containers running on `data-engineering-educational-project_default` network

## Launch Pipeline (3 Commands)

```bash
# 1. Start Airflow (standalone mode: webserver + scheduler + triggerer)
docker-compose -f docker-compose-airflow.yml up -d

# 2. Connect Airflow to Kafka's network (allows kafka:9092 connectivity)
docker network connect data-engineering-educational-project_default airflow-webserver

# 3. Access Airflow UI
# Open: http://localhost:8080
# Login: admin / <your_password_from_env> (from .env file)
```

## Pipeline Overview

```
┌─────────────────────────────────────────────────────────────┐
│ complete_etl_pipeline (runs hourly: 0 * * * *)             │
├─────────────────────────────────────────────────────────────┤
│  1. start_kafka_consumer                                    │
│     └─> Consumes from Kafka → MongoDB (30 seconds)         │
│                          ↓                                  │
│  2. check_for_new_data                                      │
│     └─> Counts records in MongoDB raw collection           │
│                          ↓                                  │
│  3. consolidate_records                                     │
│     └─> Deduplicates & consolidates → Golden records       │
│                          ↓                                  │
│  4. load_to_supabase                                        │
│     └─> Loads golden records → PostgreSQL/Supabase         │
└─────────────────────────────────────────────────────────────┘
```

## Triggering the Pipeline

### Via UI (Recommended)
1. Go to http://localhost:8080
2. Find `complete_etl_pipeline` DAG
3. Unpause the DAG (toggle switch)
4. Click "Play" button (▶️) → "Trigger DAG"

### Via API
```bash
curl -X POST "http://localhost:8080/api/v1/dags/complete_etl_pipeline/dagRuns" \
  -H "Content-Type: application/json" \
  -u "admin:<your_password_from_env>" \
  -d '{"conf":{}}'
```

## Monitoring

### Check Container Status
```bash
docker ps --filter "name=airflow"
```

### View Logs
```bash
# All logs
docker logs airflow-webserver

# Follow logs in real-time
docker logs -f airflow-webserver

# Last 50 lines
docker logs airflow-webserver --tail 50
```

### Check Password Persistence
```bash
docker exec airflow-webserver cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

## Stopping Services

```bash
# Stop containers (preserves volumes and password)
docker-compose -f docker-compose-airflow.yml down

# Stop and remove volumes (deletes password!)
docker-compose -f docker-compose-airflow.yml down -v
```

## Important Notes

🔐 **Password Persistence**: The admin password (from your `.env` file) is stored in the `airflow-data` Docker volume and persists across container restarts. It will only be regenerated if you delete the volume.

🌐 **Network Connection**: The `docker network connect` command must be run after EVERY `docker-compose up` because network connections don't persist across container recreations.

📊 **Pipeline Results**: In our last successful run:
- 753 raw messages consumed from Kafka
- 202 golden records after consolidation
- Successfully loaded to Supabase

🔍 **Troubleshooting**: See [AIRFLOW_SETUP.md](./AIRFLOW_SETUP.md) for detailed setup instructions and [DOCKER_NETWORKING.md](./DOCKER_NETWORKING.md) for networking explanation.

## Useful Commands

```bash
# Restart Airflow (preserves password)
docker-compose -f docker-compose-airflow.yml restart

# View DAG execution in terminal
docker exec airflow-webserver airflow dags list

# Test a single task
docker exec airflow-webserver airflow tasks test complete_etl_pipeline start_kafka_consumer 2025-01-01

# Check Airflow version
docker exec airflow-webserver airflow version
```
