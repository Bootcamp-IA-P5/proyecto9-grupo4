# Quick Start Guide: Apache Airflow

This guide will help you set up and run Apache Airflow to monitor your Kafka → MongoDB pipeline.

> ⚠️ **Important:** This DAG **ONLY OBSERVES** the pipeline. It does NOT execute the Kafka consumer. Make sure you have `scripts/read_from_kafka.py` running separately.

## 🚀 Quick Start

### 1. Start Airflow with Docker

```bash
docker-compose -f docker-compose-airflow.yml up -d
```

**Done!** This command:
- ✅ Starts all Airflow containers
- ✅ Automatically creates the `admin` user
- ✅ Generates a secure password

### 2. Use the Configured Credentials

The Docker compose file reads the admin credentials from your `.env` file (`AIRFLOW_ADMIN_USERNAME` / `AIRFLOW_ADMIN_PASSWORD`).

1. Make sure those variables are present before starting the stack.
2. After the containers are up, log in with the same values.
3. If you want to double-check what Airflow loaded, you can still inspect the logs:

```bash
docker logs airflow-webserver 2>&1 | Select-String -Pattern "Password for user"
```

### 3. Access Airflow

**URL:** http://localhost:8080
- Usuario: `AIRFLOW_ADMIN_USERNAME` (from `.env`)
- Password: `AIRFLOW_ADMIN_PASSWORD` (from `.env`)

---

### ♻️ Redeploying After Compose Changes

Running more than one scheduler (for example by using `airflow standalone`) can delete serialized DAG metadata and produce errors such as `DAG '...' not found in serialized_dag table`. The compose file now starts a dedicated scheduler service and an API server-only service (Airflow 3 replaces the old `webserver` command with `airflow api-server`).

Whenever you update `docker-compose-airflow.yml` or pull new changes:

1. Stop the existing stack
   ```bash
   docker-compose -f docker-compose-airflow.yml down
   ```
2. Start it again with the new configuration
   ```bash
   docker-compose -f docker-compose-airflow.yml up -d
   ```
3. (Optional) Clear any stuck DAG runs from the Airflow UI before triggering a fresh `complete_etl_pipeline` run.

This guarantees that only one scheduler is manipulating the metadata database and that tasks such as `load_to_supabase` can start normally.

---

## 🛑 Useful Commands

**Stop Airflow (keeps password):**
```bash
docker-compose -f docker-compose-airflow.yml down
```

**Stop and clean everything (regenerates password):**
```bash
docker-compose -f docker-compose-airflow.yml down -v
```

**View logs in real-time:**
```bash
docker logs -f airflow-webserver   # API server (serves the UI)
docker logs -f airflow-scheduler   # Scheduler
```

---

## 📊 Using the Monitoring DAG

> � **Note:** This DAG **observes** the pipeline. Make sure you have running:
> - Kafka broker
> - Kafka consumer ([`scripts/read_from_kafka.py`](../scripts/read_from_kafka.py))
> - MongoDB

### 1. Activate the DAG

1. In the Airflow UI, search for: `kafka_mongodb_health_monitor`
2. Toggle it on (it will turn blue/green)
3. It will execute automatically every 10 minutes

### 2. Manual Execution (Testing)

1. Click on the DAG name
2. Click "▶️ Trigger DAG"
3. Click "Trigger"

### 3. View Results

Click on the `generate_health_summary` task → "Log"

You'll see something like:
```
======================================================================
📊 KAFKA → MONGODB PIPELINE HEALTH SUMMARY
======================================================================
🚦 Overall Status: HEALTHY
----------------------------------------------------------------------
📦 Total documents: 5247
🕐 Last insertion: 0:00:12
✨ Data status: FRESH
📈 Insertion rate: 45.20 docs/min
======================================================================

💡 RECOMMENDATIONS:
   → Everything working correctly ✓
```

**What does the DAG monitor?**
- ✅ MongoDB connection
- ✅ Data freshness (last insertion time)
- ✅ Insertion rate (documents/minute)
- ✅ Estimated daily volume

---

## 🔧 Advanced Configuration

### Change Monitoring Frequency

Edit `airflow/dags/kafka_mongodb_observer.py`:

```python
schedule_interval='*/10 * * * *',  # Every 10 minutes
```

Examples:
- `'*/5 * * * *'` = Every 5 minutes
- `'0 * * * *'` = Every hour
- `'0 */6 * * *'` = Every 6 hours

### Change MongoDB Database

Edit the environment variable in `.env`:
```bash
MONGO_ATLAS_URI=mongodb+srv://user:pass@cluster.mongodb.net/
```

---

## 🐛 Troubleshooting

**DAG doesn't appear:**
- Wait 30 seconds (Airflow scans every 30s)
- Check: `docker logs airflow-scheduler`

**MongoDB connection error:**
- Verify your `MONGO_ATLAS_URI` in `.env`
- Make sure your IP is whitelisted in MongoDB Atlas

**"Data status: STALE" (data is outdated):**
- ⚠️ The Kafka consumer is NOT running
- Verify: `python scripts/read_from_kafka.py`

**"Insertion rate = 0":**
- ⚠️ The Kafka consumer is stopped
- ⚠️ Kafka has no new messages on the topic

**View complete logs:**
```bash
docker logs -f airflow-scheduler
```

---

## 📚 Resources

- [Apache Airflow 3.0 Documentation](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Cron Expression Generator](https://crontab.guru/)
- [MongoDB Atlas Documentation](https://docs.atlas.mongodb.com/)

