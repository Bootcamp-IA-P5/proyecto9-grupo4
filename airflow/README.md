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

### 2. Get the Password

Wait ~1 minute and run:

```bash
docker logs airflow-webserver 2>&1 | grep -i password
```

You'll see something like:
```
Simple auth manager | Password for user 'admin': "PASSWORD_AIRFLOW"
```

### 3. Access Airflow

**URL:** http://localhost:8080
- Usuario: `admin`
- Password: La que obtuviste arriba

- Password: El que obtuviste arriba

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
docker logs -f airflow-webserver   # Webserver
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

