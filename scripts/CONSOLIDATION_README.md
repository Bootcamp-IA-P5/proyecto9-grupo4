# Data Consolidation and ETL Pipeline

This directory contains scripts for consolidating raw Kafka data from MongoDB and loading it into Supabase (PostgreSQL).

## Overview

The complete data pipeline consists of 3 stages:

```
Kafka → MongoDB (raw) → MongoDB (golden) → Supabase (PostgreSQL)
   ↓            ↓                ↓                    ↓
Stage 1     Stage 2          Stage 3            Stage 4
(existing)  (NEW)            (NEW)              (existing)
```

### Stage 1: Kafka to MongoDB Raw (Existing)
- **Script**: `read_from_kafka.py`
- **Purpose**: Reads messages from Kafka and writes them as-is to MongoDB
- **Collection**: `kafka_data.probando_messages`
- **Status**: ✅ Already implemented

### Stage 2: MongoDB Raw to Golden Records (NEW)
- **Script**: `consolidate_mongodb_records.py`
- **Purpose**: Consolidates 5 different schemas into unified golden records
- **Input**: `kafka_data.probando_messages` (raw)
- **Output**: `kafka_data.golden_records` (consolidated)
- **Status**: ✅ Newly implemented

### Stage 3: MongoDB Golden to Supabase (NEW)
- **Script**: `mongodb_to_supabase.py`
- **Purpose**: Loads golden records into PostgreSQL/Supabase
- **Input**: `kafka_data.golden_records`
- **Output**: Supabase tables (person, bank, work, address)
- **Status**: ✅ Newly implemented

### Stage 4: Supabase Database (Existing)
- **Module**: `src/database/sql_alchemy.py`
- **Purpose**: Manages PostgreSQL connections and data models
- **Status**: ✅ Enhanced with new `read_from_mongo_collection()` function

---

## The 5 Kafka Schemas

The raw Kafka messages come in 5 different formats:

### 1. Personal Data
```json
{
  "name": "Artur",
  "last_name": "Lopes",
  "sex": ["M"],
  "telfnumber": "(351) 960341849",
  "passport": "673542480",
  "email": "marcio21@yahoo.com"
}
```

### 2. Location Data
```json
{
  "fullname": "Artur Lopes",
  "city": "Trofarello",
  "address": "Stretto Zanichelli, 56 Piano 2"
}
```

### 3. Professional Data
```json
{
  "fullname": "Artur Lopes",
  "company": "Pereyra LLC",
  "company address": "Av. 6 N° 342",
  "company_telfnumber": "939083193",
  "company_email": "trinidad15@moreno.com",
  "job": "Vendedor de comidas al mostrador"
}
```

### 4. Bank Data
```json
{
  "passport": "673542480",
  "IBAN": "GB56GISN50706931787499",
  "salary": "193945$"
}
```

### 5. Network Data
```json
{
  "address": "Stretto Zanichelli, 56 Piano 2",
  "IPv4": "183.233.46.56"
}
```

---

## Consolidation Strategy

The `consolidate_mongodb_records.py` script uses a multi-key matching strategy:

1. **Primary Key**: `passport` (links schemas 1 & 4)
2. **Secondary Key**: `fullname` (links schemas 2 & 3 to schema 1 via name + lastname)
3. **Tertiary Key**: `address` (links schema 5 to schema 2)

### Golden Record Structure
After consolidation, each person has a unified record:

```json
{
  "_id": "673542480",  // passport as unique ID
  "name": "Artur",
  "last_name": "Lopes",
  "sex": ["M"],
  "telfnumber": "(351) 960341849",
  "passport": "673542480",
  "email": "marcio21@yahoo.com",
  "city": "Trofarello",
  "address": "Stretto Zanichelli, 56 Piano 2",
  "IPv4": "183.233.46.56",
  "company": "Pereyra LLC",
  "company address": "Av. 6 N° 342",
  "company_telfnumber": "939083193",
  "company_email": "trinidad15@moreno.com",
  "job": "Vendedor de comidas al mostrador",
  "IBAN": "GB56GISN50706931787499",
  "salary": "193945$",
  "consolidated_at": "2025-11-16T10:30:00",
  "data_sources": ["personal", "location", "professional", "bank", "network"]
}
```

---

## Usage

### 1. Run Consolidation (MongoDB Raw → Golden)

```bash
# Production run
python scripts/consolidate_mongodb_records.py

# Dry run (no writes, just testing)
python scripts/consolidate_mongodb_records.py --dry-run
```

**What it does:**
- Reads all raw Kafka messages from `probando_messages`
- Detects schema types automatically
- Groups records by individual (passport)
- Merges all 5 schemas into golden records
- Writes/updates `golden_records` collection

**Output:**
```
📊 FINAL STATISTICS
  total_raw_messages: 5000
  personal_records: 1000
  location_records: 1000
  professional_records: 1000
  bank_records: 1000
  network_records: 1000
  golden_records_created: 950
  golden_records_updated: 50
  unmatched_records: 0
```

### 2. Load to Supabase (MongoDB Golden → PostgreSQL)

```bash
python scripts/mongodb_to_supabase.py
```

**What it does:**
- Connects to MongoDB `golden_records` collection
- Connects to Supabase PostgreSQL database
- Transforms golden records into relational model
- Inserts into tables: `person`, `bank`, `work`, `address`, `person_address`
- Handles duplicates gracefully (skips existing records)

**Output:**
```
✅ ETL Pipeline completed successfully!
Summary: 950 new records inserted, 50 duplicate records skipped, 0 records with errors.
```

### 3. Run Complete Pipeline (Automated)

The Airflow DAG `complete_etl_pipeline` orchestrates everything:

```
Task 1: check_for_new_data
   ↓
Task 2: consolidate_records
   ↓
Task 3: load_to_supabase
   ↓
Task 4: validate_supabase_data
```

**Schedule**: Runs every hour (configurable)

To trigger manually in Airflow UI:
1. Go to http://localhost:8080
2. Find `complete_etl_pipeline` DAG
3. Click "Trigger DAG"

---

## Configuration

All scripts use environment variables from `.env`:

```env
# MongoDB Configuration
MONGO_ATLAS_URI=mongodb+srv://user:pass@cluster.mongodb.net/

# Supabase Configuration
SQL_USER=postgres
SQL_PASSWORD=your_password
SQL_HOST=db.xxx.supabase.co
SQL_PORT=5432
SQL_DBNAME=postgres
```

---

## Data Quality Features

### Deduplication
- ✅ Consolidation script is **idempotent** (can run multiple times safely)
- ✅ Supabase loader checks for existing records by passport
- ✅ No duplicate data created on re-runs

### Error Handling
- ✅ Validates required fields (passport, name, lastname)
- ✅ Handles missing optional fields gracefully
- ✅ Continues processing on individual record errors
- ✅ Comprehensive logging for debugging

### Data Completeness Tracking
- ✅ `data_sources` field tracks which schemas contributed
- ✅ Statistics show matched vs unmatched records
- ✅ Progress tracking for large datasets

---

## Monitoring

### Check MongoDB Collections

```python
# Check raw messages
from pymongo import MongoClient
client = MongoClient(os.getenv('MONGO_ATLAS_URI'))
db = client['kafka_data']
print(f"Raw messages: {db['probando_messages'].count_documents({})}")
print(f"Golden records: {db['golden_records'].count_documents({})}")
```

### Check Supabase Tables

```sql
-- In Supabase SQL Editor
SELECT 
  (SELECT COUNT(*) FROM person) as persons,
  (SELECT COUNT(*) FROM bank) as banks,
  (SELECT COUNT(*) FROM work) as work,
  (SELECT COUNT(*) FROM address) as addresses;
```

### Airflow Monitoring

The existing `kafka_mongodb_health_monitor` DAG monitors the Kafka → MongoDB stage.

The new `complete_etl_pipeline` DAG includes validation tasks that check data integrity.

---

## Troubleshooting

### Issue: No golden records created
**Cause**: Raw messages might not have matching passports
**Solution**: Check logs for "unmatched_records" count and review raw data quality

### Issue: Supabase loader fails
**Cause**: Missing required fields or connection issues
**Solution**: 
1. Verify `.env` has correct Supabase credentials
2. Check logs for specific error messages
3. Run consolidation with `--dry-run` to test

### Issue: Duplicate records in Supabase
**Cause**: Passport field not being used as unique key
**Solution**: Records are automatically skipped if passport exists - check logs

---

## Future Enhancements

- [ ] Add schema versioning for evolution handling
- [ ] Implement CDC (Change Data Capture) for real-time updates
- [ ] Add data quality metrics dashboard
- [ ] Implement soft deletes for records
- [ ] Add data lineage tracking
- [ ] Create reconciliation reports

---

## Testing

Test data is available in `tests/data/test_data.json`:

```bash
# Test with sample data
python -c "
import json
from src.database.sql_alchemy import connect, read_from_mongo
session = connect()
with open('tests/data/test_data.json') as f:
    data = json.load(f)
read_from_mongo(session, data)
"
```

---

## Architecture Diagram

```
┌─────────────┐
│   Kafka     │
│  (5 schemas)│
└──────┬──────┘
       │ read_from_kafka.py
       ↓
┌─────────────────────────┐
│  MongoDB                │
│  probando_messages      │  ← Raw, unconsolidated
│  (raw Kafka messages)   │
└──────┬──────────────────┘
       │ consolidate_mongodb_records.py
       ↓
┌─────────────────────────┐
│  MongoDB                │
│  golden_records         │  ← Consolidated, person-level
│  (golden records)       │
└──────┬──────────────────┘
       │ mongodb_to_supabase.py
       ↓
┌─────────────────────────┐
│  Supabase (PostgreSQL)  │
│  - person               │
│  - bank                 │  ← Relational model
│  - work                 │
│  - address              │
│  - person_address       │
└─────────────────────────┘
```

---

## Questions?

Contact the data engineering team or check the logs for detailed execution information.
