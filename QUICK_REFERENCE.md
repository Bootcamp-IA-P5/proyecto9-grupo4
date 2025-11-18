# Quick Reference Guide: Data Consolidation Pipeline

## 🚀 Quick Start

### First Time Setup
```bash
# 1. Ensure .env is configured with MongoDB and Supabase credentials
# 2. Make sure Kafka consumer is running (read_from_kafka.py)
# 3. Wait for some raw data to accumulate in MongoDB
```

### Run Complete Pipeline
```bash
# Option A: Automated (Recommended)
# Just enable the 'complete_etl_pipeline' DAG in Airflow UI
# It runs automatically every hour

# Option B: Manual
python scripts/consolidate_mongodb_records.py
python scripts/mongodb_to_supabase.py
```

## 📋 Common Commands

### Check Data Status
```bash
# Check MongoDB collections
python src/database/check_mongodb.py

# Test consolidation logic
python tests/test_consolidation.py

# Dry run consolidation (no writes)
python scripts/consolidate_mongodb_records.py --dry-run
```

### Monitor Pipeline
```bash
# View Airflow UI
# http://localhost:8080 (admin/admin)

# Check specific DAG
# Look for: complete_etl_pipeline
```

### Troubleshooting
```bash
# View detailed logs
# Logs are printed to console with color coding:
# 🔵 INFO - Normal operations
# 🟡 WARNING - Potential issues
# 🔴 ERROR - Problems requiring attention

# Check last consolidation stats
# Look for "FINAL STATISTICS" section in logs
```

## 📊 Understanding the Output

### Consolidation Statistics
```
total_raw_messages: 5000      # Total Kafka messages processed
personal_records: 1000        # Schema 1 (personal data)
location_records: 1000        # Schema 2 (location)
professional_records: 1000    # Schema 3 (work info)
bank_records: 1000            # Schema 4 (financial)
network_records: 1000         # Schema 5 (IP addresses)
golden_records_created: 950   # New consolidated records
golden_records_updated: 50    # Updated existing records
unmatched_records: 0          # Records that couldn't be matched
```

### Supabase Load Summary
```
Summary: 
  950 new records inserted
  50 duplicate records skipped
  0 records with errors
```

## 🔧 Configuration

### Environment Variables (.env)
```env
# MongoDB Atlas
MONGO_ATLAS_URI=mongodb+srv://username:password@cluster.mongodb.net/

# Supabase PostgreSQL
SQL_USER=postgres
SQL_PASSWORD=your_secure_password
SQL_HOST=db.xxxxx.supabase.co
SQL_PORT=5432
SQL_DBNAME=postgres
```

## 🎯 When to Run Each Script

| Script | When to Run | Frequency |
|--------|------------|-----------|
| `read_from_kafka.py` | Always running | Continuous |
| `consolidate_mongodb_records.py` | When raw messages accumulate | Hourly via Airflow |
| `mongodb_to_supabase.py` | After consolidation | Hourly via Airflow |

## ⚠️ Important Notes

1. **Idempotent Operations**: All scripts can be run multiple times safely
2. **Order Matters**: Run consolidation before loading to Supabase
3. **Kafka First**: Ensure Kafka consumer is running to get raw data
4. **Dry Run**: Always test with `--dry-run` flag first on new data

## 🐛 Common Issues & Solutions

| Issue | Solution |
|-------|----------|
| No golden records created | Check if raw messages exist in MongoDB |
| Consolidation fails | Verify MONGO_ATLAS_URI in .env |
| Supabase load fails | Check SQL_* credentials in .env |
| Duplicate errors | Records are automatically skipped (expected) |
| Missing fields in golden records | Some schemas may be optional - check data_sources field |

## 📈 Monitoring Checklist

Daily:
- [ ] Check Airflow DAG status (green = healthy)
- [ ] Verify raw message count increasing
- [ ] Confirm golden records are being created

Weekly:
- [ ] Review consolidation statistics
- [ ] Check for unmatched_records trends
- [ ] Validate Supabase data quality

## 🔗 Related Documentation

- **Full Details**: `scripts/CONSOLIDATION_README.md`
- **Implementation**: `IMPLEMENTATION_SUMMARY.md`
- **Airflow Setup**: `airflow/README.md`
- **Contributing**: `CONTRIBUTING.md`

## 💡 Pro Tips

1. **Use dry-run mode** when testing: `--dry-run`
2. **Monitor Airflow UI** for pipeline health
3. **Check logs** for detailed execution info
4. **Validate data** in Supabase after first run
5. **Scale gradually** - test with small batches first

## 🆘 Getting Help

1. Check the logs for error messages
2. Review `scripts/CONSOLIDATION_README.md`
3. Test with sample data: `python tests/test_consolidation.py`
4. Contact the data engineering team

---

**Remember**: The pipeline is designed to be fault-tolerant and idempotent. Don't be afraid to re-run scripts if something goes wrong!
