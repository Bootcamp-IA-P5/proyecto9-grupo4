# Project Goals

The primary goal of this project is to implement a robust and scalable data pipeline that processes data in real-time. The pipeline demonstrates the integration of several key technologies:

-   **Data Ingestion**: Consume streaming data from a **Kafka** topic with 5 different schemas.
-   **Data Staging**: Store the raw, unstructured data in a **MongoDB** collection, which acts as a flexible staging area.
-   **Data Consolidation**: Merge the 5 disparate schemas into unified "golden records" representing complete individual profiles.
-   **Data Processing & Storage**: Transform the consolidated data from MongoDB and load it into a structured **PostgreSQL** (Supabase) relational database for querying and analysis.

This project serves as a practical example of building an ETL (Extract, Transform, Load) process suitable for modern data engineering challenges, all within a containerized development environment with Apache Airflow orchestration.

The project management board can be found [here](https://github.com/orgs/Bootcamp-IA-P5/projects/16).

## Pipeline Architecture

```
┌─────────────┐
│   Kafka     │  5 schemas: Personal, Location, Professional, Bank, Network
│  (5 schemas)│
└──────┬──────┘
       │ read_from_kafka.py
       ↓
┌─────────────────────────┐
│  MongoDB                │
│  probando_messages      │  ← Raw, unconsolidated Kafka messages
│  (staging)              │
└──────┬──────────────────┘
       │ consolidate_mongodb_records.py
       ↓
┌─────────────────────────┐
│  MongoDB                │
│  golden_records         │  ← Consolidated person-level records
│  (consolidated)         │
└──────┬──────────────────┘
       │ mongodb_to_supabase.py
       ↓
┌─────────────────────────┐
│  Supabase (PostgreSQL)  │
│  - person               │
│  - bank                 │  ← Normalized relational model
│  - work                 │
│  - address              │
└─────────────────────────┘
```

# Getting Started

To get the development environment up and running, follow these steps. For more detailed contribution guidelines, see [`CONTRIBUTING.md`](./CONTRIBUTING.md).

### Prerequisites

- **Docker Desktop**: The project uses VS Code Dev Containers.
- **VS Code**: With the Dev Containers extension installed.
- **Running Services**: You need access to running instances of Kafka, MongoDB, and PostgreSQL.

### Setup

1.  **Clone the repository**:
    ```sh
    git clone https://github.com/Bootcamp-IA-P5/proyecto9-grupo4.git
    cd proyecto9-grupo4
    ```
2.  **Configure Environment**: Copy the `.env.example` file to `.env` and add your credentials for Kafka and PostgreSQL.
3.  **Launch Dev Container**: Open the project folder in VS Code and run the command `Dev Containers: Reopen in Container`. This will build the Docker container and install all dependencies from `requirements.txt`.

# Project structure

```
proyecto9-grupo4/
├── airflow/                       # Apache Airflow for pipeline orchestration
│   ├── dags/
│   │   ├── kafka_mongodb_observer.py      # DAG for monitoring Kafka→MongoDB pipeline
│   │   └── complete_etl_pipeline.py       # 🆕 Full ETL orchestration DAG
│   ├── README.md                  # Airflow setup and usage guide
│   └── setup_airflow.sh          # Installation script
├── src/
│   ├── core/
│   │   └── logger.py              # Logging configuration and utilities
│   ├── database/
│   │   ├── models/
│   │   │   └── sql.py             # SQLAlchemy ORM models
│   │   ├── sql_alchemy.py         # 🔄 Enhanced: PostgreSQL connection & ETL functions
│   │   ├── write_to_mongodb.py    # Functions to connect and write to MongoDB
│   │   └── check_mongodb.py       # MongoDB inspection utilities
│   └── __init__.py                # Makes 'src' a Python package
├── config/
│   ├── settings.py                # Environment/configuration variables
│   └── kafka.ini                  # Kafka-specific configuration
├── tests/
│   ├── data/
│   │   └── test_data.json         # Sample golden records for testing
│   └── test_consolidation.py      # 🆕 Tests for consolidation logic
├── scripts/
│   ├── read_from_kafka.py         # 1️⃣ Reads from Kafka → MongoDB (raw)
│   ├── consolidate_mongodb_records.py  # 🆕 2️⃣ Consolidates 5 schemas → golden records
│   ├── mongodb_to_supabase.py     # 🆕 3️⃣ Loads golden records → Supabase
│   ├── CONSOLIDATION_README.md    # 🆕 Detailed consolidation documentation
│   ├── sql_load_db.py             # Utility to load test data into SQL database
│   ├── sql_clean_db.py            # Utility to clean and recreate SQL database
│   └── sql_dump_db.py             # Utility to export SQL data to CSV
├── .devcontainer/
│   └── devcontainer.json          # VSCode Dev Container configuration
├── .github/
│   └── ISSUE_TEMPLATE/            # GitHub issue templates
├── requirements.txt               # List of project dependencies
├── .env.example                   # Example environment variables file
└── README.md                      # Project documentation
```

🆕 = Newly added components for data consolidation

# Usage

This section describes how to run the various scripts provided in the project.

## Complete ETL Pipeline

### Option 1: Automated with Airflow (Recommended)

The complete pipeline is orchestrated by the `complete_etl_pipeline` DAG:

```sh
# 1. Start Airflow (if not already running)
cd airflow
./setup_airflow.sh

# 2. Access Airflow UI: http://localhost:8080 (admin/admin)
# 3. Enable and trigger the 'complete_etl_pipeline' DAG
```

The DAG runs hourly and executes:
1. ✅ Check for new raw Kafka messages
2. 🔄 Consolidate records (5 schemas → golden records)
3. 📤 Load golden records → Supabase
4. ✔️ Validate data integrity

### Option 2: Manual Execution

Run each stage of the pipeline manually:

**Stage 1: Kafka → MongoDB (Raw)**
```sh
python -m scripts.read_from_kafka
```
This continuously reads Kafka messages and stores them in `kafka_data.probando_messages`.

**Stage 2: MongoDB Raw → Golden Records**
```sh
# Production run
python scripts/consolidate_mongodb_records.py

# Dry run (test without writing)
python scripts/consolidate_mongodb_records.py --dry-run
```
This consolidates the 5 schemas into unified golden records in `kafka_data.golden_records`.

**Stage 3: MongoDB Golden → Supabase**
```sh
python scripts/mongodb_to_supabase.py
```
This loads golden records into PostgreSQL tables (person, bank, work, address).

## Testing

Test the consolidation logic with sample data:
```sh
python tests/test_consolidation.py
```

## Database Utilities

**Load test data into PostgreSQL:**
```sh
python -m scripts.sql_load_db [-h|--file FILE]
```

**Clean the PostgreSQL database:**
```sh
python -m scripts.sql_clean_db [-h|-f|--force]
```

**Dump PostgreSQL database to CSV:**
```sh
python -m scripts.sql_dump_db [-h|[-o|--output] OUTPUT]
```

## Monitoring & Observability

### Airflow DAGs

1. **`kafka_mongodb_health_monitor`** - Monitors stage 1 (Kafka → MongoDB)
   - Checks MongoDB connectivity
   - Validates data freshness
   - Tracks insertion rates

2. **`complete_etl_pipeline`** - Orchestrates full pipeline
   - Runs consolidation
   - Loads to Supabase
   - Validates data integrity

Access Airflow UI: http://localhost:8080 (admin/admin)

### Manual Monitoring

**Check MongoDB collections:**
```sh
python src/database/check_mongodb.py
```

**Check Supabase tables:**
```sql
-- In Supabase SQL Editor
SELECT 
  (SELECT COUNT(*) FROM person) as persons,
  (SELECT COUNT(*) FROM bank) as banks,
  (SELECT COUNT(*) FROM work) as work,
  (SELECT COUNT(*) FROM address) as addresses;
```
