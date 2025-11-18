# Project Goals

The primary goal of this project is to implement a robust and scalable data pipeline that processes data in real-time. The pipeline demonstrates the integration of several key technologies:

-   **Data Ingestion**: Consume streaming data from a **Kafka** topic.
-   **Data Staging**: Store the raw, unstructured data in a **MongoDB** collection, which acts as a flexible staging area.
-   **Data Processing & Storage**: Transform the data from MongoDB and load it into a structured **PostgreSQL** relational database for querying and analysis.

This project serves as a practical example of building an ETL (Extract, Transform, Load) process suitable for modern data engineering challenges, all within a containerized development environment.

The project management board can be found [here](https://github.com/orgs/Bootcamp-IA-P5/projects/16).

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
├── airflow/                           # Apache Airflow for pipeline orchestration
│   ├── dags/
│   │   └── kafka_mongodb_observer.py  # DAG for monitoring Kafka→MongoDB pipeline
│   ├── README.md                      # Airflow setup and usage guide
│   └── setup_airflow.sh               # Installation script
├── src/
│   ├── core/
│   │   ├── kafka_consumer.py          # 1. (tentative) Reads from Kafka and writes to MongoDB (Collection A)
│   │   ├── data_processor.py          # 2. (tentative) Processing logic: A -> B
│   │   ├── rel_writer.py              # 3. (tentative) Read from B and write to Relational DB
│   │   └── logger.py                  # Logging configuration and utilities
│   ├── database/
│   │   ├── models/
│   │   │   └── sql.py                 # SQLAlchemy ORM models
│   │   ├── sql_alchemy.py             # Functions to connect and write to PostgreSQL
│   │   ├── write_to_mongodb.py        # Functions to connect and write to MongoDB
│   │   └── write_to_postgresql.py     # Functions to connect and write to PostgreSQL
│   └── __init__.py                    # Makes 'src' a Python package
├── config/
│   ├── settings.py                    # (tentative) Environment/configuration variables read at startup
│   └── kafka.ini                      # (tentative) Kafka-specific configuration file (or .env)
├── tests/
│   ├── data
│   │   └── test_data.json             # Fake data to test load from mongo to PostgreSQL 
│   ├── test_kafka_consumer.py         # (tentative) Tests the Kafka consumer
│   ├── test_data_processor.py         # (tentative) Tests the data processor
│   └── test_mongodb_connector.py      # (tentative) Tests the MongoDB connector
├── scripts/
│   ├── mongo_consolidate.py           # Consolidate data from Kafka to MongoDB (Golden, deprecated)
│   ├── read_from_kafka.py             # Reads from Kafka and writes to MongoDB
│   ├── sql_load_db.py                 # Utility to load test data into the SQL database
│   ├── sql_clean_db.py                # Utility to clean and recreate the SQL database
│   └── sql_dump_db.py                 # Utility to export SQL data to a CSV file
├── .devcontainer/
│   └── devcontainer.json              # VSCode Dev Container configuration
├── .github/
│   └── ISSUE_TEMPLATE/                # GitHub issue templates
├── requirements.txt                   # List of project dependencies (e.g., kafka-python, pymongo, sqlalchemy)
├── .env.example                       # Example environment variables file
└── README.md                          # Project documentation
```

# Usage

This section describes how to run the various scripts provided in the project.

### Running Scripts

1. **Read from Kafka and write to MongoDB**
```sh
python -m scripts.read_from_kafka
```

2. **Monitor pipeline with Airflow** (see [`airflow/README.md`](airflow/README.md) for setup)
```sh
# Terminal 1 - Start scheduler
export AIRFLOW_HOME="$(pwd)/airflow"
airflow scheduler

# Terminal 2 - Start webserver
export AIRFLOW_HOME="$(pwd)/airflow"
airflow webserver --port 8080

# Access UI: http://localhost:8080 (admin/admin)
```

3. Load data into PostgreSQL
```sh
python -m scripts.sql_load_db [-h|--file FILE| --bulk]
```

4. Clean the PostgreSQL database
```sh
python -m scripts.sql_clean_db [-h|-f|--force]
```

5. Dump PostgreSQL database into a csv file
```sh
python -m scripts.dump_db [-h|[-o|--output] OUTPUT]
```

## 📊 Observability with Apache Airflow

This project includes Apache Airflow for pipeline monitoring and orchestration. The DAG `kafka_mongodb_health_monitor` provides:

- **Real-time health checks**: MongoDB connectivity and data freshness
- **Metrics tracking**: Insertion rate, document count, pipeline status
- **Visual monitoring**: Graph view of pipeline dependencies
- **Alerting**: Detect stale data or pipeline failures

For complete setup instructions, see [`airflow/README.md`](airflow/README.md).
