"""
A script to populate the PostgreSQL database from various data sources.

This script can load data into the database in two main ways:
1. From a local JSON file (typically for testing or specific datasets).
2. From the 'golden' MongoDB collection, which contains the consolidated and
   cleaned data from the Kafka pipeline.

It supports both single-record and bulk-insertion methods for performance flexibility.
"""
import json
import os
import argparse
from sqlalchemy.exc import SQLAlchemyError

from src.database.write_to_postgresql import connect, read_from_mongo_json, read_from_mongo, read_from_mongo_bulk
from src.core.logger import Logger

log = Logger()

def load_json_data(file_path):
    """
    Loads and parses JSON data from a specified file path.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        dict or list or None: The loaded JSON data (as a dict or list),
                              or None if the file is not found or cannot be parsed.
    """
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        log.error(f"Data file not found at {file_path}")
    except json.JSONDecodeError:
        log.error(f"Could not decode JSON from {file_path}. Check file format.")
    return None

def main(data_file, batch):
    """
    Orchestrates the data loading process into the PostgreSQL database.

    This function handles connecting to the database, determining the data source
    (either a local file or MongoDB), and selecting the insertion strategy
    (single vs. bulk). It ensures the database session is properly managed
    and closed.

    Args:
        data_file (str or None): Path to the JSON data file. If None, data is
                                 sourced from MongoDB.
        batch (bool): If True, uses the bulk insertion method for loading.
    """
    if data_file:
        log.info(f"Attempting to load data from: {data_file}")
        json_data = load_json_data(data_file)
        if not json_data:
            log.critical("Aborting due to file loading error.")
            return

    session = connect()
    if not session:
        log.critical("Could not establish a database connection. Data loading aborted.")
        return

    try:
        if batch:
            read_from_mongo_bulk(session)
        else:
            if data_file:
                read_from_mongo_json(session, json_data)
            else:
                read_from_mongo(session)
        log.info(f"Successfully loaded data from {data_file} into the database.")
    except SQLAlchemyError as e:
        session.rollback()
        log.error(f"A database error occurred: {e}")
    except KeyError as e:
        session.rollback()
        log.error(f"A key error occurred, check if the JSON data structure is correct: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    DEFAULT_DATA_FILE = os.path.join(os.path.dirname(__file__), '..', 'tests', 'data', 'test_data.json')

    parser = argparse.ArgumentParser(description="Load test data into the PostgreSQL database.")
    parser.add_argument('--file', default=None, help=f"Path to the JSON data file. Use {DEFAULT_DATA_FILE} for testing purposes.")
    parser.add_argument('--bulk', action='store_true', help="Use the bulk insertion/batch commit method for better performance.")
    args = parser.parse_args()

    main(args.file, args.bulk)