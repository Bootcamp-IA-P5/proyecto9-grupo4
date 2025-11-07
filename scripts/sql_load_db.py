import json
import os
import argparse
from sqlalchemy.exc import SQLAlchemyError

from src.database.sql_alchemy import connect, read_from_mongo
from src.core.logger import Logger

log = Logger()

def load_json_data(file_path):
    """
    Loads JSON data from the specified file path.
    
    Args:
        file_path (str): The path to the JSON file.
        
    Returns:
        dict: The loaded JSON data, or None if an error occurs.
    """
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        log.error(f"Data file not found at {file_path}")
    except json.JSONDecodeError:
        log.error(f"Could not decode JSON from {file_path}. Check file format.")
    return None

def main(data_file):
    """
    Main function to connect to the database, load data from a file,
    and populate the database tables.
    
    Args:
        data_file (str): Path to the JSON data file to load.
    """
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
        read_from_mongo(session, json_data)
        log.info(f"✅ Successfully loaded data from {data_file} into the database.")
    except SQLAlchemyError as e:
        session.rollback()
        log.error(f"A database error occurred: {e}")
    except KeyError as e:
        session.rollback()
        log.error(f"A key error occurred, check if the JSON data structure is correct: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    # This data is used solely for testing purposes and simulates records
    # that would typically come from an external source like MongoDB.
    DEFAULT_DATA_FILE = os.path.join(os.path.dirname(__file__), '..', 'tests', 'data', 'test_data.json')

    parser = argparse.ArgumentParser(description="Load test data into the PostgreSQL database.")
    parser.add_argument('--file', default=DEFAULT_DATA_FILE, help=f"Path to the JSON data file. Defaults to {DEFAULT_DATA_FILE}")
    args = parser.parse_args()

    main(args.file)