import json
import os
from src.database.sql_alchemy import connect, read_from_mongo

# Define the path to the JSON file containing the fake test data.
# This data is used solely for testing purposes and simulates records
# that would typically come from an external source like MongoDB.
TEST_DATA_FILE = os.path.join(os.path.dirname(__file__), '..', 'tests', 'data', 'test_data.json')

# Load the JSON data from the external file.
try:
    with open(TEST_DATA_FILE, 'r') as f:
        json_data = json.load(f)
except FileNotFoundError:
    print(f"❌ Error: Test data file not found at {TEST_DATA_FILE}")
    exit(1)
except json.JSONDecodeError:
    print(f"❌ Error: Could not decode JSON from {TEST_DATA_FILE}. Check file format.")
    exit(1)

session = connect()

if session:
    try:
        read_from_mongo(session, json_data)
        print(f"✅ Successfully loaded data from {TEST_DATA_FILE} into the database.")
    except Exception as e:
        session.rollback()
        print(f"❌ Error loading data into the database: {e}")
    finally:
        session.close()
else:
    print("❌ Could not establish a database connection. Data loading aborted.")