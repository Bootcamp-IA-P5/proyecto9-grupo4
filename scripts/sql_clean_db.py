"""
A utility script to completely wipe and recreate the PostgreSQL database schema.

This script drops all tables defined in the SQLAlchemy models (via Base.metadata)
and then recreates them. It is a destructive operation and should be used with
caution, typically in development or testing environments to reset the database
to a clean state.

It can be run with a `--force` flag to bypass the interactive confirmation prompt.
"""
import os
import sys
import argparse
from sqlalchemy.exc import SQLAlchemyError

# Add project root to Python path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.database.write_to_postgresql import connect
from src.database.models.sql import Base
from src.core.logger import Logger

log = Logger()

def recreate_database(engine, force=False):
    """
    Drops all tables defined in the SQLAlchemy Base metadata and recreates them.

    This is a destructive operation that will result in the loss of all data.
    It prompts the user for confirmation unless the `force` flag is set to True.

    Args:
        engine: The SQLAlchemy engine instance connected to the target database.
        force (bool): If True, bypasses the interactive confirmation prompt.
                      Defaults to False.
    """
    if not force:
        log.warning("This will drop all tables and delete all data in the database.")
        confirm = input("Are you sure you want to continue? (y/n) ")
        if confirm.lower() != 'y':
            log.info("Aborted by user.")
            return

    log.info("Dropping all tables...")
    try:
        Base.metadata.drop_all(engine)
        log.info("Recreating all tables from models...")
        Base.metadata.create_all(engine)
        log.info("Database has been successfully cleaned and recreated.")
    except SQLAlchemyError as e:
        log.error(f"An error occurred during database recreation: {e}")

def main():
    """
    Parses command-line arguments and orchestrates the database recreation process.

    Connects to the database, handles the `--force` argument, and calls the
    `recreate_database` function.
    """
    parser = argparse.ArgumentParser(description="Clean and recreate the database schema based on SQLAlchemy models.")
    parser.add_argument('--force', '-f', action='store_true', help="Force the operation without asking for confirmation.")
    args = parser.parse_args()

    session = connect()
    if session:
        try:
            recreate_database(session.get_bind(), args.force)
        finally:
            session.close()
    else:
        log.critical("Could not establish a database connection.")

if __name__ == "__main__":
    main()