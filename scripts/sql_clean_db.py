import argparse
from sqlalchemy.exc import SQLAlchemyError

from src.database.sql_alchemy import connect
from src.database.models.sql import Base
from src.core.logger import Logger

log = Logger()

def recreate_database(engine, force=False):
    """
    Drops all tables defined in the SQLAlchemy Base metadata and recreates them.
    This is a destructive operation.

    Args:
        engine: The SQLAlchemy engine instance to use.
        force (bool): If True, bypasses the confirmation prompt.
    """
    if not force:
        log.warning("This will drop all tables and delete all data in the database.")
        confirm = input("Are you sure you want to continue? (y/n) ")
        if confirm.lower() != 'y':
            log.info("Aborted by user.")
            return

    log.info("Dropping all tables...")
    try:
        # Drop all tables defined in the Base metadata
        Base.metadata.drop_all(engine)
        log.info("Recreating all tables from models...")
        # Create all tables defined in the Base metadata
        Base.metadata.create_all(engine)
        log.info("✅ Database has been successfully cleaned and recreated.")
    except SQLAlchemyError as e:
        log.error(f"An error occurred during database recreation: {e}")

def main():
    """
    Main function to parse arguments and trigger database recreation.
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