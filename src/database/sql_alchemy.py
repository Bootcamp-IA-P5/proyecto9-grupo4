from email import message
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

from src.database.models.sql import Address, Bank, Person, PersonAddress, Work
from src.core.logger import Logger

log = Logger()

def connect():
    """
    Establishes a connection to the PostgreSQL database using credentials from environment variables.

    It loads database configuration from a .env file, constructs the connection URL,
    and creates a SQLAlchemy session.

    Returns:
        sqlalchemy.orm.Session: A new session object if the connection is successful, otherwise None.
    """
    load_dotenv()
    log.debug("Connecting to PostgreSQL...")

    USER = os.getenv("SQL_USER")
    PASSWORD = os.getenv("SQL_PASSWORD")
    HOST = os.getenv("SQL_HOST")
    PORT = os.getenv("SQL_PORT")
    DBNAME = os.getenv("SQL_DBNAME")

    DB_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

    try:
        # Create a new database engine
        engine = create_engine(DB_URL)

        # Create a session factory
        Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        log.debug("✓ Connected to PostgreSQL successfully!")
        # Create a new session
        return Session()

    except Exception as e:
        log.error(f"❌ Error connecting: {e}")
        return None
    
def read_from_mongo(session, json):
    """
    Reads JSON data (presumably from MongoDB), transforms it, and populates the PostgreSQL database.

    This function iterates through a list of records, creates corresponding Person, Bank, Work,
    and Address objects, establishes their relationships, and commits them to the database.

    Args:
        session (sqlalchemy.orm.Session): The database session to use for the transaction.
        json (dict): A dictionary containing the data to be processed, expected to have a "Golden" key
                     with a list of records.
    """
    log.info("Starting to read from MongoDB and populate PostgreSQL...")
    # Iterate over each record in the "Golden" key of the input JSON
    records_processed = 0
    records_skipped = 0
    for record in json["Golden"]:
        # Check if a person with this passport ID already exists in the database.
        log.debug(f"Checking for existing person with passport: {record['_id']}")
        existing_person = session.query(Person).filter(Person.passport == record["_id"]).first()
        if existing_person:
            log.warning(f"Skipping existing person with passport: {record['_id']}")
            records_skipped += 1
            continue

        person = Person(
            passport=record["_id"],
            name=record["name"],
            last_name=record["last_name"],
            sex=record["sex"][0],
            email=record["email"],
            phone=record["telfnumber"]
        )
        # Add the person to the session and flush to get the generated primary key (id)
        session.add(person)
        session.flush()
        log.debug(f"Flushed person {person.passport} to get ID: {person.id}")
        person_id = person.id

        # Create a Bank object related to the person
        bank = Bank(
            person_id=person_id,
            iban=record["IBAN"],
            salary=record["salary"]
        )
        
        # Create a Work object related to the person
        work = Work(
            person_id=person_id,
            company=record["company"],
            position=record["job"],
            phone=record["company_telfnumber"],
            email=record["company_email"]
        )

        # Create an Address object for the person's home
        home_address = Address(
            street_name=record["address"],
            city=record["city"],
            ip_address=record["IPv4"]
        )
        
        # Create an Address object for the person's work
        work_address = Address(
            street_name=record["company address"],
            city=record.get("city", "Unknown"), # Use the person's city as a fallback
            ip_address="0.0.0.0"
        )

        # Create the association between the person and their home address
        home_association = PersonAddress(
            address=home_address,
            type='Home'
        )
        
        # Create the association between the person and their work address
        work_association = PersonAddress(
            address=work_address,
            type='Work'
        )

        # Add the address associations to the person's relationship collection
        person.address_associations.extend([home_association, work_association])

        # Add all the newly created objects to the session to be persisted
        session.add_all([bank, work, home_address, work_address, home_association, work_association])
        records_processed += 1

    # Commit the transaction to save all changes to the database
    session.commit()
    log.info("Transaction committed successfully.")
    log.info(f"Summary: {records_processed} new records inserted, {records_skipped} duplicate records skipped.")
    