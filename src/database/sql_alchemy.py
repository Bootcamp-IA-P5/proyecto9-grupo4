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


def read_from_mongo_collection(session, mongo_collection):
    """
    Reads golden records directly from a MongoDB collection and populates the PostgreSQL database.

    This is the preferred method for production use - it reads directly from MongoDB's golden_records
    collection instead of requiring a pre-formatted JSON structure.

    Args:
        session (sqlalchemy.orm.Session): The database session to use for the transaction.
        mongo_collection: PyMongo collection object pointing to the golden_records collection.
    """
    log.info("Starting to read from MongoDB golden collection and populate PostgreSQL...")
    records_processed = 0
    records_skipped = 0
    records_with_errors = 0
    records_incomplete = 0  # Track records without all 5 schemas
    successfully_processed_passports = []  # Track passports for MongoDB update
    
    # Define the 5 required schema types for complete records
    REQUIRED_SCHEMAS = {'personal', 'bank', 'location', 'professional', 'network'}
    
    # Query filter: only records with all 5 schemas AND not yet uploaded
    # Note: $ne will match both False and missing field (null)
    query_filter = {
        'uploaded_2_SQL': {'$ne': True},  # Match False or missing field
        'data_sources': {'$all': list(REQUIRED_SCHEMAS)}  # Must contain all 5 schema types
    }
    
    # Get total count for progress tracking
    total_records = mongo_collection.count_documents(query_filter)
    log.info(f"Found {total_records} complete golden records to process (with all 5 schemas, not yet uploaded)")
    
    for idx, record in enumerate(mongo_collection.find(query_filter), 1):
        if idx % 100 == 0:
            log.info(f"Progress: {idx}/{total_records} records processed")
        
        try:
            # Check if a person with this passport ID already exists in the database
            passport = record.get('_id') or record.get('passport')
            if not passport:
                log.warning(f"Skipping record without passport: {record}")
                records_with_errors += 1
                continue
            
            log.debug(f"Checking for existing person with passport: {passport}")
            existing_person = session.query(Person).filter(Person.passport == passport).first()
            if existing_person:
                log.warning(f"Skipping existing person with passport: {passport}")
                records_skipped += 1
                continue

            # Validate required fields
            name = record.get("name")
            last_name = record.get("last_name")
            if not name or not last_name:
                log.warning(f"Skipping record with missing name/last_name: {passport}")
                records_with_errors += 1
                continue
            
            # Handle sex field (might be array or string)
            sex_value = record.get("sex")
            if isinstance(sex_value, list):
                sex = sex_value[0] if sex_value else None
            else:
                sex = sex_value
            
            # Validate sex is not None (NOT NULL constraint in database)
            if not sex:
                log.warning(f"Skipping record {passport}: missing required 'sex' field")
                records_with_errors += 1
                continue
            
            # Calculate full_name by combining name and last_name
            full_name = f"{name} {last_name}"
            
            # Create Person object
            person = Person(
                passport=passport,
                name=name,
                last_name=last_name,
                full_name=full_name,
                sex=sex,
                email=record.get("email"),
                phone=record.get("telfnumber")
            )
            
            # Add the person to the session and flush to get the generated primary key (id)
            session.add(person)
            session.flush()
            log.debug(f"Flushed person {person.passport} to get ID: {person.id}")
            person_id = person.id

            # Create a Bank object related to the person (if bank data exists)
            if record.get("IBAN") or record.get("salary"):
                bank = Bank(
                    person_id=person_id,
                    iban=record.get("IBAN"),
                    salary=record.get("salary")
                )
                session.add(bank)
            
            # Create a Work object related to the person (if work data exists)
            if record.get("company"):
                work = Work(
                    person_id=person_id,
                    company=record.get("company"),
                    position=record.get("job"),
                    phone=record.get("company_telfnumber"),
                    email=record.get("company_email")
                )
                session.add(work)

            # Create an Address object for the person's home (if address exists)
            if record.get("address"):
                # Handle IPv4 field - use default if None or missing
                ipv4_value = record.get("IPv4")
                home_ip = ipv4_value if ipv4_value else "0.0.0.0"
                
                home_address = Address(
                    street_name=record.get("address"),
                    city=record.get("city", "Unknown"),
                    ip_address=home_ip
                )
                
                # Create the association between the person and their home address
                home_association = PersonAddress(
                    address=home_address,
                    type='Home'
                )
                person.address_associations.append(home_association)
                session.add_all([home_address, home_association])
            
            # Create an Address object for the person's work (if company address exists)
            if record.get("company address"):
                # Only create work address if we have city data (NOT NULL constraint)
                work_city = record.get("city")
                if work_city:  # Skip work address if city is None
                    work_address = Address(
                        street_name=record.get("company address"),
                        city=work_city,
                        ip_address="0.0.0.0"
                    )
                    
                    # Create the association between the person and their work address
                    work_association = PersonAddress(
                        address=work_address,
                        type='Work'
                    )
                    person.address_associations.append(work_association)
                    session.add_all([work_address, work_association])

            # Mark as successfully processed (will be committed if transaction succeeds)
            successfully_processed_passports.append(passport)
            records_processed += 1
            
        except Exception as e:
            log.error(f"Error processing record {passport}: {e}")
            records_with_errors += 1
            # Rollback the session to clear the error state
            session.rollback()
            # Continue processing other records

    # Commit the transaction to save all changes to the database
    try:
        session.commit()
        log.info("Transaction committed successfully.")
        
        # After successful commit, update MongoDB to mark records as uploaded
        if successfully_processed_passports:
            log.info(f"Updating {len(successfully_processed_passports)} MongoDB golden records to mark as uploaded...")
            
            # Bulk update MongoDB records to set uploaded_2_SQL = True
            from pymongo import UpdateMany
            update_result = mongo_collection.update_many(
                {'_id': {'$in': successfully_processed_passports}},
                {'$set': {'uploaded_2_SQL': True}}
            )
            log.info(f"✓ Marked {update_result.modified_count} golden records as uploaded in MongoDB")
        
        log.info(f"Summary: {records_processed} new records inserted, {records_skipped} duplicate records skipped, {records_with_errors} records with errors.")
    except Exception as e:
        session.rollback()
        log.error(f"Error committing transaction: {e}")
        raise