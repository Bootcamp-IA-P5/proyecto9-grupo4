from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

from src.database.models.sql import Address, Bank, Person, PersonAddress, Work
from src.core.logger import Logger

from src.database.write_to_mongodb import connect_to_mongodb
from scripts.read_from_kafka import MONGO_DATABASE, MONGO_COLLECTION

log = Logger()
id = "_id"
golden = "golden_records"

def connect():
    """
    Establishes a connection to the PostgreSQL database using credentials from environment variables.

    It loads database configuration from a .env file, constructs the connection URL,
    and creates a SQLAlchemy session.

    Returns:
        sqlalchemy.orm.Session or None: A new session object if the connection is successful, 
                                        otherwise None.
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
        engine = create_engine(DB_URL)
        Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        log.debug("✓ Connected to PostgreSQL successfully!")
        return Session()

    except Exception as e:
        log.error(f"❌ Error connecting: {e}")
        return None
    
def read_from_mongo(session, batch_size=1000):
    """
    DEPRECATED: Reads data from MongoDB and inserts it into PostgreSQL record by record.

    This function iterates through records in the 'golden' MongoDB collection, creates
    SQLAlchemy objects for each, and commits them to the database in batches.
    It is inefficient due to the high number of individual database operations
    (especially `session.flush()` per record) and has been replaced by `read_from_mongo_bulk`.

    Args:
        session (sqlalchemy.orm.Session): The database session to use.
        batch_size (int): The number of records to process before a commit.
    """
    mongo, raw_data = connect_to_mongodb(MONGO_DATABASE, MONGO_COLLECTION)
    cursor = mongo[MONGO_DATABASE][golden].find({})

    records_processed = 0
    records_skipped = 0

    log.warning("This method is deprecated. Use read_from_mongo_bulk instead.")
    return

    for record in cursor:
        if record.get(id, None) is None:
            records_skipped += 1
            log.debug("Passport is missing. Skipping record.")
            continue

        sex_raw = record.get("sex", None)
        sex = sex_raw[0] if sex_raw else ""
        person = Person(
            passport=record[id],
            full_name=record.get('fullname', ''),
            name=record.get("name", ""),
            last_name=record.get("last_name", ""),
            sex=sex,
            email=record.get("email", ""),
            phone=record.get("telfnumber", "")
        )
        session.add(person)
        session.flush() 
        person_id = person.id
        
        bank = Bank(
            person_id=person_id,
            iban=record.get("IBAN", ''),
            salary=record.get("salary", "")
        )
        
        work = Work(
            person_id=person_id,
            company=record.get("company", ""),
            position=record.get("job", ""),
            phone=record.get("company_telfnumber", ""),
            email=record.get("company_email", "")
        )
        
        home_address = Address(
            street_name=record.get("address", ""),
            city=record.get("city", ""),
            ip_address=record.get("IPv4", "")
        )
        
        work_address = Address(
            street_name=record.get("company address", ""),
            city=record.get("city", ""),
            ip_address=""
        )

        home_association = PersonAddress(
            address=home_address,
            type='Home'
        )
        
        work_association = PersonAddress(
            address=work_address,
            type='Work'
        )

        person.address_associations.extend([home_association, work_association])

        session.add_all([bank, work, home_address, work_address, home_association, work_association])
        records_processed += 1

        if records_processed % batch_size == 0:
            session.commit()
            log.info(f"Committed batch of {batch_size} records. Total processed: {records_processed}")

    session.commit()
    log.info(f"Final commit. Summary: {records_processed} new records processed. {records_skipped} records skipped.")


def read_from_mongo_json(session, json):
    """
    Reads JSON data and populates the PostgreSQL database.

    This function is designed for loading data from a JSON object, which typically
    simulates a data structure fetched from MongoDB for testing or specific file-based loads.
    It iterates through records, checks for existing persons by passport to avoid duplicates,
    and inserts new data.

    Args:
        session (sqlalchemy.orm.Session): The database session to use for the transaction.
        json (dict): A dictionary containing the data, expected to have a "Golden" key
                     with a list of records.
    """
    log.info("Starting to read from MongoDB and populate PostgreSQL...")

    records_processed = 0
    records_skipped = 0
    for record in json["Golden"]:
        log.debug(f"Checking for existing person with passport: {record[id]}")
        existing_person = session.query(Person).filter(Person.passport == record[id]).first()
        if existing_person:
            log.warning(f"Skipping existing person with passport: {record[id]}")
            records_skipped += 1
            continue

        person = Person(
            passport=record[id],
            name=record["name"],
            last_name=record["last_name"],
            sex=record["sex"][0],
            email=record["email"],
            phone=record["telfnumber"]
        )
        session.add(person)
        session.flush()
        log.debug(f"Flushed person {person.passport} to get ID: {person.id}")
        person_id = person.id

        bank = Bank(
            person_id=person_id,
            iban=record["IBAN"],
            salary=record["salary"]
        )
        work = Work(
            person_id=person_id,
            company=record["company"],
            position=record["job"],
            phone=record["company_telfnumber"],
            email=record["company_email"]
        )

        home_address = Address(
            street_name=record["address"],
            city=record["city"],
            ip_address=record["IPv4"]
        )
        work_address = Address(
            street_name=record["company address"],
            city=record.get("city", "Unknown"), 
            ip_address="0.0.0.0"
        )

        home_association = PersonAddress(
            address=home_address,
            type='Home'
        )
        work_association = PersonAddress(
            address=work_address,
            type='Work'
        )

        person.address_associations.extend([home_association, work_association])

        session.add_all([bank, work, home_address, work_address, home_association, work_association])
        records_processed += 1

    session.commit()
    log.info("Transaction committed successfully.")
    log.info(f"Summary: {records_processed} new records inserted, {records_skipped} duplicate records skipped.")


def prepare_data_for_bulk(cursor):
    """
    Prepares data from a MongoDB cursor for a two-step bulk insertion process.

    This function iterates through MongoDB records and transforms them into lists
    of dictionaries suitable for `bulk_insert_mappings`. It separates data into
    `people_data` (for the Person table) and `address_data` (for the Address table).
    Crucially, it adds temporary `temp_passport` and `temp_type` fields to the
    address dictionaries to allow linking them back to the correct person after
    their primary keys have been generated.

    Args:
        cursor (pymongo.cursor.Cursor): A cursor for the MongoDB collection to process.

    Returns:
        tuple: A tuple containing (people_data, address_data, records_processed).
    """
    people_data = []
    address_data = []
    records_skipped = 0
    records_processed = 0

    for record in cursor:
        if record.get(id, None) is None:
            records_skipped += 1
            log.debug("Passport is missing. Skipping record.")
            continue
        passport = record[id]
        
        sex_raw = record.get("sex", None)
        sex = sex_raw[0] if sex_raw else ""
        
        people_data.append({
            'passport': passport,
            'full_name': record.get("fullname") if record.get("fullname") is not None else "",
            'name': record.get("name") if record.get("name") is not None else "",
            'last_name': record.get("last_name") if record.get("last_name") is not None else "",
            'sex': sex,
            'email': record.get("email") if record.get("email") is not None else "",
            'phone': record.get("telfnumber") if record.get("telfnumber") is not None else "",
        })

        street_name = record.get("address") if record.get("address") is not None else ""
        city = record.get("city") if record.get("city") is not None else ""
        ip_address = record.get("IPv4") if record.get("IPv4") is not None else ""
        if street_name or city or ip_address:
            address_data.append({
                'street_name': street_name.replace('\n', ' ').replace('\r', ' ').strip(),
                'city': city,
                'ip_address': ip_address,
                'temp_passport': passport, 
                'temp_type': 'Home'
            })
        
        street_name = record.get("company address") if record.get("company address") is not None else ""
        if street_name:
            address_data.append({
                'street_name': street_name.replace('\n', ' ').replace('\r', ' ').strip(),
                'city': "",
                'ip_address': "",
                'temp_passport': passport,
                'temp_type': 'Work'
            })
        
        records_processed += 1
        
    log.info(f"Data preparation complete. Processed {records_processed} records.")
    return people_data, address_data, records_processed


def bulk_insert_people_and_addresses(session, people_data, address_data, batch_size=5000):
    """
    Performs Step 1 of the bulk load: inserting independent records.

    This function takes the prepared lists of person and address data and uses
    `bulk_insert_mappings` to efficiently insert them into their respective tables.
    This step populates the tables that do not have foreign key dependencies on
    other tables being created in this process.

    Args:
        session (sqlalchemy.orm.Session): The database session.
        people_data (list): A list of dictionaries for Person records.
        address_data (list): A list of dictionaries for Address records.
        batch_size (int): The number of records to insert per bulk operation.
    """
    log.info(f"Starting Step 1: Bulk inserting {len(people_data)} People and {len(address_data)} Addresses.")

    for i in range(0, len(people_data), batch_size):
        batch = people_data[i:i + batch_size]
        session.bulk_insert_mappings(Person, batch)
        
    for i in range(0, len(address_data), batch_size):
        batch = address_data[i:i + batch_size]
        session.bulk_insert_mappings(Address, batch)
        
    session.commit()
    log.info("Step 1 commit successful.")


def bulk_insert_dependents(session, cursor_original_data):
    """
    Performs Step 2 of the bulk load: inserting dependent records.

    This function first queries the database to create mapping dictionaries that link
    business keys (like passport) to their newly generated primary keys (IDs).
    It then re-iterates the original MongoDB data, using these maps to look up the
    correct foreign keys. Finally, it prepares and bulk-inserts the data for the
    dependent tables (Bank, Work, PersonAddress).

    Args:
        session (sqlalchemy.orm.Session): The database session.
        cursor_original_data (list): The original list of records from MongoDB,
                                     used to build the dependent objects.
    """
    log.info("Starting Step 2: Retrieving IDs and inserting dependents.")

    person_map = {
        p.passport: p.id for p in session.query(Person.id, Person.passport).all()
    }

    address_map = {}
    address_results = session.query(
        Address.id, 
        Address.temp_passport, 
        Address.temp_type
    ).all()

    for a in address_results:
        address_map[(a.temp_passport, a.temp_type)] = a.id
        
    bank_data = []
    work_data = []
    person_address_data = []
    
    for record in cursor_original_data:
        passport = record.get(id)
        if not passport: continue
        
        person_id = person_map.get(passport)
        if not person_id: 
            log.warning(f"Could not find ID for person with passport {passport}. Skipping dependents.")
            continue
            
        home_address_id = address_map.get((passport, 'Home'))
        work_address_id = address_map.get((passport, 'Work'))

        iban = record.get("IBAN") if record.get("IBAN") is not None else ""
        salary = record.get("salary") if record.get("salary") is not None else ""
        if iban or salary:
            bank_data.append({
                'person_id': person_id,
                'iban': iban,
                'salary': salary,
            })

        company = record.get("company") if record.get("company") is not None else ""
        position = record.get("job") if record.get("job") is not None else ""
        phone = record.get("company_telfnumber") if record.get("company_telfnumber") is not None else ""
        email = record.get("company_email") if record.get("company_email") is not None else ""
        if company or position or phone or email:
            work_data.append({
                'person_id': person_id,
                'company': company,
                'position': position,
                'phone': phone,
                'email': email,
            })
        
        if home_address_id:
            person_address_data.append({
                'person_id': person_id,
                'address_id': home_address_id,
                'type': 'Home'
            })
            
        if work_address_id:
            person_address_data.append({
                'person_id': person_id,
                'address_id': work_address_id,
                'type': 'Work'
            })
            
    log.info(f"Inserting {len(bank_data)} Banks, {len(work_data)} Works, {len(person_address_data)} Associations.")
    
    session.bulk_insert_mappings(Bank, bank_data)
    session.bulk_insert_mappings(Work, work_data)
    session.bulk_insert_mappings(PersonAddress, person_address_data)
    
    session.commit()
    log.info("Step 2 commit successful.")

def read_from_mongo_bulk(session):
    """
    Orchestrates the entire high-performance bulk loading process from MongoDB to PostgreSQL.

    This is the main function for bulk data transfer. It performs the following steps:
    1. Fetches all relevant records from MongoDB into a list.
    2. Calls `prepare_data_for_bulk` to transform the data.
    3. Calls `bulk_insert_people_and_addresses` to insert independent records.
    4. Calls `bulk_insert_dependents` to insert records with foreign key relationships.

    Args:
        session (sqlalchemy.orm.Session): The database session to use.
    """
    mongo, _ = connect_to_mongodb(MONGO_DATABASE, MONGO_COLLECTION)
    cursor = mongo[MONGO_DATABASE][golden].find({"data_sources": { "$size": 5}})
    cursor_list = list(cursor)
    people_data, address_data, records_processed = prepare_data_for_bulk(cursor_list)
    bulk_insert_people_and_addresses(session, people_data, address_data)
    bulk_insert_dependents(session, cursor_list)

    log.info(f"Process finished. Total records processed: {records_processed}")

def drop_temp_columns(session):
    """
    Drops the temporary columns from the 'ADDRESS' table used during bulk insertion.

    This function executes a raw SQL `ALTER TABLE` command to remove the
    `temp_passport` and `temp_type` columns. It should be run after the bulk
    load process is complete to clean up the schema.

    Args:
        session (sqlalchemy.orm.Session): The database session.
    """
    try:
        SCHEMA_NAME = "public"

        connection = session.connection()
        sql_command = f'ALTER TABLE "{SCHEMA_NAME}"."ADDRESS" DROP COLUMN temp_passport, DROP COLUMN temp_type;'
        connection.execute(text(sql_command))
        session.commit()
        
        print("Successfully dropped 'temp_passport' and 'temp_type' using direct SQL execution.")

    except Exception as e:
        session.rollback()
        print(f"Error executing DDL: {e}")

    finally:
        # Always close the session
        session.close()