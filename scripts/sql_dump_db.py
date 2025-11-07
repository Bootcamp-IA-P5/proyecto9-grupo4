"""
This script queries the PostgreSQL database to create a denormalized "flat" dataset
of person information. It joins data from the Person, Bank, Work, and Address tables
and exports the final result into a CSV file named 'person_dataset.csv'.
"""
import pandas as pd
import argparse
from sqlalchemy import select
from sqlalchemy.orm import aliased
from sqlalchemy.exc import SQLAlchemyError
from src.database.models.sql import Address, Bank, Person, PersonAddress, Work
from src.database.sql_alchemy import connect
from src.core.logger import Logger

log = Logger()

def create_person_dataset_query():
    """
    Constructs and returns a SQLAlchemy query to create a denormalized "flat" dataset
    of person information by joining multiple tables.

    Returns:
        sqlalchemy.sql.Select: The constructed SELECT statement.
    """
    # --- Alias Setup for Self-Referential Joins ---
    # We need to join to the Address table twice (for 'Home' and 'Work' addresses).
    # Aliases are used to distinguish between these two joins.
    HomeAddress = aliased(Address)
    WorkAddress = aliased(Address)
    HomeAssociation = aliased(PersonAddress)
    WorkAssociation = aliased(PersonAddress)

    # .label() gives specific, non-conflicting names to the columns in the result set.
    return select(
        Person.passport.label('person_passport'),
        Person.name.label('person_name'),
        Person.last_name.label('person_last_name'),
        Person.sex.label('person_sex'),
        Person.email.label('person_email'),
        Person.phone.label('person_phone'),
        HomeAddress.street_name.label('home_street'),
        HomeAddress.city.label('home_city'),
        HomeAddress.ip_address.label('home_ip'),
        Bank.iban.label('bank_iban'),
        Bank.salary.label('bank_salary'),
        Work.company.label('work_company'),
        Work.position.label('work_position'),
        Work.phone.label('work_phone'),
        Work.email.label('work_email'),
        WorkAddress.street_name.label('work_street'),
        WorkAddress.city.label('work_city')
    ).join(Bank, Person.id == Bank.person_id) \
     .join(Work, Person.id == Work.person_id) \
     .join(HomeAssociation, Person.id == HomeAssociation.person_id) \
     .join(HomeAddress, (HomeAssociation.address_id == HomeAddress.id) & (HomeAssociation.type == 'Home')) \
     .join(WorkAssociation, Person.id == WorkAssociation.person_id) \
     .join(WorkAddress, (WorkAssociation.address_id == WorkAddress.id) & (WorkAssociation.type == 'Work'))

def main(output_file):
    """
    Main function to execute the database query and export the results to a CSV file.
    """
    session = connect()
    if not session:
        log.critical("Could not establish a database connection. Aborting.")
        return

    try:
        stmt = create_person_dataset_query()
        log.info(f"Executing query to create dataset '{output_file}'...")
        
        # Execute the query and get a result object
        cursor_result = session.execute(stmt)
        result = cursor_result.all()
        column_names = cursor_result.keys()

        if result:
            log.info(f"Query returned {len(result)} records.")
        else:
            log.warning("Query returned no data. The output file will be empty.")
        
        # Create a pandas DataFrame from the query results.
        df = pd.DataFrame(result, columns=column_names)

        # Save the DataFrame to a CSV file.
        df.to_csv(output_file, index=False)
        log.info(f"✅ Successfully exported {len(df)} records to {output_file}")

    except SQLAlchemyError as e:
        log.error(f"A database error occurred: {e}")
    except IOError as e:
        log.error(f"An error occurred while writing the file: {e}")
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}")
    finally:
        log.info("Closing database session.")
        session.close()

if __name__ == "__main__":
    DEFAULT_FILENAME = 'person_dataset.csv'
    
    parser = argparse.ArgumentParser(
        description="Query the PostgreSQL database to create a denormalized 'flat' dataset and export it to a CSV file."
    )
    parser.add_argument(
        '--output',
        '-o',
        default=DEFAULT_FILENAME,
        help=f"Path to the output CSV file. Defaults to '{DEFAULT_FILENAME}' in the current directory."
    )
    args = parser.parse_args()

    main(args.output)
