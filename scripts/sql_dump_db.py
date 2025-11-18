"""
This script queries the PostgreSQL database to create a denormalized "flat" dataset
of person information. It joins data from the Person, Bank, Work, and Address tables
and exports the final result into a CSV file.
"""
import pandas as pd
import argparse
from sqlalchemy import and_, select
from sqlalchemy.orm import aliased
from sqlalchemy.exc import SQLAlchemyError
from src.database.models.sql import Address, Bank, Person, PersonAddress, Work
from src.database.write_to_postgresql import connect
from src.core.logger import Logger

log = Logger()

def create_person_dataset_query():
    """
    Constructs a SQLAlchemy query to create a denormalized person dataset.

    This function builds a complex query that performs the following joins:
    - Starts with the `Person` table.
    - Performs LEFT OUTER JOINs to `Bank` and `Work` tables on `person_id`.
    - Uses aliased tables (`HomeAddress`, `WorkAddress`) to join to the `Address`
      table twice, once for 'Home' addresses and once for 'Work' addresses,
      through the `PersonAddress` association table. This allows a single person
      record to have both home and work address details in one row.

    All joins are outer joins (`isouter=True`) to ensure that persons are included
    even if they lack bank, work, or address information. Column labels are used
    to prevent name collisions in the final flat structure.

    Returns:
        sqlalchemy.sql.Select: The constructed SELECT statement, ready for execution.
    """
    HomeAddress = aliased(Address)
    WorkAddress = aliased(Address)
    HomeAssociation = aliased(PersonAddress)
    WorkAssociation = aliased(PersonAddress)

    return select(
        Person.passport.label('person_passport'),
        Person.name.label('person_name'),
        Person.last_name.label('person_last_name'),
        Person.full_name.label('person_full_name'),
        Person.sex.label('person_sex'),
        Person.email.label('person_email'),
        Person.phone.label('person_phone'),
        Bank.iban.label('bank_iban'),
        Bank.salary.label('bank_salary'),
        Work.company.label('work_company'),
        Work.position.label('work_position'),
        Work.phone.label('work_phone'),
        Work.email.label('work_email'),
        HomeAddress.street_name.label('home_street'),
        HomeAddress.city.label('home_city'),
        HomeAddress.ip_address.label('home_ip'),
        WorkAddress.street_name.label('work_street'),
        WorkAddress.city.label('work_city')
    ).join(Bank, Person.id == Bank.person_id, isouter=True) \
     .join(Work, Person.id == Work.person_id, isouter=True) \
     .join(
        HomeAssociation, 
        and_(Person.id == HomeAssociation.person_id, HomeAssociation.type == 'Home'),
        isouter=True
    ) \
     .join(
        HomeAddress, 
        HomeAssociation.address_id == HomeAddress.id, 
        isouter=True
    ) \
     .join(
        WorkAssociation, 
        and_(Person.id == WorkAssociation.person_id, WorkAssociation.type == 'Work'),
        isouter=True
    ) \
     .join(
        WorkAddress, 
        WorkAssociation.address_id == WorkAddress.id, 
        isouter=True
    )

def main(output_file):
    """
    Executes the database query and exports the results to a CSV file.

    This function orchestrates the process:
    1. Connects to the database.
    2. Calls `create_person_dataset_query()` to get the SQL statement.
    3. Executes the query and fetches all results.
    4. Converts the results into a pandas DataFrame.
    5. Saves the DataFrame to the specified CSV file.
    """
    session = connect()
    if not session:
        log.critical("Could not establish a database connection. Aborting.")
        return

    try:
        stmt = create_person_dataset_query()
        log.info(f"Executing query to create dataset '{output_file}'...")
        
        cursor_result = session.execute(stmt)
        result = cursor_result.all()
        column_names = cursor_result.keys()

        if result:
            log.info(f"Query returned {len(result)} records.")
        else:
            log.warning("Query returned no data. The output file will be empty.")
        
        df = pd.DataFrame(result, columns=column_names)

        df.to_csv(output_file, index=False)
        log.info(f"Successfully exported {len(df)} records to {output_file}")

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
