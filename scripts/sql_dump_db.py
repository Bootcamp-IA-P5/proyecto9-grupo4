"""
This script queries the PostgreSQL database to create a denormalized "flat" dataset
of person information. It joins data from the Person, Bank, Work, and Address tables
and exports the final result into a CSV file named 'person_dataset.csv'.
"""
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session, aliased
from src.database.models.sql import Address, Bank, Person, PersonAddress, Work
from src.database.sql_alchemy import connect

# Establish a connection to the database to get a session object.
session=connect()

# --- Alias Setup for Self-Referential Joins ---
# We need to join the Person table to the Address table twice:
# 1. To get the 'Home' address.
# 2. To get the 'Work' address.
# To distinguish between these two joins on the same tables (Address and PersonAddress),
# we create aliases for them.
HomeAddress = aliased(Address)
WorkAddress = aliased(Address)
HomeAssociation = aliased(PersonAddress)
WorkAssociation = aliased(PersonAddress)

# --- SQLAlchemy Query Construction ---
# Build a SELECT statement to fetch data from multiple tables.
# .label() is used to give specific, non-conflicting names to the columns in the result set.
stmt = select(
    # Person details
    Person.passport.label('person_passport'),
    Person.name.label('person_name'),
    Person.last_name.label('person_last_name'),
    Person.sex.label('person_sex'),
    Person.email.label('person_email'),
    Person.phone.label('person_phone'),
    # Home address details from the aliased HomeAddress table
    HomeAddress.street_name.label('home_street'),
    HomeAddress.city.label('home_city'),
    HomeAddress.ip_address.label('home_ip'),
    # Bank details
    Bank.iban.label('bank_iban'),
    Bank.salary.label('bank_salary'),
    # Work details
    Work.company.label('work_company'),
    Work.position.label('work_position'),
    Work.phone.label('work_phone'),
    Work.email.label('work_email'),
    # Work address details from the aliased WorkAddress table
    WorkAddress.street_name.label('work_street'),
    WorkAddress.city.label('work_city')
).join(
    Bank, Person.id == Bank.person_id
).join(
    Work, Person.id == Work.person_id
).join(
    # Join for Home Address: Person -> HomeAssociation -> HomeAddress
    HomeAssociation, Person.id == HomeAssociation.person_id
).join(
    HomeAddress, 
    # This condition ensures we are joining the correct address record (type 'Home')
    (HomeAssociation.address_id == HomeAddress.id) & (HomeAssociation.type == 'Home')
).join(
    # Join for Work Address: Person -> WorkAssociation -> WorkAddress
    WorkAssociation, Person.id == WorkAssociation.person_id
).join(
    WorkAddress, 
    # This condition ensures we are joining the correct address record (type 'Work')
    (WorkAssociation.address_id == WorkAddress.id) & (WorkAssociation.type == 'Work')
)

print("Executing query and fetching data...")

# Execute the query and fetch all results into a list of Row objects.
result = session.execute(stmt).all()

# --- DataFrame Creation and CSV Export ---

# Dynamically get the column names (labels) from the select statement.
column_names = [col.name if hasattr(col, 'name') else col.key for col in stmt.selected_columns]
# Create a pandas DataFrame from the query results and the column names.
df = pd.DataFrame(result, columns=column_names)

# Define the output filename and save the DataFrame to a CSV file.
csv_filename = 'person_dataset.csv'
df.to_csv(csv_filename, index=False)

print(f"\n✅ Successfully exported data to {csv_filename}")
print("--- First 5 Rows of the Dataset ---")
print(df.head())