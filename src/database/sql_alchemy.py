from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

from src.database.models.sql import Person

def connect():
    # Load environment variables from .env
    load_dotenv()

    # Fetch variables
    USER = os.getenv("SQL_USER")
    PASSWORD = os.getenv("SQL_PASSWORD")
    HOST = os.getenv("SQL_HOST")
    PORT = os.getenv("SQL_PORT")
    DBNAME = os.getenv("SQL_DBNAME")

    # Construct the SQLAlchemy connection string
    DB_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

    try:
        # Create engine
        engine = create_engine(DB_URL)

        Session = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        return Session()

    except Exception as e:
        print(f"❌ Error connecting: {e}")
        return None
    
# Example. This is how to add to the table
# new_person = Person(
#     passport="B9876543", 
#     name="María", 
#     last_name="Gómez", 
#     sex="F", 
#     email="maria.gomez@ejemplo.com", 
#     phone="+987654321"
# )
# session.add(new_person)
# session.commit()
# session = connect()

# Example. How to read from the database
# people = session.query(Person).all()

# for person in people:
#     print(f"{person.last_name}, {person.name}")

# person=session.get(Person,1)
# print(person)
