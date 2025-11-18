from sqlalchemy import Column, BigInteger, ForeignKey, String
from typing import List
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Person(Base):
    """
    Represents a person in the database.
    """
    __tablename__ = 'PERSON' 

    id = Column(BigInteger, primary_key=True, autoincrement=True)   # Unique identifier for the person
    passport = Column(String(255), nullable=False)                  # Passport number of the person
    name = Column(String(255), nullable=False)                      # First name of the person
    last_name = Column(String(255), nullable=False)                 # Last name of the person
    full_name = Column(String(255), nullable=False)                 # Full name of the person
    sex = Column(String(255), nullable=False)                       # Sex of the person
    email = Column(String(255), nullable=False)                     # Email address of the person
    phone = Column(String(255), nullable=False)                     # Phone number of the person

    # --- Relationships ---

    # One-to-many relationship with Bank
    banks: List["Bank"] = relationship(
        "Bank", back_populates="person", cascade="all, delete-orphan"
    )
    # One-to-many relationship with Work
    works: List["Work"] = relationship(
        "Work", back_populates="person", cascade="all, delete-orphan"
    )
    # One-to-many relationship to the PersonAddress association object
    address_associations: List["PersonAddress"] = relationship(
        "PersonAddress", back_populates="person", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return (f"Person(id={self.id}, name='{self.name} {self.last_name}', "
                f"passport='{self.passport}')")
    
class Address(Base):
    """
    Represents a physical address in the database.
    """
    __tablename__ = 'ADDRESS'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)   # Unique identifier for the address
    street_name = Column(String(255), nullable=False)               # Street name of the address
    city = Column(String(255), nullable=False)                      # City of the address
    ip_address = Column(String(255), nullable=False)                # Associated IP address

    # --- TEMPORARY COLUMNS FOR BULK INSERT MAPPING ---
    # These fields must be added so SQLAlchemy can map them in Step 1 
    # and query them in Step 2. They can be removed after the migration.
    temp_passport = Column(String(50), nullable=True)
    temp_type = Column(String(10), nullable=True)

    # --- Relationships ---

    # One-to-many relationship to the PersonAddress association object
    person_associations: List["PersonAddress"] = relationship(
        "PersonAddress", back_populates="address", cascade="all, delete-orphan"
    )
    
    def __repr__(self):
        return f"Address(id={self.id}, city='{self.city}', street='{self.street_name}')"

class PersonAddress(Base):
    """
    Association table linking Person and Address.
    This represents a many-to-many relationship, allowing a person to have multiple addresses (e.g., Home, Work)
    and an address to be associated with multiple people.
    """
    __tablename__ = 'PERSON_ADDRESS'
    
    # --- Composite Primary Key ---
    address_id = Column(BigInteger, ForeignKey("ADDRESS.id"), primary_key=True) # Foreign key to ADDRESS.id
    person_id = Column(BigInteger, ForeignKey("PERSON.id"), primary_key=True)   # Foreign key to PERSON.id
    
    # --- Attributes ---
    type = Column(String(255), nullable=False) # Type of address (e.g., 'Home', 'Work')

    # --- Relationships ---

    # Many-to-one relationship with Address
    address = relationship("Address", back_populates="person_associations")
    # Many-to-one relationship with Person
    person = relationship("Person", back_populates="address_associations")
    
    def __repr__(self):
        return f"PersonAddress(person_id={self.person_id}, address_id={self.address_id}, type='{self.type}')"
    
class Bank(Base):
    """
    Represents bank details associated with a person.
    """
    __tablename__ = 'BANK'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)                           # Unique identifier for the bank record
    person_id = Column(BigInteger, ForeignKey("PERSON.id"), nullable=False) # Foreign key to PERSON.id
    iban = Column(String, nullable=False)                                                   # IBAN of the bank account
    salary = Column(String, nullable=False)                                                 # Salary associated with the person

    # --- Relationships ---

    # Many-to-one relationship with Person
    person = relationship("Person", back_populates="banks")
    
    def __repr__(self):
        return f"Bank(id={self.id}, person_id={self.person_id}, iban='{self.iban}')"
    
class Work(Base):
    """
    Represents work/employment details for a person.
    """
    __tablename__ = 'WORK'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)           # Unique identifier for the work record
    person_id = Column(BigInteger, ForeignKey('PERSON.id'), nullable=False) # Foreign key to PERSON.id
    company = Column(String(255), nullable=False)                           # Company name
    position = Column(String(255), nullable=False)                          # Job position/title
    phone = Column(String(255), nullable=False)                             # Work phone number
    email = Column(String(255), nullable=False)                             # Work email address
    
    # --- Relationships ---

    # Many-to-one relationship with Person
    person = relationship("Person", back_populates="works")

    def __repr__(self):
        return f"Work(id={self.id}, company='{self.company}', person_id={self.person_id})"