from sqlalchemy import Column, BigInteger, ForeignKey, String
from typing import List
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import declarative_base, Mapped, mapped_column, relationship

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
    full_name = Column(String(511), nullable=False)                 # Full name (name + last_name)
    sex = Column(String(255), nullable=False)                       # Sex of the person
    email = Column(String(255), nullable=False)                     # Email address of the person
    phone = Column(String(255), nullable=False)                     # Phone number of the person

    # --- Relationships ---

    # One-to-many relationship with Bank
    banks: Mapped[List["Bank"]] = relationship(
        back_populates="person", cascade="all, delete-orphan"
    )
    # One-to-many relationship with Work
    works: Mapped[List["Work"]] = relationship(
        back_populates="person", cascade="all, delete-orphan"
    )
    # One-to-many relationship to the PersonAddress association object
    address_associations: Mapped[List["PersonAddress"]] = relationship(
        back_populates="person", cascade="all, delete-orphan"
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

    # --- Relationships ---

    # One-to-many relationship to the PersonAddress association object
    person_associations: Mapped[List["PersonAddress"]] = relationship(
        back_populates="address", cascade="all, delete-orphan"
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
    address_id: Mapped[int] = mapped_column(ForeignKey("ADDRESS.id"), primary_key=True) # Foreign key to ADDRESS.id
    person_id: Mapped[int] = mapped_column(ForeignKey("PERSON.id"), primary_key=True)   # Foreign key to PERSON.id
    
    # --- Attributes ---
    type: Mapped[str] = mapped_column(String(255), nullable=False) # Type of address (e.g., 'Home', 'Work')

    # --- Relationships ---

    # Many-to-one relationship with Address
    address: Mapped["Address"] = relationship(back_populates="person_associations")
    # Many-to-one relationship with Person
    person: Mapped["Person"] = relationship(back_populates="address_associations")
    
    def __repr__(self):
        return f"PersonAddress(person_id={self.person_id}, address_id={self.address_id}, type='{self.type}')"
    
class Bank(Base):
    """
    Represents bank details associated with a person.
    """
    __tablename__ = 'BANK'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)                           # Unique identifier for the bank record
    person_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("PERSON.id"), nullable=False) # Foreign key to PERSON.id
    iban = Column(String, nullable=False)                                                   # IBAN of the bank account
    salary = Column(String, nullable=False)                                                 # Salary associated with the person

    # --- Relationships ---

    # Many-to-one relationship with Person
    person: Mapped["Person"] = relationship(back_populates="banks")
    
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
    person: Mapped["Person"] = relationship(back_populates="works")

    def __repr__(self):
        return f"Work(id={self.id}, company='{self.company}', person_id={self.person_id})"