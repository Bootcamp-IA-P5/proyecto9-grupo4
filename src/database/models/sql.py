from sqlalchemy import Column, BigInteger, ForeignKey, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import declarative_base, relationship, Session, sessionmaker

Base = declarative_base()

class Person(Base):
    __tablename__ = 'PERSON' 

    id = Column(BigInteger, primary_key=True, autoincrement=True)   
    passport = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False)
    last_name = Column(String(255), nullable=False)
    sex = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    phone = Column(String(255), nullable=False)

    def __repr__(self):
        return (f"Person(id={self.id}, name='{self.name} {self.last_name}', "
                f"passport='{self.passport}')")
    
class Address(Base):
    __tablename__ = 'ADDRESS'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    street_name = Column(String(255), nullable=False)
    city = Column(String(255), nullable=False)
    ip_address = Column(String(255), nullable=False)

    people_links = relationship("PersonAddress", back_populates="address")
    
    def __repr__(self):
        return f"Address(id={self.id}, city='{self.city}', street='{self.street_name}')"
    
class PersonAddress(Base):
    __tablename__ = 'PERSON_ADDRESS'
    
    address_id = Column(BigInteger, ForeignKey('ADDRESS.id'), primary_key=True)
    person_id = Column(BigInteger, ForeignKey('PERSON.id'), primary_key=True)
    type = Column(String(255), nullable=False)

    address = relationship("Address", back_populates="people_links")
    person = relationship("Person", back_populates="address_links")
    
    def __repr__(self):
        return f"PersonAddress(person_id={self.person_id}, address_id={self.address_id}, type='{self.type}')"
    
class Bank(Base):
    __tablename__ = 'BANK'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    person_id = Column(BigInteger, ForeignKey('PERSON.id'), nullable=False)
    iban = Column(String, nullable=False)
    salary = Column(String, nullable=False)

    # Relación: Múltiples cuentas bancarias pertenecen a UNA Persona
    person = relationship("Person", back_populates="banks")
    
    def __repr__(self):
        return f"Bank(id={self.id}, person_id={self.person_id}, iban='{self.iban}')"
    
class Work(Base):
    __tablename__ = 'WORK'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    person_id = Column(BigInteger, ForeignKey('PERSON.id'), nullable=False) 
    company = Column(String(255), nullable=False)
    position = Column(String(255), nullable=False)
    phone = Column(String(255), nullable=False)
    email = Column(String(255), nullable=False)
    
    person = relationship("Person", back_populates="works")

    def __repr__(self):
        return f"Work(id={self.id}, company='{self.company}', person_id={self.person_id})"