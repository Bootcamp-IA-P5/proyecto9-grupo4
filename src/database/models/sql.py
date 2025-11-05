from sqlalchemy import Column, BigInteger, String
from sqlalchemy.ext.declarative import declarative_base

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
    