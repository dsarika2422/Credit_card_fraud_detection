from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import DATABASE_URL

Base = declarative_base()

class Transaction(Base):
    __tablename__ = 'transactions'

    id = Column(Integer, primary_key=True)
    transaction_id = Column(String, unique=True)
    card_id = Column(String)
    amount = Column(Float)
    merchant = Column(String)
    country = Column(String)
    timestamp = Column(DateTime)
    transaction_type = Column(String)

class FraudPrediction(Base):
    __tablename__ = 'fraud_predictions'

    id = Column(Integer, primary_key=True)
    transaction_id = Column(String, unique=True)
    risk_score = Column(Float)
    fraud_label = Column(Boolean)

def setup_database():
    """
    Create database tables
    """
    engine = create_engine(DATABASE_URL, echo=False)

    # Create all tables
    Base.metadata.create_all(engine)

    print("Database tables created successfully!")

    return engine

def get_session():
    """
    Get database session
    """
    engine = create_engine(DATABASE_URL, echo=False)
    Session = sessionmaker(bind=engine)
    return Session()

if __name__ == "__main__":
    setup_database()