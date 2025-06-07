# app/db/session.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.db import Base

# SQLAlchemy database URL
DATABASE_URL =  "postgresql+psycopg2://root:root@localhost:5432/file_parsing"

# Create database engine
engine = create_engine(DATABASE_URL, connect_args={
                       "check_same_thread": False} if "sqlite" in DATABASE_URL else {})

# Create a session factory
SessionLocal = sessionmaker(autoflush=False, bind=engine)

# Create all tables
Base.metadata.create_all(bind=engine)

# Dependency to get a session instance


def get_db():
    """Yield a new database session for dependency injection in FastAPI routes."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
