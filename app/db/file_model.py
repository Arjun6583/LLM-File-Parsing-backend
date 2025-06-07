from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey, ARRAY, Float
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship 
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class FileUpload(Base):
    __tablename__ = "file_upload"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    file_name = Column(String(255), nullable=False)
    file_type = Column(String(50), nullable=False)
    uploaded_date = Column(TIMESTAMP(timezone=True), server_default=func.now())

    # Relationships
    processing_log = relationship("FileProcessingLog", back_populates="file_upload", cascade="all, delete-orphan", uselist=False)
    players = relationship("Player", back_populates="file_upload", cascade="all, delete-orphan")  # plural, as you may have many players
    mapped_data = relationship("MappedData", back_populates="file_upload", cascade="all, delete-orphan")


class Player(Base):
    __tablename__ = "player"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    file_id = Column(Integer, ForeignKey("file_upload.id", ondelete="CASCADE"), nullable=False)
    player_name = Column(String(255), nullable=False)
    matches = Column(Integer, nullable=False)
    innings = Column(Integer, nullable=False)

    file_upload = relationship("FileUpload", back_populates="players")
    batting = relationship("Batting", back_populates="player", cascade="all, delete-orphan", uselist=False)
    bowling = relationship("Bowling", back_populates="player", cascade="all, delete-orphan", uselist=False)


class FileProcessingLog(Base):
    __tablename__ = "file_processing_log"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    file_id = Column(Integer, ForeignKey("file_upload.id", ondelete="CASCADE"), nullable=False, unique=True)
    matched_columns = Column(ARRAY(String), nullable=True)
    unmatched_columns = Column(ARRAY(String), nullable=True)
    missing_columns = Column(ARRAY(String), nullable=True)
    total_records = Column(Integer, nullable=True)
    status = Column(String(50), nullable=True)
    file_upload = relationship("FileUpload", back_populates="processing_log")


class MappedData(Base):
    __tablename__ = "mapped_data"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    file_id = Column(Integer, ForeignKey("file_upload.id", ondelete="CASCADE"), nullable=False)
    data = Column(JSONB, nullable=False)

    file_upload = relationship("FileUpload", back_populates="mapped_data")


class Batting(Base):
    __tablename__ = "batting"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("player.id", ondelete="CASCADE"), nullable=False)
    total_runs = Column(Integer, nullable=False)
    average = Column(Float, nullable=False)
    highest_score = Column(String, nullable=False)
    century = Column(Integer, nullable=False)
    half_century = Column(Integer, nullable=False)

    player = relationship("Player", back_populates="batting")


class Bowling(Base):
    __tablename__ = "bowling"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("player.id", ondelete="CASCADE"), nullable=False)
    wickets = Column(Integer, nullable=False)
    five_wickets = Column(Integer, nullable=False)
    best_bowling = Column(String(50), nullable=False)

    player = relationship("Player", back_populates="bowling")
