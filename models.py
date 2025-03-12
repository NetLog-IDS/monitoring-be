from sqlalchemy import Column, String, TIMESTAMP, Integer, JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import declarative_base
from database import Base

Base = declarative_base()

class NetworkPacket(Base):
    __tablename__ = "network_packets"
    uid = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(String)
    timestamp = Column(TIMESTAMP, index=True)
    order = Column(Integer)
    layers = Column(JSON) 

class IntrusionPrediction(Base):
    __tablename__ = "intrusion_predictions"
    uid = Column(Integer, primary_key=True, autoincrement=True)
    prediction = Column(String)
    fid = Column(String)
    timestamp = Column(TIMESTAMP, server_default=func.now(), index=True)

class NetworkFlow(Base):
    __tablename__ = "network_flows"
    uid = Column(Integer, primary_key=True, autoincrement=True)
    fid = Column(String)
    srcIp = Column(String)
    srcPort = Column(Integer)
    dstIp = Column(String)
    dstPort = Column(Integer)
    timestamp = Column(TIMESTAMP, index=True)
    data = Column(JSON)