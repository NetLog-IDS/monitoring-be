from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from models import NetworkPacket, IntrusionPrediction, NetworkFlow
from schemas import NetworkPacketCreate, IntrusionPredictionCreate
from sqlalchemy.orm import Session
from database import SessionLocal
import time

def create_network_packet(raw: dict):
    # raw['timestamp'] = datetime.utcfromtimestamp(int(raw['timestamp']))
    print(raw['timestamp'], int(raw['timestamp']) // 1_000_000)
    raw['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(raw['timestamp']) // 1_000_000))
    print(raw['timestamp'])
    data = NetworkPacketCreate(**raw)
    # print(data)
    db: Session = SessionLocal() 
    try:
        db_packet = NetworkPacket(
            id=data.id,
            timestamp=data.timestamp,
            order=data.order,
            layers=data.layers
        )
        db.add(db_packet)
        db.commit()
        db.refresh(db_packet)
        return db_packet
    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()  

def get_network_packets(limit: int = 10):
    db: Session = SessionLocal() 
    try:
        return db.query(NetworkPacket).order_by(NetworkPacket.timestamp.desc()).limit(limit).all()
    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()

def create_intrusion_detection_result(raw: dict):
    data = IntrusionPredictionCreate(**raw)
    db: Session = SessionLocal() 
    try:
        db_result = IntrusionPrediction(
            prediction=data.prediction,
            fid=data.fid
        )
        db.add(db_result)
        db.commit()
        db.refresh(db_result)
        return db_result
    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()

def get_intrusion_detection_results(limit: int = 10):
    db: Session = SessionLocal() 
    try:
        return db.query(IntrusionPrediction.prediction, IntrusionPrediction.fid).order_by(IntrusionPrediction.timestamp.desc()).limit(limit).all()
    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()

def create_network_flows(raw_flow: dict):
    print(raw_flow)
    db: Session = SessionLocal() 
    try:
        flow = NetworkFlow(
            fid=raw_flow["fid"],
            timestamp=raw_flow["timestamp"],
            srcIp = raw_flow["srcIp"],
            srcPort = raw_flow["srcPort"],
            dstIp = raw_flow["dstIp"],
            dstPort = raw_flow["dstPort"], 
            data = raw_flow
        )
        db.add(flow)
        db.commit()
        db.refresh(flow)
        return flow
    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()  

def get_network_flows(limit: int = 10):
    db: Session = SessionLocal() 
    try:
        return db.query(NetworkFlow).where(NetworkFlow.timestamp < datetime.now()).order_by(NetworkFlow.timestamp.desc()).limit(limit).all()
    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()
