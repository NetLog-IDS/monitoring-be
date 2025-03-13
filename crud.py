from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from models import NetworkPacket, IntrusionPrediction, NetworkFlow
from schemas import NetworkPacketCreate, IntrusionPredictionCreate
from sqlalchemy.orm import Session
from database import SessionLocal
import time

def create_network_packet(raw: dict):
    raw['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(raw['timestamp']) // 1_000_000))
    # print(raw['timestamp'])
    data = NetworkPacketCreate(**raw)
    db: Session = SessionLocal() 
    try:
        db_packet = NetworkPacket(
            id=data.id,
            timestamp=data.timestamp,
            srcIp = data.layers["network"]["src"],
            srcPort = data.layers["transport"]["src_port"],
            dstIp = data.layers["network"]["dst"],
            dstPort = data.layers["transport"]["dst_port"],
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

def get_network_packets_for_flow(timestamp_start, timestamp_end, srcIp, srcPort, dstIp, dstPort):
    db: Session = SessionLocal() 
    # print(timestamp_start, timestamp_end, srcIp, srcPort, dstIp, dstPort)
    try: 
        return db.query(NetworkPacket).filter(
            NetworkPacket.timestamp >= timestamp_start, NetworkPacket.timestamp <= timestamp_end,
            or_(
                and_(
                    NetworkPacket.srcIp == srcIp,
                    NetworkPacket.srcPort == srcPort,
                    NetworkPacket.dstIp == dstIp,
                    NetworkPacket.dstPort == dstPort
                ),
                and_(
                    NetworkPacket.srcIp == dstIp,
                    NetworkPacket.srcPort == dstPort,
                    NetworkPacket.dstIp == srcIp,
                    NetworkPacket.dstPort == srcPort
                )
            )
        ).order_by(NetworkPacket.timestamp.asc()).all()
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
    db: Session = SessionLocal() 
    try:
        flow = NetworkFlow(
            fid=raw_flow["fid"],
            timestamp=datetime.strptime(raw_flow["timestamp"], "%d/%m/%Y %I:%M:%S %p"),
            srcIp = raw_flow["srcIp"],
            srcPort = raw_flow["srcPort"],
            dstIp = raw_flow["dstIp"],
            dstPort = raw_flow["dstPort"], 
            data = raw_flow
        )
        # print(datetime.strptime(raw_flow["timestamp"], "%d/%m/%Y %I:%M:%S %p")) # datetime.strptime(flow.timestamp, "%d/%m/%Y %H:%M:%S")
        db.add(flow)
        db.commit()
        db.refresh(flow)
        return flow
    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()  

def get_network_flows_with_fid(fid: str = None):
    db: Session = SessionLocal() 
    try:
        return db.query(NetworkFlow).where(NetworkFlow.fid == fid).order_by(NetworkFlow.timestamp.desc()).first()
    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()
