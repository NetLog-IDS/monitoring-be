from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_
from models import NetworkPacket, NetworkFlow, Email, DosPrediction, PortScanPrediction
from schemas import NetworkPacketCreate, IntrusionPredictionCreate
from sqlalchemy.orm import Session
from database import SessionLocal
import time

def create_network_packet(raw: dict):
    raw['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(raw['timestamp']) // 1_000_000))
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

def create_intrusion_detection_result(raw: dict, topic: str):
    db: Session = SessionLocal() 
    try:
        raw['TIMESTAMP_START'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(raw['TIMESTAMP_START']) // 1_000_000))
        raw['TIMESTAMP_END'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(raw['TIMESTAMP_END']) // 1_000_000))
        if topic == "DOS":
            db_result = DosPrediction(
                ip_src = raw["IP_SRC"],
                status = raw["STATUS"],
                timestamp_start = raw["TIMESTAMP_START"],
                timestamp_end = raw["TIMESTAMP_END"],
            )
            db.add(db_result)
            db.commit()
            db.refresh(db_result)
            return db_result
        elif topic == "PORT_SCAN":
            db_result = PortScanPrediction(
                ip_src = raw["IP_SRC"],
                status = raw["STATUS"],
                timestamp_start = raw["TIMESTAMP_START"],
                timestamp_end = raw["TIMESTAMP_END"],
            )
            db.add(db_result)
            db.commit()
            db.refresh(db_result)
            return db_result
        else:
            raise Exception("Invalid topic")

    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()

def get_intrusion_detection_results(limit: int = 10):
    db: Session = SessionLocal() 
    try:
        dos = db.query(DosPrediction).order_by(DosPrediction.timestamp_start.desc()).limit(limit).all()
        portscan = db.query(PortScanPrediction).order_by(PortScanPrediction.timestamp_start.desc()).limit(limit).all()
        return {"dos": dos, "portscan": portscan}
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
            duration = raw_flow["flowDuration"], 
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

def get_network_flows_with_fid(fid: str = None):
    db: Session = SessionLocal() 
    try:
        return db.query(NetworkFlow).where(NetworkFlow.fid == fid).order_by(NetworkFlow.timestamp.desc()).first()
    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()

def create_email_subscription(email: str):
    db: Session = SessionLocal() 
    try:
        db_email = Email(email=email)
        db.add(db_email)
        db.commit()
        db.refresh(db_email)
        return db_email
    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()

def get_email_subscriptions():
    db: Session = SessionLocal() 
    try:
        return db.query(Email.email).all()
    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()

def delete_email_subscription(email: str):
    db: Session = SessionLocal() 
    try:
        db_email = db.query(Email).where(Email.email == email).first()
        if not db_email:
            raise Exception("Email not found")
        db.delete(db_email)
        db.commit()
        return db_email
    except Exception as e:
        db.rollback()  
        raise e
    finally:
        db.close()
