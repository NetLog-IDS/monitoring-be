from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from models import NetworkPacket
from schemas import NetworkPacketCreate
from sqlalchemy.orm import Session
from database import SessionLocal

def create_network_packet(raw: dict):
    raw['timestamp'] = datetime.utcfromtimestamp(int(raw['timestamp'])/1_000_000).isoformat()
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
