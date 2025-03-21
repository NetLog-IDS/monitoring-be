from database.database import get_database

db = get_database()
network_packets_collection = db["network_packets"]

async def create_network_packet(data: dict):
    result = await network_packets_collection.insert_one(data)
    return {"inserted_id": str(result.inserted_id)}

async def get_network_packets(limit: int = 10):
    cursor = network_packets_collection.find().sort("_id", -1).limit(limit)
    
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    
    return results

# def get_network_packets_for_flow(timestamp_start, timestamp_end, srcIp, srcPort, dstIp, dstPort):
#     db: Session = SessionLocal() 
#     try: 
#         return db.query(NetworkPacket).filter(
#             NetworkPacket.timestamp >= timestamp_start, NetworkPacket.timestamp <= timestamp_end,
#             or_(
#                 and_(
#                     NetworkPacket.srcIp == srcIp,
#                     NetworkPacket.srcPort == srcPort,
#                     NetworkPacket.dstIp == dstIp,
#                     NetworkPacket.dstPort == dstPort
#                 ),
#                 and_(
#                     NetworkPacket.srcIp == dstIp,
#                     NetworkPacket.srcPort == dstPort,
#                     NetworkPacket.dstIp == srcIp,
#                     NetworkPacket.dstPort == srcPort
#                 )
#             )
#         ).order_by(NetworkPacket.timestamp.asc()).all()
#     except Exception as e:
#         db.rollback()  
#         raise e
#     finally:
#         db.close()