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

async def create_network_packets_batch(docs):
    result = await network_packets_collection.insert_many(docs)
    return {"inserted_id": str(result.inserted_ids)}