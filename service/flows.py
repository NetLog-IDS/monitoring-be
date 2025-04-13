from database.database import get_database

db = get_database()
network_flows_collection = db["network_flows"]

async def create_network_flows(data: dict):
    result = await network_flows_collection.insert_one(data)
    return {"inserted_id": str(result.inserted_id)}

async def get_network_flows(limit: int = 10):
    cursor = network_flows_collection.find().sort("_id", -1)
    
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    
    return results

async def create_network_flows_batch(docs):
    result = await network_flows_collection.insert_many(docs)
    return {"inserted_id": str(result.inserted_ids)}