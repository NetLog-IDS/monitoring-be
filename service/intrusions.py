from database.database import get_database

db = get_database()
intrusion_collection = db["intrusion"]

async def create_intrusion_detection_batch(docs):
    result = await intrusion_collection.insert_many(docs)
    return {"inserted_id": str(result.inserted_ids)}

async def create_intrusion_detection_result(raw: dict, topic: str):
    data = raw.copy()
    data["topic"] = topic
    result = await intrusion_collection.insert_one(data)
    return {"inserted_id": str(result.inserted_id)}

async def get_intrusion_detection_results(limit: int = 10):
    cursor = intrusion_collection.find().sort("_id", -1).limit(limit)
    
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    
    return results

async def get_all_intrusion_results(limit: int = 25, detected: bool = None):
    if detected:
        status = "DETECTED"
        cursor = intrusion_collection.find({"STATUS": status}).sort("MONITORING_TIME", -1).limit(limit)
    else:
        cursor = intrusion_collection.find().sort("MONITORING_TIME", -1).limit(limit)
    
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    
    return results

async def delete_all_intrusion_results():
    await intrusion_collection.delete_many({})


