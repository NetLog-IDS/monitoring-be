from database.database import get_database
from datetime import datetime, timezone

db = get_database()
dos_collection = db["dos"]

async def create_dos_intrusion_detection_batch(docs):
    current_time = int(datetime.now(timezone.utc).timestamp())
    for doc in docs:
        doc["MONITORING_TIME_END"] = current_time
        doc['TIME_DIFF_SECONDS'] = doc['MONITORING_TIME_END'] - doc['SNIFF_TIMESTAMP_START']
    
    result = await dos_collection.insert_many(docs)
    return {"inserted_id": str(result.inserted_ids)}

async def get_all_dos_intrusion_results(limit: int = 25, detected: bool = None):
    if detected:
        status = "DETECTED"
        cursor = dos_collection.find({"STATUS": status}).sort("MONITORING_TIME", -1).limit(limit)
    else:
        cursor = dos_collection.find().sort("MONITORING_TIME", -1).limit(limit)
    
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    
    return results





