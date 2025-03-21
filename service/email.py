from database.database import get_database

db = get_database()
email_collection = db["email_subscription"]

async def create_email_subscription(email: str):
    data = {"email": email}
    result = await email_collection.insert_one(data)
    return {"inserted_id": str(result.inserted_id)}

async def get_email_subscriptions():
    cursor = email_collection.find().sort("_id", -1)
    
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    
    return results

async def delete_email_subscription(email: str):
    result = await email_collection.delete_many({"email": email})
    return result.deleted_count