from database.database import get_database
from datetime import datetime, timezone
from bson import ObjectId

db = get_database()
email_collection = db["email_subscription"]

async def create_email_subscription(email: str):
    data = {"email": email, "last_sent": int(datetime.now(timezone.utc).timestamp())}
    result = await email_collection.insert_one(data)
    return {"inserted_id": str(result.inserted_id)}

async def get_email_subscriptions():
    cursor = email_collection.find().sort("_id", -1)
    
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    
    return results

async def update_email_subscription(email):
    if not email:
        raise ValueError("Email list cannot be empty.")
    
    try:
        object_ids = [ObjectId(e) for e in email]
    except Exception as e:
        raise ValueError(f"Invalid ObjectId format in email list: {email}") from e

    result = await email_collection.update_many(
        {"_id": {"$in": object_ids}},
        {"$set": {"last_sent": int(datetime.now(timezone.utc).timestamp())}}
    )
    
    return result.modified_count

async def delete_email_subscription(email: str):
    result = await email_collection.delete_many({"email": email})
    if(result.deleted_count == 0):
        raise Exception("Email not found")
    return result.deleted_count