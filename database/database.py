from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
import os

load_dotenv(override=True)

def get_database():
    CONNECTION_STRING = os.getenv("CONNECTION_STRING")
    client = AsyncIOMotorClient(CONNECTION_STRING)
    return client['network-intrusion-detection']
