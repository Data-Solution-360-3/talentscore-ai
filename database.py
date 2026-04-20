from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from bson import ObjectId
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "talentscore")

client = None
db = None


async def connect():
    global client, db
    client = AsyncIOMotorClient(
        MONGO_URI,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=30000,
        tls=True,
        tlsAllowInvalidCertificates=True
    )
    db = client[DB_NAME]
    await db.screenings.create_index("created_at")
    await db.screenings.create_index("recommendation")
    await db.screenings.create_index("overall_score")
    await db.jobs.create_index("created_at")
    print(f"[DB] Connected to MongoDB — database: {DB_NAME}")


async def disconnect():
    global client
    if client:
        client.close()
        print("[DB] Disconnected from MongoDB")