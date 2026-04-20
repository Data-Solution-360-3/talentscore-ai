"""
database.py — MongoDB connection and CRUD operations for CV Screener
Collections:
  - screenings   : all CV screening results
  - candidates   : unique candidate profiles
  - jobs         : job postings
"""

from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from bson import ObjectId
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("DB_NAME", "talentscore")

client = None
db     = None


async def connect():
    global client, db
    client = AsyncIOMotorClient(MONGO_URI)
    db     = client[DB_NAME]
    # Indexes for fast queries
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


# ─────────────────────────────────────────────────────────────
# SCREENINGS
# ─────────────────────────────────────────────────────────────

async def save_screening(result: dict) -> str:
    """Save a screening result. Returns the inserted document ID."""
    doc = {
        **result,
        "created_at": datetime.utcnow(),
    }
    # Remove non-serializable fields
    doc.pop("_id", None)
    inserted = await db.screenings.insert_one(doc)
    return str(inserted.inserted_id)


async def get_all_screenings(limit: int = 200) -> list:
    """Get all screenings, newest first."""
    cursor = db.screenings.find({}).sort("created_at", -1).limit(limit)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results


async def get_screening_by_id(screening_id: str) -> dict | None:
    """Get a single screening by ID."""
    doc = await db.screenings.find_one({"_id": ObjectId(screening_id)})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


async def get_screening_stats() -> dict:
    """Aggregate stats across all screenings."""
    pipeline = [
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "avg_score": {"$avg": "$overall_score"},
                "avg_coverage": {"$avg": "$skills_coverage_pct"},
                "strong_hires": {
                    "$sum": {"$cond": [{"$eq": ["$recommendation", "STRONG HIRE"]}, 1, 0]}
                },
                "hires": {
                    "$sum": {"$cond": [{"$eq": ["$recommendation", "HIRE"]}, 1, 0]}
                },
                "maybes": {
                    "$sum": {"$cond": [{"$eq": ["$recommendation", "MAYBE"]}, 1, 0]}
                },
                "rejects": {
                    "$sum": {"$cond": [{"$eq": ["$recommendation", "REJECT"]}, 1, 0]}
                },
            }
        }
    ]
    results = await db.screenings.aggregate(pipeline).to_list(1)
    if not results:
        return {
            "total": 0, "avg_score": 0, "avg_coverage": 0,
            "strong_hires": 0, "hires": 0, "maybes": 0, "rejects": 0
        }
    stats = results[0]
    stats.pop("_id", None)
    stats["avg_score"] = round(stats["avg_score"] or 0, 1)
    stats["avg_coverage"] = round(stats["avg_coverage"] or 0, 1)
    return stats


async def get_skills_gap_frequency() -> list:
    """Return top missing skills across all screenings."""
    pipeline = [
        {"$unwind": "$critical_gaps"},
        {"$group": {"_id": "$critical_gaps", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    results = await db.screenings.aggregate(pipeline).to_list(10)
    return [{"skill": r["_id"], "count": r["count"]} for r in results]


async def get_dimension_averages() -> list:
    """Return average score per dimension across all screenings."""
    pipeline = [
        {"$unwind": "$dimensions"},
        {
            "$group": {
                "_id": "$dimensions.name",
                "avg_score": {"$avg": "$dimensions.score"},
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"avg_score": -1}}
    ]
    results = await db.screenings.aggregate(pipeline).to_list(20)
    return [{"name": r["_id"], "avg_score": round(r["avg_score"], 1), "count": r["count"]} for r in results]


async def delete_screening(screening_id: str) -> bool:
    result = await db.screenings.delete_one({"_id": ObjectId(screening_id)})
    return result.deleted_count > 0


# ─────────────────────────────────────────────────────────────
# JOBS
# ─────────────────────────────────────────────────────────────

async def save_job(job: dict) -> str:
    doc = {**job, "created_at": datetime.utcnow(), "candidates_count": 0, "active": True}
    doc.pop("_id", None)
    inserted = await db.jobs.insert_one(doc)
    return str(inserted.inserted_id)


async def get_all_jobs() -> list:
    cursor = db.jobs.find({"active": True}).sort("created_at", -1)
    jobs = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        jobs.append(doc)
    return jobs


async def delete_job(job_id: str) -> bool:
    result = await db.jobs.update_one(
        {"_id": ObjectId(job_id)},
        {"$set": {"active": False}}
    )
    return result.modified_count > 0


async def increment_job_candidates(job_id: str):
    await db.jobs.update_one(
        {"_id": ObjectId(job_id)},
        {"$inc": {"candidates_count": 1}}
    )


# ─────────────────────────────────────────────────────────────
# BATCH JOBS
# ─────────────────────────────────────────────────────────────

async def create_batch_job(total: int, jd_preview: str) -> str:
    """Create a batch job record. Returns batch_id."""
    doc = {
        "total": total,
        "completed": 0,
        "succeeded": 0,
        "failed": 0,
        "status": "running",
        "jd_preview": jd_preview[:200],
        "progress": [],
        "created_at": __import__("datetime").datetime.utcnow(),
    }
    inserted = await db.batch_jobs.insert_one(doc)
    return str(inserted.inserted_id)


async def update_batch_progress(batch_id: str, index: int, status: str, filename: str, score=None, recommendation=None, error=None):
    """Update progress for one CV in a batch job."""
    from bson import ObjectId
    entry = {"index": index, "status": status, "filename": filename}
    if score is not None:      entry["score"] = score
    if recommendation:         entry["recommendation"] = recommendation
    if error:                  entry["error"] = error

    inc = {"completed": 1}
    if status == "done":    inc["succeeded"] = 1
    if status == "failed":  inc["failed"] = 1

    await db.batch_jobs.update_one(
        {"_id": ObjectId(batch_id)},
        {
            "$inc": inc,
            "$push": {"progress": entry},
        }
    )


async def finish_batch_job(batch_id: str, summary: dict):
    """Mark a batch job as finished."""
    from bson import ObjectId
    await db.batch_jobs.update_one(
        {"_id": ObjectId(batch_id)},
        {"$set": {
            "status": "done",
            "finished_at": __import__("datetime").datetime.utcnow(),
            "succeeded": summary.get("succeeded", 0),
            "failed": summary.get("failed", 0),
        }}
    )


async def get_batch_job(batch_id: str) -> dict | None:
    """Get a batch job by ID."""
    from bson import ObjectId
    doc = await db.batch_jobs.find_one({"_id": ObjectId(batch_id)})
    if doc:
        doc["_id"] = str(doc["_id"])
        doc["created_at"] = str(doc.get("created_at", ""))
    return doc


async def get_all_batch_jobs(limit: int = 50) -> list:
    """Get all batch jobs newest first."""
    cursor = db.batch_jobs.find({}).sort("created_at", -1).limit(limit)
    jobs = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        doc["created_at"] = str(doc.get("created_at", ""))
        doc.pop("progress", None)  # Don't send full progress list in summary
        jobs.append(doc)
    return jobs
