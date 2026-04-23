from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from bson import ObjectId
import os
import ssl
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("DB_NAME", "talentscore")

client = None
db     = None


async def connect():
    global client, db
    client = AsyncIOMotorClient(
        MONGO_URI,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=30000,
        tlsAllowInvalidCertificates=True,
        tlsAllowInvalidHostnames=True,
    )
    db = client[DB_NAME]
    await client.admin.command("ping")
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


async def save_screening(result: dict) -> str:
    doc = {**result, "created_at": datetime.utcnow()}
    doc.pop("_id", None)
    inserted = await db.screenings.insert_one(doc)
    return str(inserted.inserted_id)


async def get_all_screenings(limit: int = 200) -> list:
    cursor = db.screenings.find({}).sort("created_at", -1).limit(limit)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results


async def get_screening_by_id(screening_id: str) -> dict | None:
    doc = await db.screenings.find_one({"_id": ObjectId(screening_id)})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


async def get_screening_stats() -> dict:
    pipeline = [
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "avg_score": {"$avg": "$overall_score"},
                "avg_coverage": {"$avg": "$skills_coverage_pct"},
                "strong_hires": {"$sum": {"$cond": [{"$eq": ["$recommendation", "STRONG HIRE"]}, 1, 0]}},
                "hires": {"$sum": {"$cond": [{"$eq": ["$recommendation", "HIRE"]}, 1, 0]}},
                "maybes": {"$sum": {"$cond": [{"$eq": ["$recommendation", "MAYBE"]}, 1, 0]}},
                "rejects": {"$sum": {"$cond": [{"$eq": ["$recommendation", "REJECT"]}, 1, 0]}},
            }
        }
    ]
    results = await db.screenings.aggregate(pipeline).to_list(1)
    if not results:
        return {"total": 0, "avg_score": 0, "avg_coverage": 0,
                "strong_hires": 0, "hires": 0, "maybes": 0, "rejects": 0}
    stats = results[0]
    stats.pop("_id", None)
    stats["avg_score"] = round(stats["avg_score"] or 0, 1)
    stats["avg_coverage"] = round(stats["avg_coverage"] or 0, 1)
    return stats


async def get_skills_gap_frequency() -> list:
    pipeline = [
        {"$unwind": "$critical_gaps"},
        {"$group": {"_id": "$critical_gaps", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    results = await db.screenings.aggregate(pipeline).to_list(10)
    return [{"skill": r["_id"], "count": r["count"]} for r in results]


async def get_dimension_averages() -> list:
    pipeline = [
        {"$unwind": "$dimensions"},
        {"$group": {
            "_id": "$dimensions.name",
            "avg_score": {"$avg": "$dimensions.score"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"avg_score": -1}}
    ]
    results = await db.screenings.aggregate(pipeline).to_list(20)
    return [{"name": r["_id"], "avg_score": round(r["avg_score"], 1),
             "count": r["count"]} for r in results]


async def delete_screening(screening_id: str) -> bool:
    result = await db.screenings.delete_one({"_id": ObjectId(screening_id)})
    return result.deleted_count > 0


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


async def create_batch_job(total: int, jd_preview: str) -> str:
    doc = {
        "total": total, "completed": 0, "succeeded": 0, "failed": 0,
        "status": "running", "jd_preview": jd_preview[:200],
        "progress": [], "created_at": datetime.utcnow(),
    }
    inserted = await db.batch_jobs.insert_one(doc)
    return str(inserted.inserted_id)


async def update_batch_progress(batch_id: str, index: int, status: str,
                                 filename: str, score=None,
                                 recommendation=None, error=None):
    entry = {"index": index, "status": status, "filename": filename}
    if score is not None: entry["score"] = score
    if recommendation:    entry["recommendation"] = recommendation
    if error:             entry["error"] = error
    inc = {"completed": 1}
    if status == "done":   inc["succeeded"] = 1
    if status == "failed": inc["failed"] = 1
    await db.batch_jobs.update_one(
        {"_id": ObjectId(batch_id)},
        {"$inc": inc, "$push": {"progress": entry}}
    )


async def finish_batch_job(batch_id: str, summary: dict):
    await db.batch_jobs.update_one(
        {"_id": ObjectId(batch_id)},
        {"$set": {
            "status": "done",
            "finished_at": datetime.utcnow(),
            "succeeded": summary.get("succeeded", 0),
            "failed": summary.get("failed", 0),
        }}
    )


async def get_batch_job(batch_id: str) -> dict | None:
    doc = await db.batch_jobs.find_one({"_id": ObjectId(batch_id)})
    if doc:
        doc["_id"] = str(doc["_id"])
        doc["created_at"] = str(doc.get("created_at", ""))
    return doc


async def get_all_batch_jobs(limit: int = 50) -> list:
    cursor = db.batch_jobs.find({}).sort("created_at", -1).limit(limit)
    jobs = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        doc["created_at"] = str(doc.get("created_at", ""))
        doc.pop("progress", None)
        jobs.append(doc)
    return jobs


# ─────────────────────────────────────────────────────────────
# USERS / COMPANIES
# ─────────────────────────────────────────────────────────────

async def create_user(email: str, hashed_password: str, company_name: str, role: str = "client") -> str:
    """Create a new user account."""
    existing = await db.users.find_one({"email": email.lower()})
    if existing:
        raise ValueError("Email already registered")
    doc = {
        "email": email.lower().strip(),
        "password": hashed_password,
        "company_name": company_name.strip(),
        "role": role,  # "admin" or "client"
        "active": True,
        "created_at": datetime.utcnow(),
        "screening_count": 0,
        "plan": "trial",  # trial / basic / pro
    }
    inserted = await db.users.insert_one(doc)
    return str(inserted.inserted_id)


async def get_user_by_email(email: str) -> dict | None:
    doc = await db.users.find_one({"email": email.lower()})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


async def get_user_by_id(user_id: str) -> dict | None:
    try:
        doc = await db.users.find_one({"_id": ObjectId(user_id)})
        if doc:
            doc["_id"] = str(doc["_id"])
        return doc
    except Exception:
        return None


async def get_all_users() -> list:
    cursor = db.users.find({}).sort("created_at", -1)
    users = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        doc.pop("password", None)  # Never return password
        users.append(doc)
    return users


async def update_user(user_id: str, updates: dict):
    updates.pop("password", None)  # Use change_password for that
    await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": updates})


async def increment_screening_count(user_id: str, by: int = 1):
    """Increment monthly screening count. Resets at start of each month."""
    from datetime import datetime
    now = datetime.utcnow()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    user = await db.users.find_one({"_id": ObjectId(user_id)})
    if user:
        last_reset = user.get("month_reset_at")
        if not last_reset or last_reset < month_start:
            # New month — reset count
            await db.users.update_one(
                {"_id": ObjectId(user_id)},
                {"$set": {"screening_count": by, "month_reset_at": month_start}}
            )
            return
    # Increment by batch size
    await db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$inc": {"screening_count": by},
         "$set": {"month_reset_at": month_start}}
    )


async def sync_screening_count(user_id: str):
    """Recalculate screening_count from actual DB count — call after batch."""
    count = await db.screenings.count_documents({"user_id": user_id})
    await db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {"screening_count": count}}
    )


# ─────────────────────────────────────────────────────────────
# TENANT-SCOPED QUERIES (filter by company/user)
# ─────────────────────────────────────────────────────────────

async def get_screenings_for_user(user_id: str, limit: int = 200) -> list:
    # Match by user_id (string match - handles both ObjectId string and plain string)
    cursor = db.screenings.find({
        "$or": [
            {"user_id": user_id},
            {"user_id": str(user_id)},
        ]
    }).sort("created_at", -1).limit(limit)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results


async def get_stats_for_user(user_id: str) -> dict:
    pipeline = [
        {"$match": {"user_id": user_id}},
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "avg_score": {"$avg": "$overall_score"},
                "avg_coverage": {"$avg": "$skills_coverage_pct"},
                "strong_hires": {"$sum": {"$cond": [{"$eq": ["$recommendation", "STRONG HIRE"]}, 1, 0]}},
                "hires": {"$sum": {"$cond": [{"$eq": ["$recommendation", "HIRE"]}, 1, 0]}},
                "maybes": {"$sum": {"$cond": [{"$eq": ["$recommendation", "MAYBE"]}, 1, 0]}},
                "rejects": {"$sum": {"$cond": [{"$eq": ["$recommendation", "REJECT"]}, 1, 0]}},
            }
        }
    ]
    results = await db.screenings.aggregate(pipeline).to_list(1)
    if not results:
        return {"total": 0, "avg_score": 0, "avg_coverage": 0,
                "strong_hires": 0, "hires": 0, "maybes": 0, "rejects": 0}
    stats = results[0]
    stats.pop("_id", None)
    stats["avg_score"] = round(stats["avg_score"] or 0, 1)
    stats["avg_coverage"] = round(stats["avg_coverage"] or 0, 1)
    return stats


async def get_jobs_for_user(user_id: str) -> list:
    cursor = db.jobs.find({"user_id": user_id, "active": True}).sort("created_at", -1)
    jobs = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        jobs.append(doc)
    return jobs


async def get_skills_gaps_for_user(user_id: str) -> list:
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$unwind": "$critical_gaps"},
        {"$group": {"_id": "$critical_gaps", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    results = await db.screenings.aggregate(pipeline).to_list(10)
    return [{"skill": r["_id"], "count": r["count"]} for r in results]


async def get_dimension_averages_for_user(user_id: str) -> list:
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$unwind": "$dimensions"},
        {"$group": {
            "_id": "$dimensions.name",
            "avg_score": {"$avg": "$dimensions.score"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"avg_score": -1}}
    ]
    results = await db.screenings.aggregate(pipeline).to_list(20)
    return [{"name": r["_id"], "avg_score": round(r["avg_score"], 1),
             "count": r["count"]} for r in results]


# ─────────────────────────────────────────────────────────────
# EMAIL VERIFICATION OTP
# ─────────────────────────────────────────────────────────────

async def store_otp(email: str, otp: str, company_name: str, password_hash: str):
    """Store pending registration with OTP. Expires in 15 minutes."""
    await db.pending_registrations.delete_many({"email": email.lower()})
    await db.pending_registrations.insert_one({
        "email": email.lower(),
        "otp": otp,
        "company_name": company_name,
        "password_hash": password_hash,
        "created_at": datetime.utcnow(),
        "expires_at": datetime.utcnow().replace(
            minute=(datetime.utcnow().minute + 15) % 60
        ),
        "attempts": 0
    })


async def verify_otp(email: str, otp: str) -> dict | None:
    """Verify OTP. Returns pending registration data if valid."""
    from datetime import timedelta
    cutoff = datetime.utcnow() - timedelta(minutes=15)
    doc = await db.pending_registrations.find_one({
        "email": email.lower(),
        "otp": otp,
        "created_at": {"$gt": cutoff}
    })
    if doc:
        await db.pending_registrations.delete_one({"_id": doc["_id"]})
    return doc


async def delete_pending(email: str):
    await db.pending_registrations.delete_many({"email": email.lower()})


# ─────────────────────────────────────────────────────────────
# PAYMENTS & BILLING
# ─────────────────────────────────────────────────────────────

async def save_payment(payment: dict) -> str:
    doc = {**payment, "created_at": datetime.utcnow()}
    inserted = await db.payments.insert_one(doc)
    return str(inserted.inserted_id)


async def get_payments_for_user(user_id: str) -> list:
    cursor = db.payments.find({"user_id": user_id}).sort("created_at", -1).limit(20)
    payments = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        payments.append(doc)
    return payments


async def update_user_subscription(user_id: str, plan: str, subscription_data: dict = None):
    updates = {"plan": plan}
    if subscription_data:
        updates["subscription"] = subscription_data
    await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": updates})


# ─────────────────────────────────────────────────────────────
# TEAM MEMBERS
# ─────────────────────────────────────────────────────────────

async def invite_team_member(owner_user_id: str, email: str, role: str, company_name: str) -> str:
    """Create a team member invitation."""
    existing = await db.users.find_one({"email": email.lower()})
    if existing:
        raise ValueError("This email is already registered.")
    doc = {
        "email": email.lower(),
        "owner_user_id": owner_user_id,
        "company_name": company_name,
        "role": role,  # "viewer" or "screener"
        "status": "pending",
        "invited_at": datetime.utcnow(),
    }
    inserted = await db.team_invites.insert_one(doc)
    return str(inserted.inserted_id)


async def get_team_members(owner_user_id: str) -> list:
    """Get all team members (active users) under this account."""
    cursor = db.users.find({"owner_user_id": owner_user_id})
    members = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        doc.pop("password", None)
        members.append(doc)
    return members


async def get_team_invites(owner_user_id: str) -> list:
    cursor = db.team_invites.find({"owner_user_id": owner_user_id, "status": "pending"})
    invites = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        invites.append(doc)
    return invites


async def update_user_profile(user_id: str, profile: dict):
    """Update user profile fields."""
    allowed = ["company_name", "full_name", "phone", "website", "address", "avatar_initials"]
    updates = {k: v for k, v in profile.items() if k in allowed}
    if updates:
        await db.users.update_one({"_id": ObjectId(user_id)}, {"$set": updates})


async def update_user_notifications(user_id: str, prefs: dict):
    await db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {"notification_prefs": prefs}}
    )


async def get_full_user(user_id: str) -> dict | None:
    doc = await db.users.find_one({"_id": ObjectId(user_id)})
    if doc:
        doc["_id"] = str(doc["_id"])
        doc.pop("password", None)
    return doc
