from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
from bson import ObjectId, Binary
import os
import re
import ssl
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("DB_NAME", "talentscore")

client = None
db     = None


def serialize_mongo(doc):
    """Recursively stringify every ObjectId in a document so it is JSON-safe.

    `_id` was always converted by hand. `cv_file_id` — added to 243 screenings
    by migration 001 — was not, and it is a raw ObjectId. FastAPI cannot
    serialize it, so every /api/screenings response 500'd with "ObjectId is not
    iterable" the moment the migration ran. Converting only the field we know
    about would leave the next raw-ObjectId field to break the same way, so this
    walks the whole document at any depth. Note: job_id and user_id in
    screenings are consistently strings — this is NOT the string-vs-ObjectId
    typing behind user_match(); cv_file_id is a correctly-typed reference that
    simply was never stringified on the way out.
    """
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, list):
        return [serialize_mongo(v) for v in doc]
    if isinstance(doc, dict):
        return {k: serialize_mongo(v) for k, v in doc.items()}
    return doc


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

    # ── Public application link ──
    # Unique + sparse: jobs created before this feature have no public_token, and a
    # plain unique index would treat every one of their missing values as the same
    # null and reject all but the first.
    await db.jobs.create_index("public_token", unique=True, sparse=True)
    await db.applications.create_index("job_id")
    await db.applications.create_index([("job_id", 1), ("email", 1)])
    await db.applications.create_index("status")
    await db.applications.create_index("submitted_at")

    # TTL indexes. MongoDB only expires a document whose indexed field holds a BSON
    # date — documents missing the field are ignored forever. That is exactly how
    # the 243 CVs migrated by migration 001 stay exempt: their expires_at was
    # unset, so this index can never touch them.
    await db.application_files.create_index("expires_at", expireAfterSeconds=0)
    # Proctoring snapshots: candidate personal data — TTL'd like the CV PDFs,
    # fetched only through owner-gated endpoints, keyed by interview token.
    await db.proctor_snapshots.create_index("expires_at", expireAfterSeconds=0)
    await db.proctor_snapshots.create_index("token")
    # 360° performance: the assignment row is the ACL — index its two lookup
    # paths, and keep one row per (cycle, subject, reviewer, relation).
    await db.perf_cycles.create_index("user_id")
    await db.perf_assignments.create_index([("user_id", 1), ("reviewer_employee_id", 1)])
    await db.perf_assignments.create_index("cycle_id")
    await db.perf_assignments.create_index(
        [("cycle_id", 1), ("subject_employee_id", 1), ("reviewer_employee_id", 1), ("relation", 1)],
        unique=True)
    await db.application_files.create_index("screening_id")
    await db.application_files.create_index("application_id")
    await db.rate_hits.create_index("expires_at", expireAfterSeconds=0)
    await db.rate_hits.create_index([("bucket", 1), ("at", 1)])

    # ── Video interviews (Viva) — Phase 1 ──
    await db.interviews.create_index("public_token", unique=True, sparse=True)
    await db.interviews.create_index("user_id")
    await db.interview_written_answers.create_index([("interview_id", 1), ("email", 1)])
    await db.interview_written_answers.create_index("status")
    await db.interview_sessions.create_index("created_at")
    await db.interview_sessions.create_index("status")
    await db.live_interviews.create_index("public_token", unique=True, sparse=True)
    await db.live_interviews.create_index("user_id")
    await db.employees.create_index("user_id")
    await db.employees.create_index([("user_id", 1), ("email", 1)])
    await db.leave_requests.create_index([("user_id", 1), ("status", 1)])
    await db.leave_requests.create_index("employee_id")
    await db.attendance.create_index(
        [("user_id", 1), ("employee_id", 1), ("date", 1)], unique=True)

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
        results.append(serialize_mongo(doc))
    return results


async def get_screening_by_id(screening_id: str) -> dict | None:
    doc = await db.screenings.find_one({"_id": ObjectId(screening_id)})
    if doc:
        return serialize_mongo(doc)
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


class DuplicateJobError(Exception):
    """Raised when a job with the same title already exists for this user."""
    def __init__(self, existing_id: str, title: str):
        self.existing_id = existing_id
        self.title = title
        super().__init__(f"A job titled '{title}' already exists.")


async def save_job(job: dict) -> str:
    """Insert a job, refusing a second active job with the same title for the same user.

    Two "Data Analyst" documents is how the Jobs page ended up double-counting: each
    duplicate claimed both jobs' candidates through the old title-matching filter.
    Case-insensitive and whitespace-trimmed, since 'Data Analyst ' and 'data analyst'
    are the same role to a recruiter.
    NOTE: this is a read-then-write check, not a database constraint — two truly
    simultaneous requests can still both pass it. A unique partial index on
    (user_id, lowercased title, active) is the airtight fix; see the Stage 2 notes.
    """
    title = (job.get("title") or "").strip()
    if not title:
        raise ValueError("Job title is required.")
    existing = await db.jobs.find_one(
        {
            "user_id": job.get("user_id"),
            "active": True,
            "title": {"$regex": f"^{re.escape(title)}$", "$options": "i"},
        },
        {"_id": 1},
    )
    if existing:
        raise DuplicateJobError(str(existing["_id"]), title)

    # Minted at creation, not on first toggle: the workflow is create job -> copy
    # link -> paste into a job board, and making the link appear only after
    # finding a toggle is how a dead link gets posted. Harmless until is_public.
    doc = {**job, "title": title, "created_at": datetime.utcnow(), "candidates_count": 0,
           "active": True, "is_public": False, "public_token": generate_public_token()}
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
    """Recalculate this month's screening_count from actual DB records."""
    from datetime import datetime
    now = datetime.utcnow()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # user_match, not plain equality: a screening whose user_id was stored as a
    # non-string is invisible to an equality match, so the month count runs low and
    # the user is handed free quota. This is the number the batch limit enforces on.
    count = await db.screenings.count_documents({
        **user_match(user_id),
        "created_at": {"$gte": month_start}
    })
    await db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {"screening_count": count, "month_reset_at": month_start}}
    )
    return count


# ─────────────────────────────────────────────────────────────
# TENANT-SCOPED QUERIES (filter by company/user)
# ─────────────────────────────────────────────────────────────

def user_match(user_id: str) -> dict:
    """The single user_id matching rule for tenant-scoped screening queries.

    get_screenings_for_user used this $or form while get_stats_for_user used a plain
    equality match, so the two could disagree about how many screenings a user has —
    the same class of bug as the four different "shortlisted" counts in the UI.
    The $or form is the superset and is now used by both; equality would silently
    drop records whose user_id was stored as a non-string.
    """
    return {"$or": [{"user_id": user_id}, {"user_id": str(user_id)}]}


async def count_screenings_for_user(user_id: str) -> int:
    """Unfiltered total, so callers can tell a full page from a truncated one."""
    return await db.screenings.count_documents(user_match(user_id))


async def get_screenings_for_user(user_id: str, limit: int = 200) -> list:
    cursor = db.screenings.find(user_match(user_id)).sort("created_at", -1).limit(limit)
    results = []
    async for doc in cursor:
        results.append(serialize_mongo(doc))
    return results


async def get_stats_for_user(user_id: str) -> dict:
    pipeline = [
        {"$match": user_match(user_id)},
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


# ─────────────────────────────────────────────────────────────
# PUBLIC APPLICATION LINKS
# ─────────────────────────────────────────────────────────────

import secrets
import hashlib
from datetime import timedelta

# Retention for CVs uploaded through a public link. The apply page tells the
# candidate 30 days; if this constant moves, that sentence has to move with it.
APPLICATION_PDF_RETENTION_DAYS = 30

# Spend caps. Each screening is ~4 GPT-4o calls, ~$0.054.
CAP_PER_JOB = 200      # ~$10.80 per posting
CAP_PER_DAY = 100      # ~$5.40/day
CAP_PER_MONTH = 500    # ~$27/month — the one that bounds a card charge

MAX_APPLICATION_PDF_BYTES = 2 * 1024 * 1024  # 2MB


def generate_public_token() -> str:
    """22 URL-safe characters, 128 bits. Not enumerable, carries no information."""
    return secrets.token_urlsafe(16)


def hash_ip(ip: str) -> str:
    """Hash a client IP for rate-limit bucketing. Raw IPs are never stored.

    Salted with SECRET_KEY so the digests aren't reversible via a rainbow table
    of the whole IPv4 space, which is small enough to enumerate unsalted.
    """
    salt = os.getenv("SECRET_KEY", "topcandidate-fallback-salt")
    return hashlib.sha256(f"{salt}:{ip}".encode()).hexdigest()[:32]


async def get_job_by_public_token(token: str) -> dict | None:
    """Resolve a public token to a live, publicly-listed job.

    Paused (is_public False), rotated (token no longer matches), and deleted
    (active False) jobs all resolve to None so the caller can return one
    indistinguishable closed page for every case.
    """
    doc = await db.jobs.find_one({"public_token": token, "is_public": True, "active": True})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


async def ensure_job_token(job_id: str) -> str | None:
    """Return the job's public token, minting one if it has none.

    Tokens are minted at job creation now, so this only fires for jobs that
    predate the feature. A token existing is harmless — is_public is what
    decides whether the URL resolves.
    """
    try:
        job = await db.jobs.find_one({"_id": ObjectId(job_id)}, {"public_token": 1})
    except Exception:
        return None
    if not job:
        return None
    if job.get("public_token"):
        return job["public_token"]
    token = generate_public_token()
    await db.jobs.update_one({"_id": ObjectId(job_id)}, {"$set": {"public_token": token}})
    return token


# NOTE ON AUTHORISATION for the two functions below: they take an already
# authorised job_id and do NOT re-check ownership. They used to, with a
# user_id equality match that had no admin exemption — while the route
# authorised through the admin-aware owned_job(). Two definitions of "may this
# user act on this job" that disagreed, so an admin acting on a job owned by
# another of their own accounts passed the route and was then refused here as
# a 404. One check, in the route. Do not add a second one here.

async def set_job_public(job_id: str, is_public: bool) -> dict | None:
    """Pause or resume a public link. Keeps the existing token.

    The reversible switch. Rotating the token is the irreversible one; they are
    separate on purpose, so pausing doesn't cost you the URL you already posted
    and revoking one doesn't require deleting the job.
    """
    try:
        oid = ObjectId(job_id)
    except Exception:
        return None
    job = await db.jobs.find_one({"_id": oid})
    if not job:
        return None
    token = job.get("public_token") or generate_public_token()
    await db.jobs.update_one(
        {"_id": oid}, {"$set": {"is_public": is_public, "public_token": token}}
    )
    return {**job, "is_public": is_public, "public_token": token, "_id": str(job["_id"])}


async def rotate_job_token(job_id: str) -> dict | None:
    """Kill a leaked link permanently. The old URL can never be revived."""
    try:
        oid = ObjectId(job_id)
    except Exception:
        return None
    job = await db.jobs.find_one({"_id": oid})
    if not job:
        return None
    token = generate_public_token()
    await db.jobs.update_one(
        {"_id": oid},
        {"$set": {"public_token": token, "token_rotated_at": datetime.utcnow()}},
    )
    return {**job, "public_token": token, "_id": str(job["_id"])}


def user_match_field(field: str, user_id: str) -> dict:
    """user_match, for collections whose owner field isn't called user_id.

    Same reason as user_match: the id is not reliably stored as a string, so a
    plain equality match silently drops rows.
    """
    return {"$or": [{field: user_id}, {field: str(user_id)}]}


# ── Spend caps ───────────────────────────────────────────────

async def reserve_spend(key: str, cap: int) -> bool:
    """Claim one unit of spend under `cap`, atomically. True if claimed.

    A read-then-write (`if count < cap: count += 1`) is a race: ten concurrent
    uploads all read 199, all pass the check, and all spend. Here the check IS
    the update — the filter requires count < cap and the increment happens in
    the same round trip, so concurrency cannot overshoot the cap.

    This is a reservation, not a tally: the slot is claimed BEFORE the money is
    spent, never after.
    """
    await db.spend_counters.update_one(
        {"_id": key},
        {"$setOnInsert": {"count": 0, "created_at": datetime.utcnow()}},
        upsert=True,
    )
    doc = await db.spend_counters.find_one_and_update(
        {"_id": key, "count": {"$lt": cap}},
        {"$inc": {"count": 1}, "$set": {"last_at": datetime.utcnow()}},
    )
    return doc is not None


async def release_spend(key: str) -> None:
    """Give a reservation back — only for failures BEFORE any API call.

    If the pipeline died partway through, the money is already gone and the
    counter should keep saying so.
    """
    await db.spend_counters.update_one({"_id": key, "count": {"$gt": 0}}, {"$inc": {"count": -1}})


async def reserve_screening_slot(job_id: str) -> tuple[bool, str]:
    """Reserve against all three caps. Returns (ok, which_cap_blocked).

    Reserved in order job → day → month, releasing the earlier ones if a later
    cap refuses, so a blocked application never leaves a phantom reservation
    holding budget it didn't use.
    """
    now = datetime.utcnow()
    k_job = f"job:{job_id}"
    k_day = f"day:{now:%Y-%m-%d}"
    k_month = f"month:{now:%Y-%m}"

    if not await reserve_spend(k_job, CAP_PER_JOB):
        return False, "job"
    if not await reserve_spend(k_day, CAP_PER_DAY):
        await release_spend(k_job)
        return False, "day"
    if not await reserve_spend(k_month, CAP_PER_MONTH):
        await release_spend(k_day)
        await release_spend(k_job)
        return False, "month"
    return True, ""


async def release_screening_slot(job_id: str) -> None:
    now = datetime.utcnow()
    await release_spend(f"job:{job_id}")
    await release_spend(f"day:{now:%Y-%m-%d}")
    await release_spend(f"month:{now:%Y-%m}")


async def get_spend_state(job_id: str) -> dict:
    now = datetime.utcnow()
    async def n(key):
        d = await db.spend_counters.find_one({"_id": key}, {"count": 1})
        return (d or {}).get("count", 0)
    return {
        "job": {"used": await n(f"job:{job_id}"), "cap": CAP_PER_JOB},
        "day": {"used": await n(f"day:{now:%Y-%m-%d}"), "cap": CAP_PER_DAY},
        "month": {"used": await n(f"month:{now:%Y-%m}"), "cap": CAP_PER_MONTH},
    }


# ── Rate limiting ────────────────────────────────────────────

async def rate_limit_allows(bucket: str, limit: int, window_seconds: int) -> bool:
    """Sliding-window counter. True if this hit is allowed.

    Count-then-insert can let a couple of extra requests through under exact
    simultaneity. That is acceptable here: this layer exists to blunt casual
    spam, and the spend cap — which IS atomic — is what actually bounds cost.

    Rows carry expires_at and are reaped by a TTL index, so there is no cleanup
    job and the collection cannot grow without bound.
    """
    now = datetime.utcnow()
    cutoff = now - timedelta(seconds=window_seconds)
    used = await db.rate_hits.count_documents({"bucket": bucket, "at": {"$gte": cutoff}})
    if used >= limit:
        return False
    await db.rate_hits.insert_one({
        "bucket": bucket,
        "at": now,
        "expires_at": now + timedelta(seconds=window_seconds),
    })
    return True


# ── Applications ─────────────────────────────────────────────

async def upsert_application(job: dict, name: str, email: str, phone: str,
                             filename: str, ip_hash: str) -> tuple[str, bool]:
    """Create or replace a pending application. Returns (application_id, replaced).

    job_id and user_id come off the job document, which was itself resolved from
    the token — so an application that belongs to no job cannot be constructed.
    This is what keeps public applicants out of the job-orphan problem.
    """
    email = (email or "").strip().lower()
    now = datetime.utcnow()
    existing = await db.applications.find_one(
        {"job_id": str(job["_id"]), "email": email, "status": {"$in": ["pending", "stored_unscored", "scoring"]}}
    )

    doc = {
        "job_id": str(job["_id"]),
        "user_id": job.get("user_id"),
        "name": (name or "").strip(),
        "email": email,
        "phone": (phone or "").strip(),
        "cv_filename": filename,
        "status": "pending",
        "submitted_at": now,
        "submitted_ip_hash": ip_hash,
    }

    if existing:
        # Keep the latest. Candidates re-upload after fixing a typo, and refusing
        # them reads as broken. The old PDF goes with the old record.
        await db.application_files.delete_many({"application_id": str(existing["_id"])})
        await db.applications.update_one({"_id": existing["_id"]}, {"$set": doc})
        return str(existing["_id"]), True

    res = await db.applications.insert_one(doc)
    return str(res.inserted_id), False


async def store_application_pdf(application_id: str, job_id: str, user_id: str,
                                data: bytes, filename: str) -> str:
    now = datetime.utcnow()
    res = await db.application_files.insert_one({
        "application_id": application_id,
        "job_id": job_id,
        "user_id": user_id,
        "data": Binary(data),
        "filename": filename,
        "size": len(data),
        "source": "public_apply",
        "created_at": now,
        "expires_at": now + timedelta(days=APPLICATION_PDF_RETENTION_DAYS),
    })
    return str(res.inserted_id)


async def count_pending_applications(job_id: str) -> dict:
    pending = await db.applications.count_documents(
        {"job_id": job_id, "status": {"$in": ["pending", "stored_unscored"]}}
    )
    unscored = await db.applications.count_documents({"job_id": job_id, "status": "stored_unscored"})
    total = await db.applications.count_documents({"job_id": job_id})
    return {"pending": pending, "unscored": unscored, "total": total}


# ── Cached JD parse (one per job, not one per candidate) ────

# Bump when the JD parse prompt or its output schema changes, so cached parses
# from an older schema are re-parsed instead of silently scored against
# missing fields.
JD_PARSE_SCHEMA_VERSION = 1


def jd_fingerprint(jd_text: str) -> str:
    """Identity of a JD parse: the text itself, the schema, and the model.

    Whitespace-normalised so that reformatting a description doesn't force a
    re-parse, while any real edit does.
    """
    normalized = " ".join((jd_text or "").split())
    return hashlib.sha256(
        f"{JD_PARSE_SCHEMA_VERSION}:{normalized}".encode()
    ).hexdigest()


async def get_cached_jd_parse(job_id: str, jd_text: str, model: str) -> dict | None:
    """Return the job's cached parsed JD, or None if absent or stale."""
    try:
        job = await db.jobs.find_one(
            {"_id": ObjectId(job_id)},
            {"parsed_jd": 1, "parsed_jd_hash": 1, "parsed_jd_model": 1},
        )
    except Exception:
        return None
    if not job or not job.get("parsed_jd"):
        return None
    if job.get("parsed_jd_hash") != jd_fingerprint(jd_text):
        return None            # the description was edited
    if job.get("parsed_jd_model") != model:
        return None            # a different model would parse it differently
    return job["parsed_jd"]


async def save_jd_parse(job_id: str, jd_text: str, model: str, parsed: dict) -> None:
    try:
        await db.jobs.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": {
                "parsed_jd": parsed,
                "parsed_jd_hash": jd_fingerprint(jd_text),
                "parsed_jd_model": model,
                "parsed_jd_at": datetime.utcnow(),
            }},
        )
    except Exception as e:
        # A cache write failing must never fail a screening — the next call
        # simply parses inline again.
        print(f"[JD-CACHE] could not store parse for job {job_id}: {e}")


async def get_applications_for_job(job_id: str, user_id: str, status: str = "") -> list:
    q = {"job_id": job_id, **user_match_field("user_id", user_id)}
    if status:
        q["status"] = status
    cursor = db.applications.find(q, {"cv_pdf_b64": 0}).sort("submitted_at", -1).limit(500)
    out = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        out.append(doc)
    return out


# ─────────────────────────────────────────────────────────────
# VIDEO INTERVIEWS (Viva) — Phase 1 (English-only)
# ─────────────────────────────────────────────────────────────

# Interview status vocabulary is deliberately small in Phase 1: an interview is
# a published question with a public recording link. Answers (a separate
# collection) arrive in Phase 2 with the storage/transcription pipeline.
async def create_interview(user_id: str, question: str, job_id: str = None,
                           job_title: str = None,
                           written_questions: list | None = None) -> dict:
    """Create an interview and its public recording token.

    answer_language is hardcoded 'en' in Phase 1 and stored now so Phase 2's
    Bangla path needs no migration over existing records. The Bangla option is
    surfaced in the UI but inert — nothing can submit a non-'en' answer.
    """
    token = generate_public_token()
    doc = {
        "user_id": user_id,
        "job_id": job_id,
        "job_title": job_title,
        "question": (question or "").strip(),
        # Written segment (Step 1 of proctored-async). Empty list = no written phase.
        "written_questions": [str(q).strip() for q in (written_questions or []) if str(q).strip()][:5],
        "answer_language": "en",   # Phase 1: English only. Do not infer.
        "public_token": token,
        "is_public": True,
        "active": True,
        "created_at": datetime.utcnow(),
    }
    res = await db.interviews.insert_one(doc)
    doc["_id"] = str(res.inserted_id)
    return doc


async def get_interview_by_token(token: str) -> dict | None:
    """Resolve a recording token to a live interview.

    Same probe-resistant contract as the apply link: unknown, unpublished
    (is_public False), and deleted (active False) all return None so the public
    page renders one identical closed response for every case.
    """
    doc = await db.interviews.find_one(
        {"public_token": token, "is_public": True, "active": True}
    )
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


async def save_written_submission(interview_id: str, name: str, email: str,
                                  answers: list) -> tuple[str, bool]:
    """Store a candidate's written answers, replacing any earlier unscored
    submission from the same email (same keep-the-latest rule as the apply
    link — a resubmit after fixing a typo should not read as broken).
    A submission that has already been SCORED is never replaced: rescoring on
    demand would let a candidate re-roll a paid scoring call.
    Returns (submission_id, replaced)."""
    email = (email or "").strip().lower()
    now = datetime.utcnow()
    doc = {
        "interview_id": interview_id,
        "name": (name or "").strip(),
        "email": email,
        "answers": answers,          # [{question, answer_text}]
        "answer_language": "en",     # Phase 1: English only
        "status": "pending",         # pending -> scoring -> scored | failed
        "submitted_at": now,
    }
    existing = await db.interview_written_answers.find_one(
        {"interview_id": interview_id, "email": email})
    if existing and existing.get("status") == "scored":
        return str(existing["_id"]), False
    if existing:
        await db.interview_written_answers.update_one(
            {"_id": existing["_id"]}, {"$set": doc})
        return str(existing["_id"]), True
    res = await db.interview_written_answers.insert_one(doc)
    return str(res.inserted_id), False


async def get_written_submissions(interview_id: str) -> list:
    cursor = db.interview_written_answers.find(
        {"interview_id": interview_id}).sort("submitted_at", -1).limit(200)
    out = []
    async for doc in cursor:
        out.append(serialize_mongo(doc))
    return out


# ── Candidate-facing live interviews (the recruiter/candidate split) ──

# A leaked candidate link must not be able to drain OpenAI spend: every session
# mint consumes one unit of this per-token budget, atomically. 15 covers a full
# interview plus generous L1 drop-recovery re-mints; then the link is dead.
LIVE_MINT_BUDGET = 15


async def create_live_interview(user_id: str, config: dict) -> dict:
    """Recruiter creates a live interview; returns the doc with its candidate token.

    The config is stored SERVER-SIDE and drives the session from here on. The
    candidate page receives only what it operationally needs (name, count,
    proctoring mode) — it cannot leak settings it never gets.
    """
    token = generate_public_token()
    doc = {
        "user_id": user_id,
        "public_token": token,
        "config": config,
        "answer_language": "en",
        "mints": 0,
        "completed_sessions": 0,
        "active": True,
        "created_at": datetime.utcnow(),
    }
    res = await db.live_interviews.insert_one(doc)
    doc["_id"] = str(res.inserted_id)
    return doc


async def get_live_interview_by_token(token: str) -> dict | None:
    """Probe-resistant resolve: unknown, deactivated, and already-completed
    tokens all return None so the page renders one identical closed response."""
    doc = await db.live_interviews.find_one(
        {"public_token": token, "active": True, "completed_sessions": {"$lt": 1}})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


async def reserve_live_mint(token: str) -> bool:
    """Atomically consume one unit of the token's mint budget. Same
    check-IS-the-update pattern as the spend caps — concurrency can't overshoot."""
    doc = await db.live_interviews.find_one_and_update(
        {"public_token": token, "active": True, "mints": {"$lt": LIVE_MINT_BUDGET}},
        {"$inc": {"mints": 1}, "$set": {"last_mint_at": datetime.utcnow()}},
    )
    return doc is not None


async def complete_live_interview(token: str) -> None:
    await db.live_interviews.update_one(
        {"public_token": token}, {"$inc": {"completed_sessions": 1}})


# ── Proctoring snapshots (the stored, reviewable record) ─────
# Continuous A/V is NOT persisted here — that needs object storage (a later
# phase). What IS stored: a hard-capped set of small JPEG frames per
# interview, TTL'd, and readable only through owner-gated endpoints. The cap
# is atomic on the live-interview doc, so a hostile client can't fill the
# database however fast it posts.

SNAPSHOT_CAP_PER_INTERVIEW = 12
SNAPSHOT_MAX_BYTES = 150_000
SNAPSHOT_TTL_DAYS = 30


async def reserve_snapshot_slot(token: str) -> bool:
    doc = await db.live_interviews.find_one_and_update(
        {"public_token": token, "active": True,
         "snap_count": {"$not": {"$gte": SNAPSHOT_CAP_PER_INTERVIEW}}},
        {"$inc": {"snap_count": 1}},
    )
    return doc is not None


async def save_proctor_snapshot(token: str, user_id: str, application_id: str | None,
                                kind: str, data: bytes) -> None:
    now = datetime.utcnow()
    await db.proctor_snapshots.insert_one({
        "token": token,
        "user_id": user_id,
        "application_id": application_id,
        "kind": kind if kind in ("cam", "scr") else "cam",
        "data": Binary(data),
        "size": len(data),
        "created_at": now,
        "expires_at": now + timedelta(days=SNAPSHOT_TTL_DAYS),
    })


async def get_snapshots_for_token(token: str, limit: int = 20) -> list:
    out = []
    cursor = db.proctor_snapshots.find({"token": token}).sort("created_at", 1).limit(limit)
    async for doc in cursor:
        out.append({"kind": doc.get("kind", "cam"),
                    "created_at": doc.get("created_at"),
                    "data": bytes(doc.get("data") or b"")})
    return out


# ── One-link flow: viva-after-CV gating ──────────────────────

# Threshold default sits where MAYBE begins: clearly-weak CVs are filtered,
# anyone plausible gets the interview. Both are per-job overridable.
VIVA_THRESHOLD_DEFAULT = 48
VIVA_DAILY_LAUNCH_CAP_DEFAULT = 25


async def set_job_viva(job_id: str, viva: dict | None) -> None:
    """Attach (or clear) the viva-after-CV config on a job.

    job_id arrives pre-authorized by the route's owned_job() — no ownership
    re-check here. Re-checking with different rules is how the public-link
    toggle silently 404'd for admins; helpers trust the route's authz.
    """
    if viva is None:
        await db.jobs.update_one({"_id": ObjectId(job_id)}, {"$unset": {"viva": ""}})
    else:
        await db.jobs.update_one({"_id": ObjectId(job_id)}, {"$set": {"viva": viva}})


async def update_job_interview_questions(job_id: str, patch: dict) -> None:
    """Patch the job's interview_questions container ({draft, approved} slots).

    Draft and approved live in SEPARATE slots so generating or editing can
    never clobber the set candidates are actively getting: the launch path
    reads only .approved, and approval is the single move that copies a draft
    over it. job_id is pre-authorized by the route (owned_job).
    """
    sets = {f"interview_questions.{k}": v for k, v in patch.items()}
    await db.jobs.update_one({"_id": ObjectId(job_id)}, {"$set": sets})


# ── Employees (HRM module 1) ─────────────────────────────────
# The foundation record for every future HRM module: attendance, leave,
# performance, and payroll will all reference the stable employee _id.
# Tenant-scoped by user_id with the same user_match discipline as jobs
# and screenings — an admin only ever sees their own company's staff.

EMPLOYEE_STATUSES = ("active", "on_leave", "terminated")


async def create_employee(user_id: str, fields: dict) -> str:
    now = datetime.utcnow()
    doc = {**fields, "user_id": user_id, "created_at": now, "updated_at": now}
    res = await db.employees.insert_one(doc)
    return str(res.inserted_id)


async def get_employees_for_user(user_id: str) -> list:
    cursor = db.employees.find(user_match_field("user_id", user_id)).sort("created_at", -1)
    return [serialize_mongo(d) async for d in cursor]


async def get_employee_for_user(employee_id: str, user_id: str) -> dict | None:
    try:
        oid = ObjectId(employee_id)
    except Exception:
        return None
    doc = await db.employees.find_one({"_id": oid, **user_match_field("user_id", user_id)})
    return serialize_mongo(doc) if doc else None


async def update_employee_for_user(employee_id: str, user_id: str, fields: dict) -> bool:
    try:
        oid = ObjectId(employee_id)
    except Exception:
        return False
    res = await db.employees.update_one(
        {"_id": oid, **user_match_field("user_id", user_id)},
        {"$set": {**fields, "updated_at": datetime.utcnow()}})
    return res.matched_count == 1


async def find_employee_by_email(user_id: str, email: str) -> dict | None:
    doc = await db.employees.find_one(
        {**user_match_field("user_id", user_id), "email": (email or "").strip().lower()})
    return serialize_mongo(doc) if doc else None


# ── Employee login (HRM module 2, Part 2) ────────────────────
# A DISTINCT identity from the admin `users` collection: employee
# credentials live on the employee record itself. The invite token is a
# bearer credential (plaintext + expiry, same discipline as job tokens);
# the password is bcrypt-hashed by the caller before it reaches here.

async def set_employee_invite(employee_id: str, user_id: str, token: str,
                              expires: datetime) -> bool:
    try:
        oid = ObjectId(employee_id)
    except Exception:
        return False
    res = await db.employees.update_one(
        {"_id": oid, **user_match_field("user_id", user_id)},
        {"$set": {"invite_token": token, "invite_expires": expires,
                  "updated_at": datetime.utcnow()}})
    return res.matched_count == 1


async def get_employee_by_invite(token: str) -> dict | None:
    if not token:
        return None
    doc = await db.employees.find_one(
        {"invite_token": token, "invite_expires": {"$gt": datetime.utcnow()}})
    return serialize_mongo(doc) if doc else None


async def set_employee_password(employee_id: str, password_hash: str) -> bool:
    try:
        oid = ObjectId(employee_id)
    except Exception:
        return False
    res = await db.employees.update_one(
        {"_id": oid},
        {"$set": {"password_hash": password_hash, "updated_at": datetime.utcnow()},
         "$unset": {"invite_token": "", "invite_expires": ""}})
    return res.matched_count == 1


async def find_employee_logins(email: str) -> list:
    """Every employee record (across tenants) with this email that has a
    password set. Login verifies the password against each — the same email
    could exist in two companies, so identity is (email + matching password)."""
    email = (email or "").strip().lower()
    if not email:
        return []
    cursor = db.employees.find(
        {"email": email, "password_hash": {"$exists": True}})
    return [serialize_mongo(d) async for d in cursor]


async def update_employee_contact(employee_id: str, user_id: str, phone: str) -> bool:
    """Employee self-service: phone ONLY. Never role, status, allowance, or
    anything affecting their own leave entitlement — that stays admin-only."""
    try:
        oid = ObjectId(employee_id)
    except Exception:
        return False
    res = await db.employees.update_one(
        {"_id": oid, **user_match_field("user_id", user_id)},
        {"$set": {"phone": phone[:40], "updated_at": datetime.utcnow()}})
    return res.matched_count == 1


# ── Attendance & Leave (HRM module 2) ────────────────────────
# Everything references the stable employee _id. Balances are COMPUTED
# (allowance minus approved days this year), never stored as a counter —
# a computed balance cannot drift, and approving a request IS the
# deduction because the approval is an atomic pending->approved claim.

LEAVE_TYPES = ("annual", "sick", "unpaid")
DEFAULT_LEAVE_ALLOWANCES = {"annual": 20, "sick": 10}   # unpaid: tracked, not budgeted
ATTENDANCE_STATUSES = ("present", "absent", "leave", "holiday")


async def create_leave_request(user_id: str, doc: dict) -> str:
    now = datetime.utcnow()
    res = await db.leave_requests.insert_one(
        {**doc, "user_id": user_id, "created_at": now})
    return str(res.inserted_id)


async def get_leave_requests_for_user(user_id: str, status: str = "",
                                      employee_id: str = "") -> list:
    q = dict(user_match_field("user_id", user_id))
    if status:
        q["status"] = status
    if employee_id:
        q["employee_id"] = employee_id
    cursor = db.leave_requests.find(q).sort("created_at", -1).limit(500)
    return [serialize_mongo(d) async for d in cursor]


async def claim_leave_decision(request_id: str, user_id: str, status: str,
                               approver: str) -> dict | None:
    """Atomically move a PENDING request to approved/rejected. The pending
    check is inside the update — two admins clicking at once can't decide
    the same request twice. Returns the pre-update doc, or None."""
    try:
        oid = ObjectId(request_id)
    except Exception:
        return None
    doc = await db.leave_requests.find_one_and_update(
        {"_id": oid, **user_match_field("user_id", user_id), "status": "pending"},
        {"$set": {"status": status, "approver": approver,
                  "decided_at": datetime.utcnow()}})
    return serialize_mongo(doc) if doc else None


async def leave_taken_days(user_id: str, employee_id: str, year: int) -> dict:
    """Approved days per type for one employee in one calendar year (by
    start_date). Dates are stored as YYYY-MM-DD strings, so a string range
    is an exact year filter."""
    out = {t: 0 for t in LEAVE_TYPES}
    cursor = db.leave_requests.aggregate([
        {"$match": {**user_match_field("user_id", user_id),
                    "employee_id": employee_id, "status": "approved",
                    "start_date": {"$gte": f"{year}-01-01", "$lte": f"{year}-12-31"}}},
        {"$group": {"_id": "$type", "days": {"$sum": "$days"}}},
    ])
    async for row in cursor:
        if row["_id"] in out:
            out[row["_id"]] = int(row["days"])
    return out


async def mark_attendance(user_id: str, employee_id: str, date: str,
                          status: str, check_in: str = "", check_out: str = "") -> None:
    """One record per employee per day — marking again overwrites (upsert)."""
    await db.attendance.update_one(
        {"user_id": user_id, "employee_id": employee_id, "date": date},
        {"$set": {"status": status, "check_in": check_in, "check_out": check_out,
                  "updated_at": datetime.utcnow()},
         "$setOnInsert": {"created_at": datetime.utcnow()}},
        upsert=True)


async def hr_summary_counts(user_id: str) -> dict:
    """Lightweight, additive dashboard counts — tenant-scoped, independent of
    the hiring stats. Never touches CandidateStats or any screening query."""
    match = user_match_field("user_id", user_id)
    today = datetime.utcnow().strftime("%Y-%m-%d")
    employees = await db.employees.count_documents(
        {**match, "status": {"$ne": "terminated"}})
    # "Hired" = staff who came through the hiring funnel (source=hired),
    # still active. Real signal, distinct from manually-added staff.
    hired = await db.employees.count_documents(
        {**match, "status": {"$ne": "terminated"}, "source": "hired"})
    pending_leave = await db.leave_requests.count_documents(
        {**match, "status": "pending"})
    # People with an APPROVED leave spanning today — distinct employees.
    on_leave_ids = await db.leave_requests.distinct("employee_id", {
        **match, "status": "approved",
        "start_date": {"$lte": today}, "end_date": {"$gte": today}})
    # Today's attendance breakdown (only reflects what's actually been marked).
    att = {"present": 0, "absent": 0, "leave": 0, "holiday": 0}
    cursor = db.attendance.aggregate([
        {"$match": {**match, "date": today}},
        {"$group": {"_id": "$status", "n": {"$sum": 1}}},
    ])
    async for row in cursor:
        if row["_id"] in att:
            att[row["_id"]] = int(row["n"])
    return {"employees": employees, "hired": hired,
            "pending_leave": pending_leave, "on_leave_today": len(on_leave_ids),
            "present_today": att["present"], "attendance_today": att}


async def get_attendance_for_month(user_id: str, month: str,
                                   employee_id: str = "") -> list:
    q = {**user_match_field("user_id", user_id),
         "date": {"$gte": f"{month}-01", "$lte": f"{month}-31"}}
    if employee_id:
        q["employee_id"] = employee_id
    cursor = db.attendance.find(q).sort("date", 1).limit(3000)
    return [serialize_mongo(d) async for d in cursor]


async def reserve_viva_launch(job_id: str, cap: int) -> bool:
    """Atomically reserve one auto-launched interview against the job's daily
    cap. A realtime interview is the most expensive single action the public
    can trigger, so this is a hard ceiling, not advisory — same check-IS-the-
    update pattern as every other cap."""
    now = datetime.utcnow()
    return await reserve_spend(f"viva-launch:{job_id}:{now:%Y-%m-%d}", cap)


async def get_admin_screenings(limit: int = 100, skip: int = 0) -> tuple[list, int]:
    """Cross-tenant screenings for the admin portal — PROJECTED to the table's
    columns and paginated. The full documents average ~35KB each (parsed CV,
    dimensions, breakdowns); 300 of them is a 2.4MB response that stalls slow
    connections. This projection is ~50x lighter and count_documents gives the
    true platform total regardless of the page size."""
    proj = {"candidate_name": 1, "company": 1, "overall_score": 1,
            "recommendation": 1, "skills_coverage_pct": 1,
            "years_experience": 1, "created_at": 1, "user_id": 1}
    cursor = (db.screenings.find({}, proj)
              .sort("created_at", -1).skip(skip).limit(limit))
    out = []
    async for d in cursor:
        out.append(serialize_mongo(d))
    total = await db.screenings.count_documents({})
    return out, total


# ── 360° Performance (HRM module 3) — P1: data model + security boundary ──
# THE SECURITY MODEL: the review_assignments row IS the ACL. Portal reads
# take ONLY an assignment_id; the reviewer must equal the SIGNED TOKEN's
# employee_id, tenant must match, and the subject is DERIVED from the row —
# subject ids are never request parameters. A reviewer sees the subject's
# name + role + department, nothing else. Aggregates are admin-only.

PERF_RELATIONS = ("self", "manager", "peer", "subordinate")
PERF_SCALE_MAX = 20                      # matches the interview scorer's 0-20
PERF_WEIGHTS = {"manager": 0.50, "self": 0.10, "peer": 0.25, "subordinate": 0.15}
PERF_PEER_ANON_FLOOR = 3                 # hide individual peer scores below this
PERF_CYCLE_STATUSES = ("draft", "active", "closed")
PERF_DEFAULT_COMPETENCIES = [
    {"name": "Job Knowledge", "description": "Understands the work and the tools it needs."},
    {"name": "Quality of Work", "description": "Accuracy, care, and finish of what they produce."},
    {"name": "Communication", "description": "Clear, timely, and honest in speech and writing."},
    {"name": "Teamwork", "description": "Works well with others; gives and accepts help."},
    {"name": "Ownership & Reliability", "description": "Delivers what they committed to, without chasing."},
]

_PERF_SUBJECT_PROJ = {"name": 1, "role_title": 1, "department": 1}   # ALL a reviewer may see


def _perf_subject_view(emp: dict | None) -> dict:
    """The ONLY representation of a subject a reviewer ever receives."""
    e = emp or {}
    return {"name": e.get("name") or "—",
            "role_title": e.get("role_title") or "",
            "department": e.get("department") or ""}


async def create_perf_cycle(user_id: str, name: str, competencies: list | None) -> dict:
    comps = []
    for c in (competencies or [])[:8]:
        if isinstance(c, dict) and str(c.get("name", "")).strip():
            comps.append({"name": str(c["name"]).strip()[:80],
                          "description": str(c.get("description", "")).strip()[:300]})
    if len(comps) < 2:
        comps = [dict(c) for c in PERF_DEFAULT_COMPETENCIES]
    doc = {"user_id": user_id, "name": (name or "Performance cycle").strip()[:80],
           "period": "quarterly", "status": "draft",
           "competencies": comps, "weights": dict(PERF_WEIGHTS),
           "scale_max": PERF_SCALE_MAX, "peer_anon_floor": PERF_PEER_ANON_FLOOR,
           "created_at": datetime.utcnow()}
    res = await db.perf_cycles.insert_one(doc)
    doc["_id"] = str(res.inserted_id)
    return serialize_mongo(doc)


async def list_perf_cycles(user_id: str) -> list:
    out = []
    async for c in db.perf_cycles.find(
            user_match_field("user_id", user_id)).sort("created_at", -1).limit(40):
        out.append(serialize_mongo(c))
    return out


async def get_perf_cycle(user_id: str, cycle_id: str) -> dict | None:
    try:
        c = await db.perf_cycles.find_one(
            {**user_match_field("user_id", user_id), "_id": ObjectId(cycle_id)})
    except Exception:
        return None
    return serialize_mongo(c) if c else None


async def set_perf_cycle_status(user_id: str, cycle_id: str,
                                to_status: str, from_statuses: tuple) -> bool:
    try:
        res = await db.perf_cycles.update_one(
            {**user_match_field("user_id", user_id), "_id": ObjectId(cycle_id),
             "status": {"$in": list(from_statuses)}},
            {"$set": {"status": to_status, f"{to_status}_at": datetime.utcnow()}})
        return res.modified_count == 1
    except Exception:
        return False


async def delete_perf_cycle(user_id: str, cycle_id: str) -> bool:
    """Draft or closed cycles only — an active cycle must be closed first."""
    try:
        res = await db.perf_cycles.delete_one(
            {**user_match_field("user_id", user_id), "_id": ObjectId(cycle_id),
             "status": {"$in": ["draft", "closed"]}})
        if res.deleted_count == 1:
            await db.perf_assignments.delete_many({"cycle_id": cycle_id})
            return True
        return False
    except Exception:
        return False


async def add_perf_assignment(user_id: str, cycle_id: str, subject_id: str,
                              reviewer_id: str, relation: str) -> tuple[dict | None, str | None]:
    if relation not in PERF_RELATIONS:
        return None, "Unknown relation."
    if relation == "self" and subject_id != reviewer_id:
        return None, "A self review must have the same subject and reviewer."
    if relation != "self" and subject_id == reviewer_id:
        return None, "Only a self review may point at yourself."
    cycle = await get_perf_cycle(user_id, cycle_id)
    if not cycle:
        return None, "Cycle not found."
    if cycle["status"] != "draft":
        return None, "Assignments can only be edited on a draft cycle."
    # Both people must be active employees of THIS tenant.
    for label, eid in (("subject", subject_id), ("reviewer", reviewer_id)):
        try:
            emp = await db.employees.find_one(
                {**user_match_field("user_id", user_id), "_id": ObjectId(eid),
                 "status": {"$ne": "terminated"}})
        except Exception:
            emp = None
        if not emp:
            return None, f"The {label} is not an active employee of this account."
    doc = {"user_id": user_id, "cycle_id": cycle_id,
           "subject_employee_id": subject_id, "reviewer_employee_id": reviewer_id,
           "relation": relation, "status": "pending", "answers": None,
           "created_at": datetime.utcnow()}
    await db.perf_assignments.update_one(
        {"cycle_id": cycle_id, "subject_employee_id": subject_id,
         "reviewer_employee_id": reviewer_id, "relation": relation},
        {"$setOnInsert": doc}, upsert=True)
    saved = await db.perf_assignments.find_one(
        {"cycle_id": cycle_id, "subject_employee_id": subject_id,
         "reviewer_employee_id": reviewer_id, "relation": relation})
    return serialize_mongo(saved), None


async def remove_perf_assignment(user_id: str, assignment_id: str) -> bool:
    try:
        a = await db.perf_assignments.find_one(
            {**user_match_field("user_id", user_id), "_id": ObjectId(assignment_id)})
    except Exception:
        return False
    if not a:
        return False
    cycle = await get_perf_cycle(user_id, a["cycle_id"])
    if not cycle or cycle["status"] != "draft":
        return False
    await db.perf_assignments.delete_one({"_id": a["_id"]})
    return True


async def list_perf_assignments(user_id: str, cycle_id: str) -> list:
    """Admin matrix view — names joined for readability. Admin-only caller."""
    out = []
    async for a in db.perf_assignments.find(
            {**user_match_field("user_id", user_id), "cycle_id": cycle_id}):
        out.append(serialize_mongo(a))
    ids = set()
    for a in out:
        ids.add(a["subject_employee_id"])
        ids.add(a["reviewer_employee_id"])
    names = {}
    oids = []
    for i in ids:
        try:
            oids.append(ObjectId(i))
        except Exception:
            pass
    if oids:
        async for e in db.employees.find({"_id": {"$in": oids}}, {"name": 1}):
            names[str(e["_id"])] = e.get("name")
    for a in out:
        a["subject_name"] = names.get(a["subject_employee_id"], "?")
        a["reviewer_name"] = names.get(a["reviewer_employee_id"], "?")
    return out


# ── Portal side: scope comes ONLY from the signed token's claims ──

async def list_reviews_for_employee(tenant: str, employee_id: str) -> list:
    """Every assignment naming THIS employee as reviewer, on ACTIVE cycles.
    The subject appears as the minimal projection, nothing more."""
    out = []
    cycles = {}
    async for a in db.perf_assignments.find(
            {**user_match_field("user_id", tenant),
             "reviewer_employee_id": employee_id}).sort("created_at", 1):
        cid = a["cycle_id"]
        if cid not in cycles:
            try:
                c = await db.perf_cycles.find_one({"_id": ObjectId(cid)})
            except Exception:
                c = None
            # tenant re-verified even though the assignment was tenant-filtered
            cycles[cid] = c if (c and str(c.get("user_id")) == str(tenant)) else None
        cycle = cycles[cid]
        if not cycle or cycle.get("status") != "active":
            continue
        try:
            subj = await db.employees.find_one(
                {"_id": ObjectId(a["subject_employee_id"])}, _PERF_SUBJECT_PROJ)
        except Exception:
            subj = None
        out.append({"assignment_id": str(a["_id"]),
                    "relation": a["relation"], "status": a["status"],
                    "cycle_name": cycle.get("name"),
                    "subject": _perf_subject_view(subj)})
    return out


async def get_review_for_employee(tenant: str, employee_id: str,
                                  assignment_id: str) -> dict | None:
    """THE ACL CHECK. None for: unknown id, someone else's assignment, wrong
    tenant, inactive cycle — all identical, probe-resistant."""
    try:
        a = await db.perf_assignments.find_one(
            {**user_match_field("user_id", tenant), "_id": ObjectId(assignment_id),
             "reviewer_employee_id": employee_id})
    except Exception:
        return None
    if not a:
        return None
    cycle = await db.perf_cycles.find_one({"_id": ObjectId(a["cycle_id"])})
    if not cycle or str(cycle.get("user_id")) != str(tenant) or cycle.get("status") != "active":
        return None
    try:
        subj = await db.employees.find_one(
            {"_id": ObjectId(a["subject_employee_id"])}, _PERF_SUBJECT_PROJ)
    except Exception:
        subj = None
    return {"assignment_id": str(a["_id"]), "relation": a["relation"],
            "status": a["status"], "answers": a.get("answers"),
            "cycle_name": cycle.get("name"),
            "competencies": cycle.get("competencies") or [],
            "scale_max": cycle.get("scale_max", PERF_SCALE_MAX),
            "subject": _perf_subject_view(subj)}


async def submit_review_answers(tenant: str, employee_id: str, assignment_id: str,
                                answers: dict) -> tuple[bool, str]:
    """One-shot submit on a pending assignment of an active cycle. The write
    filter re-checks reviewer + tenant + status atomically."""
    view = await get_review_for_employee(tenant, employee_id, assignment_id)
    if not view:
        return False, "Review not found."
    if view["status"] != "pending":
        return False, "This review was already submitted."
    res = await db.perf_assignments.update_one(
        {**user_match_field("user_id", tenant), "_id": ObjectId(assignment_id),
         "reviewer_employee_id": employee_id, "status": "pending"},
        {"$set": {"status": "submitted", "answers": answers,
                  "submitted_at": datetime.utcnow()}})
    return (res.modified_count == 1,
            "" if res.modified_count == 1 else "This review was already submitted.")


# ── P4: aggregation — separate perspectives first, weighted headline second ──

async def aggregate_perf_cycle(user_id: str, cycle_id: str) -> dict | None:
    """Per subject: each relation's averages shown SEPARATELY (n, overall avg,
    per-competency), then a weighted headline (0-100) with weights
    re-normalized over the relations that actually submitted. Anonymity
    floor: below `peer_anon_floor` submitted peer/subordinate reviews, their
    comments are withheld (attributable) and the UI is told so — the average
    still shows. Comments that do show are sorted, never in reviewer order."""
    cycle = await get_perf_cycle(user_id, cycle_id)
    if not cycle:
        return None
    weights = cycle.get("weights") or dict(PERF_WEIGHTS)
    floor = int(cycle.get("peer_anon_floor", PERF_PEER_ANON_FLOOR))
    scale = int(cycle.get("scale_max", PERF_SCALE_MAX))

    by_subject: dict[str, list] = {}
    async for a in db.perf_assignments.find(
            {**user_match_field("user_id", user_id), "cycle_id": cycle_id}):
        by_subject.setdefault(a["subject_employee_id"], []).append(a)

    names = {}
    oids = []
    for sid in by_subject:
        try:
            oids.append(ObjectId(sid))
        except Exception:
            pass
    if oids:
        async for e in db.employees.find({"_id": {"$in": oids}}, _PERF_SUBJECT_PROJ):
            names[str(e["_id"])] = _perf_subject_view(e)

    subjects = []
    for sid, assigns in by_subject.items():
        per_rel = {}
        for rel in PERF_RELATIONS:
            rel_all = [a for a in assigns if a["relation"] == rel]
            if not rel_all:
                continue
            subs = [a for a in rel_all if a["status"] == "submitted" and a.get("answers")]
            per_comp: dict[str, list] = {}
            all_scores, comments = [], []
            for a in subs:
                for s in (a["answers"].get("scores") or []):
                    try:
                        v = float(s.get("score", 0))
                    except Exception:
                        continue
                    per_comp.setdefault(str(s.get("name")), []).append(v)
                    all_scores.append(v)
                    if str(s.get("comment", "")).strip():
                        comments.append(f"{s.get('name')}: {str(s['comment']).strip()}")
                oc = str((a["answers"].get("overall_comment") or "")).strip()
                if oc:
                    comments.append(oc)
            n = len(subs)
            hidden = rel in ("peer", "subordinate") and 0 < n < floor
            per_rel[rel] = {
                "assigned": len(rel_all), "submitted": n,
                "avg": round(sum(all_scores) / len(all_scores), 1) if all_scores else None,
                "per_competency": {k: round(sum(v) / len(v), 1) for k, v in per_comp.items()},
                "comments": [] if hidden else sorted(comments),
                "individuals_hidden": hidden,
            }
        present = {r: pr for r, pr in per_rel.items() if pr["avg"] is not None}
        headline = None
        if present:
            wsum = sum(weights.get(r, 0) for r in present)
            if wsum > 0:
                headline = round(sum(weights[r] * present[r]["avg"] for r in present)
                                 / wsum / scale * 100)
        submitted_total = sum(pr["submitted"] for pr in per_rel.values())
        subjects.append({
            "employee_id": sid,
            **(names.get(sid) or {"name": "?", "role_title": "", "department": ""}),
            "per_relation": per_rel,
            "headline": headline,
            "reviewers_total": submitted_total,
            "limited": submitted_total < 3,
            "completion": {"submitted": submitted_total,
                           "assigned": sum(pr["assigned"] for pr in per_rel.values())},
        })
    subjects.sort(key=lambda s: (1 if s["headline"] is None else 0, -(s["headline"] or 0)))
    return {"cycle": cycle, "subjects": subjects,
            "weights": weights, "anon_floor": floor, "scale_max": scale}


# ── Payroll (HRM module 2) — SKELETON: structure yes, money math HELD ──
# The tax line is a PLACEHOLDER (0) until the owner confirms BD tax slabs,
# LWP (leave-without-pay) is surfaced as days + a PROPOSED amount but is NOT
# deducted, and net is explicitly "net before tax". Marking a run "paid" is
# hard-blocked at the route until the math is confirmed. A wrong salary is
# the worst bug — nothing here computes money the owner hasn't approved.

PAYROLL_STATUSES = ("draft", "approved", "paid")
SALARY_MAX = 10_000_000   # BDT, sanity cap on any single component


def _sal_int(v) -> int:
    try:
        return max(0, min(SALARY_MAX, int(v)))
    except Exception:
        return 0


async def upsert_salary_structure(user_id: str, employee_id: str, body: dict) -> dict:
    doc = {
        "user_id": user_id,
        "employee_id": employee_id,
        "currency": "BDT",
        "basic": _sal_int(body.get("basic")),
        "house_rent": _sal_int(body.get("house_rent")),
        "medical": _sal_int(body.get("medical")),
        "transport": _sal_int(body.get("transport")),
        "pf_deduction": _sal_int(body.get("pf_deduction")),
        "updated_at": datetime.utcnow(),
    }
    await db.salary_structures.update_one(
        {**user_match_field("user_id", user_id), "employee_id": employee_id},
        {"$set": doc}, upsert=True)
    return doc


async def get_salary_structures(user_id: str) -> dict:
    out = {}
    async for s in db.salary_structures.find(user_match_field("user_id", user_id)):
        out[str(s.get("employee_id"))] = serialize_mongo(s)
    return out


async def _unpaid_leave_days(user_id: str, employee_id: str, month: str) -> int:
    """Approved unpaid-leave days whose leave STARTS in the month — the same
    string-range convention the leave module already uses. APPROXIMATION,
    flagged on every payslip: a request spanning a month boundary counts
    wholly in its start month. Do not finalize a deduction on this without
    the owner's sign-off."""
    total = 0
    async for lr in db.leave_requests.find({
            **user_match_field("user_id", user_id),
            "employee_id": employee_id, "type": "unpaid", "status": "approved",
            "start_date": {"$gte": f"{month}-01", "$lte": f"{month}-31"}}):
        try:
            total += int(lr.get("days") or 0)
        except Exception:
            pass
    return total


async def create_payroll_run(user_id: str, month: str) -> tuple[dict | None, str | None]:
    """Draft run for a month: one draft payslip per active employee WITH a
    salary structure. Replaces an existing DRAFT for the same month; refuses
    to touch an approved/paid one. Returns (run, error)."""
    existing = await db.payroll_runs.find_one(
        {**user_match_field("user_id", user_id), "month": month})
    if existing and existing.get("status") != "draft":
        return None, f"A {existing.get('status')} run already exists for {month} — it can't be replaced."
    structures = await get_salary_structures(user_id)
    if not structures:
        return None, "No salary structures set yet — set salaries on the Payroll page first."

    slips, totals = [], {"gross": 0, "pf": 0, "net_before_tax": 0}
    async for e in db.employees.find(
            {**user_match_field("user_id", user_id), "status": {"$ne": "terminated"}}):
        s = structures.get(str(e["_id"]))
        if not s:
            continue
        gross = s["basic"] + s["house_rent"] + s["medical"] + s["transport"]
        lwp_days = await _unpaid_leave_days(user_id, str(e["_id"]), month)
        # PROPOSED formula only — shown, never deducted: basic / 30 * days.
        lwp_proposed = round(s["basic"] / 30 * lwp_days) if lwp_days else 0
        net_bt = gross - s["pf_deduction"]
        slips.append({
            "employee_id": str(e["_id"]),
            "employee_name": e.get("name"),
            "department": e.get("department") or "",
            "month": month,
            "basic": s["basic"], "house_rent": s["house_rent"],
            "medical": s["medical"], "transport": s["transport"],
            "gross": gross,
            "pf_deduction": s["pf_deduction"],
            "lwp_days": lwp_days,
            "lwp_proposed_amount": lwp_proposed,   # NOT deducted — proposal only
            "tax_placeholder": 0,                   # TODO: BD tax slabs, owner-confirmed
            "net_before_tax": net_bt,
        })
        totals["gross"] += gross
        totals["pf"] += s["pf_deduction"]
        totals["net_before_tax"] += net_bt
    if not slips:
        return None, "No active employees have a salary structure for this run."

    now = datetime.utcnow()
    run_doc = {"user_id": user_id, "month": month, "status": "draft",
               "employees": len(slips), "totals": totals,
               "math_held": True,   # tax placeholder + LWP not deducted
               "created_at": now}
    if existing:
        await db.payslips.delete_many({"run_id": str(existing["_id"])})
        await db.payroll_runs.update_one({"_id": existing["_id"]}, {"$set": run_doc})
        run_id = str(existing["_id"])
    else:
        res = await db.payroll_runs.insert_one(run_doc)
        run_id = str(res.inserted_id)
    for sl in slips:
        sl["run_id"] = run_id
        sl["user_id"] = user_id
    await db.payslips.insert_many(slips)
    run_doc["_id"] = run_id
    return serialize_mongo(run_doc), None


async def list_payroll_runs(user_id: str) -> list:
    out = []
    async for r in db.payroll_runs.find(
            user_match_field("user_id", user_id)).sort("month", -1).limit(36):
        out.append(serialize_mongo(r))
    return out


async def get_payroll_run(user_id: str, run_id: str) -> tuple[dict | None, list]:
    try:
        run = await db.payroll_runs.find_one(
            {**user_match_field("user_id", user_id), "_id": ObjectId(run_id)})
    except Exception:
        return None, []
    if not run:
        return None, []
    slips = []
    async for s in db.payslips.find({"run_id": str(run["_id"])}).sort("employee_name", 1):
        slips.append(serialize_mongo(s))
    return serialize_mongo(run), slips


async def approve_payroll_run(user_id: str, run_id: str) -> bool:
    try:
        res = await db.payroll_runs.update_one(
            {**user_match_field("user_id", user_id), "_id": ObjectId(run_id), "status": "draft"},
            {"$set": {"status": "approved", "approved_at": datetime.utcnow()}})
        return res.modified_count == 1
    except Exception:
        return False


# ── KPI dashboard ────────────────────────────────────────────
# Only what the SPA cannot honestly compute client-side. The funnel, screening
# volume and per-job averages come from CandidateStats over /api/screenings —
# the same rows and definitions as every other page, deliberately NOT
# recomputed here. Every block below returns its n so the UI can label
# limited-data KPIs instead of dressing them up as trends.

async def kpi_data(user_id: str, start: datetime, end: datetime,
                   start_s: str, end_s: str) -> dict:
    """start/end are inclusive datetimes for datetime-stamped collections;
    start_s/end_s are the same bounds as YYYY-MM-DD strings for the HRM
    collections, whose dates are lexicographic strings by design."""
    scope = user_match_field("user_id", user_id)

    # ALL funnel hires, all-time — the list is tiny (employee records created
    # from screenings). The client range-filters it and derives time-to-hire,
    # time-to-fill, MTD/QTD/YTD and growth from this ONE list, so every hire
    # number on the page agrees by construction.
    emp_docs = []
    async for e in db.employees.find(
            {**scope, "source": "hired"},
            {"screening_id": 1, "created_at": 1}):
        emp_docs.append(e)
    scr_ids = []
    for e in emp_docs:
        try:
            scr_ids.append(ObjectId(str(e.get("screening_id"))))
        except Exception:
            pass
    scr_info = {}
    if scr_ids:
        async for s in db.screenings.find(
                {"_id": {"$in": scr_ids}},
                {"created_at": 1, "job_id": 1, "source": 1}):
            scr_info[str(s["_id"])] = s
    hires = []
    for e in emp_docs:
        info = scr_info.get(str(e.get("screening_id") or ""))
        hires.append({
            "hired_at": e["created_at"].strftime("%Y-%m-%d") if e.get("created_at") else None,
            "screened_at": (info["created_at"].strftime("%Y-%m-%d")
                            if info is not None and info.get("created_at") else None),
            "job_id": str(info.get("job_id")) if info is not None and info.get("job_id") else None,
            # The channel the system actually tracks — never a guessed
            # marketing source.
            "channel": ("Apply link" if info is not None and info.get("source") == "public_apply"
                        else ("Direct upload" if info is not None else None)),
        })

    iv_match = {**scope, "created_at": {"$gte": start, "$lte": end}}
    interviews = {
        "sessions": await db.interview_sessions.count_documents(iv_match),
        "completed": await db.interview_sessions.count_documents(
            {**iv_match, "status": "completed"}),
    }

    # Headcount is a NOW snapshot, not a range metric — the schema keeps no
    # employment history beyond start/end dates, and pretending otherwise
    # would be a fabricated trend.
    emp_active = {**scope, "status": {"$ne": "terminated"}}
    by_dept = []
    async for row in db.employees.aggregate([
            {"$match": emp_active},
            {"$group": {"_id": {"$ifNull": ["$department", ""]}, "n": {"$sum": 1}}},
            {"$sort": {"n": -1}}]):
        by_dept.append({"department": row["_id"] or "Unassigned", "n": row["n"]})
    headcount = {
        "total": await db.employees.count_documents(emp_active),
        "on_leave": await db.employees.count_documents({**scope, "status": "on_leave"}),
        "by_department": by_dept,
    }

    # Attendance over the range, per day and per status. Rate is computed
    # client-side as present/(present+absent) — leave and holiday are excluded
    # from the denominator, and the UI says so.
    att_days: dict[str, dict] = {}
    att_totals = {"present": 0, "absent": 0, "leave": 0, "holiday": 0}
    async for row in db.attendance.aggregate([
            {"$match": {**scope, "date": {"$gte": start_s, "$lte": end_s}}},
            {"$group": {"_id": {"date": "$date", "status": "$status"}, "n": {"$sum": 1}}}]):
        d, st = row["_id"].get("date"), row["_id"].get("status")
        if not d or st not in att_totals:
            continue
        att_days.setdefault(d, {"date": d, "present": 0, "absent": 0, "leave": 0, "holiday": 0})
        att_days[d][st] += row["n"]
        att_totals[st] += row["n"]
    attendance = {
        "days": sorted(att_days.values(), key=lambda x: x["date"]),
        "totals": att_totals,
        "marked_days": len(att_days),
    }

    # Leave: approved days whose leave STARTS in the range, by type. The
    # allowance total is the sum of per-employee ANNUAL allowances — the UI
    # must present utilization as vs-annual-allowance, which only reads
    # honestly on a year-ish range.
    leave_by_type = []
    async for row in db.leave_requests.aggregate([
            {"$match": {**scope, "status": "approved",
                        "start_date": {"$gte": start_s, "$lte": end_s}}},
            {"$group": {"_id": "$type", "days": {"$sum": "$days"}, "n": {"$sum": 1}}},
            {"$sort": {"days": -1}}]):
        leave_by_type.append({"type": row["_id"] or "other",
                              "days": row["days"], "requests": row["n"]})
    allowances = {"annual": 0, "sick": 0}
    async for e in db.employees.find(emp_active, {"leave_allowances": 1}):
        la = e.get("leave_allowances") or {}
        allowances["annual"] += int(la.get("annual") or 0)
        allowances["sick"] += int(la.get("sick") or 0)
    leave = {
        "by_type": leave_by_type,
        "allowance_totals": allowances,
        "pending_now": await db.leave_requests.count_documents(
            {**scope, "status": "pending"}),
    }

    # Employment timeline, all-time and name-free: enough to reconstruct
    # headcount at any date, plus new joiners, turnover and retention — all
    # client-side from ONE list, so those four can never disagree.
    timeline = []
    async for e in db.employees.find(
            scope, {"start_date": 1, "end_date": 1, "created_at": 1, "status": 1}):
        st = (e.get("start_date") or "").strip() or (
            e["created_at"].strftime("%Y-%m-%d") if e.get("created_at") else None)
        en = (e.get("end_date") or "").strip() or None
        if e.get("status") != "terminated":
            en = None
        if st:
            timeline.append({"start": st, "end": en})

    # Performance KPIs — the latest CLOSED cycle's headlines. None until a
    # cycle has closed; the UI keeps its honest coming-soon state until then.
    performance = None
    latest_closed = await db.perf_cycles.find_one(
        {**scope, "status": "closed"}, sort=[("closed_at", -1)])
    if latest_closed:
        agg = await aggregate_perf_cycle(user_id, str(latest_closed["_id"]))
        heads = [s["headline"] for s in (agg or {}).get("subjects", [])
                 if s.get("headline") is not None]
        if heads:
            performance = {
                "cycle_name": latest_closed.get("name"),
                "n": len(heads),
                "avg_headline": round(sum(heads) / len(heads)),
                "high_pct": round(sum(1 for h in heads if h >= 80) / len(heads) * 100),
                "high_threshold": 80,
            }

    # Jobs posted — tenant-scoped, all-time (total incl. deactivated + active).
    jobs_total = await db.jobs.count_documents(user_match_field("user_id", user_id))
    jobs_active = await db.jobs.count_documents(
        {**user_match_field("user_id", user_id), "active": True})

    return {
        "range": {"start": start_s, "end": end_s},
        "hires": hires,
        "interviews": interviews,
        "jobs": {"total": jobs_total, "active": jobs_active},
        "hr": {"headcount": headcount, "attendance": attendance,
               "leave": leave, "timeline": timeline,
               "performance": performance},
    }


# ── Live interview sessions (L4) ─────────────────────────────

async def save_interview_session(doc: dict) -> str:
    """Persist one completed/abandoned live interview: full transcript (both
    sides), config, turn count, drop/recovery/barge-in events. Metadata only —
    no audio or video is stored here (that is L5's proctoring capture)."""
    doc = {**doc, "created_at": datetime.utcnow()}
    doc.pop("_id", None)
    res = await db.interview_sessions.insert_one(doc)
    return str(res.inserted_id)


async def get_interview_sessions(limit: int = 100) -> list:
    cursor = db.interview_sessions.find(
        {}, {"transcript": {"$slice": 2}}).sort("created_at", -1).limit(limit)
    out = []
    async for doc in cursor:
        out.append(serialize_mongo(doc))
    return out


async def get_interview_session(session_id: str) -> dict | None:
    try:
        doc = await db.interview_sessions.find_one({"_id": ObjectId(session_id)})
    except Exception:
        return None
    return serialize_mongo(doc) if doc else None
