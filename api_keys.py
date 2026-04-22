"""
api_keys.py — API Key management for TopCandidate.pro
======================================================
Handles generation, validation, and rate limiting of API keys.
"""

import os
import secrets
import hashlib
from datetime import datetime, timedelta
from database import db
from bson import ObjectId


# ── KEY GENERATION ──

def generate_api_key(prefix: str = "tc_live") -> tuple[str, str]:
    """
    Generate a new API key.
    Returns (raw_key, hashed_key)
    Raw key is shown once to user. Hashed key stored in DB.
    """
    raw = f"{prefix}_{secrets.token_urlsafe(32)}"
    hashed = hashlib.sha256(raw.encode()).hexdigest()
    return raw, hashed


def hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


# ── PLAN LIMITS ──

API_PLAN_LIMITS = {
    "trial":      {"screens_per_month": 10,   "rate_per_minute": 2,  "batch_size": 10},
    "starter":    {"screens_per_month": 100,  "rate_per_minute": 10, "batch_size": 20},
    "pro":        {"screens_per_month": 500,  "rate_per_minute": 30, "batch_size": 100},
    "enterprise": {"screens_per_month": 9999, "rate_per_minute": 60, "batch_size": 100},
}


# ── DB OPERATIONS ──

async def create_api_key(user_id: str, name: str, plan: str = "trial") -> dict:
    """Create and store a new API key for a user."""
    raw_key, hashed_key = generate_api_key()
    doc = {
        "user_id": user_id,
        "name": name,
        "key_hash": hashed_key,
        "key_prefix": raw_key[:12],  # Store prefix for display e.g. "tc_live_abc1"
        "plan": plan,
        "active": True,
        "created_at": datetime.utcnow(),
        "last_used_at": None,
        "screens_this_month": 0,
        "screens_total": 0,
        "month_reset_at": datetime.utcnow().replace(day=1),
        "webhook_url": None,
    }
    inserted = await db.api_keys.insert_one(doc)
    doc["_id"] = str(inserted.inserted_id)
    doc["raw_key"] = raw_key  # Only returned once
    return doc


async def get_api_key_by_hash(key_hash: str) -> dict | None:
    """Look up an API key by its hash."""
    doc = await db.api_keys.find_one({"key_hash": key_hash, "active": True})
    if doc:
        doc["_id"] = str(doc["_id"])
    return doc


async def validate_api_key(raw_key: str) -> dict | None:
    """Validate a raw API key and return the key document."""
    if not raw_key or not raw_key.startswith("tc_"):
        return None
    key_hash = hash_key(raw_key)
    return await get_api_key_by_hash(key_hash)


async def get_keys_for_user(user_id: str) -> list:
    """Get all API keys for a user."""
    cursor = db.api_keys.find({"user_id": user_id}).sort("created_at", -1)
    keys = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        doc.pop("key_hash", None)  # Never return hash
        keys.append(doc)
    return keys


async def revoke_api_key(key_id: str, user_id: str) -> bool:
    """Revoke an API key."""
    result = await db.api_keys.update_one(
        {"_id": ObjectId(key_id), "user_id": user_id},
        {"$set": {"active": False}}
    )
    return result.modified_count > 0


async def increment_api_usage(key_id: str) -> None:
    """Increment usage counter for an API key."""
    now = datetime.utcnow()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    await db.api_keys.update_one(
        {"_id": ObjectId(key_id)},
        {
            "$inc": {"screens_this_month": 1, "screens_total": 1},
            "$set": {"last_used_at": now},
            "$setOnInsert": {"month_reset_at": month_start}
        }
    )


async def check_rate_limit(key_doc: dict) -> tuple[bool, str]:
    """
    Check if key is within rate limits.
    Returns (allowed, reason)
    """
    plan = key_doc.get("plan", "trial")
    limits = API_PLAN_LIMITS.get(plan, API_PLAN_LIMITS["trial"])

    # Check monthly limit
    screens_this_month = key_doc.get("screens_this_month", 0)
    monthly_limit = limits["screens_per_month"]

    # Reset month counter if needed
    month_start = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    reset_at = key_doc.get("month_reset_at")
    if reset_at and reset_at < month_start:
        await db.api_keys.update_one(
            {"_id": ObjectId(key_doc["_id"])},
            {"$set": {"screens_this_month": 0, "month_reset_at": month_start}}
        )
        screens_this_month = 0

    if screens_this_month >= monthly_limit:
        return False, f"Monthly limit reached ({monthly_limit} screenings). Upgrade your plan."

    return True, "ok"


async def log_api_call(key_id: str, endpoint: str, status: int, user_id: str) -> None:
    """Log an API call for analytics."""
    await db.api_logs.insert_one({
        "key_id": key_id,
        "user_id": user_id,
        "endpoint": endpoint,
        "status": status,
        "timestamp": datetime.utcnow()
    })
