"""
Migration 001 — move CV PDFs out of screening documents.

WHY
    Every screening currently carries the whole CV inside itself as
    `cv_pdf_b64`, base64-encoded. Base64 costs 33% over the raw bytes, and it
    puts a ~250KB payload inside a document whose useful content is ~13KB.
    At 270 screenings that is ~70MB of a 512MB Atlas M0 cluster.

    This moves the binary into its own collection, stored as raw BSON binData
    (no base64 tax), with a TTL index so PDFs age out automatically.

SAFETY MODEL
    Three separate commands, run in order, each verified before the next:

        1. copy    — writes application_files. Touches NOTHING in screenings.
        2. verify  — proves every screening's PDF is present, correct length,
                     and byte-identical to the original.
        3. unset   — only then removes cv_pdf_b64 from screenings.
                     REFUSES to run unless verify passes for every document.

    Nothing is destroyed until a byte-for-byte copy is confirmed to exist.
    Run with --dry-run first; that is also the default.

REVERSIBILITY — READ THIS
    `rollback` restores cv_pdf_b64 from application_files. It works only while
    those documents still exist.

    The TTL index deletes them 30 days after the expiry stamp this migration
    writes. After that, rollback has nothing to read from and the PDFs are
    gone from the database permanently.

    A DigitalOcean Droplet snapshot does NOT contain them — the droplet runs
    the app; the data lives in MongoDB Atlas, a separate service. Atlas M0
    has no automated backups.

    So: take a mongodump BEFORE running `unset`. That dump is the only copy
    that survives day 30. See MIGRATION-001.md.

USAGE
    python migrations/001_pdfs_to_application_files.py copy     --dry-run
    python migrations/001_pdfs_to_application_files.py copy     --commit
    python migrations/001_pdfs_to_application_files.py verify
    python migrations/001_pdfs_to_application_files.py unset    --commit
    python migrations/001_pdfs_to_application_files.py rollback --commit
    python migrations/001_pdfs_to_application_files.py status

Every command is idempotent and resumable. Re-running `copy` skips documents
already copied; re-running `unset` skips documents already unset.
"""

import argparse
import asyncio
import base64
import hashlib
import os
import sys
from datetime import datetime, timedelta

from bson import ObjectId, Binary
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "talentscore")

# The retention window applies from the migration date, not from when each CV
# was screened. Most existing screenings are already older than 30 days, so
# dating from created_at would expire almost all of them within the hour.
# Consequence worth knowing: every migrated PDF expires on the SAME day.
RETENTION_DAYS = 30

BATCH = 50  # documents held in memory at once — M0 is small, so is the RAM


def connect():
    client = AsyncIOMotorClient(
        MONGO_URI,
        serverSelectionTimeoutMS=30000,
        connectTimeoutMS=30000,
        socketTimeoutMS=30000,
        tlsAllowInvalidCertificates=True,
        tlsAllowInvalidHostnames=True,
    )
    return client, client[DB_NAME]


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


# ──────────────────────────────────────────────────────────────────────
# status — read-only, safe to run any time
# ──────────────────────────────────────────────────────────────────────
async def cmd_status(db):
    total = await db.screenings.count_documents({})
    with_pdf = await db.screenings.count_documents({"cv_pdf_b64": {"$exists": True}})
    linked = await db.screenings.count_documents({"cv_file_id": {"$exists": True}})
    files = await db.application_files.count_documents({})

    print(f"screenings total .................. {total}")
    print(f"  still holding cv_pdf_b64 ........ {with_pdf}")
    print(f"  linked to application_files ..... {linked}")
    print(f"application_files documents ....... {files}")

    try:
        stats = await db.command("collStats", "screenings")
    except Exception as e:
        stats = {}
        print(f"(collStats unavailable on this tier: {e})")
    print(f"\nscreenings storage ................ {human(stats.get('size', 0))}")
    print(f"screenings avg doc ................ {human(stats.get('avgObjSize', 0))}")
    try:
        fstats = await db.command("collStats", "application_files")
        print(f"application_files storage ......... {human(fstats.get('size', 0))}")
    except Exception:
        print("application_files storage ......... (collection does not exist yet)")

    idx = await db.application_files.index_information()
    ttl = [n for n, spec in idx.items() if "expireAfterSeconds" in spec]
    print(f"\nTTL index present ................. {'yes — ' + ttl[0] if ttl else 'NO'}")
    if files:
        soonest = await db.application_files.find_one(sort=[("expires_at", 1)])
        if soonest and soonest.get("expires_at"):
            left = soonest["expires_at"] - datetime.utcnow()
            print(f"first expiry in ................... {left.days} days")


# ──────────────────────────────────────────────────────────────────────
# copy — writes application_files only. screenings is untouched.
# ──────────────────────────────────────────────────────────────────────
async def cmd_copy(db, commit: bool):
    expires_at = datetime.utcnow() + timedelta(days=RETENTION_DAYS)

    cursor = db.screenings.find(
        {"cv_pdf_b64": {"$exists": True}, "cv_file_id": {"$exists": False}},
        {"cv_pdf_b64": 1, "cv_filename": 1, "job_id": 1, "user_id": 1},
    ).batch_size(10)

    copied = skipped = failed = 0
    total_bytes = saved_bytes = 0

    async for doc in cursor:
        sid = doc["_id"]
        try:
            raw = base64.b64decode(doc["cv_pdf_b64"])
        except Exception as e:
            print(f"  ! {sid} — undecodable base64, left alone: {e}")
            failed += 1
            continue

        if not raw.startswith(b"%PDF-"):
            # Not fatal: store it anyway so nothing is lost, but say so loudly.
            print(f"  ? {sid} — payload is not a PDF (no %PDF- header), copying regardless")

        b64_len = len(doc["cv_pdf_b64"])
        total_bytes += len(raw)
        saved_bytes += b64_len - len(raw)

        if not commit:
            copied += 1
            continue

        existing = await db.application_files.find_one({"screening_id": sid}, {"_id": 1})
        if existing:
            skipped += 1
            continue

        res = await db.application_files.insert_one({
            "screening_id": sid,
            "job_id": doc.get("job_id"),
            "user_id": doc.get("user_id"),
            "data": Binary(raw),
            "filename": doc.get("cv_filename"),
            "size": len(raw),
            "sha256": sha(raw),
            "source": "migration_001",
            "created_at": datetime.utcnow(),
            "expires_at": expires_at,
        })
        # Link the screening to its file. This is an ADD, not a removal —
        # cv_pdf_b64 is still present and still authoritative at this point.
        await db.screenings.update_one(
            {"_id": sid}, {"$set": {"cv_file_id": res.inserted_id}}
        )
        copied += 1

    verb = "would copy" if not commit else "copied"
    print(f"\n{verb} .......................... {copied}")
    print(f"already present, skipped .......... {skipped}")
    print(f"failed ............................ {failed}")
    print(f"raw PDF bytes ..................... {human(total_bytes)}")
    print(f"base64 overhead reclaimed ......... {human(saved_bytes)}")
    if not commit:
        print("\nDRY RUN — nothing was written. Re-run with --commit.")
    else:
        print("\nNext: run `verify` before `unset`.")


# ──────────────────────────────────────────────────────────────────────
# verify — proves the copy is byte-identical. Read-only.
# ──────────────────────────────────────────────────────────────────────
async def cmd_verify(db):
    cursor = db.screenings.find(
        {"cv_pdf_b64": {"$exists": True}}, {"cv_pdf_b64": 1, "cv_file_id": 1}
    ).batch_size(10)

    ok = missing = mismatch = 0
    bad_ids = []

    async for doc in cursor:
        sid = doc["_id"]
        f = await db.application_files.find_one({"screening_id": sid}, {"data": 1, "sha256": 1})
        if not f:
            missing += 1
            bad_ids.append(("missing", sid))
            continue
        original = base64.b64decode(doc["cv_pdf_b64"])
        if bytes(f["data"]) != original or f.get("sha256") != sha(original):
            mismatch += 1
            bad_ids.append(("mismatch", sid))
            continue
        ok += 1

    print(f"verified byte-identical ........... {ok}")
    print(f"missing from application_files .... {missing}")
    print(f"content mismatch .................. {mismatch}")

    if bad_ids:
        print("\nProblem documents:")
        for kind, sid in bad_ids[:20]:
            print(f"  {kind:10} {sid}")
        if len(bad_ids) > 20:
            print(f"  ... and {len(bad_ids) - 20} more")
        print("\nVERIFY FAILED — do not run `unset`. Re-run `copy --commit` first.")
        return False

    print("\nVERIFY PASSED — every PDF has a byte-identical copy.")
    return True


# ──────────────────────────────────────────────────────────────────────
# unset — the only destructive step. Gated on verify.
# ──────────────────────────────────────────────────────────────────────
async def cmd_unset(db, commit: bool):
    print("Re-running verification before touching anything...\n")
    if not await cmd_verify(db):
        sys.exit(1)

    n = await db.screenings.count_documents({"cv_pdf_b64": {"$exists": True}})
    print(f"\nwould remove cv_pdf_b64 from ...... {n} screenings")

    if not commit:
        print("\nDRY RUN — nothing was written. Re-run with --commit.")
        return

    print("\nHave you taken a mongodump? After the TTL fires in "
          f"{RETENTION_DAYS} days, that dump is the only remaining copy.")
    if input("Type 'yes I have a dump' to proceed: ").strip() != "yes I have a dump":
        print("Aborted. Nothing was changed.")
        return

    res = await db.screenings.update_many(
        {"cv_pdf_b64": {"$exists": True}}, {"$unset": {"cv_pdf_b64": ""}}
    )
    print(f"\nunset from ........................ {res.modified_count} screenings")
    print("Storage will not drop immediately — WiredTiger reuses freed space")
    print("internally rather than returning it. Capacity is recovered regardless.")


# ──────────────────────────────────────────────────────────────────────
# rollback — restores cv_pdf_b64, while application_files still exists
# ──────────────────────────────────────────────────────────────────────
async def cmd_rollback(db, commit: bool):
    cursor = db.screenings.find(
        {"cv_pdf_b64": {"$exists": False}, "cv_file_id": {"$exists": True}}, {"cv_file_id": 1}
    )

    restored = gone = 0
    async for doc in cursor:
        f = await db.application_files.find_one({"screening_id": doc["_id"]}, {"data": 1})
        if not f:
            # TTL has already reaped it. There is nothing to restore from here.
            gone += 1
            continue
        if commit:
            await db.screenings.update_one(
                {"_id": doc["_id"]},
                {"$set": {"cv_pdf_b64": base64.b64encode(bytes(f["data"])).decode()}},
            )
        restored += 1

    verb = "would restore" if not commit else "restored"
    print(f"{verb} ....................... {restored}")
    print(f"unrecoverable (file expired) ...... {gone}")
    if gone:
        print("\nThose PDFs are gone from the database. Restore them from your")
        print("mongodump, or they are lost.")
    if not commit:
        print("\nDRY RUN — nothing was written. Re-run with --commit.")


# ──────────────────────────────────────────────────────────────────────
async def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["status", "copy", "verify", "unset", "rollback"])
    g = p.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True)
    g.add_argument("--commit", action="store_true")
    args = p.parse_args()

    commit = bool(args.commit)

    client, db = connect()
    await client.admin.command("ping")
    print(f"connected: {DB_NAME}\n")

    try:
        if args.command == "status":
            await cmd_status(db)
        elif args.command == "copy":
            await cmd_copy(db, commit)
        elif args.command == "verify":
            await cmd_verify(db)
        elif args.command == "unset":
            await cmd_unset(db, commit)
        elif args.command == "rollback":
            await cmd_rollback(db, commit)
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
