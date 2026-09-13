"""
Stored-unscored retry sweep (scale fix, 2026-09-14) — every 30 min via
tc-screening-retry.timer.

Before this, a screening that failed transiently (an OpenAI 429/crash) or was
refused by a cap sat in stored_unscored until someone manually re-drove it.
This sweep self-heals both, with the CAP SEMANTICS kept honest:

  - CAPPED rows (capped_by set — the reservation was REFUSED, nothing was
    consumed): re-check the org's plan state (paused orgs are skipped) and
    RE-ATTEMPT the reservation; only on success clear the flag and score.
    This delivers the promised "runs again after an upgrade or next month"
    automatically.
  - TRANSIENT-FAILURE rows (error set — the crash happened AFTER the
    reservation was consumed): re-score directly, NO second reservation, so
    nothing is ever double-counted.
  - PERMANENT parse failures ("CV file missing", "No text could be
    extracted") are never retried — the outcome is deterministic and each
    retry would burn money.

Bounded and loop-proof: <= MAX_PER_RUN rows per run, sequential (this is its
own process — the app's event loop is untouched), retry_count capped at
MAX_RETRIES with LAST_RETRY_SPACING between attempts. After the cap the row
stays visible in the recruiter's pending queue exactly as today.

Runs the app's OWN score_application, so there is exactly one scoring path.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

MAX_PER_RUN = 25
MAX_RETRIES = 5
LAST_RETRY_SPACING = timedelta(minutes=30)
PERMANENT_ERRORS = ("CV file missing", "No text could be extracted")


async def run():
    import database
    await database.connect()
    import main   # the app module — imported for its helpers, server not started
    from bson import ObjectId
    db = database.db
    now = datetime.utcnow()

    retried = skipped = failed = 0
    q = {
        "status": "stored_unscored",
        "retry_count": {"$not": {"$gte": MAX_RETRIES}},
        "$or": [{"last_retry_at": {"$exists": False}},
                {"last_retry_at": {"$lt": now - LAST_RETRY_SPACING}}],
    }
    async for a in db.applications.find(q).sort("submitted_at", 1).limit(MAX_PER_RUN * 2):
        if retried >= MAX_PER_RUN:
            break
        aid = a["_id"]
        err = str(a.get("error") or "")
        if any(p in err for p in PERMANENT_ERRORS):
            continue   # deterministic failure — retrying burns money

        job = None
        if a.get("job_id"):
            try:
                job = await db.jobs.find_one({"_id": ObjectId(str(a["job_id"]))})
            except Exception:
                job = None
        if not job:
            continue
        org_id = str(job.get("org_id") or "")

        capped = bool(a.get("capped_by"))
        if capped:
            # Reservation was refused originally — must succeed NOW before
            # any money is spent. Paused orgs wait for payment.
            plan = await main._org_plan(org_id)
            if plan.get("state") == "paused":
                skipped += 1
                continue
            ok, blocked_by = await database.reserve_screening_slot(
                str(job["_id"]), org_id, plan["applicants_mo"])
            if not ok:
                await db.applications.update_one(
                    {"_id": aid}, {"$set": {"last_retry_at": now,
                                            "capped_by": blocked_by},
                                   "$inc": {"retry_count": 1}})
                skipped += 1
                continue

        await db.applications.update_one(
            {"_id": aid}, {"$set": {"status": "pending", "last_retry_at": now},
                           "$unset": {"capped_by": "", "error": ""},
                           "$inc": {"retry_count": 1}})
        try:
            await main.score_application(str(aid))
            retried += 1
        except Exception as e:
            failed += 1
            print(f"  retry failed for {aid}: {str(e)[:120]}")

    print(f"{datetime.utcnow().isoformat()}Z screening retry sweep: "
          f"retried={retried} capped/paused-skipped={skipped} failed={failed}")


if __name__ == "__main__":
    asyncio.run(run())
