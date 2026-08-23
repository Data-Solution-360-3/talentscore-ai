"""
Spend-cap test — prove the cap stops spending, not just slows it.

WHAT IT PROVES
    With only 2 job slots left, three applications are submitted. The first two
    are scored. The third is stored, is returned the SAME success response, and
    causes ZERO OpenAI calls.

    Zero calls is the claim that matters. A cap that lets the request through
    and discards the result afterwards would still spend the money. The
    reservation is taken before anything is queued, so a refused reservation
    means no background task is ever created.

HOW THE CAP IS LOWERED
    It isn't. Editing CAP_PER_JOB would need a code change and a restart, and
    would test a different configuration than the one you actually run. Instead
    the job's spend counter is pre-seeded to CAP_PER_JOB - 2, leaving exactly
    two slots. That exercises the real cap, at its real value, atomically.

    The seeded counter is removed again at the end, so the job is left as found.

USAGE
    Run ON THE DROPLET — it needs both the database and the live HTTP server.

        cd ~/app && venv/bin/python scripts/test_spend_cap.py --job-id <id>

    Omit --job-id and it picks the first job that has a public token. The job is
    switched public for the duration and restored to its previous state after.

    Exit 0 = cap held. Exit 1 = the cap leaked, or the candidate was told.
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime

import httpx
from bson import ObjectId
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MONGO_URI = os.getenv("MONGO_URI", "")
DB_NAME = os.getenv("DB_NAME", "talentscore")

# A minimal but genuinely valid PDF with extractable text, so a scored
# application follows the real path rather than failing at the parse.
PDF = (
    b"%PDF-1.4\n"
    b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
    b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
    b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 612 792]"
    b"/Resources<</Font<</F1 4 0 R>>>>/Contents 5 0 R>>endobj\n"
    b"4 0 obj<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>endobj\n"
    b"5 0 obj<</Length 120>>stream\n"
    b"BT /F1 11 Tf 60 700 Td (Cap Test Candidate) Tj 0 -18 Td "
    b"(Python developer, 4 years experience, SQL and Django.) Tj ET\n"
    b"endstream endobj\n"
    b"trailer<</Root 1 0 R>>\n%%EOF\n"
)


def say(ok, text):
    mark = "\033[32mPASS\033[0m" if ok else "\033[31mFAIL\033[0m"
    print(f"  [{mark}] {text}")
    return ok


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    ap.add_argument("--job-id", default="")
    args = ap.parse_args()

    from database import CAP_PER_JOB

    client = AsyncIOMotorClient(MONGO_URI, tlsAllowInvalidCertificates=True)
    db = client[DB_NAME]
    ok = True
    seeded_key = None
    job = None
    prior_public = None

    try:
        job = (await db.jobs.find_one({"_id": ObjectId(args.job_id)})
               if args.job_id else
               await db.jobs.find_one({"active": True, "public_token": {"$exists": True}}))
        if not job:
            print("No job with a public token. Create one first.")
            return 1

        job_id = str(job["_id"])
        token = job["public_token"]
        prior_public = job.get("is_public", False)
        print(f"\nJob     : {job.get('title','(untitled)')}  [{job_id}]")
        print(f"Cap     : CAP_PER_JOB = {CAP_PER_JOB}")

        await db.jobs.update_one({"_id": job["_id"]}, {"$set": {"is_public": True}})

        # Leave exactly two slots, using the real cap rather than a fake one.
        seeded_key = f"job:{job_id}"
        await db.spend_counters.update_one(
            {"_id": seeded_key},
            {"$set": {"count": CAP_PER_JOB - 2, "seeded_by": "test_spend_cap", "last_at": datetime.utcnow()}},
            upsert=True,
        )
        print(f"Seeded  : counter = {CAP_PER_JOB - 2}  → 2 slots remain\n")

        before_screenings = await db.screenings.count_documents({"job_id": job_id})

        print("Submitting 3 applications")
        bodies, codes = [], []
        async with httpx.AsyncClient(base_url=args.base, timeout=60.0) as c:
            for i in (1, 2, 3):
                r = await c.post(
                    f"/api/apply/{token}",
                    data={"name": f"Cap Test {i}", "email": f"captest{i}@example.com", "phone": ""},
                    files={"cv_file": (f"cap{i}.pdf", PDF, "application/pdf")},
                )
                codes.append(r.status_code)
                bodies.append(r.text)
                print(f"  #{i}  HTTP {r.status_code}  {r.text[:60]}")

        # Background scoring is fire-and-forget; give the two allowed ones time.
        print("\nWaiting for background scoring…")
        await asyncio.sleep(45)

        apps = await db.applications.find(
            {"job_id": job_id, "email": {"$regex": "^captest[123]@example.com$"}}
        ).sort("submitted_at", 1).to_list(10)

        print("\nResults")
        for a in apps:
            print(f"  {a['email']:26} status={a.get('status'):16} capped_by={a.get('capped_by') or '-'}")

        capped = [a for a in apps if a.get("status") == "stored_unscored"]
        scored = [a for a in apps if a.get("status") == "scored"]
        after_screenings = await db.screenings.count_documents({"job_id": job_id})
        new_screenings = after_screenings - before_screenings

        print("\nAssertions")
        ok &= say(len(set(codes)) == 1 and codes[0] == 200,
                  f"all three got the same HTTP status ({codes})")
        ok &= say(len(set(bodies)) == 1,
                  "all three got a BYTE-IDENTICAL body — the capped applicant is not told")
        ok &= say(len(capped) == 1, f"exactly one stored unscored (got {len(capped)})")
        ok &= say(bool(capped) and capped[0].get("capped_by") == "job",
                  "the capped one names the per-job cap")
        ok &= say(new_screenings == 2,
                  f"exactly 2 screenings created, so the 3rd made ZERO OpenAI calls "
                  f"(got {new_screenings})")
        ok &= say(len(scored) == 2, f"two applications scored (got {len(scored)})")

        counter = await db.spend_counters.find_one({"_id": seeded_key})
        ok &= say((counter or {}).get("count") == CAP_PER_JOB,
                  f"counter stopped exactly at the cap ({(counter or {}).get('count')}/{CAP_PER_JOB})")

        cvs_kept = await db.application_files.count_documents(
            {"application_id": {"$in": [str(a["_id"]) for a in capped]}}
        )
        ok &= say(cvs_kept == len(capped),
                  "the capped applicant's CV is still stored, ready to score later")

    finally:
        print("\nCleaning up")
        if job is not None:
            ids = [str(a["_id"]) for a in await db.applications.find(
                {"job_id": str(job["_id"]), "email": {"$regex": "^captest[123]@example.com$"}},
                {"_id": 1}).to_list(10)]
            await db.application_files.delete_many({"application_id": {"$in": ids}})
            await db.applications.delete_many({"_id": {"$in": [ObjectId(i) for i in ids]}})
            await db.screenings.delete_many({"applicant_email": {"$regex": "^captest[123]@example.com$"}})
            await db.jobs.update_one({"_id": job["_id"]}, {"$set": {"is_public": prior_public}})
            print(f"  removed {len(ids)} test applications, restored is_public={prior_public}")
        if seeded_key:
            await db.spend_counters.delete_one({"_id": seeded_key})
            print("  removed the seeded counter")
        client.close()

    print("\n\033[32mCAP HELD\033[0m\n" if ok else "\n\033[31mCAP LEAKED\033[0m\n")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
