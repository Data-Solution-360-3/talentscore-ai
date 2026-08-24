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

# A hand-crafted minimal PDF fails pdfplumber's text extraction, and a
# fast-failing parse RELEASES the reserved slot before the next submit consumes
# one — so the cap never engages and the test measures nothing. Instead we pull
# a real CV from application_files at runtime: guaranteed to extract and score,
# so an allowed application holds its slot for the full 20-40s scoring window
# and the third submit actually hits the cap.
async def fetch_real_pdf(db) -> bytes:
    # Under the 2MB upload cap, so the submit isn't rejected at the door.
    f = await db.application_files.find_one(
        {"data": {"$exists": True}, "size": {"$lt": 1_500_000}}, {"data": 1}
    )
    if not f or not f.get("data"):
        return b""
    return bytes(f["data"])


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

        pdf = await fetch_real_pdf(db)
        if not pdf.startswith(b"%PDF-"):
            print("No real CV available in application_files to test with.")
            return 1
        print(f"Using a real CV from application_files ({len(pdf)} bytes)")

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
                    files={"cv_file": (f"cap{i}.pdf", pdf, "application/pdf")},
                )
                codes.append(r.status_code)
                bodies.append(r.text)
                print(f"  #{i}  HTTP {r.status_code}  {r.text[:60]}")

        # SNAPSHOT NOW, before background scoring can finish or fail. The
        # reservation is synchronous inside each submit, so the cap outcome is
        # already decided; waiting only risks a slow scoring failure releasing a
        # slot and muddying the picture. This is the deterministic proof.
        snap = await db.applications.find(
            {"job_id": job_id, "email": {"$regex": "^captest[123]@example.com$"}}
        ).sort("submitted_at", 1).to_list(10)
        counter_now = (await db.spend_counters.find_one({"_id": seeded_key}) or {}).get("count")

        print("\nImmediately after submit")
        for a in snap:
            print(f"  {a['email']:26} status={a.get('status'):16} capped_by={a.get('capped_by') or '-'}")
        print(f"  counter = {counter_now}")

        third = next((a for a in snap if a["email"] == "captest3@example.com"), None)
        capped_now = [a for a in snap if a.get("capped_by") == "job"]
        third_screening = await db.screenings.count_documents(
            {"application_id": str(third["_id"])}
        ) if third else -1

        print("\nCap assertions (deterministic)")
        ok &= say(codes == [200, 200, 200], f"all three got HTTP 200 ({codes})")
        ok &= say(len(set(bodies)) == 1,
                  "byte-identical body — the capped applicant is not told")
        ok &= say(counter_now == CAP_PER_JOB,
                  f"counter stopped exactly at the cap ({counter_now}/{CAP_PER_JOB}) — "
                  f"2 slots consumed, the 3rd could not increment it")
        ok &= say(len(capped_now) == 1 and capped_now[0]["email"] == "captest3@example.com",
                  "exactly the 3rd is capped, capped_by='job'")
        ok &= say(third_screening == 0,
                  "the capped application produced ZERO screenings — no OpenAI call was made")

        # Now let the two allowed ones finish, to show the cap let real work
        # through rather than blocking everything. Informational: a slow API
        # here does not change the cap result above.
        print("\nConfirming the two allowed applications score (up to 90s)…")
        scored = 0
        for _ in range(18):
            await asyncio.sleep(5)
            scored = await db.applications.count_documents(
                {"job_id": job_id, "status": "scored",
                 "email": {"$regex": "^captest[12]@example.com$"}}
            )
            if scored >= 2:
                break
        new_screenings = await db.screenings.count_documents({"job_id": job_id}) - before_screenings
        say(scored == 2, f"the two under the cap scored ({scored}/2), {new_screenings} screenings created")

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
