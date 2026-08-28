"""
Phase 1 Step 1 helper — create a test video-interview and print its record URL.

The recruiter UI for creating interviews comes later (Phase 4). This is a
throwaway testing tool so the recording page has a real token to open on a
phone right now. It writes one document to the `interviews` collection and
prints the /viva/{token} URL.

USAGE (on the droplet)
    cd ~/app
    venv/bin/python scripts/make_test_interview.py
    venv/bin/python scripts/make_test_interview.py --question "Walk us through a project you're proud of." --title "Backend Engineer"
    venv/bin/python scripts/make_test_interview.py --list        # show existing test interviews
    venv/bin/python scripts/make_test_interview.py --delete-tests # remove ones this script made
"""

import argparse
import asyncio
import os

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "")
DB_NAME = os.getenv("DB_NAME", "talentscore")
APP_URL = os.getenv("APP_URL", "https://topcandidate.pro").rstrip("/")


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--question", default="Tell us about a problem you solved recently and how you approached it.")
    ap.add_argument("--title", default="this role")
    ap.add_argument("--list", action="store_true")
    ap.add_argument("--delete-tests", action="store_true")
    args = ap.parse_args()

    client = AsyncIOMotorClient(MONGO_URI, tlsAllowInvalidCertificates=True)
    db = client[DB_NAME]
    try:
        if args.list:
            n = 0
            async for iv in db.interviews.find({"source": "make_test_interview"}):
                n += 1
                print(f"  {iv['public_token']}  {APP_URL}/viva/{iv['public_token']}  — {iv.get('question','')[:50]}")
            print(f"\n{n} test interview(s).")
            return

        if args.delete_tests:
            r = await db.interviews.delete_many({"source": "make_test_interview"})
            print(f"deleted {r.deleted_count} test interview(s).")
            return

        # Reuse the app's own token generator + shape so this record is identical
        # to one the real create flow will produce.
        from database import connect, create_interview
        await connect()
        iv = await create_interview(user_id="test", question=args.question, job_title=args.title)
        # Tag it so --list / --delete-tests can find only script-made records.
        await db.interviews.update_one({"_id": __import__("bson").ObjectId(iv["_id"])},
                                       {"$set": {"source": "make_test_interview"}})
        print("\nTest interview created.")
        print(f"  question : {iv['question']}")
        print(f"  language : {iv['answer_language']}  (Phase 1: English only)")
        print(f"\n  OPEN THIS ON YOUR PHONE:\n  {APP_URL}/viva/{iv['public_token']}\n")
    finally:
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
