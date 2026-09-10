"""
Funnel invite sweep (Part 2) — daily via tc-invite-reminders.timer.

Two jobs, both idempotent and fail-soft:
  1. REMINDERS: invited, not completed, link expiring within ~36h, and no
     reminder sent yet (reminder_sent_at guard) -> ONE reminder email,
     honest wording (same template as the invite, reminder subject).
  2. CAP RETRIES: candidates who qualified while the day's interview cap was
     full (viva_capped, no token) are re-run through the normal invite path —
     so a full day never silently loses a qualified candidate.

Runs the app's OWN helpers (invite_funnel_candidate / the shared launch
path) so there is exactly one launch/invite implementation.
"""

import asyncio
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()


async def run():
    import database
    await database.connect()
    import main   # the app module — imported for its helpers, server not started
    from bson import ObjectId
    from email_service import send_interview_invite_email

    db = database.db
    now = datetime.utcnow()
    soon = now + timedelta(hours=36)
    sent = retried = 0

    # ── 1) reminders ─────────────────────────────────────────
    for a in await db.applications.find(
            {"funnel": "mcq", "interview_token": {"$exists": True},
             "invited_at": {"$exists": True},
             "reminder_sent_at": {"$exists": False}}).to_list(500):
        li = await db.live_interviews.find_one(
            {"public_token": a["interview_token"]},
            {"completed_sessions": 1, "expires_at": 1, "invite_window_days": 1})
        if not li or int(li.get("completed_sessions") or 0) >= 1:
            continue
        exp = li.get("expires_at")
        if not exp or not (now < exp <= soon):
            continue
        job = await db.jobs.find_one({"_id": ObjectId(str(a["job_id"]))})
        if not job:
            continue
        owner = await db.users.find_one({"_id": ObjectId(str(job.get("user_id")))}) \
            if job.get("user_id") else None
        ok, info = send_interview_invite_email(
            to_email=str(a.get("email") or ""),
            candidate_name=str(a.get("name") or ""),
            company=(owner or {}).get("company_name") or "the hiring team",
            job_title=str(job.get("title") or "the role"),
            link=f"{main.APP_URL}/interview/{a['interview_token']}",
            deadline_str=exp.strftime("%d %b %Y"),
            days=int(li.get("invite_window_days") or 5),
            reply_to=(owner or {}).get("email") or "",
            reminder=True,
            language=("bn" if (job.get("interview_language") or "en").lower() == "bn" else "en"))
        await db.applications.update_one(
            {"_id": a["_id"]},
            {"$set": {"reminder_sent_at": now,
                      "reminder_email_id": info if ok else None}})
        sent += 1
        print(f"reminder -> {a.get('email')} (sent={ok}, id={info if ok else '-'})")

    # ── 2) cap retries ───────────────────────────────────────
    for a in await db.applications.find(
            {"funnel": "mcq", "status": "scored", "viva_capped": True,
             "interview_token": {"$exists": False}}).to_list(500):
        job = await db.jobs.find_one({"_id": ObjectId(str(a["job_id"]))})
        if not job or not (job.get("viva") or {}).get("enabled"):
            continue
        out = await main.invite_funnel_candidate(job, str(a["_id"]))
        if out.get("invited"):
            await db.applications.update_one({"_id": a["_id"]},
                                             {"$unset": {"viva_capped": ""}})
            retried += 1
            print(f"cap-retry invited -> {a.get('email')}")
        elif out.get("capped"):
            break   # today's budget is full again — stop, tomorrow's run continues

    print(f"{now.isoformat()}Z sweep done: reminders={sent} cap_retries={retried}")


if __name__ == "__main__":
    asyncio.run(run())
