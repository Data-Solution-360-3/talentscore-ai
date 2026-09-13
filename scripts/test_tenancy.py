"""
Tenancy isolation gate (A1) — proves the org_id cutover actually isolates.

WHAT IT PROVES
    Two orgs, four accounts: org A holds an owner, a recruiter and a viewer;
    org B holds an owner. Every claim the migration makes is exercised over
    real HTTP against the live server:

      1. LISTS  — each org's list endpoints return that org's rows and no
         other org's; the recruiter sees the owner's rows (Q2: hiring data
         is shared inside the org).
      2. DIRECT-ID PROBES — org A aiming org B's real ids at every per-doc
         endpoint (screening get/attempts/cv/stage/delete/email, job
         update/delete/applications) gets 403/404, never 200.
      3. ROLES — the viewer reads but cannot write anywhere; the recruiter
         writes hiring data but cannot invite; only the owner invites.
      4. HRM — neither recruiter nor owner reaches /api/employees (the
         super-admin gate is strictly stronger than Q2's owner-only rule).
      5. CANDIDATE PATH — a row written through upsert_application (the
         exact function the public apply flow calls) carries the JOB'S org.

    Fixtures are created directly in the database (this runs on the server,
    like smoke_test), authenticated with minted JWTs, and removed afterwards.
    Nothing here calls OpenAI or touches real tenant rows.

USAGE
    python scripts/test_tenancy.py                        # against production
    python scripts/test_tenancy.py --base http://127.0.0.1:8000

    Exit 0 = every check passed. Exit 1 = any isolation failure.
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime

import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from pymongo import MongoClient
from auth import create_token

DEFAULT_BASE = "https://topcandidate.pro"
MARK = "tenancy-gate.test"          # every fixture carries this, cleanup keys on it

failures = []


def check(name: str, ok: bool, note: str = ""):
    colour = "\033[32mPASS\033[0m" if ok else "\033[31mFAIL\033[0m"
    print(f"  {colour}  {name}{('  — ' + note) if note else ''}")
    if not ok:
        failures.append(name)


def token_for(user_doc) -> str:
    return create_token({
        "user_id": str(user_doc["_id"]),
        "email": user_doc["email"],
        "company": user_doc.get("company_name", ""),
        "role": "client",
    })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=DEFAULT_BASE)
    args = ap.parse_args()
    base = args.base.rstrip("/")

    uri = os.getenv("MONGO_URI")
    if not uri:
        print("MONGO_URI not set — this gate runs on the server."); sys.exit(1)
    dbx = MongoClient(uri, serverSelectionTimeoutMS=15000,
                      tlsAllowInvalidCertificates=True)[os.getenv("DB_NAME", "talentscore")]

    # ── fixtures ────────────────────────────────────────────────────────────
    now = datetime.utcnow()
    org_a = str(dbx.orgs.insert_one({"name": "TenancyGate A", "owner_user_id": "",
                                     "fixture": MARK, "created_at": now}).inserted_id)
    org_b = str(dbx.orgs.insert_one({"name": "TenancyGate B", "owner_user_id": "",
                                     "fixture": MARK, "created_at": now}).inserted_id)

    def mk_user(email, org, org_role):
        return dbx.users.insert_one({
            "email": email, "password": "!", "company_name": f"TenancyGate {org_role}",
            "role": "client", "active": True, "created_at": now, "screening_count": 0,
            "plan": "trial", "org_id": org, "org_role": org_role, "fixture": MARK,
        })

    owner_a = mk_user(f"owner-a@{MARK}", org_a, "owner")
    rec_a   = mk_user(f"rec-a@{MARK}",   org_a, "recruiter")
    view_a  = mk_user(f"view-a@{MARK}",  org_a, "viewer")
    owner_b = mk_user(f"owner-b@{MARK}", org_b, "owner")
    dbx.orgs.update_one({"_id": {"$eq": __import__("bson").ObjectId(org_a)}},
                        {"$set": {"owner_user_id": str(owner_a.inserted_id)}})

    def mk_screening(org, uid, **extra):
        return str(dbx.screenings.insert_one({
            "user_id": str(uid), "org_id": org, "candidate_name": "Fixture Person",
            "job_title": "Fixture Role", "overall_score": 50, "recommendation": "MAYBE",
            "stage": "pending", "fixture": MARK, "created_at": now, **extra,
        }).inserted_id)

    def mk_job(org, uid):
        return str(dbx.jobs.insert_one({
            "user_id": str(uid), "org_id": org, "title": f"Fixture Job {org[-5:]}",
            "description": "fixture", "active": True, "fixture": MARK, "created_at": now,
        }).inserted_id)

    job_a, job_b = mk_job(org_a, owner_a.inserted_id), mk_job(org_b, owner_b.inserted_id)

    # scr_a carries a full personal-data trail across every collection the
    # erasure cascade must clear (Batch 5); scr_b stays minimal.
    erase_email = f"erase-me@{MARK}"
    from bson import ObjectId as _OID
    app_a = str(dbx.applications.insert_one({
        "job_id": job_a, "email": erase_email, "name": "Fixture Person",
        "org_id": org_a, "status": "pending", "submitted_at": now,
        "fixture": MARK}).inserted_id)
    scr_a = mk_screening(org_a, owner_a.inserted_id,
                         application_id=app_a, applicant_email=erase_email)
    scr_b = mk_screening(org_b, owner_b.inserted_id)
    dbx.application_files.insert_one({
        "application_id": app_a, "screening_id": _OID(scr_a), "data": b"pdfbytes",
        "org_id": org_a, "fixture": MARK, "created_at": now})
    dbx.interview_sessions.insert_one({
        "application_id": app_a, "org_id": org_a, "created_at": now,
        "transcript": [{"role": "ai", "text": "q"}, {"role": "you", "text": "a"}],
        "fixture": MARK})
    dbx.live_interviews.insert_one({
        "application_id": app_a, "org_id": org_a, "created_at": now, "fixture": MARK})
    dbx.proctor_snapshots.insert_one({
        "application_id": app_a, "org_id": org_a, "token": f"tok-{MARK}",
        "created_at": now, "fixture": MARK})
    dbx.interview_written_answers.insert_one({
        "email": erase_email, "org_id": org_a, "created_at": now, "fixture": MARK})
    dbx.email_history.insert_one({
        "screening_id": scr_a, "org_id": org_a, "sent_at": now,
        "body": "personal email body", "fixture": MARK})

    t_owner_a = token_for(dbx.users.find_one({"_id": owner_a.inserted_id}))
    t_rec_a   = token_for(dbx.users.find_one({"_id": rec_a.inserted_id}))
    t_view_a  = token_for(dbx.users.find_one({"_id": view_a.inserted_id}))
    t_owner_b = token_for(dbx.users.find_one({"_id": owner_b.inserted_id}))

    def hdr(t):
        return {"Authorization": f"Bearer {t}"}

    try:
        with httpx.Client(base_url=base, timeout=30, follow_redirects=True) as c:

            print("\nLists — each org sees its own rows and only its own")
            r = c.get("/api/screenings", headers=hdr(t_owner_a))
            body = r.text
            check("owner A list has A's row", r.status_code == 200 and scr_a in body)
            check("owner A list lacks B's row", scr_b not in body)
            r = c.get("/api/screenings", headers=hdr(t_rec_a))
            check("recruiter A sees org A's row (shared hiring data)",
                  r.status_code == 200 and scr_a in r.text)
            r = c.get("/api/screenings", headers=hdr(t_owner_b))
            check("owner B list has B's row", r.status_code == 200 and scr_b in r.text)
            check("owner B list lacks A's row", scr_a not in r.text)
            r = c.get("/api/jobs", headers=hdr(t_owner_a))
            check("owner A jobs has A's job", r.status_code == 200 and job_a in r.text)
            check("owner A jobs lacks B's job", job_b not in r.text)

            print("\nDirect-ID probes — org A aiming at org B's real ids")
            probes = [
                ("GET",    f"/api/screenings/{scr_b}",          {}),
                ("GET",    f"/api/screenings/{scr_b}/attempts", {}),
                ("GET",    f"/api/screenings/{scr_b}/cv",       {}),
                ("POST",   f"/api/screenings/{scr_b}/stage",    {"stage": "pending"}),
                ("DELETE", f"/api/screenings/{scr_b}",          {}),
                ("POST",   f"/api/candidates/{scr_b}/email",
                           {"subject": "x", "body": "x"}),
                ("PUT",    f"/api/jobs/{job_b}",                {"title": "hijack"}),
                ("DELETE", f"/api/jobs/{job_b}",                {}),
                ("GET",    f"/api/jobs/{job_b}/applications",   {}),
                ("GET",    f"/api/jobs/{job_b}/mcq-funnel",     {}),
                ("POST",   f"/api/jobs/{job_b}/mcq-advance",    {"n": "5"}),
                ("POST",   f"/api/jobs/{job_b}/mcq-release",
                           {"application_id": "0" * 24, "action": "release"}),
                ("POST",   f"/api/jobs/{job_b}/mcq-reinvite",
                           {"application_id": "0" * 24}),
                ("GET",    f"/api/jobs/{job_b}/mcq-report/{'0' * 24}", {}),
                ("GET",    f"/api/jobs/{job_b}/pipeline",       {}),
                ("GET",    f"/api/jobs/{job_b}/mcq-snapshots/{'0' * 24}", {}),
            ]
            for method, path, data in probes:
                r = c.request(method, path, headers=hdr(t_owner_a),
                              data=data if method in ("POST", "PUT") else None)
                check(f"{method} {path.replace(scr_b, '<B-scr>').replace(job_b, '<B-job>')} refused",
                      r.status_code in (403, 404), f"got {r.status_code}")
            still = dbx.screenings.find_one({"_id": __import__("bson").ObjectId(scr_b)})
            check("B's screening still exists after A's delete probe", bool(still))
            stillj = dbx.jobs.find_one({"_id": __import__("bson").ObjectId(job_b)})
            check("B's job still exists and untitled-hijacked",
                  bool(stillj) and stillj.get("title") != "hijack")

            print("\nRoles — viewer read-only, recruiter writes hiring, owner invites")
            r = c.get("/api/screenings", headers=hdr(t_view_a))
            check("viewer A can read org A's screenings",
                  r.status_code == 200 and scr_a in r.text)
            r = c.post(f"/api/screenings/{scr_a}/stage", headers=hdr(t_view_a),
                       data={"stage": "shortlisted"})
            check("viewer A cannot write (stage change 403)", r.status_code == 403,
                  f"got {r.status_code}")
            r = c.delete(f"/api/jobs/{job_a}", headers=hdr(t_view_a))
            check("viewer A cannot delete a job", r.status_code == 403, f"got {r.status_code}")
            r = c.post("/api/team/invite", headers=hdr(t_view_a),
                       data={"email": f"nobody1@{MARK}", "role": "viewer"})
            check("viewer A cannot invite", r.status_code == 403, f"got {r.status_code}")
            r = c.post(f"/api/screenings/{scr_a}/stage", headers=hdr(t_rec_a),
                       data={"stage": "shortlisted"})
            check("recruiter A CAN change stage on org A's screening",
                  r.status_code == 200, f"got {r.status_code}")
            r = c.post("/api/team/invite", headers=hdr(t_rec_a),
                       data={"email": f"nobody2@{MARK}", "role": "viewer"})
            check("recruiter A cannot invite (owner-only)", r.status_code == 403,
                  f"got {r.status_code}")

            print("\nOpenAI cost dashboard — PLATFORM ADMIN only (clients never see our cost)")
            r = c.get("/api/usage/cost", headers=hdr(t_view_a))
            check("viewer A cannot read cost data", r.status_code == 403, f"got {r.status_code}")
            r = c.get("/api/usage/cost", headers=hdr(t_rec_a))
            check("recruiter A cannot read cost data", r.status_code == 403, f"got {r.status_code}")
            r = c.get("/api/usage/cost", headers=hdr(t_owner_a))
            check("org owner A cannot read OpenAI cost (super-admin only since billing view)",
                  r.status_code == 403, f"got {r.status_code}")

            print("\nClient billing summary — own org, owner/admin only, read-only")
            r = c.get("/api/org/billing-usage", headers=hdr(t_view_a))
            check("viewer A cannot read the billing summary", r.status_code == 403,
                  f"got {r.status_code}")
            r = c.get("/api/org/billing-usage", headers=hdr(t_rec_a))
            check("recruiter A cannot read the billing summary", r.status_code == 403,
                  f"got {r.status_code}")
            r = c.get("/api/org/billing-usage", headers=hdr(t_owner_a))
            body = r.json() if r.status_code == 200 else {}
            check("owner A reads own-org billing summary", r.status_code == 200
                  and set(body.get("counts") or {}) == {"mcq", "cv", "interview"}
                  and "total_tk" in body,
                  f"got {r.status_code}")
            check("billing summary never leaks OpenAI cost fields",
                  r.status_code == 200 and "est_usd" not in r.text
                  and "openai" not in r.text.lower(),
                  "cost fields present in client response")
            check("owner A's cost payload carries NO org-B reference",
                  org_b not in r.text and scr_b not in r.text)

            print("\nBranding — owner-only edit, own org only (no cross-org parameter exists)")
            r = c.post("/api/org/branding", headers=hdr(t_view_a),
                       json={"primary_color": "#111111"})
            check("viewer A cannot edit branding", r.status_code == 403, f"got {r.status_code}")
            r = c.post("/api/org/branding", headers=hdr(t_rec_a),
                       json={"primary_color": "#111111"})
            check("recruiter A cannot edit branding (owner-only)", r.status_code == 403,
                  f"got {r.status_code}")
            r = c.get("/api/org/branding", headers=hdr(t_owner_a))
            check("owner A reads own branding", r.status_code == 200, f"got {r.status_code}")

            print("\nSubdomain slug — unique across orgs, reserved words blocked")
            r = c.post("/api/org/branding", headers=hdr(t_owner_a),
                       json={"subdomain": "tnc-gate-slug"})
            check("owner A claims a subdomain slug", r.status_code == 200
                  and r.json().get("subdomain") == "tnc-gate-slug", f"got {r.status_code}")
            r = c.post("/api/org/branding", headers=hdr(t_owner_b),
                       json={"subdomain": "tnc-gate-slug"})
            check("owner B cannot claim org A's slug (409)", r.status_code == 409,
                  f"got {r.status_code}")
            r = c.post("/api/org/branding", headers=hdr(t_owner_a),
                       json={"subdomain": "www"})
            check("reserved word 'www' refused (400)", r.status_code == 400,
                  f"got {r.status_code}")
            r = c.post("/api/org/branding", headers=hdr(t_owner_a),
                       json={"subdomain": "Bad_Slug!"})
            check("invalid slug shape refused (400)", r.status_code == 400,
                  f"got {r.status_code}")
            r = c.post("/api/org/branding", headers=hdr(t_owner_a),
                       json={"subdomain": ""})
            check("owner A clears the slug again", r.status_code == 200
                  and r.json().get("subdomain") == "", f"got {r.status_code}")

            print("\nBilling (client-price view) — super-admin ONLY, never org members")
            for name, tok in (("owner A", t_owner_a), ("recruiter A", t_rec_a),
                              ("viewer A", t_view_a)):
                r = c.get("/api/admin/billing/rates", headers=hdr(tok))
                check(f"{name} cannot read billing rates", r.status_code == 403,
                      f"got {r.status_code}")
                r = c.get("/api/admin/billing/usage", headers=hdr(tok))
                check(f"{name} cannot read billing usage", r.status_code == 403,
                      f"got {r.status_code}")
            r = c.post("/api/admin/billing/rates", headers=hdr(t_owner_a),
                       json={"scope": "default", "mcq_tk": "1", "cv_tk": "1",
                             "interview_tk": "1"})
            check("owner A cannot set billing rates", r.status_code == 403,
                  f"got {r.status_code}")
            r = c.post("/api/admin/payment-details", headers=hdr(t_owner_a),
                       json={"bank_name": "x"})
            check("owner A cannot set payment details (super-admin only)",
                  r.status_code == 403, f"got {r.status_code}")

            print("\nManual payments — owner-only submit, own-org visibility, admin-only queue")
            pay_data = {"plan_id": "screening_starter", "payment_method": "bkash",
                        "transaction_id": "TNCGATE-TXN-1", "amount": "1000",
                        "period": "monthly"}
            r = c.post("/api/payments/manual", headers=hdr(t_view_a), data=pay_data)
            check("viewer A cannot submit a payment", r.status_code == 403,
                  f"got {r.status_code}")
            r = c.post("/api/payments/manual", headers=hdr(t_rec_a), data=pay_data)
            check("recruiter A cannot submit a payment (owner-only)",
                  r.status_code == 403, f"got {r.status_code}")
            r = c.post("/api/payments/manual", headers=hdr(t_owner_a),
                       data={**pay_data, "plan_id": "not_a_real_tier"})
            check("unknown tier refused (400)", r.status_code == 400,
                  f"got {r.status_code}")
            r = c.post("/api/payments/manual", headers=hdr(t_owner_a), data=pay_data)
            check("owner A submits a payment", r.status_code == 200,
                  f"got {r.status_code}")
            r = c.get("/api/payments/mine", headers=hdr(t_owner_a))
            check("owner A sees own submission", r.status_code == 200
                  and "TNCGATE-TXN-1" in r.text, f"got {r.status_code}")
            r = c.get("/api/payments/mine", headers=hdr(t_owner_b))
            check("owner B does NOT see org A's submission", r.status_code == 200
                  and "TNCGATE-TXN-1" not in r.text, f"got {r.status_code}")
            r = c.get("/api/payments/mine", headers=hdr(t_view_a))
            check("viewer A cannot read payment submissions", r.status_code == 403,
                  f"got {r.status_code}")
            for name, tok in (("owner A", t_owner_a), ("recruiter A", t_rec_a),
                              ("viewer A", t_view_a)):
                r = c.get("/api/admin/manual-payments", headers=hdr(tok))
                check(f"{name} cannot read the verification queue",
                      r.status_code == 403, f"got {r.status_code}")
            r = c.post("/api/admin/manual-payments/000000000000000000000000/approve",
                       headers=hdr(t_owner_a))
            check("owner A cannot approve a payment", r.status_code == 403,
                  f"got {r.status_code}")
            r = c.post("/api/admin/manual-payments/000000000000000000000000/reject",
                       headers=hdr(t_owner_a), data={"reason": "x"})
            check("owner A cannot reject a payment", r.status_code == 403,
                  f"got {r.status_code}")

            print("\nPlan payment states — active/grace/paused computed from paid_until")
            from datetime import timedelta as _tdx
            from bson import ObjectId as _OIDX
            def set_paid_until(dtv):
                dbx.orgs.update_one({"_id": _OIDX(org_a)}, {"$set": {"plan": {
                    "tier": "screening_starter", "family": "screening",
                    "status": "active", "paid_until": dtv}}})
            set_paid_until(datetime.utcnow() + _tdx(days=10))
            r = c.get("/api/org/billing-usage", headers=hdr(t_owner_a))
            st = ((r.json() if r.status_code == 200 else {}).get("plan") or {})
            check("future paid_until -> state active", st.get("state") == "active",
                  f"got {st.get('state')}")
            set_paid_until(datetime.utcnow() - _tdx(days=1))
            r = c.get("/api/org/billing-usage", headers=hdr(t_owner_a))
            st = ((r.json() if r.status_code == 200 else {}).get("plan") or {})
            check("1 day past -> state grace with days left",
                  st.get("state") == "grace" and 1 <= int(st.get("grace_days_left") or 0) <= 4,
                  f"got {st.get('state')}/{st.get('grace_days_left')}")
            set_paid_until(datetime.utcnow() - _tdx(days=10))
            r = c.get("/api/org/billing-usage", headers=hdr(t_owner_a))
            st = ((r.json() if r.status_code == 200 else {}).get("plan") or {})
            check("10 days past -> state paused", st.get("state") == "paused",
                  f"got {st.get('state')}")
            dbx.orgs.update_one({"_id": _OIDX(org_a)}, {"$unset": {"plan": ""}})
            r = c.get("/api/org/billing-usage", headers=hdr(t_owner_a))
            st = ((r.json() if r.status_code == 200 else {}).get("plan") or {})
            check("no tier -> Legacy stays active (never gated)",
                  st.get("state") == "active" and st.get("assigned") is False,
                  f"got {st.get('state')}/{st.get('assigned')}")

            print("\nHRM — hidden from every non-super-admin org member (Q2)")
            for name, tok in (("recruiter A", t_rec_a), ("owner A", t_owner_a)):
                r = c.get("/api/employees", headers=hdr(tok))
                check(f"{name} cannot reach HRM", r.status_code == 403, f"got {r.status_code}")

            print("\nCandidate path — application rows carry the JOB'S org")
            async def _apply():
                import database
                await database.connect()
                job_doc = await database.db.jobs.find_one(
                    {"_id": __import__("bson").ObjectId(job_a)})
                app_id, _ = await database.upsert_application(
                    job_doc, "Fixture Applicant", f"applicant@{MARK}", "",
                    "cv.pdf", "fixturehash")
                row = await database.db.applications.find_one(
                    {"_id": __import__("bson").ObjectId(app_id)})
                return row
            row = asyncio.run(_apply())
            check("upsert_application stamped org A's org_id",
                  bool(row) and str(row.get("org_id")) == org_a,
                  f"org_id={row.get('org_id') if row else None}")

            print("\nErasure (Batch 5) — owner-only, org-scoped, full cascade")
            r = c.post(f"/api/candidates/{scr_a}/erase", headers=hdr(t_view_a),
                       data={"confirm": "ERASE"})
            check("viewer A cannot erase", r.status_code == 403, f"got {r.status_code}")
            r = c.post(f"/api/candidates/{scr_a}/erase", headers=hdr(t_rec_a),
                       data={"confirm": "ERASE"})
            check("recruiter A cannot erase (owner-only)", r.status_code == 403,
                  f"got {r.status_code}")
            r = c.post(f"/api/candidates/{scr_b}/erase", headers=hdr(t_owner_a),
                       data={"confirm": "ERASE"})
            check("owner A CANNOT erase org B's candidate (cross-org)",
                  r.status_code in (403, 404), f"got {r.status_code}")
            still_b = dbx.screenings.find_one({"_id": _OID(scr_b)})
            check("org B's screening survived the cross-org erase attempt", bool(still_b))
            r = c.post(f"/api/candidates/{scr_a}/erase", headers=hdr(t_owner_a),
                       data={"confirm": "delete"})
            check("wrong confirm string refused (must type ERASE)",
                  r.status_code == 400, f"got {r.status_code}")
            r = c.post(f"/api/candidates/{scr_a}/erase", headers=hdr(t_owner_a),
                       data={"confirm": "ERASE"})
            check("owner A CAN erase own candidate", r.status_code == 200,
                  f"got {r.status_code}")
            residue = {
                "screenings": dbx.screenings.count_documents({"_id": _OID(scr_a)}),
                "applications": dbx.applications.count_documents({"_id": _OID(app_a)}),
                "application_files": dbx.application_files.count_documents(
                    {"application_id": app_a}),
                "interview_sessions": dbx.interview_sessions.count_documents(
                    {"application_id": app_a}),
                "live_interviews": dbx.live_interviews.count_documents(
                    {"application_id": app_a}),
                "proctor_snapshots": dbx.proctor_snapshots.count_documents(
                    {"application_id": app_a}),
                "interview_written_answers": dbx.interview_written_answers.count_documents(
                    {"email": erase_email}),
                "email_history": dbx.email_history.count_documents(
                    {"screening_id": scr_a}),
            }
            for coll_name, left in residue.items():
                check(f"erasure cleared {coll_name}", left == 0, f"{left} rows left")
            tomb = dbx.erasure_tombstones.find_one({"org_id": org_a})
            check("anonymized tombstone exists", bool(tomb))
            leak = [k for k in (tomb or {}) if k in
                    ("email", "name", "candidate_name", "applicant_email", "phone")] \
                or [v for v in (tomb or {}).values()
                    if isinstance(v, str) and erase_email in v]
            check("tombstone carries NO personal data", bool(tomb) and not leak,
                  f"leaked: {leak}" if leak else "")

    finally:
        # ── cleanup: everything carrying the fixture mark, plus side effects ──
        dbx.users.delete_many({"fixture": MARK})
        dbx.orgs.delete_many({"fixture": MARK})
        dbx.screenings.delete_many({"fixture": MARK})
        dbx.jobs.delete_many({"fixture": MARK})
        dbx.applications.delete_many({"email": {"$regex": MARK}})
        dbx.team_invites.delete_many({"email": {"$regex": MARK}})
        dbx.manual_payments.delete_many({"email": {"$regex": MARK}})
        dbx.email_history.delete_many({"screening_id": {"$in": [scr_a, scr_b]}})
        for coll in ("applications", "application_files", "interview_sessions",
                     "live_interviews", "proctor_snapshots",
                     "interview_written_answers", "email_history"):
            dbx[coll].delete_many({"fixture": MARK})
        dbx.erasure_tombstones.delete_many({"org_id": {"$in": [org_a, org_b]}})

    print(f"\n{len(failures)} FAILED" if failures else "\nALL TENANCY CHECKS PASSED")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
