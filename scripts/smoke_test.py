"""
Post-deploy smoke test — hit every route once, fail on any 5xx.

WHY THIS EXISTS
    `from database import db` captured None at import, so 40 call sites across
    main.py and api_keys.py were dead and only failed when a human clicked
    them. Three endpoints were found by accident, weeks after shipping. This
    would have caught all 40 in about ten seconds.

    The point is not clever assertions. It is that every route gets *executed*
    once, so an import-time or startup-order mistake cannot hide behind an
    endpoint nobody happened to click.

USAGE
    export SMOKE_EMAIL='you@example.com'
    export SMOKE_PASSWORD='...'
    python scripts/smoke_test.py                        # against production
    python scripts/smoke_test.py --base http://127.0.0.1:8000

    Exit 0 = no 5xx. Exit 1 = at least one 5xx, or login failed.

WHAT IT WILL NOT DO
    It never calls anything that spends money or destroys data: no batch
    screening, no public application submit, no screen-pending, no rotate-token,
    no deletes. The public-link toggle IS exercised, but it is set to the value
    the job already has, so the write is a no-op while the whole code path —
    including owned_job(), the one that was broken — still runs for real.
"""

import argparse
import os
import sys

import httpx

DEFAULT_BASE = "https://topcandidate.pro"

# 5xx is the failure signal. A 401/403/404 can be a correct answer, and is
# reported but not fatal — the thing being hunted is the server falling over.
FATAL_FROM = 500


def line(status, method, path, note=""):
    if status is None:
        mark, colour = "ERR ", "\033[31m"
    elif status >= FATAL_FROM:
        mark, colour = "FAIL", "\033[31m"
    elif status >= 400:
        mark, colour = "warn", "\033[33m"
    else:
        mark, colour = "ok  ", "\033[32m"
    reset = "\033[0m"
    code = "---" if status is None else str(status)
    print(f"  {colour}{mark}{reset} {code:>3}  {method:<6} {path}{('  — ' + note) if note else ''}")


def mint_admin_token(admin_email: str):
    """Mint a JWT for an existing admin directly from SECRET_KEY — no password.

    The post-deploy smoke test runs on the server, where it has .env
    (SECRET_KEY) and the database. So it authenticates the way the app itself
    would, without depending on any standing known-password account. The old
    seeded admin (admin@talentscore.ai / Admin@123) was removed precisely so no
    such account exists; this is what replaces it.

    Returns (token, resolved_email) or (None, reason). Never raises.
    """
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from dotenv import load_dotenv
        load_dotenv()
        from pymongo import MongoClient
        from auth import create_token

        uri = os.getenv("MONGO_URI")
        dbname = os.getenv("DB_NAME", "talentscore")
        if not uri:
            return None, "MONGO_URI not set (remote run — use --password fallback)"
        cli = MongoClient(uri, serverSelectionTimeoutMS=15000, tlsAllowInvalidCertificates=True)
        user = cli[dbname].users.find_one({"email": admin_email.lower()})
        cli.close()
        if not user:
            return None, f"admin {admin_email} not found"
        if user.get("role") != "admin":
            return None, f"{admin_email} is not an admin"
        token = create_token({
            "user_id": str(user["_id"]),
            "email": user["email"],
            "company": user.get("company_name", ""),
            "role": "admin",
        })
        return token, user["email"]
    except Exception as e:
        return None, f"mint unavailable: {str(e)[:70]}"


def mint_employee_token():
    """Mint a real role=employee JWT from SECRET_KEY — the same way the login
    endpoint does. No DB needed: the boundary we test (admin/hiring endpoints
    reject an employee token) is enforced on the role claim alone."""
    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from dotenv import load_dotenv
        load_dotenv()
        from auth import create_token
        return create_token({
            "role": "employee",
            "employee_id": "000000000000000000000000",
            "tenant": "000000000000000000000000",
            "name": "Smoke Employee",
        })
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=os.getenv("SMOKE_BASE", DEFAULT_BASE))
    ap.add_argument("--admin-email",
                    default=os.getenv("SMOKE_ADMIN_EMAIL", "tarafdersakib08@gmail.com"),
                    help="Admin to mint a token for (server-side runs). No password needed.")
    ap.add_argument("--email", default=os.getenv("SMOKE_EMAIL", ""),
                    help="Fallback password login (remote runs only).")
    ap.add_argument("--password", default=os.getenv("SMOKE_PASSWORD", ""))
    args = ap.parse_args()

    base = args.base.rstrip("/")
    failures = []
    checked = 0

    print(f"\nSmoke test → {base}\n")

    with httpx.Client(base_url=base, timeout=45.0, follow_redirects=False) as c:

        # ── Unauthenticated surface ──────────────────────────────────────
        print("Public routes")
        for method, path in [
            ("GET", "/health"),
            ("GET", "/"),
            ("GET", "/landing"),
            ("GET", "/login"),
            ("GET", "/docs"),
            ("GET", "/api/v1/ping"),
            ("GET", "/apply/definitely-not-a-real-token"),   # must be the closed page, not a stack trace
        ]:
            try:
                r = c.request(method, path)
                line(r.status_code, method, path)
                checked += 1
                if r.status_code >= FATAL_FROM:
                    failures.append((method, path, r.status_code))
            except Exception as e:
                line(None, method, path, str(e)[:60])
                failures.append((method, path, "exception"))

        # ── SEO routes: assert 200 AND the expected content, not just no-5xx ──
        print("\nSEO / crawlability")
        seo_checks = [
            ("/robots.txt", "Sitemap: https://topcandidate.pro/sitemap.xml", "sitemap pointer present"),
            ("/sitemap.xml", "<loc>https://topcandidate.pro/</loc>", "landing url listed"),
        ]
        for path, needle, ok_note in seo_checks:
            try:
                r = c.get(path)
                ok = r.status_code == 200 and needle in (r.text or "")
                line(r.status_code, "GET", path, ok_note if ok else "MISSING EXPECTED CONTENT")
                checked += 1
                if not ok:
                    failures.append(("GET", path, r.status_code if r.status_code >= 500 else "malformed"))
            except Exception as e:
                line(None, "GET", path, str(e)[:60])
                failures.append(("GET", path, "exception"))
        # favicon must be a real ICO (bytes), served 200
        try:
            r = c.get("/favicon.ico")
            ok = r.status_code == 200 and len(r.content) > 100
            line(r.status_code, "GET", "/favicon.ico", f"{len(r.content)} bytes" if ok else "EMPTY/MISSING")
            checked += 1
            if not ok:
                failures.append(("GET", "/favicon.ico", r.status_code))
        except Exception as e:
            line(None, "GET", "/favicon.ico", str(e)[:60])
            failures.append(("GET", "/favicon.ico", "exception"))
        # The PNG favicon (Google) and the PWA icon must serve real bytes too.
        for path in ("/static/favicon-48.png", "/static/icon-512.png"):
            try:
                r = c.get(path)
                ok = r.status_code == 200 and len(r.content) > 100
                line(r.status_code, "GET", path, f"{len(r.content)} bytes" if ok else "EMPTY/MISSING")
                checked += 1
                if not ok:
                    failures.append(("GET", path, r.status_code))
            except Exception as e:
                line(None, "GET", path, str(e)[:60])
                failures.append(("GET", path, "exception"))

        # ── Viva Live (L0) — checked here, PRE-AUTH, on purpose ──
        # The token-mint endpoint spends real money per call, so the check must
        # never hit it with valid auth. Unauthenticated it MUST be gated (401/403);
        # a 200 here would mean an open spend hole. And the public page must load.
        print("\nViva Live")
        try:
            r = c.get("/viva-live")
            body = r.text or ""
            # STRUCTURAL needles, not copy text. The first version asserted the
            # phrase "network test", which the L1 rewrite renamed — producing a
            # false MALFORMED on a perfectly good page. Assert what the page
            # needs to FUNCTION: its start button, the WebRTC API call, a
            # closing </html>, and no unreplaced {{placeholders}}.
            ok = (r.status_code == 200
                  and 'id="btn-start"' in body
                  and "RTCPeerConnection" in body
                  and "</html>" in body
                  and "{{" not in body)
            line(r.status_code, "GET", "/viva-live",
                 "well-formed page" if ok else "MALFORMED — missing structure or placeholder leak")
            checked += 1
            if not ok:
                failures.append(("GET", "/viva-live", r.status_code))
        except Exception as e:
            line(None, "GET", "/viva-live", str(e)[:60]); failures.append(("GET", "/viva-live", "exception"))
        try:
            # POST since L1 (recovery context in the body). Still pre-auth: a
            # non-401/403 here is an open spend hole.
            r = c.post("/api/viva-live/token", json={"transcript": []})
            gated = r.status_code in (401, 403)
            line(r.status_code, "POST", "/api/viva-live/token",
                 "gated (owner-only)" if gated else "NOT GATED — spend hole!")
            checked += 1
            if not gated:
                failures.append(("POST", "/api/viva-live/token", r.status_code))
        except Exception as e:
            line(None, "POST", "/api/viva-live/token", str(e)[:60]); failures.append(("POST", "/api/viva-live/token", "exception"))
        # L4 gates — always on a FRESH credential-free client (the written-
        # endpoint lesson: a gate check must never inherit auth from `c`).
        try:
            with httpx.Client(base_url=base, timeout=30.0) as unauth:
                r = unauth.get("/api/viva-live/sessions")
            gated = r.status_code in (401, 403)
            line(r.status_code, "GET", "/api/viva-live/sessions",
                 "gated (owner-only)" if gated else "NOT GATED — interview data leak!")
            checked += 1
            if not gated:
                failures.append(("GET", "/api/viva-live/sessions", r.status_code))
        except Exception as e:
            line(None, "GET", "/api/viva-live/sessions", str(e)[:60]); failures.append(("GET", "/api/viva-live/sessions", "exception"))
        try:
            r = c.get("/viva-live/sessions")
            body = r.text or ""
            ok = (r.status_code == 200 and 'id="list-card"' in body
                  and "AI assessment" in body and "</html>" in body and "{{" not in body)
            line(r.status_code, "GET", "/viva-live/sessions",
                 "results page well-formed" if ok else "MALFORMED")
            checked += 1
            if not ok:
                failures.append(("GET", "/viva-live/sessions", r.status_code))
        except Exception as e:
            line(None, "GET", "/viva-live/sessions", str(e)[:60]); failures.append(("GET", "/viva-live/sessions", "exception"))
        try:
            r = c.get("/viva-live/loadspike")
            body = r.text or ""
            ok = (r.status_code == 200 and 'id="btn-start"' in body
                  and "getDisplayMedia" in body and "</html>" in body and "{{" not in body)
            line(r.status_code, "GET", "/viva-live/loadspike",
                 "spike page well-formed" if ok else "MALFORMED")
            checked += 1
            if not ok:
                failures.append(("GET", "/viva-live/loadspike", r.status_code))
        except Exception as e:
            line(None, "GET", "/viva-live/loadspike", str(e)[:60]); failures.append(("GET", "/viva-live/loadspike", "exception"))
        # Candidate-link surfaces (the recruiter/candidate split).
        try:
            r = c.get("/interview/smoke-not-a-real-token")
            body = r.text or ""
            ok = r.status_code < 500 and "accepting applications" in body.lower()
            line(r.status_code, "GET", "/interview/{unknown}",
                 "unified closed page" if ok else "NOT the closed page")
            checked += 1
            if not ok:
                failures.append(("GET", "/interview/{unknown}", r.status_code))
        except Exception as e:
            line(None, "GET", "/interview/{unknown}", str(e)[:60]); failures.append(("GET", "/interview/{unknown}", "exception"))
        try:
            r = c.post("/api/interview/smoke-not-a-real-token/session-token", json={})
            ok = r.status_code == 404   # dead token: neutral refusal, no mint, no spend
            line(r.status_code, "POST", "/api/interview/{unknown}/session-token",
                 "refused, no spend" if ok else "UNEXPECTED")
            checked += 1
            if not ok:
                failures.append(("POST", "/api/interview/{unknown}/session-token", r.status_code))
        except Exception as e:
            line(None, "POST", "/api/interview/{unknown}/session-token", str(e)[:60]); failures.append(("POST", "/api/interview/{unknown}/session-token", "exception"))
        try:
            with httpx.Client(base_url=base, timeout=30.0) as unauth:
                r = unauth.post("/api/viva-live/create", json={"questions": ["x"]})
            gated = r.status_code in (401, 403)
            line(r.status_code, "POST", "/api/viva-live/create",
                 "gated (owner-only)" if gated else "NOT GATED — anyone could mint links!")
            checked += 1
            if not gated:
                failures.append(("POST", "/api/viva-live/create", r.status_code))
        except Exception as e:
            line(None, "POST", "/api/viva-live/create", str(e)[:60]); failures.append(("POST", "/api/viva-live/create", "exception"))
        try:
            with httpx.Client(base_url=base, timeout=30.0) as unauth:
                r = unauth.post("/api/viva-live/preview-session", json={"questions": ["x"]})
            gated = r.status_code in (401, 403)
            line(r.status_code, "POST", "/api/viva-live/preview-session",
                 "gated (owner-only)" if gated else "NOT GATED")
            checked += 1
            if not gated:
                failures.append(("POST", "/api/viva-live/preview-session", r.status_code))
        except Exception as e:
            line(None, "POST", "/api/viva-live/preview-session", str(e)[:60]); failures.append(("POST", "/api/viva-live/preview-session", "exception"))
        # Employees (HRM module 1) — personal data: every route must refuse a
        # credential-free client. Verified here from the first build.
        for method, path in (("GET", "/api/employees"), ("POST", "/api/employees"),
                             ("PUT", "/api/employees/000000000000000000000000")):
            try:
                with httpx.Client(base_url=base, timeout=30.0) as unauth:
                    r = unauth.request(method, path, json={} if method != "GET" else None)
                gated = r.status_code in (401, 403)
                line(r.status_code, method, path.replace("000000000000000000000000", "{id}"),
                     "gated (owner-only)" if gated else "NOT GATED — employee data leak!")
                checked += 1
                if not gated:
                    failures.append((method, path, r.status_code))
            except Exception as e:
                line(None, method, path, str(e)[:60]); failures.append((method, path, "exception"))
        # Attendance & Leave (HRM module 2) — personal data, admin-only in
        # Part 1. Every route must refuse a credential-free client.
        for method, path in (("GET", "/api/hr/summary"),
                             ("GET", "/api/leave/requests"), ("POST", "/api/leave/requests"),
                             ("POST", "/api/leave/requests/000000000000000000000000/decide"),
                             ("GET", "/api/leave/balances"),
                             ("GET", "/api/attendance?month=2026-08"),
                             ("POST", "/api/attendance/mark")):
            try:
                with httpx.Client(base_url=base, timeout=30.0) as unauth:
                    r = unauth.request(method, path, json={} if method != "GET" else None)
                gated = r.status_code in (401, 403)
                line(r.status_code, method, path.replace("000000000000000000000000", "{id}"),
                     "gated (owner-only)" if gated else "NOT GATED — HR data leak!")
                checked += 1
                if not gated:
                    failures.append((method, path, r.status_code))
            except Exception as e:
                line(None, method, path, str(e)[:60]); failures.append((method, path, "exception"))
        # ── EMPLOYEE-TOKEN BOUNDARY (HRM Part 2, security point 6) ──
        # A real role=employee token MUST be refused by every admin/hiring
        # endpoint, and admitted only by the six /api/me/* routes. This is the
        # deploy gate the user demanded: a regression that leaks an employee
        # through an admin route fails the build here.
        emp_tok = mint_employee_token()
        if not emp_tok:
            line(None, "AUTH", "employee-token boundary", "could not mint employee token (SECRET_KEY?)")
            failures.append(("AUTH", "employee-token", "mint failed"))
        else:
            print("\nEmployee-token boundary — must be REJECTED by admin/hiring")
            forbidden = [
                ("GET", "/api/employees"), ("POST", "/api/employees"),
                ("GET", "/api/screenings"), ("GET", "/api/jobs"),
                ("GET", "/api/stats"), ("GET", "/api/analytics/skills-gaps"),
                ("GET", "/api/hr/summary"),
                ("GET", "/api/leave/requests"), ("POST", "/api/leave/requests"),
                ("GET", "/api/leave/balances"),
                ("GET", "/api/attendance?month=2026-08"), ("POST", "/api/attendance/mark"),
                ("GET", "/api/admin/diagnostics"), ("GET", "/api/keys"),
            ]
            for method, path in forbidden:
                try:
                    with httpx.Client(base_url=base, timeout=30.0) as ec:
                        ec.headers["Authorization"] = f"Bearer {emp_tok}"
                        r = ec.request(method, path, json={} if method == "POST" else None)
                    ok = r.status_code == 403   # get_current_user refuses employee role
                    line(r.status_code, method, path,
                         "employee refused" if ok else "LEAK — employee reached admin route!")
                    checked += 1
                    if not ok:
                        failures.append(("EMP-FORBIDDEN", path, r.status_code))
                except Exception as e:
                    line(None, method, path, str(e)[:60]); failures.append(("EMP-FORBIDDEN", path, "exception"))

            print("\nEmployee-token boundary — must be ADMITTED by /api/me/* (guard passes)")
            allowed = [("GET", "/api/me"), ("GET", "/api/me/leave"),
                       ("GET", "/api/me/attendance"), ("PATCH", "/api/me"),
                       ("POST", "/api/me/leave")]
            for method, path in allowed:
                try:
                    with httpx.Client(base_url=base, timeout=30.0) as ec:
                        ec.headers["Authorization"] = f"Bearer {emp_tok}"
                        r = ec.request(method, path, json={} if method in ("POST", "PATCH") else None)
                    # The guard admitting the employee is the point — 401/403 would
                    # mean the boundary wrongly rejects its own role. (404 = guard
                    # passed, synthetic employee_id simply has no record: correct.)
                    ok = r.status_code not in (401, 403)
                    line(r.status_code, method, path,
                         "guard admits employee" if ok else "employee wrongly rejected")
                    checked += 1
                    if not ok:
                        failures.append(("EMP-ALLOWED", path, r.status_code))
                except Exception as e:
                    line(None, method, path, str(e)[:60]); failures.append(("EMP-ALLOWED", path, "exception"))

        # One-link flow (CV upload → conditional interview).
        try:
            # A probe with a fake token+id must get EXACTLY the flat received
            # payload — no score, no state hints, indistinguishable from a real
            # gated-out candidate.
            r = c.get("/api/apply/smoke-not-a-real-token/status/000000000000000000000000")
            payload = {}
            try:
                payload = r.json()
            except Exception:
                pass
            ok = r.status_code == 200 and payload == {"state": "received"}
            line(r.status_code, "GET", "/api/apply/{unknown}/status/{id}",
                 "flat 'received', leaks nothing" if ok else f"LEAKY: {str(payload)[:60]}")
            checked += 1
            if not ok:
                failures.append(("GET", "/api/apply/{unknown}/status/{id}", r.status_code))
        except Exception as e:
            line(None, "GET", "/api/apply/{unknown}/status/{id}", str(e)[:60]); failures.append(("GET", "/api/apply/{unknown}/status/{id}", "exception"))
        try:
            with httpx.Client(base_url=base, timeout=30.0) as unauth:
                r = unauth.post("/api/jobs/000000000000000000000000/viva", json={"enabled": True})
            gated = r.status_code in (401, 403)
            line(r.status_code, "POST", "/api/jobs/{id}/viva",
                 "gated (owner-only)" if gated else "NOT GATED — anyone could attach gates!")
            checked += 1
            if not gated:
                failures.append(("POST", "/api/jobs/{id}/viva", r.status_code))
        except Exception as e:
            line(None, "POST", "/api/jobs/{id}/viva", str(e)[:60]); failures.append(("POST", "/api/jobs/{id}/viva", "exception"))
        try:
            with httpx.Client(base_url=base, timeout=30.0) as unauth:
                r = unauth.get("/api/kpi")
            gated = r.status_code in (401, 403)
            line(r.status_code, "GET", "/api/kpi",
                 "gated (tenant-scoped)" if gated else "NOT GATED — KPI data leak!")
            checked += 1
            if not gated:
                failures.append(("GET", "/api/kpi", r.status_code))
        except Exception as e:
            line(None, "GET", "/api/kpi", str(e)[:60]); failures.append(("GET", "/api/kpi", "exception"))
        for iq_path in ("/api/jobs/000000000000000000000000/interview-questions/generate",
                        "/api/jobs/000000000000000000000000/interview-questions"):
            try:
                with httpx.Client(base_url=base, timeout=30.0) as unauth:
                    r = unauth.post(iq_path, json={})
                gated = r.status_code in (401, 403)
                line(r.status_code, "POST", iq_path.replace("000000000000000000000000", "{id}"),
                     "gated (owner-only)" if gated else "NOT GATED")
                checked += 1
                if not gated:
                    failures.append(("POST", iq_path, r.status_code))
            except Exception as e:
                line(None, "POST", iq_path, str(e)[:60]); failures.append(("POST", iq_path, "exception"))
        try:
            r = c.get("/viva-live/check")
            body = r.text or ""
            ok = (r.status_code == 200 and 'id="btn-start"' in body
                  and "never a verdict" in body and "</html>" in body and "{{" not in body)
            line(r.status_code, "GET", "/viva-live/check",
                 "device-check page well-formed" if ok else "MALFORMED")
            checked += 1
            if not ok:
                failures.append(("GET", "/viva-live/check", r.status_code))
        except Exception as e:
            line(None, "GET", "/viva-live/check", str(e)[:60]); failures.append(("GET", "/viva-live/check", "exception"))

        # ── Authenticate ─────────────────────────────────────────────────
        print("\nAuthenticating")
        # Primary: mint a token from SECRET_KEY (server-side, no password).
        token_jwt, who = mint_admin_token(args.admin_email)
        if token_jwt:
            c.headers["Authorization"] = f"Bearer {token_jwt}"
            # Prove the minted token is actually accepted before trusting it.
            me = c.get("/api/auth/me")
            line(me.status_code, "AUTH", f"minted token for {who}",
                 "accepted" if me.status_code == 200 else "REJECTED")
            if me.status_code != 200:
                print("\nMinted token rejected — cannot test authenticated routes.\n")
                return 1
        elif args.email and args.password:
            # Fallback for remote runs without DB/SECRET_KEY access.
            r = c.post("/api/auth/login", data={"email": args.email, "password": args.password})
            line(r.status_code, "POST", "/api/auth/login (password fallback)")
            if r.status_code != 200:
                print("\nLogin failed — cannot test authenticated routes.\n")
                return 1
        else:
            print(f"\nNo auth available: {who}. Authenticated routes skipped.")
            print("Run on the server (mints from SECRET_KEY) or set SMOKE_EMAIL/PASSWORD.\n")
            return 1 if failures else 0

        # ── Resolve real ids, so path params aren't guesses ──────────────
        job_id = screening_id = token = None
        jr = c.get("/api/jobs")
        if jr.status_code == 200:
            jobs = jr.json().get("jobs", [])
            if jobs:
                job_id = jobs[0].get("_id")
                token = jobs[0].get("public_token")
        sr = c.get("/api/screenings?limit=1")
        if sr.status_code == 200:
            rows = sr.json().get("screenings", [])
            if rows:
                screening_id = rows[0].get("_id")

        print("\nAuthenticated routes")
        checks = [
            ("GET", "/app", None),
            ("GET", "/batch", None),
            ("GET", "/settings", None),
            ("GET", "/candidate", None),
            ("GET", "/admin", None),
            ("GET", "/api/stats", None),
            ("GET", "/api/jobs", None),
            ("GET", "/api/screenings?limit=5", None),
            ("GET", "/api/analytics/skills-gaps", None),
            ("GET", "/api/analytics/dimension-averages", None),
            ("GET", "/api/debug/my-screenings", None),
            ("GET", "/api/admin/check-users", None),
            ("GET", "/api/admin/diagnostics", None),
            ("GET", "/api/keys", None),
            ("GET", "/api/employees", None),
            ("GET", "/api/hr/summary", None),
            ("GET", "/api/leave/requests", None),
            ("GET", "/api/leave/balances", None),
            ("GET", "/api/kpi", None),
        ]
        if job_id:
            checks += [
                ("GET", f"/api/jobs/{job_id}/details", None),
                ("GET", f"/api/jobs/{job_id}/applications", None),
            ]
        if screening_id:
            checks += [
                ("GET", f"/api/screenings/{screening_id}", None),
                # The CV viewer. Bare db. use — dead before the handle fix.
                ("GET", f"/api/screenings/{screening_id}/cv", None),
            ]
        if token:
            checks += [("GET", f"/apply/{token}", "live public link")]

        for method, path, note in checks:
            try:
                r = c.request(method, path)
                line(r.status_code, method, path, note or "")
                checked += 1
                if r.status_code >= FATAL_FROM:
                    failures.append((method, path, r.status_code))
            except Exception as e:
                line(None, method, path, str(e)[:60])
                failures.append((method, path, "exception"))

        # ── Regression locks: two bugs the smoke test already caught once ──
        # A 5xx is already fatal above, but these assert on the RESPONSE, not
        # just the status, so a subtler break (200 with a garbled body) is
        # caught too. Each appends to `failures` on its own terms.
        print("\nRegression locks")

        # Landing page: structural (renders whole, all sections) + honesty
        # (the day-one rules — no accuracy %, no "trusted by", no testimonials
        # — stay enforced by machine, not memory).
        try:
            r = c.get("/")
            body = r.text or ""
            structural = (r.status_code == 200 and 'id="what"' in body and 'id="how"' in body
                          and 'id="scoring"' in body and 'id="access"' in body
                          and "</html>" in body and "{{" not in body)
            banned = [s for s in ("trusted by", "testimonial", "accuracy", "star rating")
                      if s in body.lower()]
            ok = structural and not banned
            note = ("landing whole + honesty rules hold" if ok
                    else (f"BANNED COPY: {banned}" if banned else "MALFORMED — section missing"))
            line(r.status_code, "GET", "/ (landing lock)", note)
            checked += 1
            if not ok:
                failures.append(("GET", "/ (landing lock)", r.status_code))
        except Exception as e:
            line(None, "GET", "/ (landing lock)", str(e)[:60])
            failures.append(("GET", "/ (landing lock)", "exception"))

        # Mixed spoken/typed: a typed question MUST produce a tool-registered
        # session, and a spoken-only config must not. preview-session runs the
        # exact validate->normalize->instructions pipeline of a candidate mint
        # (no OpenAI call, no DB write), so a silent regression anywhere in
        # that chain fails the deploy here instead of mid-interview.
        try:
            r = c.post("/api/viva-live/preview-session", json={
                "questions": [{"text": "Spoken q", "mode": "spoken"},
                              {"text": "Typed q", "mode": "typed"}]})
            body = r.json() if r.status_code == 200 else {}
            modes = [q.get("mode") for q in body.get("questions", [])]
            ok = (r.status_code == 200 and modes == ["spoken", "typed"]
                  and body.get("has_typed") is True and body.get("tool_registered") is True
                  and body.get("tool_name") == "begin_typed_answer"
                  and body.get("typed_rules_in_instructions") is True)
            line(r.status_code, "POST", "/api/viva-live/preview-session (typed)",
                 "typed q -> tool registered + rules in prompt" if ok
                 else f"TYPED FLOW BROKEN: {str(body)[:80]}")
            checked += 1
            if not ok:
                failures.append(("POST", "/api/viva-live/preview-session (typed)", r.status_code))
        except Exception as e:
            line(None, "POST", "/api/viva-live/preview-session (typed)", str(e)[:60])
            failures.append(("POST", "/api/viva-live/preview-session (typed)", "exception"))
        # Scenario-based written section: a config with a scenario MUST
        # register BOTH tools, put the scenario verbatim + section rules into
        # the instructions, and grow the turn budget to hold its questions.
        try:
            r = c.post("/api/viva-live/preview-session", json={
                "questions": [{"text": "Spoken q", "mode": "spoken"}],
                "max_turns": 1,
                "scenario": {"text": "A smoke-test scenario about a report deadline.",
                             "questions": ["What would you do first?",
                                           "How would you communicate the delay?"]}})
            body = r.json() if r.status_code == 200 else {}
            sc = body.get("scenario") or {}
            ok = (r.status_code == 200
                  and body.get("scenario_tool_registered") is True
                  and body.get("scenario_in_instructions") is True
                  and body.get("tool_registered") is True          # typed panel still armed
                  and body.get("typed_rules_in_instructions") is True
                  and len(sc.get("questions", [])) == 2
                  and int(body.get("max_turns", 0)) >= 3)          # 1 spoken + 2 scenario
            line(r.status_code, "POST", "/api/viva-live/preview-session (scenario)",
                 "scenario -> both tools + rules + budget grown" if ok
                 else f"SCENARIO FLOW BROKEN: {str(body)[:90]}")
            checked += 1
            if not ok:
                failures.append(("POST", "/api/viva-live/preview-session (scenario)", r.status_code))
        except Exception as e:
            line(None, "POST", "/api/viva-live/preview-session (scenario)", str(e)[:60])
            failures.append(("POST", "/api/viva-live/preview-session (scenario)", "exception"))
        # Job-based sets: 10 questions must survive the pipeline intact — this
        # exact check catches a cap regression (questions were [:3] before
        # job-based sets raised the limits) and a mode-mangling one.
        try:
            ten = [{"text": f"Question {i+1}", "mode": "typed" if i < 3 else "spoken"}
                   for i in range(10)]
            r = c.post("/api/viva-live/preview-session", json={"questions": ten})
            body = r.json() if r.status_code == 200 else {}
            got = body.get("questions", [])
            n_typed = sum(1 for q in got if q.get("mode") == "typed")
            ok = (r.status_code == 200 and len(got) == 10 and n_typed == 3
                  and body.get("has_typed") is True and body.get("tool_registered") is True)
            line(r.status_code, "POST", "/api/viva-live/preview-session (10q)",
                 "10-question set intact (7 spoken · 3 typed)" if ok
                 else f"SET MANGLED: {len(got)} qs, {n_typed} typed")
            checked += 1
            if not ok:
                failures.append(("POST", "/api/viva-live/preview-session (10q)", r.status_code))
        except Exception as e:
            line(None, "POST", "/api/viva-live/preview-session (10q)", str(e)[:60])
            failures.append(("POST", "/api/viva-live/preview-session (10q)", "exception"))
        try:
            r = c.post("/api/viva-live/preview-session", json={
                "questions": ["Plain legacy string question"]})
            body = r.json() if r.status_code == 200 else {}
            ok = (r.status_code == 200 and body.get("has_typed") is False
                  and body.get("tool_registered") is False
                  and body.get("typed_rules_in_instructions") is False)
            line(r.status_code, "POST", "/api/viva-live/preview-session (spoken)",
                 "spoken-only -> no tool, no typed rules" if ok
                 else f"SPOKEN PATH POLLUTED: {str(body)[:80]}")
            checked += 1
            if not ok:
                failures.append(("POST", "/api/viva-live/preview-session (spoken)", r.status_code))
        except Exception as e:
            line(None, "POST", "/api/viva-live/preview-session (spoken)", str(e)[:60])
            failures.append(("POST", "/api/viva-live/preview-session (spoken)", "exception"))

        # /api/screenings 500'd on a raw ObjectId (cv_file_id) it failed to
        # serialize. Assert it returns JSON with a screenings LIST — a
        # serialization regression cannot pass this even if it somehow avoids a 5xx.
        try:
            r = c.get("/api/screenings?limit=5")
            ok = r.status_code == 200
            body = r.json() if ok else {}
            ok = ok and isinstance(body.get("screenings"), list)
            line(r.status_code, "GET", "/api/screenings",
                 "screenings[] present" if ok else "MALFORMED — no screenings list")
            checked += 1
            if not ok:
                failures.append(("GET", "/api/screenings", r.status_code if r.status_code >= 500 else "malformed"))
        except Exception as e:
            line(None, "GET", "/api/screenings", str(e)[:60])
            failures.append(("GET", "/api/screenings", "exception"))

        # /apply/{token}: an unknown token must return the unified closed page —
        # never a 500, never a stack trace. This locks the probe-resistant
        # response: unknown and paused are indistinguishable.
        try:
            r = c.get("/apply/smoke-test-definitely-not-real")
            body = r.text or ""
            ok = r.status_code < 500 and "accepting applications" in body.lower()
            line(r.status_code, "GET", "/apply/{unknown}",
                 "unified closed page" if ok else "NOT the closed page")
            checked += 1
            if not ok:
                failures.append(("GET", "/apply/{unknown}", r.status_code))
        except Exception as e:
            line(None, "GET", "/apply/{unknown}", str(e)[:60])
            failures.append(("GET", "/apply/{unknown}", "exception"))

        # /viva/{token}: an unknown recording token must return the same closed
        # page — never a 500. (Phase 1 video interview.)
        try:
            r = c.get("/viva/smoke-test-definitely-not-real")
            body = r.text or ""
            ok = r.status_code < 500 and "accepting applications" in body.lower()
            line(r.status_code, "GET", "/viva/{unknown}",
                 "unified closed page" if ok else "NOT the closed page")
            checked += 1
            if not ok:
                failures.append(("GET", "/viva/{unknown}", r.status_code))
        except Exception as e:
            line(None, "GET", "/viva/{unknown}", str(e)[:60])
            failures.append(("GET", "/viva/{unknown}", "exception"))

        # Written segment: an unknown token must 404 (never 5xx, never a
        # model call), and the results endpoint must be gated pre-auth.
        try:
            r = c.post("/api/viva/smoke-not-real/written",
                       json={"name": "x", "email": "x@y.com", "answers": []})
            ok = r.status_code == 404
            line(r.status_code, "POST", "/api/viva/{unknown}/written",
                 "rejected" if ok else "UNEXPECTED")
            checked += 1
            if not ok:
                failures.append(("POST", "/api/viva/{unknown}/written", r.status_code))
        except Exception as e:
            line(None, "POST", "/api/viva/{unknown}/written", str(e)[:60])
            failures.append(("POST", "/api/viva/{unknown}/written", "exception"))
        try:
            # GATE CHECK — must run WITHOUT credentials. The client `c` carries
            # the admin token by this point in the run, and the first version of
            # this check used it — testing whether the door was locked while
            # holding the key. It reported a well-guarded endpoint as NOT GATED
            # (the endpoint's first line is require_admin; raw unauth curl gives
            # 401). Every gate assertion now uses a fresh, credential-free
            # client, so placement in this file can never contaminate it again.
            with httpx.Client(base_url=base, timeout=30.0) as unauth:
                r = unauth.get("/api/interviews/000000000000000000000000/written")
            gated = r.status_code in (401, 403)
            line(r.status_code, "GET", "/api/interviews/{id}/written",
                 "gated (owner-only)" if gated else "NOT GATED")
            checked += 1
            if not gated:
                failures.append(("GET", "/api/interviews/{id}/written", r.status_code))
        except Exception as e:
            line(None, "GET", "/api/interviews/{id}/written", str(e)[:60])
            failures.append(("GET", "/api/interviews/{id}/written", "exception"))

        # If a real token exists, it must render EITHER the form (public) or the
        # same closed page (paused) — but never a 500. This is the check that
        # would have flagged the applicant-facing page being down.
        if token:
            try:
                r = c.get(f"/apply/{token}")
                body = r.text or ""
                shows_form = "apply for this role" in body.lower()
                shows_closed = "accepting applications" in body.lower()
                ok = r.status_code < 500 and (shows_form or shows_closed)
                state = "form (public)" if shows_form else "closed (paused)" if shows_closed else "UNRECOGNISED"
                line(r.status_code, "GET", "/apply/{real}", state)
                checked += 1
                if not ok:
                    failures.append(("GET", "/apply/{real}", r.status_code))
            except Exception as e:
                line(None, "GET", "/apply/{real}", str(e)[:60])
                failures.append(("GET", "/apply/{real}", "exception"))

        # ── The write that was broken, exercised without changing state ──
        if job_id:
            print("\nMutating route (set to its current value — full path, no state change)")
            cur = c.get(f"/api/jobs/{job_id}/details")
            is_public = bool(cur.json().get("is_public")) if cur.status_code == 200 else False
            path = f"/api/jobs/{job_id}/public"
            try:
                r = c.post(path, data={"is_public": str(is_public).lower()})
                line(r.status_code, "POST", path, f"is_public={is_public} (unchanged)")
                checked += 1
                if r.status_code >= FATAL_FROM:
                    failures.append(("POST", path, r.status_code))
            except Exception as e:
                line(None, "POST", path, str(e)[:60])
                failures.append(("POST", path, "exception"))

    print(f"\n{checked} routes checked.")
    if failures:
        print(f"\n\033[31m{len(failures)} FAILED\033[0m")
        for m, p, s in failures:
            print(f"    {s}  {m} {p}")
        print()
        return 1
    print("\033[32mNo 5xx.\033[0m\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
