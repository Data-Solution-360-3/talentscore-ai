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
