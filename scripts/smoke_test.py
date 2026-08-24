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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=os.getenv("SMOKE_BASE", DEFAULT_BASE))
    ap.add_argument("--email", default=os.getenv("SMOKE_EMAIL", ""))
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

        # ── Log in ───────────────────────────────────────────────────────
        if not args.email or not args.password:
            print("\nNo SMOKE_EMAIL / SMOKE_PASSWORD set — authenticated routes skipped.")
            print("Those are where the dead-handle bug lived, so set them.\n")
            return 1 if failures else 0

        print("\nAuthenticating")
        r = c.post("/api/auth/login", data={"email": args.email, "password": args.password})
        line(r.status_code, "POST", "/api/auth/login")
        if r.status_code != 200:
            print("\nLogin failed — cannot test authenticated routes.\n")
            return 1

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
