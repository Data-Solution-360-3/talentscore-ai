# TopCandidate.pro — AI CV Screening Platform

AI-powered CV screening built with FastAPI + OpenAI GPT-4o + MongoDB Atlas, deployed on Render.

**Live:** https://talentscore-ai.onrender.com
**Repo:** github.com/Data-Solution-360-3/talentscore-ai

## What this is

TopCandidate.pro screens CVs against job descriptions using a 3-step GPT-4o pipeline
(parse CV, parse JD, score twice and average) and ranks candidates. It supports:

- **Web app** — upload up to 100 CVs in a batch, stream live progress, view ranked results
- **Public REST API (v1)** — `/api/v1/screen` for programmatic use, keyed with `X-API-Key`
- **Team workspaces** — invite members via email, per-company data isolation
- **Plans** — trial / starter / pro / enterprise, enforced by monthly quota + per-plan batch size
- **Payments** — Stripe (international) + SSLCommerz (Bangladesh) + manual bank/bkash/nagad

## Patches applied in this build

This drop fixes four bugs identified in the previous version:

1. **Screening counter frozen (#2, #5)** — `batch_screen_endpoint` referenced a `results`
   variable that was local to a nested function and therefore undefined in the enclosing
   scope, raising `NameError` right after the `done` event was sent. The stream closed
   cleanly so the frontend never noticed, but `increment_screening_count` and
   `sync_screening_count` never ran. Cached count drifted from reality (showed 6 while
   DB had 32), which also broke plan-limit enforcement since the limit check trusted the
   stale cache. Fixed: the correct variable is `summary` (returned by `batch_task`).

2. **Plan limits not enforcing (#5)** — the limit check read the cached `screening_count`
   field on the user doc. Now calls `sync_screening_count(user_id)` right before
   checking, so enforcement is self-healing against any future cache drift.

3. **No per-plan batch-size limit (#5)** — the endpoint only capped at a global 100.
   Now enforces `API_PLAN_LIMITS[plan].batch_size` (trial=10, starter=20, pro=100,
   enterprise=100) before accepting the upload.

4. **Team invite emails silently failing (#4)** — `render.yaml` only declared
   `OPENAI_API_KEY`, `MONGO_URI`, and `DB_NAME`, so `GMAIL_USER` / `GMAIL_APP_PASSWORD`
   were empty strings in production and `send_team_invite_email` short-circuited to
   `return False`. Now declares all env vars; set their values in the Render dashboard.

5. **Latent `NameError` on API v1 endpoints** — `APP_URL` was referenced in
   `/api/v1/screen` and `/api/v1/results` but never defined at module level in main.py.
   Fixed with `APP_URL = os.getenv("APP_URL", "https://topcandidate.pro")`.

6. **Tenant isolation hole on job details** — `/api/jobs/{job_id}/details` had no
   `user_id` filter, so any authenticated user could read any job by ID. Now enforces
   tenant scope (admins excepted). Also fixed a bare `except Exception` that was
   converting legitimate 404s into 400s with the 404 message as the 400 detail.

## Still open (frontend — needs templates)

- **Settings page buttons not all working (#1)** — backend endpoints look correct.
  Most likely cause is frontend JS. Re-check that:
  - `/api/user/notifications` is POSTed as `FormData` with a `prefs` field containing
    `JSON.stringify(prefsObj)`, NOT as `application/json`.
  - API-key revoke uses `DELETE /api/keys/{key_id}` with credentials included.
  - Billing portal button POSTs to `/api/payments/portal`, not GET.

- **JD not auto-filling on batch page (#3)** — backend `/api/jobs/{job_id}/details`
  returns a `description` field. Verify the batch page:
  - Parses `job_id` from `window.location.search` on load.
  - Calls `fetch('/api/jobs/' + jobId + '/details', { credentials: 'include' })`.
  - Populates the JD textarea with `data.description` (not `data.job_description`).

## Environment variables

All set in Render → Environment:

| Variable | Purpose |
|---|---|
| `OPENAI_API_KEY` | OpenAI API key (sk-...) |
| `MONGO_URI` | MongoDB Atlas connection string |
| `DB_NAME` | Database name (default: talentscore) |
| `SECRET_KEY` | JWT signing key |
| `APP_URL` | Public URL, used in email links & API `report_url` |
| `GMAIL_USER` | Gmail address for SMTP sends |
| `GMAIL_APP_PASSWORD` | 16-char Gmail App Password (NOT regular password) |
| `STRIPE_SECRET_KEY` | Stripe secret (sk_live_ or sk_test_) |
| `STRIPE_PUBLISHABLE_KEY` | Stripe publishable (pk_...) |
| `STRIPE_WEBHOOK_SECRET` | Stripe webhook signing secret (whsec_...) |
| `STRIPE_PRICE_STARTER` | Stripe price ID for Starter plan |
| `STRIPE_PRICE_PRO` | Stripe price ID for Pro plan |
| `SSLCOMMERZ_STORE_ID` | SSLCommerz store ID |
| `SSLCOMMERZ_STORE_PASS` | SSLCommerz store password |
| `SSLCOMMERZ_SANDBOX` | "true" for testing, "false" for live |

## Local development

```bash
python -m venv .venv
source .venv/bin/activate           # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env                # then fill in real values
uvicorn main:app --reload
```

Hit http://127.0.0.1:8000 — default admin is `admin@talentscore.ai` / `Admin@123`
(auto-created on first run if no admin exists).

## Verify the patches after deploying

After pushing this build to Render and setting the Gmail env vars in the dashboard:

1. **Unstick your count.** While logged in, `POST /api/user/fix-count`. Response should
   jump from 6 → 32 (or whatever your actual DB count is).
2. **Verify counter stays correct.** Run a small batch (2-3 CVs). The displayed count
   should increment by exactly that many. Before this build, it would stay frozen.
3. **Verify team invites send.** `POST /api/team/invite` with an email. Response should
   include `"email_sent": true`. Check the inbox.
4. **Verify plan limits.** On a trial account with 10/10 used, the next batch upload
   should get a 429 with "Monthly limit reached (10/10 on Trial plan)".

## Architecture notes

- **No frontend templates in this zip.** The `.gitignore` excludes `templates/` and
  `static/` — those live in the main repo. Drop this zip's contents on top of your
  checkout and the existing templates keep working.
- **Tenant isolation** happens via `user_id` on every screening/job/payment doc.
  Admin role bypasses the filter. If you ever add a new collection, follow the pattern
  in `get_screenings_for_user()`.
- **Concurrency** in batch screening is capped at 5 parallel OpenAI calls by default
  (`batch.py:CONCURRENCY_LIMIT`). Raise to 10 if on a higher OpenAI tier.
