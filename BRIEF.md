# TopCandidate.pro — Project Brief

**Last verified: 2026-08-22**, after the unauthenticated-admin-endpoint incident.
Paste this as the first message in a fresh Claude session.

Anything marked ⚠️ is unverified or a known-false claim from an earlier version. Do not treat ⚠️ items as done.
Anything marked ✅ was verified against live production or the code on the date shown.

---

## 1. Who I am + how I work

- I'm Sakib, founder of LinkX360 / Data Solution 360 — small tech company, Dhaka, Bangladesh.
- English is my second language. I type fast and have typos. Answer what I mean; don't ask for clarification unless genuinely ambiguous.
- Windows + Anaconda Prompt. Claude Code locally in the project folder.
- I prefer concise responses, clear filenames, short deploy commands at the end. Build first, explain briefly.
- Push back on bad ideas. If I'm about to hurt myself, say so directly. Don't just execute.
- Batch related changes — don't ship one file at a time when three need to change together.
- Never paste real secrets. If I paste one by accident, tell me to rotate immediately.
- **Say "we can't tell" rather than guessing.** A confident wrong answer costs me more than an admitted gap.

### ✅ Rule learned the hard way — 2026-08-22, migration 001

**Ship the new read path BEFORE the destructive step that removes the old one.**

During migration 001 I asked for `unset --commit` (delete `cv_pdf_b64`) and *then* wanted to confirm the CV viewer still worked against the new store. Claude refused and was right to: nothing read `application_files` yet, so `unset` would have 404'd the viewer for all 243 candidates, and the verification would have been testing a path that did not exist with the fallback already deleted.

The correct order is always:

1. Add the new read path, preferring the new store, **falling back** to the old one
2. Deploy and confirm — the read is now exercised against the new store with the old one still underneath
3. Only then run the destructive step
4. Confirm again, this time with no fallback available to mask a silent failure

Step 4 is the one that actually proves it. While a fallback exists, a broken new path looks identical to a working one.

Generalises past this migration: **a verification is meaningless if the thing it would fall back to has already been deleted.** If I ask for these in the wrong order, say so.

## 2. What TopCandidate.pro is

AI CV screening SaaS. Recruiter uploads a job description + batch of PDFs → GPT-4o scores each candidate on a 6-dimension weighted rubric → ranked, filterable list with detailed reports, pipeline stages, and candidate emails from inside the app.

Live: https://topcandidate.pro
Admin account: tarafdersakib08@gmail.com

**Customers: no longer just me.** ✅ LinkX360 and Data Solution 360 are mine. There is also one outside account — `rabeya.zaman@smartbd.com` (SmartBD), self-registered, 0 screenings as of 2026-08-22. Do not write copy claiming more than "early access."

## 3. Tech stack

- Backend: FastAPI (Python 3.12) + gunicorn/UvicornWorker
- DB: MongoDB Atlas M0 free tier, cluster `cluster0.eaqxhze.mongodb.net`, DB `talentscore`
- AI: OpenAI GPT-4o, `openai` Python SDK v1.30
- Frontend: Server-rendered HTML + vanilla JS, no framework. `index.html`, `batch.html`, `settings.html`
- Email: Gmail SMTP (recruiter's own Gmail, app password)
- Auth: JWT + bcrypt
- Payments: none. Fully manual. `payment_service.py` is a dead stub.

## 4. Deployment

Migrated Render → DigitalOcean Droplet on 2026-08-22.

- Droplet: $18/mo, 2GB RAM, 60GB SSD, 1 vCPU, Singapore (sgp1)
- IP: 178.128.58.53
- SSH: `ssh topcandidate@178.128.58.53` (sudo, SSH-key only)
- App: `/home/topcandidate/app/`, venv at `app/venv/`
- Service: `topcandidate.service`, 2 gunicorn workers → 127.0.0.1:8000, 300s timeout, journalctl logs
- Nginx: 80/443 → 8000, `proxy_buffering off` for SSE, 20MB upload cap
- SSL: Let's Encrypt, certbot auto-renew, covers apex + www
- DNS: DigitalOcean nameservers, set at Hostinger
- Render: suspended 2026-08-22, delete after 2026-08-24

`.env` (chmod 600): `OPENAI_API_KEY`, `MONGO_URI`, `DB_NAME=talentscore`, `SECRET_KEY`, `GMAIL_USER`, `GMAIL_APP_PASSWORD`, `APP_URL`. OPENAI_API_KEY and SECRET_KEY both rotated 2026-08-22.

### ✅ The Droplet venv is the compile checker — not Docker

**There is no Python on the Windows machine.** `C:\Users\LENOVO\anaconda3` does not exist; the PATH entries pointing at it are stale leftovers from an uninstall. Docker Desktop is installed but the daemon is not running, and starting it is not the fix — the Droplet already has the exact interpreter and dependency set that production runs.

Compile-check on the Droplet, before the restart, every time:

```bash
cd ~/app && git pull origin main && venv/bin/python -m py_compile main.py database.py batch.py scorer.py email_service.py && sudo systemctl restart topcandidate
```

If `py_compile` fails, nothing restarts and the old process keeps serving. That is the point of chaining it with `&&`.

```bash
sudo journalctl -u topcandidate -f
```

## 5. Repo

- GitHub: https://github.com/Data-Solution-360-3/talentscore-ai (main)
- Local: `C:\Users\LENOVO\Desktop\cv-screener-v5`

```
main.py           ~92KB  — endpoints, auth, batch, admin, email
scorer.py         ~53KB  — GPT pipeline (DO NOT TOUCH without discussion)
batch.py           ~5KB  — parallel orchestrator, CONCURRENCY_LIMIT=3
email_service.py  ~16KB  — Gmail SMTP, OTP, templates
database.py              — Motor async Mongo
auth.py                  — JWT + bcrypt
api_keys.py
payment_service.py       — dead stub, no payments exist
templates/index.html   ~174KB
templates/batch.html    ~46KB
templates/settings.html ~77KB
```

Read order for a new assistant: `scorer.py` → `main.py` → `templates/index.html` → `batch.py` → `email_service.py`.

## 6. Scorer v2 pipeline

Two-pass scoring with confidence gates:

1. Parse CV (GPT) → personal, work_experience, education, skills, certifications, total_years_experience
2. Parse JD (GPT, parallel) → required_skills, technologies, experience_years, education, certifications, role_title
3. Self-consistency validator (deterministic) — catches "Senior Manager, 1yr exp", triggers cheap retry
4. Tenure analysis (deterministic) — Stable / Moderate / Frequent switcher / Early career. Needs ≥2yr career AND ≥3 roles before flagging switcher
5. Two-pass scoring, parallel: STRICT_ANGLE @ temp 0.2, UPSIDE_ANGLE @ temp 0.3, blended 60/40
6. Hard-gap detection (deterministic) — −15 to −25 per gap, capped −35. Not auto-reject
7. Skill normalization — ~40 synonyms (Postgres↔PostgreSQL, k8s↔Kubernetes, etc.)
8. CV authenticity check (deterministic) — cliché density, sentence-length variance, bullet parallelism, typography → `ai_likelihood_pct` 0-100. Does not affect score. Pure signal
9. Confidence gate — strict/upside disagree >12pts (Low confidence) → downgrade STRONG HIRE/HIRE to MAYBE. REJECT never upgraded
10. Weighted recomputation — final = sum(dim × weight × 5) − penalty. Deterministic, not trusted from model

Dimensions (0-20 each): Skills Match, Experience Relevance, Education & Certifications, Achievement & Impact, Role Alignment, Stability & Tenure
Default weights: 25/20/10/20/15/10
Presets (8): balanced, sales, engineering_senior, engineering_junior, manager, operations, data_analytics, creative
Thresholds: ≥80 STRONG HIRE, ≥65 HIRE, ≥48 MAYBE, <48 REJECT
Cost: ~4 GPT-4o calls/CV ≈ $0.054 (৳6)

These are defined concepts — scoring dimensions, weights, hard gaps, authenticity, tenure. Don't reinvent them.

## 7. Stage vocabulary (canonical — decided 2026-08-22)

Four stages only: **Screening, Interview, Shortlisted, Rejected.**

- "Offer" — deleted. Was never in the model. ⚠️ Verify nobody is stranded in it.
- "Review needed" — a derived view, never a stored stage. Defined as `stage = screening AND no human action yet`.
- Dedup key: email from `parsed_cv.personal.email` if present, else `candidate_name + job_id`. Records with empty/null/`'unknown'` name are never merged — each stays distinct. Dedup is per job: one person applying to two roles is two candidacies.
- Canonical implementation: `dedupKey()` in `templates/index.html` (~line 2016). `/api/admin/diagnostics` mirrors it server-side. **If those two ever disagree on `distinct_candidates`, the definitions have drifted — that is the bug, fix it before trusting either number.**
- Dashboard shows both "AI recommends hire" and "You shortlisted" as separate, honestly-labelled numbers. The gap between them is the useful signal.

## 8. ✅ Security incident — 2026-08-22

**Five endpoints were reachable with no authentication at all.** Found and closed the same day (commit `82b446d`).

The worst was `POST /api/admin/fix-user-role`, which took an email and a role as form fields from anyone on the internet. Its docstring said "accessible without auth for emergency use." Any visitor could grant themselves `role: "admin"` and then read every tenant's screenings through the admin branch of `/api/screenings`.

`GET /api/admin/check-users` was verified leaking live — HTTP 200, no credentials, returning every user's email, company, and role.

**Deleted** (one-time migration hacks that had already done their job; `unassigned_screenings` is 0):

- `POST /api/admin/fix-user-role` — privilege escalation
- `POST /api/admin/assign-to-email/{email}` — bulk reassignment of records to any registered email
- `GET /api/fix-now` — hardcoded elevate + bulk reassign

Recovering a lost admin role is an Atlas console job now, not a public route.

**Guarded** with the new `require_admin()` helper (`main.py`, just after `get_current_user`), which reads role fresh from the DB instead of trusting the JWT:

- `GET /api/admin/check-users`
- `GET /api/admin/fix-counts`
- `GET /api/debug/my-screenings` — was authenticated but sampled `db.screenings.find({})` across **all** tenants for any logged-in user

**Every new admin endpoint goes through `require_admin()`.** The old pattern was an inline role check copy-pasted per endpoint, which is exactly how three of them ended up with no check at all.

### ⚠️ Blast radius is unknowable

There is **no audit log and no application-level access log.** `log_api_call()` in `api_keys.py` writes to `db.api_logs`, but only the `/api/v1/*` key-authenticated routes call it — none of the deleted endpoints did. The `users` collection stores `created_at` but there is no `last_login`, no `role_changed_at`, and no record of who changed a role.

Nginx access logs on the Droplet are the only remaining trail, and they are **rotated and time-limited** — they will not reach back to when these endpoints were first deployed. What survives is worth grepping:

```bash
sudo zgrep -hE "fix-now|fix-user-role|assign-to-email|check-users" /var/log/nginx/access.log*
```

Any line whose client IP is not mine is worth investigating. **A clean result does not mean it never happened** — it means it did not happen inside the retained window. We cannot tell.

### Standing follow-up

Deleting the escalation route stops *new* escalation. It does not demote anyone who already used it. **Re-check the admin list after any incident**, via `/api/admin/diagnostics` → `admins[]`, which returns `created_at` for each.

## 9. ✅ Answered — the old §9 blocking questions

**Is my account `role: "admin"`? Yes.** Confirmed live 2026-08-22. `tarafdersakib08@gmail.com` is admin. Every Dashboard number is therefore a correct **cross-tenant total**, not a per-tenant one. There is a second admin, `admin@talentscore.ai` (company "TalentScore AI", 0 screenings) — looks like an early seed account from before the rename, but **confirm you created it.**

**Screening ownership as of 2026-08-22** — 263 total, **0 unassigned**:

| account | all-time screenings |
|---|---|
| tarafdersakib08@gmail.com | 147 |
| sakib@datasolution360.com | 103 |
| admin@datasolution360.com | 13 |

Sums exactly to 263.

**The 139 "orphaned candidates" are job-orphaned, not user-orphaned.** They have a `user_id`; what they lack is a `job_id`. The §10 backfill plan was written against the wrong problem — re-scope it before running anything.

### ⚠️ Tenant isolation is NOT fixed — and no longer hypothetical

Still a proposal. Earlier briefs claimed "tenant isolation on all endpoints"; that claim is false. It was previously described as harmless "until a third company signs up." **That already happened** — `rabeya.zaman@smartbd.com` is a real outside account. She has 0 screenings, so nothing of hers has leaked. The moment she screens a CV, an admin dashboard shows it. Fix before that happens.

### ✅ PR #1 is merged

`fe9926f`, merging `fix/data-integrity-and-landing-honesty`. Stage 2 is on `main`. The old "no PR was created" note was stale.

## 10. Open — planned, not applied

- **Backfill for the 139 job-orphaned candidates** — plan needs re-scoping (see §9). Must be reversible. Take the Droplet snapshot first.
- **Merge duplicate job documents** — two "Data Analyst", two "Full-Stack AI Engineer". Genuinely separate docs (`save_job` was a bare `insert_one`). New inserts now blocked by 409; existing duplicates still need merging. `save_job`'s check is read-then-write, not a DB constraint — a unique partial index on (user_id, lowercased title, active) is the airtight fix.
- ✅ **`/api/screen`'s 400 is deliberate.** It raises `400 "Single CV screening is disabled. Please use batch screening at /batch."` on its first line, with dead code kept below and a comment saying so. **The UI markup for it is already gone** — no element with `id="screen-btn"`, `screen-input`, `screen-result`, or `jd` exists in `index.html`. `runScreen()` at index.html:1223 and its helpers are orphaned JS that nothing can trigger; every `getElementById` in them returns null. There is no broken button. Cleanup only, no user impact.
- **"5 process in parallel" is false in exactly 4 places** — `CONCURRENCY_LIMIT = 3`. Fix the copy or the constant, not both blindly: `templates/admin.html:786`, `templates/batch.html:152`, `templates/landing.html:338`, `templates/landing.html:481`.
- **Stage 3** — not started: remove "TIME SAVED 35h" card (`index.html:631`, invented multiplier, same species as the deleted 98% claim); RUN 1 / RUN 2 / JD USED audit columns are empty on every row — populate from the two-pass scorer or drop them; "(no job linked)" row is 139 candidates.
- **Stage 4** — not started: 9 filter controls in one row → "More filters" disclosure; MISSING SKILLS column is the heaviest thing on the page and pushes Education off-screen; drop View buttons, make rows clickable; 3 action buttons per row → overflow menu.

### ⚠️ `screening_count` — narrower than it first looked

An earlier session reported this as broken billing ("mine says 9, actual 147"). **That comparison was wrong**, and the mislabeling is in `/api/admin/check-users`: `screening_count` is a **monthly** counter that resets via `month_reset_at`, while that endpoint's `actual_screening_count` is an **all-time** `count_documents`. Two different questions, printed side by side as if one were the other. `/api/admin/diagnostics` reports `screenings_this_month` and `screenings_all_time` separately.

Quota enforcement is also **not** reading the stale field: `/api/batch/screen` calls `sync_screening_count()` immediately before the limit check (main.py ~line 604), so the number it enforces on is recomputed from actual records every time. It is self-healing at the exact moment it matters.

**The one real bug left** is in `sync_screening_count()` (database.py ~line 322): it matches `{"user_id": user_id}` by plain equality instead of using `user_match(user_id)`, the `$or` helper built for exactly this. Any screening whose `user_id` was stored as a non-string is invisible to it, so the month count runs low and the user gets free quota. Proposed fix:

```python
count = await db.screenings.count_documents({
    **user_match(user_id),
    "created_at": {"$gte": month_start},
})
```

No recount migration is needed — `sync_screening_count()` overwrites the stored value before every batch. **Not yet applied.**

## 10b. ✅ Migration 001 — CV PDFs out of screening docs (2026-08-22, complete)

`cv_pdf_b64` held the whole CV base64-encoded inside every screening. Moved to an `application_files` collection as raw BSON binData. Script: `migrations/001_pdfs_to_application_files.py` (`status` / `copy` / `verify` / `unset` / `rollback`, defaults to `--dry-run`, idempotent).

Ran `copy --commit` → `verify` (243/243 byte-and-sha identical) → `unset --commit`. Read path shipped first as `e86499c`.

**Result — logical data:**

| | before | after |
|---|---|---|
| screenings, avg document | 260.1KB | **8.2KB** |
| screenings, logical size | 66.8MB | **2.1MB** |
| base64 overhead reclaimed | — | 16.2MB |

8.2KB per screening beat the 13KB estimate. Capacity for new screenings is roughly **60,000**, not the ~2,000 the old shape allowed.

### ⚠️ On-disk is NOT the same number — read this before assuming there's headroom

`collStats.size` is logical data. `storageSize` is what is actually allocated on disk, and **WiredTiger does not return freed space** — it reuses it internally for new writes.

Measured immediately after `unset`:

```
screenings         logical=2.1MB   onDisk=110.9MB
application_files  logical=48.6MB  onDisk=93.0MB
DB TOTAL           dataSize=50.8MB storageSize=204.1MB
```

So the database is holding **204MB on disk** while containing 51MB of data. Atlas M0's 512MB cap is measured against disk usage, so the honest figure is ~204/512MB, not ~51/512MB. **Check the Atlas Metrics tab, not the logical numbers, before assuming room.** The 110.9MB behind `screenings` is free for reuse and will absorb thousands of new screenings without growing — it just will not show as recovered. `compact` is not available on M0.

When the TTL fires, `application_files` logical drops to ~0 and its 93MB on-disk stays allocated and reusable.

### Retention state

- 243 files carry `expires_at` = 2026-09-21 (30 days from migration). **All expire on the same day** — dating from `created_at` would have expired nearly all of them immediately, since most screenings are older than 30 days. New applications get proper rolling expiry.
- **The TTL index does not exist yet.** Until it is created in `connect()`, nothing expires. Creating it starts the clock for real.
- 20 of the 263 screenings never had a PDF (they predate PDF storage or came through the single-screen path). Their CV viewer was already empty.

### Backup reality

A **Droplet snapshot contains none of this** — the droplet runs the app, the data is in Atlas. **Atlas M0 has no automated backups**; continuous backup starts at M10. The only copy of the pre-migration state is the 46MB `mongodump` archive taken 2026-08-22, held in two locations off the droplet. Once the TTL fires, that archive is the only copy of the 243 PDFs that exists anywhere.

```bash
mongodump --uri="$MONGO_URI" --db=talentscore --gzip --archive=$HOME/talentscore-$(date +%F).gz
```

Worth doing on a schedule regardless of this feature.

## 11. Ops TODO

- [x] Droplet snapshot taken, and a 46MB mongodump archived in two locations off-droplet (2026-08-22)
- [ ] Put the mongodump on a schedule — Atlas M0 has no backups of its own
- [ ] Watch Atlas storageSize (204MB/512MB), not dataSize — see §10b
- [ ] Tighten Atlas IP whitelist to `178.128.58.53/32` if it's currently `0.0.0.0/0`
- [ ] Grep nginx logs for the deleted endpoints (§8), then confirm the admin list
- [ ] Confirm `admin@talentscore.ai` is an account you created
- [ ] Delete Render web service after 2026-08-24
- [ ] Batch load test: 10 CVs, watch memory, only then consider `CONCURRENCY_LIMIT` 3 → 5
- [ ] Recolor `candidate.html` + `admin.html` to light theme (files not yet uploaded)
- [ ] Delete `payment_service.py` stub

## 12. UI conventions

Palette: navy `#142848` / indigo `#1E3A5F`, orange `#F57C2E` / `#E16A1F`, bg `#FAFBFC`, surfaces `#FFFFFF`, text `#142848` / `#4A5970` / `#8593AB`. Semantic: green `#16A34A`, red `#DC2626`, amber `#D97706`, blue `#2563EB`. Score colors (softer): green `#15803d`, amber `#B45309`, dusty rose `#BE3144`.

Tokens in every `:root`: `--r-xs/sm/md/lg/xl/pill`, `--shadow-sm/md/lg/xl`.

Reuse these helpers — don't invent new components: `emptyState()`, `emptyStateRow()`, `toast()`, `confirmDialog()`, `promptDialog()`, `icon()`, `scoreRing()`, `sortableHeader()`, `applySort()`, `applyPagination()`, `paginationFooter()`, `skeleton*()`, `openModal()` / `closeModal()`, `CandidateStats`.

Backend: `require_admin(request)` for every admin endpoint. `user_match(user_id)` for every tenant-scoped query.

## 13. Deploy workflow

```bash
# Windows
cd C:\Users\LENOVO\Desktop\cv-screener-v5
git add <files>
git commit -m "feat: <short message>"
git push origin main
```

```bash
# Droplet — compile-check is chained, so a syntax error never restarts the service
ssh topcandidate@178.128.58.53
cd ~/app && git pull origin main && venv/bin/python -m py_compile main.py database.py batch.py && sudo systemctl restart topcandidate
sudo journalctl -u topcandidate -f   # Ctrl+C when clean
```

Test at https://topcandidate.pro with Ctrl+Shift+R.

## 14. Do not build

Mobile app, video interviews, candidate chatbot, jobs board, LinkedIn scraping.

## 15. Worth its own session

Score distribution looks compressed: avg 44, shortlist rate 8%, pipeline is wall-to-wall MAYBE, and a 0-years-experience candidate scored HIRE at 72 for a Data Analyst role. That's rubric calibration, not UI. Don't touch it while data integrity is still open.

---
END OF BRIEF
