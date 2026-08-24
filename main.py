from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request, Response, Depends, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pathlib import Path
import uvicorn
import asyncio
import json
import os
import base64

from scorer import extract_pdf_text, run_screening_pipeline
from api_keys import (
    create_api_key, get_keys_for_user, validate_api_key,
    revoke_api_key, increment_api_usage, check_rate_limit,
    log_api_call, API_PLAN_LIMITS
)
from payment_service import (
    create_stripe_checkout, verify_stripe_webhook,
    create_sslcommerz_payment, verify_sslcommerz_payment,
    cancel_stripe_subscription, create_stripe_portal_session,
    STRIPE_PUBLISHABLE_KEY, PLANS
)
from email_service import (
    generate_otp, send_verification_email, send_welcome_email,
    send_candidate_email, substitute_template, DEFAULT_TEMPLATES, TEMPLATE_VARIABLES,
)
from batch import run_batch_screening, CONCURRENCY_LIMIT
from auth import (
    hash_password, verify_password, create_token,
    get_token_from_request, decode_token
)
from database import (
    connect, disconnect,
    save_screening, get_all_screenings, get_screening_by_id,
    get_screening_stats, get_skills_gap_frequency, get_dimension_averages,
    delete_screening,
    save_job, get_all_jobs, delete_job,
    create_batch_job, update_batch_progress, finish_batch_job,
    get_batch_job, get_all_batch_jobs,
    create_user, get_user_by_email, get_user_by_id, get_all_users,
    update_user, increment_screening_count, sync_screening_count,
    store_otp, verify_otp, delete_pending,
    get_screenings_for_user, get_stats_for_user, get_jobs_for_user,
    count_screenings_for_user, DuplicateJobError,
    get_skills_gaps_for_user, get_dimension_averages_for_user,
    save_payment, get_payments_for_user, update_user_subscription,
    invite_team_member, get_team_members, get_team_invites,
    update_user_profile, update_user_notifications, get_full_user,
    generate_public_token, hash_ip, get_job_by_public_token, set_job_public,
    rotate_job_token, ensure_job_token, user_match_field, reserve_screening_slot,
    release_screening_slot, get_spend_state, rate_limit_allows,
    upsert_application, store_application_pdf, count_pending_applications,
    get_applications_for_job, user_match,
    MAX_APPLICATION_PDF_BYTES, APPLICATION_PDF_RETENTION_DAYS,
    CAP_PER_JOB, CAP_PER_DAY, CAP_PER_MONTH,
)

import database as _database


class _LiveDB:
    """Resolves database.db at attribute access, not at import.

    database.py starts with `db = None` and `connect()` rebinds that module
    global at startup. `from database import db` therefore captures None
    permanently — the importing module never sees the rebind. Every bare
    `db.collection` in this file was silently dead, failing at call time with
    `AttributeError: 'NoneType' object has no attribute 'jobs'`, and only when
    somebody actually clicked the endpoint.

    The endpoints that worked did `from database import db` INSIDE the function
    body, which re-reads the module attribute per call. This does the same thing
    once, for every call site, so a bare `db.` use can never be dead again.

    If it is reached before connect(), it says so instead of surfacing as a
    NoneType attribute error three frames away from the real cause.
    """

    def __getattr__(self, name):
        live = _database.db
        if live is None:
            raise RuntimeError(
                "Database handle requested before connect() — check the lifespan startup order."
            )
        return getattr(live, name)


db = _LiveDB()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
APP_URL        = os.getenv("APP_URL", "https://topcandidate.pro")


# ─────────────────────────────────────────────────────────────
# Standalone admin UI for manual payment review.
# Served at /admin/payments. Self-contained HTML — doesn't depend
# on admin.html being updated. Admins can bookmark this URL to
# review pending payment submissions and approve them.
# ─────────────────────────────────────────────────────────────
MANUAL_PAYMENTS_ADMIN_HTML = """<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>Manual Payments · Admin</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',system-ui,-apple-system,sans-serif;background:#07080f;color:#eef0f8;padding:2rem;min-height:100vh}
.wrap{max-width:1200px;margin:0 auto}
.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:2rem}
h1{font-size:22px;font-weight:800;letter-spacing:-.4px}
.back{color:#8b95b0;text-decoration:none;font-size:13px;padding:8px 14px;border:1px solid #252b3b;border-radius:8px;background:#171b26}
.back:hover{background:#1e2333;color:#eef0f8}
.card{background:#0f1117;border:1px solid #1e2333;border-radius:12px;padding:1.5rem;margin-bottom:1rem}
.card h3{font-size:14px;font-weight:700;margin-bottom:1rem;color:#a5b4fc}
.tbl{width:100%;border-collapse:collapse;font-size:13px}
.tbl th{text-align:left;padding:10px 12px;font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.07em;color:#4a5270;border-bottom:1px solid #1e2333;background:#171b26}
.tbl td{padding:12px;border-bottom:1px solid #1e2333;color:#8b95b0}
.tbl tr:last-child td{border-bottom:none}
.tbl tr:hover{background:#0a0b12}
.tid{font-family:monospace;font-size:11px;color:#a5b4fc;background:#171b26;padding:2px 8px;border-radius:4px}
.badge{font-size:11px;font-weight:600;padding:3px 10px;border-radius:20px;border:1px solid;display:inline-block}
.b-pending{background:rgba(245,158,11,.1);color:#f59e0b;border-color:rgba(245,158,11,.3)}
.b-approved{background:rgba(16,185,129,.1);color:#10b981;border-color:rgba(16,185,129,.3)}
.b-rejected{background:rgba(244,63,94,.1);color:#fda4af;border-color:rgba(244,63,94,.3)}
.btn{padding:6px 12px;font-size:12px;font-weight:600;border-radius:6px;border:none;cursor:pointer;font-family:inherit;margin-right:4px}
.btn-ok{background:#10b981;color:#fff}.btn-ok:hover{background:#0ea672}
.btn-bad{background:rgba(244,63,94,.15);color:#fda4af;border:1px solid rgba(244,63,94,.3)}.btn-bad:hover{background:rgba(244,63,94,.25)}
.empty{text-align:center;padding:3rem;color:#4a5270}
.sel{background:#171b26;border:1px solid #252b3b;color:#eef0f8;border-radius:6px;padding:5px 8px;font-size:12px;font-family:inherit}
.note{font-size:11px;color:#4a5270;max-width:200px;overflow-wrap:break-word}
</style></head><body>
<div class="wrap">
 <div class="top">
  <h1>💳 Manual Payment Submissions</h1>
  <a class="back" href="/admin">← Admin home</a>
 </div>
 <div class="card">
  <h3 style="color:#f59e0b">Pending review</h3>
  <table class="tbl"><thead><tr>
   <th>Submitted</th><th>Company</th><th>Email</th><th>Plan</th><th>Method</th>
   <th>Transaction ID</th><th>Amount</th><th>Note</th><th>Actions</th>
  </tr></thead><tbody id="pending-tbody"><tr><td colspan="9" class="empty">Loading…</td></tr></tbody></table>
 </div>
 <div class="card">
  <h3 style="color:#10b981">Approved history</h3>
  <table class="tbl"><thead><tr>
   <th>Approved</th><th>Company</th><th>Email</th><th>Plan</th><th>Method</th>
   <th>Transaction ID</th><th>Amount</th><th>Approved by</th>
  </tr></thead><tbody id="approved-tbody"><tr><td colspan="8" class="empty">Loading…</td></tr></tbody></table>
 </div>
</div>
<script>
async function load(){
 const res = await fetch('/api/admin/manual-payments',{credentials:'include'});
 if(res.status===401){window.location.href='/login';return;}
 if(res.status===403){document.body.innerHTML='<div style=\"padding:2rem;color:#fda4af\">Admin only.</div>';return;}
 const data = await res.json();
 const payments = data.payments || [];
 const pending  = payments.filter(p=>p.status==='pending_review');
 const approved = payments.filter(p=>p.status==='approved');

 const fmt = d => d ? new Date(d).toLocaleString('en-GB',{day:'numeric',month:'short',year:'numeric',hour:'2-digit',minute:'2-digit'}) : '—';
 const esc = s => String(s||'').replace(/[&<>\"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[c]));

 document.getElementById('pending-tbody').innerHTML = pending.length ? pending.map(p=>`
  <tr>
   <td>${fmt(p.created_at)}</td>
   <td style=\"color:#eef0f8;font-weight:600\">${esc(p.company)}</td>
   <td>${esc(p.email)}</td>
   <td><span class=\"badge b-pending\">${esc(p.plan_id)}</span></td>
   <td>${esc(p.payment_method)}</td>
   <td><span class=\"tid\">${esc(p.transaction_id)}</span></td>
   <td style=\"color:#eef0f8;font-weight:600\">${esc(p.amount)}</td>
   <td><div class=\"note\">${esc(p.note)||'—'}</div></td>
   <td>
    <select class=\"sel\" id=\"plan-${p._id}\">
      <option value=\"starter\" ${p.plan_id==='starter'?'selected':''}>Starter</option>
      <option value=\"pro\" ${p.plan_id==='pro'?'selected':''}>Pro</option>
      <option value=\"enterprise\" ${p.plan_id==='enterprise'?'selected':''}>Enterprise</option>
    </select>
    <button class=\"btn btn-ok\" onclick=\"approve('${p._id}')\">Approve</button>
    <button class=\"btn btn-bad\" onclick=\"reject('${p._id}')\">Reject</button>
   </td>
  </tr>`).join('') : '<tr><td colspan=\"9\" class=\"empty\">No pending submissions 🎉</td></tr>';

 document.getElementById('approved-tbody').innerHTML = approved.length ? approved.map(p=>`
  <tr>
   <td>${fmt(p.approved_at||p.created_at)}</td>
   <td style=\"color:#eef0f8;font-weight:600\">${esc(p.company)}</td>
   <td>${esc(p.email)}</td>
   <td><span class=\"badge b-approved\">${esc(p.plan_id)}</span></td>
   <td>${esc(p.payment_method)}</td>
   <td><span class=\"tid\">${esc(p.transaction_id)}</span></td>
   <td style=\"color:#eef0f8;font-weight:600\">${esc(p.amount)}</td>
   <td>${esc(p.approved_by)||'—'}</td>
  </tr>`).join('') : '<tr><td colspan=\"8\" class=\"empty\">No approved payments yet.</td></tr>';
}
async function approve(id){
 const plan = document.getElementById('plan-'+id).value;
 if(!confirm('Approve this payment and upgrade user to '+plan+'?'))return;
 const fd = new FormData(); fd.append('plan', plan);
 const r = await fetch('/api/admin/manual-payments/'+id+'/approve',{method:'POST',body:fd,credentials:'include'});
 const d = await r.json();
 if(r.ok){alert('✓ '+(d.message||'Approved'));load();}
 else alert('Error: '+(d.detail||'Failed'));
}
async function reject(id){
 if(!confirm('Reject this payment submission? User will NOT be upgraded.'))return;
 const fd = new FormData(); fd.append('status','rejected');
 const r = await fetch('/api/admin/manual-payments/'+id+'/reject',{method:'POST',body:fd,credentials:'include'});
 if(r.ok){alert('Rejected.');load();}
 else{const d = await r.json().catch(()=>({})); alert('Error: '+(d.detail||'Failed'));}
}
load();
</script>
</body></html>"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect()
    from database import db as mongodb
    from bson import ObjectId

    # The default-admin seed (admin@talentscore.ai / Admin@123) was removed.
    # It stood up a live admin with a guessable, hardcoded password, and because
    # it was `if not admin: create`, it would silently resurrect the account with
    # the same password on the next restart if the account were ever deleted.
    # The account has been removed from the database. Do NOT reintroduce any
    # seeded credential here — an admin is made via the admin-only create-user
    # endpoint or by promoting an existing (self-registered) account.

    # Keep tarafdersakib08@gmail.com admin (role only, no credential) — the
    # owner's lockout safety-net, so a role reset can never lock them out.
    sakib = await get_user_by_email("tarafdersakib08@gmail.com")
    if sakib:
        await mongodb.users.update_one(
            {"_id": ObjectId(sakib["_id"])},
            {"$set": {"role": "admin"}}
        )
        print(f"[AUTH] tarafdersakib08@gmail.com → admin")

    yield
    await disconnect()


app = FastAPI(title="TopCandidate", version="5.0.0", lifespan=lifespan)

# Always return JSON for API errors, never HTML
from fastapi import Request as FastAPIRequest
from fastapi.responses import JSONResponse as FJSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: FastAPIRequest, exc: StarletteHTTPException):
    if request.url.path.startswith("/api/"):
        return FJSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail, "status": exc.status_code}
        )
    # For non-API routes, redirect 401 to login
    if exc.status_code == 401:
        return RedirectResponse("/login")
    return FJSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")


# ─────────────────────────────────────────────────────────────
# AUTH HELPER
# ─────────────────────────────────────────────────────────────

async def get_current_user(request: Request) -> dict:
    token = get_token_from_request(request)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    payload = decode_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired session")
    return payload


async def require_admin(request: Request) -> dict:
    """Authenticate the caller, then confirm they are an admin.

    Role is read fresh from the DB rather than trusted from the JWT, so a
    demoted account loses admin the moment it is demoted instead of when its
    token expires. Every admin-only endpoint goes through here — the previous
    per-endpoint copies are how three of them ended up with no check at all.
    """
    user = await get_current_user(request)
    db_user = await get_user_by_id(user["user_id"])
    if not db_user or db_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    return user


# ─────────────────────────────────────────────────────────────
# PAGES
# ─────────────────────────────────────────────────────────────

def read_template(name: str) -> str:
    path = Path(__file__).parent / "templates" / name
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


@app.get("/", response_class=HTMLResponse)
@app.get("/landing", response_class=HTMLResponse)
async def landing_page():
    return read_template("landing.html")


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    token = get_token_from_request(request)
    if token and decode_token(token):
        return RedirectResponse("/app")
    return read_template("login.html")


@app.get("/app", response_class=HTMLResponse)
async def home(request: Request):
    token = get_token_from_request(request)
    if not token or not decode_token(token):
        return RedirectResponse("/login")
    return read_template("index.html")


@app.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    token = get_token_from_request(request)
    if not token or not decode_token(token):
        return RedirectResponse("/login")
    payload = decode_token(token)
    # Always check DB for latest role
    db_user = await get_user_by_id(payload.get("user_id", ""))
    if not db_user or db_user.get("role") != "admin":
        return RedirectResponse("/app")
    return read_template("admin.html")


@app.get("/admin/payments", response_class=HTMLResponse)
async def admin_payments_page(request: Request):
    """Standalone admin page for reviewing manual payment submissions.
    Not part of admin.html yet — serves inline so admins can access it right away."""
    token = get_token_from_request(request)
    if not token or not decode_token(token):
        return RedirectResponse("/login")
    payload = decode_token(token)
    db_user = await get_user_by_id(payload.get("user_id", ""))
    if not db_user or db_user.get("role") != "admin":
        return RedirectResponse("/app")
    return HTMLResponse(MANUAL_PAYMENTS_ADMIN_HTML)


@app.get("/batch", response_class=HTMLResponse)
async def batch_page(request: Request):
    token = get_token_from_request(request)
    if not token or not decode_token(token):
        return RedirectResponse("/login")
    return read_template("batch.html")


@app.get("/candidate", response_class=HTMLResponse)
async def candidate_page(request: Request):
    token = get_token_from_request(request)
    if not token or not decode_token(token):
        return RedirectResponse("/login")
    return read_template("candidate.html")


# ─────────────────────────────────────────────────────────────
# AUTH ENDPOINTS
# ─────────────────────────────────────────────────────────────

@app.post("/api/auth/register")
async def register(
    email: str = Form(...),
    password: str = Form(...),
    company_name: str = Form(...),
):
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters.")
    # Check if email already registered
    existing = await get_user_by_email(email)
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered. Please sign in.")
    # Generate OTP and store pending registration
    otp = generate_otp()
    await store_otp(
        email=email,
        otp=otp,
        company_name=company_name,
        password_hash=hash_password(password)
    )
    # Send verification email
    sent = send_verification_email(to_email=email, company_name=company_name, otp=otp)
    if not sent:
        raise HTTPException(status_code=500, detail="Failed to send verification email. Please try again.")
    return JSONResponse({"success": True, "message": "Verification code sent to your email."})


@app.post("/api/auth/verify")
async def verify_email(
    email: str = Form(...),
    otp: str = Form(...),
):
    pending = await verify_otp(email=email, otp=otp)
    if not pending:
        raise HTTPException(status_code=400, detail="Invalid or expired verification code. Please try again.")
    # Create the user account
    try:
        user_id = await create_user(
            email=email,
            hashed_password=pending["password_hash"],
            company_name=pending["company_name"],
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Send welcome email
    send_welcome_email(to_email=email, company_name=pending["company_name"])

    # Fetch user to get correct role from DB
    new_user = await get_user_by_email(email)
    token = create_token({
        "user_id": user_id,
        "email": email.lower(),
        "company": pending["company_name"],
        "role": new_user.get("role", "client") if new_user else "client"
    })
    resp = JSONResponse({"success": True, "company": pending["company_name"]})
    resp.set_cookie("access_token", token, httponly=True, max_age=30*24*3600, samesite="lax")
    return resp


@app.post("/api/auth/resend-otp")
async def resend_otp(email: str = Form(...)):
    # Check pending registration exists
    from database import db
    pending = await db.pending_registrations.find_one({"email": email.lower()})
    if not pending:
        raise HTTPException(status_code=400, detail="No pending registration found. Please register again.")
    otp = generate_otp()
    await store_otp(
        email=email,
        otp=otp,
        company_name=pending["company_name"],
        password_hash=pending["password_hash"]
    )
    send_verification_email(to_email=email, company_name=pending["company_name"], otp=otp)
    return JSONResponse({"success": True})


@app.post("/api/auth/login")
async def login(
    email: str = Form(...),
    password: str = Form(...),
):
    user = await get_user_by_email(email)
    if not user or not verify_password(password, user["password"]):
        raise HTTPException(status_code=401, detail="Invalid email or password.")
    if not user.get("active", True):
        raise HTTPException(status_code=403, detail="Account suspended. Contact support.")

    # Always use fresh role from DB (not cached value)
    token = create_token({
        "user_id": user["_id"],
        "email": user["email"],
        "company": user.get("company_name", ""),
        "role": user.get("role", "client"),
    })
    resp = JSONResponse({"success": True, "company": user["company_name"]})
    resp.set_cookie("access_token", token, httponly=True, max_age=30*24*3600, samesite="lax")
    return resp


@app.post("/api/auth/logout")
async def logout():
    resp = JSONResponse({"success": True})
    resp.delete_cookie("access_token")
    return resp


@app.get("/api/auth/me")
async def me(request: Request):
    user = await get_current_user(request)
    # Always fetch fresh data from DB to get latest role/plan
    db_user = await get_user_by_id(user["user_id"])
    if db_user:
        return {
            "user_id": user["user_id"],
            "email": db_user.get("email", user["email"]),
            "company": db_user.get("company_name", user.get("company", "")),
            "role": db_user.get("role", "client"),
            "plan": db_user.get("plan", "trial"),
            "screening_count": db_user.get("screening_count", 0),
            "full_name": db_user.get("full_name", ""),
            "phone": db_user.get("phone", ""),
            "notification_prefs": db_user.get("notification_prefs", {}),
        }
    return {
        "user_id": user["user_id"],
        "email": user["email"],
        "company": user.get("company", ""),
        "role": user.get("role", "client"),
        "plan": "trial",
        "screening_count": 0,
    }


# ─────────────────────────────────────────────────────────────
# SINGLE CV SCREENING (no API key needed — uses server key)
# ─────────────────────────────────────────────────────────────

@app.post("/api/screen")
async def screen_endpoint(
    request: Request,
    cv_file: UploadFile = File(...),
    job_description: str = Form(...),
    job_id: str = Form(""),
    job_title: str = Form(""),
):
    raise HTTPException(status_code=400, detail="Single CV screening is disabled. Please use batch screening at /batch.")
    # Dead code below kept for reference
    user = await get_current_user(request)

    if not cv_file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted.")
    if len(job_description.strip()) < 50:
        raise HTTPException(status_code=400, detail="Job description too short.")
    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OpenAI API key not configured on server.")

    file_bytes = await cv_file.read()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    cv_text, error = extract_pdf_text(file_bytes)
    if error:
        raise HTTPException(status_code=422, detail=error)

    # Look up the job's custom weights (if any) — same flow as batch endpoint
    job_weights = None
    if job_id:
        try:
            from database import db as mongodb
            from bson import ObjectId
            job_doc = await mongodb.jobs.find_one({"_id": ObjectId(job_id)}, {"weights": 1})
            if job_doc and isinstance(job_doc.get("weights"), dict):
                job_weights = job_doc["weights"]
        except Exception:
            pass

    result, error = await run_screening_pipeline(
        cv_text=cv_text,
        jd_text=job_description.strip(),
        api_key=OPENAI_API_KEY,
        weights=job_weights,
    )
    if error:
        raise HTTPException(status_code=500, detail=error)

    # Tag with user/company/job
    result["user_id"] = user["user_id"]
    result["company"] = user["company"]
    if job_id:    result["job_id"]    = job_id
    if job_title: result["job_title"] = job_title

    # Store PDF
    result["cv_pdf_b64"] = base64.b64encode(file_bytes).decode("utf-8")
    result["cv_filename"] = cv_file.filename

    doc_id = await save_screening(result)
    result["_id"] = doc_id
    result.pop("cv_pdf_b64", None)

    await increment_screening_count(user["user_id"])
    return result


# ─────────────────────────────────────────────────────────────
# BATCH SCREENING
# ─────────────────────────────────────────────────────────────

@app.post("/api/batch/screen")
async def batch_screen_endpoint(
    request: Request,
    cv_files: list[UploadFile] = File(...),
    job_description: str = Form(...),
    job_id: str = Form(""),
    job_title: str = Form(""),
):
    user = await get_current_user(request)

    # A batch with no job_id produces candidates that belong to no role — they drop out
    # of every per-job view and pile up in Analytics as "(no job linked)". The batch UI
    # has always labelled this field required; nothing enforced it until now.
    if not job_id.strip():
        raise HTTPException(
            status_code=400,
            detail="Select a job posting before screening. Candidates must be linked to a role."
        )

    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OpenAI API key not configured on server.")
    if len(job_description.strip()) < 50:
        raise HTTPException(status_code=400, detail="Job description too short.")
    if not cv_files:
        raise HTTPException(status_code=400, detail="No files uploaded.")
    if len(cv_files) > 100:
        raise HTTPException(status_code=400, detail="Max 100 CVs per batch.")

    # ── ENFORCE PLAN LIMITS ──
    db_user = await get_user_by_id(user["user_id"])
    plan = db_user.get("plan", "trial") if db_user else "trial"
    plan_limits       = {"trial": 10, "starter": 100, "pro": 500, "enterprise": 999999}
    plan_batch_limits = {"trial": 10, "starter": 20,  "pro": 100, "enterprise": 100}
    monthly_limit = plan_limits.get(plan, 10)
    batch_limit   = plan_batch_limits.get(plan, 10)

    # Per-plan batch-size ceiling (was only capped at global 100 before)
    if len(cv_files) > batch_limit:
        raise HTTPException(
            status_code=429,
            detail=f"{plan.capitalize()} plan allows {batch_limit} CVs per batch. You uploaded {len(cv_files)}. Upgrade for larger batches."
        )

    # Re-sync count from actual DB records — never trust the cached field.
    # This is self-healing: if screening_count drifts from reality, this fixes it.
    current_count = await sync_screening_count(user["user_id"])
    print(f"[BATCH-LIMIT] user={user['user_id']} plan={plan} count={current_count} limit={monthly_limit} uploading={len(cv_files)}")

    if current_count >= monthly_limit:
        raise HTTPException(
            status_code=429,
            detail=f"Monthly limit reached ({current_count}/{monthly_limit} on {plan.capitalize()} plan). Upgrade at topcandidate.pro/settings"
        )
    remaining = monthly_limit - current_count
    if len(cv_files) > remaining:
        raise HTTPException(
            status_code=429,
            detail=f"Only {remaining} screening(s) left this month on {plan.capitalize()} plan. You uploaded {len(cv_files)} CVs. Upgrade your plan."
        )

    files = []
    for f in cv_files:
        if not f.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail=f"{f.filename} is not a PDF.")
        file_bytes = await f.read()
        if file_bytes:
            files.append((f.filename, file_bytes))

    if not files:
        raise HTTPException(status_code=400, detail="All uploaded files were empty.")

    jd_text  = job_description.strip()
    user_id  = user["user_id"]
    company  = user["company"]
    batch_id = await create_batch_job(total=len(files), jd_preview=jd_text[:200])
    queue    = asyncio.Queue()

    # ── Look up per-job custom weights (if any) ──
    # Without job_id, fall through to scorer defaults. Same path for any job that
    # hasn't had weights set — preserves backward compat for existing data.
    job_weights = None
    if job_id:
        try:
            from database import db as mongodb
            from bson import ObjectId
            job_doc = await mongodb.jobs.find_one({"_id": ObjectId(job_id)}, {"weights": 1})
            if job_doc and isinstance(job_doc.get("weights"), dict):
                job_weights = job_doc["weights"]
                print(f"[BATCH-WEIGHTS] using custom weights for job {job_id}: {job_weights}")
        except Exception as e:
            print(f"[BATCH-WEIGHTS] failed to load weights for job {job_id}: {e}")

    async def on_progress(index, status, filename, result, error=None):
        score = result.get("overall_score") if result else None
        rec   = result.get("recommendation") if result else None
        event = {
            "type": "result" if (result and status == "done") else "progress",
            "index": index, "filename": filename, "status": status,
            "score": round(score) if score else None,
            "recommendation": rec, "error": error, "batch_id": batch_id,
        }
        if result and status == "done":
            event["result"] = {k: v for k, v in result.items() if k not in ("parsed_cv", "parsed_jd", "cv_pdf_b64")}
        await queue.put(event)
        await update_batch_progress(batch_id=batch_id, index=index, status=status,
                                     filename=filename, score=round(score) if score else None,
                                     recommendation=rec, error=error)

    async def event_generator():
        yield f"data: {json.dumps({'type':'start','batch_id':batch_id,'total':len(files),'concurrency':CONCURRENCY_LIMIT})}\n\n"

        async def run_with_user_tag():
            # Build extra fields to save WITH each screening
            extra = {
                "user_id": user_id,
                "company": company,
            }
            if job_id:    extra["job_id"]    = job_id
            if job_title: extra["job_title"] = job_title

            # One JD parse for the whole batch instead of one per CV — so every
            # candidate in this run is measured against an identical rubric.
            jd_req = await resolve_jd_requirements(job_id, jd_text)
            results = await run_batch_screening(
                files=files, jd_text=jd_text,
                api_key=OPENAI_API_KEY, on_progress=on_progress,
                extra_fields=extra,
                weights=job_weights,
                jd_requirements=jd_req,
            )
            # Also tag results in memory for the response
            for r in results.get("results", []):
                r["user_id"] = user_id
                r["company"] = company
                if job_id:    r["job_id"]    = job_id
                if job_title: r["job_title"] = job_title
            return results

        batch_task = asyncio.create_task(run_with_user_tag())

        completed = 0
        while completed < len(files):
            # If the batch task crashed, stop waiting on the queue — bail immediately.
            if batch_task.done() and batch_task.exception() is not None:
                err = batch_task.exception()
                import traceback
                print(f"[BATCH-ERROR] Task crashed: {err}")
                traceback.print_exception(type(err), err, err.__traceback__)
                yield f"data: {json.dumps({'type':'error','message':f'Batch processing failed: {err}','batch_id':batch_id})}\n\n"
                return
            try:
                event = await asyncio.wait_for(queue.get(), timeout=30.0)
                yield f"data: {json.dumps(event, default=str)}\n\n"
                if event.get("status") in ("done", "failed"):
                    completed += 1
            except asyncio.TimeoutError:
                # Keepalive — but also re-check task state on every timeout
                if batch_task.done():
                    if batch_task.exception() is not None:
                        err = batch_task.exception()
                        import traceback
                        print(f"[BATCH-ERROR] Task crashed during wait: {err}")
                        traceback.print_exception(type(err), err, err.__traceback__)
                        yield f"data: {json.dumps({'type':'error','message':f'Batch processing failed: {err}','batch_id':batch_id})}\n\n"
                        return
                    # Task finished cleanly but queue didn't deliver all events — exit loop
                    break
                yield f"data: {json.dumps({'type':'keepalive'})}\n\n"

        try:
            summary = await batch_task
        except Exception as e:
            import traceback
            print(f"[BATCH-ERROR] Awaiting task failed: {e}")
            traceback.print_exception(type(e), e, e.__traceback__)
            yield f"data: {json.dumps({'type':'error','message':f'Batch processing failed: {e}','batch_id':batch_id})}\n\n"
            return
        await finish_batch_job(batch_id, summary)
        done_event = {
            "type": "done", "batch_id": batch_id,
            "total": summary["total"], "succeeded": summary["succeeded"],
            "failed": summary["failed"], "failed_files": summary["failed_files"],
            "ranked": [
                {
                    "rank": r.get("rank"), "filename": r.get("filename"),
                    "candidate_name": r.get("candidate_name", "Unknown"),
                    "current_title": r.get("current_title", "—"),
                    "overall_score": round(r.get("overall_score", 0)),
                    "recommendation": r.get("recommendation"),
                    "skills_coverage_pct": r.get("skills_coverage_pct", 0),
                    "years_experience": r.get("years_experience", "?"),
                    "_id": r.get("_id"),
                }
                for r in summary["results"]
            ],
        }
        yield f"data: {json.dumps(done_event, default=str)}\n\n"
        # Increment by number of CVs actually processed.
        # `summary` IS the results dict from run_batch_screening.
        succeeded = summary.get("succeeded", 0)
        if succeeded > 0:
            await increment_screening_count(user_id, by=succeeded)
        # Always sync from actual DB count — source of truth.
        # This is what keeps screening_count accurate after every batch.
        await sync_screening_count(user_id)

    return StreamingResponse(
        event_generator(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


# ─────────────────────────────────────────────────────────────
# SCREENINGS (tenant-scoped)
# ─────────────────────────────────────────────────────────────

@app.get("/api/screenings")
async def list_screenings(request: Request, limit: int = 2000):
    user = await get_current_user(request)
    try:
        db_user = await get_user_by_id(user["user_id"])
        fresh_role = db_user.get("role", "client") if db_user else user.get("role", "client")
    except Exception:
        fresh_role = user.get("role", "client")

    # The dashboard computes every stat client-side over this whole list, so a silent
    # truncation shows up as wrong numbers rather than a missing page. Cap generously
    # and tell the client when it hit the ceiling.
    limit = max(1, min(limit, 10000))

    if fresh_role == "admin":
        screenings = await get_all_screenings(limit=limit)
        total = len(screenings)
    else:
        screenings = await get_screenings_for_user(user["user_id"], limit=limit)
        total = await count_screenings_for_user(user["user_id"])

    return {
        "screenings": screenings,
        "count": len(screenings),
        "total": total,
        "truncated": len(screenings) < total,
    }


@app.get("/api/screenings/{screening_id}")
async def get_screening(request: Request, screening_id: str):
    user = await get_current_user(request)
    doc = await get_screening_by_id(screening_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Not found.")
    # Allow access if no user_id (legacy data) or if it belongs to this user
    if user["role"] != "admin" and doc.get("user_id") and doc.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Access denied.")
    return doc


@app.delete("/api/screenings/{screening_id}")
async def delete_screening_endpoint(request: Request, screening_id: str):
    user = await get_current_user(request)
    doc = await get_screening_by_id(screening_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Not found.")
    if user["role"] != "admin" and doc.get("user_id") and doc.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Access denied.")
    await delete_screening(screening_id)
    return {"deleted": True}


@app.post("/api/screenings/{screening_id}/stage")
async def update_screening_stage(
    request: Request,
    screening_id: str,
    stage: str = Form(...),
):
    """Update a candidate's pipeline stage: pending / shortlisted / interview / rejected.
    Persists the decision from the candidate modal action buttons (Reject / Shortlist / Move to interview)
    and the ✓/✗ quick-action buttons in the candidates table."""
    # IMPORTANT: import db inside the function. `db` is assigned via `global` inside
    # database.connect() on startup — any module-level import captures the pre-startup
    # value (None) and stays None forever. Every other endpoint in this file uses the
    # same in-function import pattern for this reason.
    from database import db as mongodb
    from bson import ObjectId
    from datetime import datetime as _dt
    if stage not in ("pending", "shortlisted", "interview", "rejected"):
        raise HTTPException(status_code=400, detail="Invalid stage. Must be one of: pending, shortlisted, interview, rejected.")
    user = await get_current_user(request)
    try:
        oid = ObjectId(screening_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid screening ID.")
    doc = await mongodb.screenings.find_one({"_id": oid}, {"user_id": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="Screening not found.")
    # Tenant check — admins bypass
    db_user = await get_user_by_id(user["user_id"])
    is_admin = bool(db_user and db_user.get("role") == "admin")
    if not is_admin and doc.get("user_id") and doc.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Access denied.")
    await mongodb.screenings.update_one(
        {"_id": oid},
        {"$set": {
            "stage": stage,
            "stage_updated_at": _dt.utcnow(),
            "stage_updated_by": user.get("email", ""),
        }}
    )
    return {"success": True, "screening_id": screening_id, "stage": stage}


# ─────────────────────────────────────────────────────────────
# CANDIDATE EMAIL — send / templates / history
# ─────────────────────────────────────────────────────────────

@app.post("/api/candidates/{screening_id}/email")
async def send_email_to_candidate(
    request: Request,
    screening_id: str,
    subject:  str = Form(...),
    body:     str = Form(...),
    template_type: str = Form(""),   # "interview"|"rejection"|"offer"|"custom" — for history label
    to_email: str = Form(""),        # optional override (else pulled from CV)
):
    """Send an email to a candidate via the recruiter's connected Gmail.
    Records the send in `email_history` so the UI can show 'Email sent on X' next to the candidate."""
    from database import db as mongodb
    from bson import ObjectId
    from datetime import datetime as _dt
    user = await get_current_user(request)

    # Look up screening + tenant check
    try:
        oid = ObjectId(screening_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid screening ID.")
    doc = await mongodb.screenings.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Candidate not found.")
    db_user = await get_user_by_id(user["user_id"])
    is_admin = bool(db_user and db_user.get("role") == "admin")
    if not is_admin and doc.get("user_id") and doc.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Access denied.")

    # Resolve target email: explicit override > parsed CV email
    target = (to_email or "").strip() or (doc.get("parsed_cv", {}).get("personal", {}) or {}).get("email", "")
    if not target or "@" not in target:
        raise HTTPException(
            status_code=400,
            detail="No email address found in this candidate's CV. Provide one in the 'To' field."
        )

    # Recruiter's own email goes in Reply-To so the candidate can reply directly to them,
    # not to the shared TopCandidate Gmail bot.
    recruiter_email = (db_user.get("email") if db_user else "") or user.get("email", "")

    ok, err = send_candidate_email(
        to_email=target,
        subject=subject,
        body_text=body,
        reply_to=recruiter_email,
    )
    if not ok:
        raise HTTPException(status_code=502, detail=err)

    # Record the send so the UI can show "Email sent" next to this candidate
    await mongodb.email_history.insert_one({
        "screening_id":   screening_id,
        "user_id":        user["user_id"],
        "company":        user.get("company", ""),
        "candidate_name": doc.get("candidate_name", "Unknown"),
        "to_email":       target,
        "subject":        subject,
        "body":           body,
        "template_type":  template_type or "custom",
        "sent_at":        _dt.utcnow(),
        "sent_by_email":  recruiter_email,
    })
    return {"success": True, "to": target}


@app.get("/api/user/email-templates")
async def get_email_templates(request: Request):
    """Return the user's saved candidate-email templates, falling back to defaults
    for any templates they haven't customized."""
    user = await get_current_user(request)
    db_user = await get_user_by_id(user["user_id"]) or {}
    saved = db_user.get("email_templates") or {}
    # Merge defaults so we always return all three templates
    merged = {}
    for kind, default_tpl in DEFAULT_TEMPLATES.items():
        s = saved.get(kind) or {}
        merged[kind] = {
            "subject": s.get("subject") or default_tpl["subject"],
            "body":    s.get("body")    or default_tpl["body"],
            "is_custom": bool(s.get("subject") or s.get("body")),
        }
    return {"templates": merged, "variables": [{"name": n, "desc": d} for n, d in TEMPLATE_VARIABLES]}


@app.put("/api/user/email-templates")
async def update_email_templates(
    request: Request,
    templates: str = Form(...),    # JSON-encoded {kind: {subject, body}}
):
    """Save the user's customized email templates. Body is a JSON string so the form
    layer doesn't have to know about nested structures."""
    from database import db as mongodb
    from bson import ObjectId
    user = await get_current_user(request)
    try:
        parsed = json.loads(templates) if templates else {}
    except Exception:
        raise HTTPException(status_code=400, detail="Templates must be valid JSON.")
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="Templates must be a dict keyed by template type.")
    # Keep only the known template kinds — don't allow arbitrary keys to bloat the user doc
    clean = {}
    for kind in DEFAULT_TEMPLATES:
        if kind in parsed and isinstance(parsed[kind], dict):
            clean[kind] = {
                "subject": str(parsed[kind].get("subject", ""))[:300],
                "body":    str(parsed[kind].get("body", ""))[:8000],
            }
    await mongodb.users.update_one(
        {"_id": ObjectId(user["user_id"])},
        {"$set": {"email_templates": clean}}
    )
    return {"success": True, "saved": list(clean.keys())}


@app.get("/api/candidates/{screening_id}/email-history")
async def get_candidate_email_history(request: Request, screening_id: str):
    """Return the email history for a single candidate so the UI can show
    "✓ Interview email sent on May 18" or warn about double-sends."""
    from database import db as mongodb
    user = await get_current_user(request)
    # Same tenant rule as the send endpoint
    cursor = mongodb.email_history.find(
        {"screening_id": screening_id, "user_id": user["user_id"]}
    ).sort("sent_at", -1)
    out = []
    async for row in cursor:
        row["_id"] = str(row["_id"])
        out.append(row)
    return {"history": out}


@app.get("/api/screenings/{screening_id}/cv")
async def get_cv_pdf(request: Request, screening_id: str):
    from bson import ObjectId
    from fastapi.responses import Response as FastResponse
    user = await get_current_user(request)
    try:
        oid = ObjectId(screening_id)
    except Exception:
        raise HTTPException(status_code=404, detail="CV file not found.")

    doc = await db.screenings.find_one(
        {"_id": oid},
        {"cv_pdf_b64": 1, "cv_filename": 1, "user_id": 1, "cv_file_id": 1},
    )
    if not doc:
        raise HTTPException(status_code=404, detail="CV file not found.")
    # Ownership before content: the old order answered "does this screening have a
    # CV?" for screenings the caller doesn't own.
    if user["role"] != "admin" and doc.get("user_id") and doc.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Access denied.")

    # application_files is the primary store; cv_pdf_b64 is the legacy copy that
    # migration 001 leaves in place until it is unset. The new path is tried FIRST
    # deliberately — while both exist, every read exercises the new one with the old
    # one still there to fall back on, so a broken read surfaces now rather than
    # after the retention window reaps the copies.
    pdf_bytes = None
    if doc.get("cv_file_id"):
        f = await db.application_files.find_one({"screening_id": oid}, {"data": 1, "filename": 1})
        if f and f.get("data"):
            pdf_bytes = bytes(f["data"])

    if pdf_bytes is None and doc.get("cv_pdf_b64"):
        pdf_bytes = base64.b64decode(doc["cv_pdf_b64"])

    if pdf_bytes is None:
        # Expected state once a PDF has passed its retention window. The parsed
        # profile on the screening is unaffected, so say that rather than "not found".
        raise HTTPException(
            status_code=404,
            detail="The original CV is no longer stored. The parsed profile is retained.",
        )

    filename = doc.get("cv_filename") or "cv.pdf"
    return FastResponse(content=pdf_bytes, media_type="application/pdf",
                        headers={"Content-Disposition": f"inline; filename={filename}"})


# ─────────────────────────────────────────────────────────────
# PUBLIC APPLICATION LINKS
#
# The only unauthenticated write surface in the app. Everything here is built
# around two facts: a public link spends OpenAI money, and a candidate must
# never learn anything about how they were scored.
# ─────────────────────────────────────────────────────────────

import html as _html
from datetime import datetime as _dt, timedelta as _td

# Every failure mode of a public link — unknown token, paused job, rotated
# token, deleted job — renders THIS page. No 404-vs-403 difference, no timing
# hint, nothing to probe with.
CLOSED_PAGE_HTML = """<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Position not available — TopCandidate.pro</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
<style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:'Inter',system-ui,sans-serif;
background:#FAFBFC;color:#142848;min-height:100vh;display:grid;place-items:center;padding:1.5rem}
.c{background:#fff;border:1px solid #E4E8F0;border-radius:16px;padding:2.5rem 2rem;max-width:440px;
text-align:center;box-shadow:0 4px 12px rgba(20,40,72,.06)}
h1{font-size:1.35rem;font-weight:800;margin-bottom:.75rem;letter-spacing:-.5px}
p{color:#4A5970;line-height:1.65;font-size:.95rem}
a{color:#E16A1F;font-weight:600;text-decoration:none}</style></head>
<body><div class="c"><h1>This position isn't accepting applications</h1>
<p>The link may have expired or the role may have closed. If someone sent you
this link, ask them for an up-to-date one.</p>
<p style="margin-top:1.25rem"><a href="https://topcandidate.pro">TopCandidate.pro</a></p>
</div></body></html>"""


async def resolve_jd_requirements(job_id: str, jd_text: str):
    """Parse a job's description once and reuse it for every candidate.

    Cost is the smaller half of this. The larger half is fairness: the JD parse
    is a GPT call, and parsing it per CV means two candidates for the same
    posting can be scored against subtly different parsed requirements. One
    parse per job means every candidate is measured against the same rubric.

    Returns None on any failure, which makes the pipeline parse inline exactly
    as it did before — the cache can never be the reason a screening fails.
    """
    from database import get_cached_jd_parse, save_jd_parse
    from scorer import parse_jd_only, PIPELINE_MODEL

    if not job_id or not (jd_text or "").strip():
        return None
    try:
        cached = await get_cached_jd_parse(job_id, jd_text, PIPELINE_MODEL)
        if cached:
            return cached
        parsed = await parse_jd_only(jd_text, OPENAI_API_KEY)
        if parsed:
            await save_jd_parse(job_id, jd_text, PIPELINE_MODEL, parsed)
        return parsed
    except Exception as e:
        print(f"[JD-CACHE] falling back to inline parse for job {job_id}: {e}")
        return None


async def owned_job(job_id: str, user: dict) -> dict:
    """Fetch a job the caller owns, or raise. Tenant scoping for every dashboard route."""
    from bson import ObjectId as _OID
    try:
        oid = _OID(job_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Job not found.")
    job = await db.jobs.find_one({"_id": oid})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    db_user = await get_user_by_id(user["user_id"])
    is_admin = bool(db_user and db_user.get("role") == "admin")
    owner = job.get("user_id")
    if not is_admin and owner and str(owner) != str(user["user_id"]):
        raise HTTPException(status_code=403, detail="Access denied.")
    job["_id"] = str(job["_id"])
    return job


def _closed_link_page() -> HTMLResponse:
    """The single response for every non-resolving token.

    get_job_by_public_token() returns None for all four closed states —
    unknown, paused (is_public False), rotated (token no longer matches), and
    deleted (active False) — so they all land here and are byte-for-byte
    identical: same status, same body. A token cannot be probed for existence,
    because "does not exist" and "exists but paused" are indistinguishable.
    """
    return HTMLResponse(CLOSED_PAGE_HTML, status_code=404)


@app.get("/apply/{token}", response_class=HTMLResponse)
async def public_apply_page(token: str):
    job = await get_job_by_public_token(token)
    if not job:
        return _closed_link_page()

    owner = await get_user_by_id(str(job.get("user_id"))) if job.get("user_id") else None
    company = (owner or {}).get("company_name") or "this company"

    def esc(v, fallback=""):
        return _html.escape(str(v if v not in (None, "") else fallback))

    page = read_template("apply.html")
    for key, val in {
        "{{JOB_TITLE}}": esc(job.get("title"), "Open position"),
        "{{COMPANY}}": esc(company),
        "{{LOCATION}}": esc(job.get("loc") or job.get("location"), "Not specified"),
        "{{JOB_TYPE}}": esc(job.get("type"), "Full-time"),
        "{{DEPARTMENT}}": esc(job.get("dept") or job.get("department"), ""),
        "{{JD_TEXT}}": esc(job.get("description"), "No description provided."),
        "{{TOKEN}}": esc(token),
        "{{RETENTION_DAYS}}": str(APPLICATION_PDF_RETENTION_DAYS),
        "{{MAX_MB}}": str(MAX_APPLICATION_PDF_BYTES // (1024 * 1024)),
    }.items():
        page = page.replace(key, val)
    return HTMLResponse(page)


async def score_application(application_id: str):
    """Score one public application. Runs AFTER the response is sent.

    Nothing here talks to the candidate. If it fails, the application drops back
    to stored_unscored and shows up in the recruiter's pending queue, which is
    the same path a capped application takes — so the manual "Score all pending"
    button doubles as the retry mechanism. No queue infrastructure needed.
    """
    from bson import ObjectId as _OID
    try:
        app_doc = await db.applications.find_one({"_id": _OID(application_id)})
        if not app_doc:
            return
        job = await db.jobs.find_one({"_id": _OID(app_doc["job_id"])})
        if not job:
            return

        f = await db.application_files.find_one({"application_id": application_id}, {"data": 1})
        if not f or not f.get("data"):
            await db.applications.update_one(
                {"_id": _OID(application_id)},
                {"$set": {"status": "stored_unscored", "error": "CV file missing"}},
            )
            await release_screening_slot(app_doc["job_id"])
            return

        await db.applications.update_one({"_id": _OID(application_id)}, {"$set": {"status": "scoring"}})

        # pdfplumber is synchronous CPU work. On the event loop it would stall
        # every other request in this worker for the duration of the parse.
        cv_text, err = await asyncio.to_thread(extract_pdf_text, bytes(f["data"]))
        if err or not (cv_text or "").strip():
            await db.applications.update_one(
                {"_id": _OID(application_id)},
                {"$set": {"status": "stored_unscored", "error": err or "No text could be extracted"}},
            )
            # No API call happened, so the reservation is genuinely unused.
            await release_screening_slot(app_doc["job_id"])
            return

        weights = job.get("weights") if isinstance(job.get("weights"), dict) else None
        jd_text = job.get("description") or ""
        # Public applicants trickle in one at a time, so without this every
        # single applicant would buy their own JD parse — and be scored against
        # it rather than against the same one as everyone else on the posting.
        jd_req = await resolve_jd_requirements(app_doc["job_id"], jd_text)
        result, err = await run_screening_pipeline(
            cv_text=cv_text,
            jd_text=jd_text,
            api_key=OPENAI_API_KEY,
            weights=weights,
            jd_requirements=jd_req,
        )
        if err or not result:
            # Money may already be spent — the reservation is NOT released.
            await db.applications.update_one(
                {"_id": _OID(application_id)},
                {"$set": {"status": "stored_unscored", "error": err or "Scoring failed"}},
            )
            return

        parsed_name = ((result.get("parsed_cv") or {}).get("personal") or {}).get("name")
        if not parsed_name or str(parsed_name).strip().lower() in ("", "unknown"):
            result["candidate_name"] = app_doc.get("name") or "Unknown"

        result.update({
            "job_id": app_doc["job_id"],
            "job_title": job.get("title"),
            "user_id": app_doc.get("user_id"),
            "source": "public_apply",
            "application_id": application_id,
            "applicant_name": app_doc.get("name"),
            "applicant_email": app_doc.get("email"),
            "applicant_phone": app_doc.get("phone"),
            "cv_filename": app_doc.get("cv_filename"),
        })
        screening_id = await save_screening(result)

        # Point the stored PDF at the screening too, so the existing CV viewer
        # resolves it exactly like a batch-uploaded one.
        await db.application_files.update_one(
            {"application_id": application_id},
            {"$set": {"screening_id": _OID(screening_id)}},
        )
        await db.screenings.update_one(
            {"_id": _OID(screening_id)}, {"$set": {"cv_file_id": f["_id"]}}
        )
        await db.applications.update_one(
            {"_id": _OID(application_id)},
            {"$set": {"status": "scored", "screening_id": screening_id, "scored_at": _dt.utcnow(),
                      "error": None}},
        )
        await sync_screening_count(str(app_doc.get("user_id")))
    except Exception as e:
        print(f"[APPLY] scoring failed for {application_id}: {e}")
        try:
            await db.applications.update_one(
                {"_id": _OID(application_id)}, {"$set": {"status": "stored_unscored", "error": str(e)[:300]}}
            )
        except Exception:
            pass


@app.post("/api/apply/{token}")
async def public_apply_submit(
    request: Request,
    background: BackgroundTasks,
    token: str,
    name: str = Form(...),
    email: str = Form(...),
    phone: str = Form(""),
    cv_file: UploadFile = File(...),
):
    job = await get_job_by_public_token(token)
    if not job:
        raise HTTPException(status_code=404, detail="This position isn't accepting applications.")

    email_norm = (email or "").strip().lower()
    if "@" not in email_norm or len(email_norm) < 5:
        raise HTTPException(status_code=400, detail="Please enter a valid email address.")
    if not (name or "").strip():
        raise HTTPException(status_code=400, detail="Please enter your name.")

    # Read one byte past the limit: enough to know it's oversized, not enough to
    # let a 200MB upload sit in memory while we find out.
    data = await cv_file.read(MAX_APPLICATION_PDF_BYTES + 1)
    if len(data) > MAX_APPLICATION_PDF_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Your CV must be under {MAX_APPLICATION_PDF_BYTES // (1024*1024)}MB. Please upload a smaller PDF.",
        )
    if not data:
        raise HTTPException(status_code=400, detail="The file appears to be empty.")
    # Magic bytes, not Content-Type — the header is set by the client and the
    # extension is decoration. This is the only check that reads the actual file.
    if not data.startswith(b"%PDF-"):
        raise HTTPException(status_code=400, detail="Please upload a PDF file.")

    client_ip = (request.headers.get("x-forwarded-for", "") or "").split(",")[0].strip() \
        or (request.client.host if request.client else "unknown")
    iph = hash_ip(client_ip)

    # CGNAT means IP alone is useless in Bangladesh — thousands of subscribers
    # share one address. Every bucket below is keyed on the email, with the IP
    # only ever narrowing it further.
    if not await rate_limit_allows(f"email-day:{email_norm}", 5, 86400):
        raise HTTPException(status_code=429, detail="You've applied to several roles today. Please try again tomorrow.")
    if not await rate_limit_allows(f"ipmail-hr:{iph}:{email_norm}", 3, 3600):
        raise HTTPException(status_code=429, detail="Too many attempts. Please try again in an hour.")

    job_id = str(job["_id"])
    application_id, replaced = await upsert_application(
        job, name, email_norm, phone, cv_file.filename or "cv.pdf", iph
    )
    await db.application_files.delete_many({"application_id": application_id})
    await store_application_pdf(application_id, job_id, str(job.get("user_id") or ""),
                                data, cv_file.filename or "cv.pdf")

    # The reservation happens here, before anything is queued. If it fails the
    # application is kept and simply isn't scored — the candidate is never told.
    ok, blocked_by = await reserve_screening_slot(job_id)
    if ok:
        background.add_task(score_application, application_id)
    else:
        await db.applications.update_one(
            {"_id": __import__("bson").ObjectId(application_id)},
            {"$set": {"status": "stored_unscored", "capped_by": blocked_by}},
        )
        print(f"[APPLY] cap '{blocked_by}' reached — application {application_id} stored unscored")

    # Byte-identical whether new, replaced, scored, or capped. A different
    # response for a repeat submission would let anyone test whether a given
    # address had already applied.
    return {"success": True, "message": "Application received"}


# ── Dashboard side ───────────────────────────────────────────

@app.post("/api/jobs/{job_id}/public")
async def toggle_job_public(request: Request, job_id: str, is_public: bool = Form(...)):
    user = await get_current_user(request)
    await owned_job(job_id, user)
    job = await set_job_public(job_id, is_public)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {
        "success": True,
        "is_public": is_public,
        "public_token": job.get("public_token"),
        "url": f"{APP_URL}/apply/{job.get('public_token')}" if job.get("public_token") else None,
    }


@app.post("/api/jobs/{job_id}/rotate-token")
async def rotate_job_public_token(request: Request, job_id: str):
    """Kill switch. Separate from the pause toggle on purpose: pausing keeps the
    URL you already posted, rotating destroys it permanently."""
    user = await get_current_user(request)
    await owned_job(job_id, user)
    job = await rotate_job_token(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {"success": True, "public_token": job["public_token"],
            "url": f"{APP_URL}/apply/{job['public_token']}"}


@app.get("/api/jobs/{job_id}/applications")
async def list_job_applications(request: Request, job_id: str):
    user = await get_current_user(request)
    await owned_job(job_id, user)
    apps = await get_applications_for_job(job_id, user["user_id"])
    counts = await count_pending_applications(job_id)
    spend = await get_spend_state(job_id)
    return {
        "applications": apps,
        "counts": counts,
        "spend": spend,
        "cost_per_screening": 0.054,
        "estimated_cost": round(counts["pending"] * 0.054, 2),
    }


@app.delete("/api/applications/{application_id}")
async def discard_application(request: Request, application_id: str):
    """Throw away junk without paying to score it."""
    from bson import ObjectId as _OID
    user = await get_current_user(request)
    try:
        oid = _OID(application_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Application not found.")
    doc = await db.applications.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Application not found.")
    await owned_job(doc["job_id"], user)
    await db.application_files.delete_many({"application_id": application_id})
    await db.applications.update_one({"_id": oid}, {"$set": {"status": "discarded"}})
    return {"success": True}


@app.post("/api/jobs/{job_id}/screen-pending")
async def screen_pending_applications(request: Request, job_id: str):
    """Score every pending application for a job, via the existing batch pipeline.

    Deliberate authenticated action, so it bypasses the DAILY cap — but still
    counts against the per-job and monthly ones, which are the caps that bound
    what a single posting and a single month can cost.
    """
    from bson import ObjectId as _OID
    user = await get_current_user(request)
    job = await owned_job(job_id, user)

    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OpenAI API key not configured on server.")

    pending = await db.applications.find(
        {"job_id": job_id, "status": {"$in": ["pending", "stored_unscored"]}}
    ).sort("submitted_at", 1).to_list(200)
    if not pending:
        raise HTTPException(status_code=400, detail="No pending applications for this job.")

    async def event_generator():
        yield f"data: {json.dumps({'type':'start','total':len(pending)})}\n\n"
        done = failed = 0
        for i, app_doc in enumerate(pending):
            aid = str(app_doc["_id"])
            ok = await reserve_spend_for_manual(job_id)
            if not ok:
                yield f"data: {json.dumps({'type':'capped','index':i,'name':app_doc.get('name','')})}\n\n"
                continue
            await score_application(aid)
            fresh = await db.applications.find_one({"_id": _OID(aid)}, {"status": 1})
            if fresh and fresh.get("status") == "scored":
                done += 1
            else:
                failed += 1
            yield f"data: {json.dumps({'type':'progress','index':i,'done':done,'failed':failed,'total':len(pending),'name':app_doc.get('name','')})}\n\n"
        counts = await count_pending_applications(job_id)
        yield f"data: {json.dumps({'type':'complete','done':done,'failed':failed,'counts':counts})}\n\n"

    return StreamingResponse(
        event_generator(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def reserve_spend_for_manual(job_id: str) -> bool:
    """Per-job and monthly only — the daily cap exists to bound what an unattended
    public link can spend overnight, and this is neither unattended nor public."""
    from database import reserve_spend, release_spend
    now = _dt.utcnow()
    if not await reserve_spend(f"job:{job_id}", CAP_PER_JOB):
        return False
    if not await reserve_spend(f"month:{now:%Y-%m}", CAP_PER_MONTH):
        await release_spend(f"job:{job_id}")
        return False
    return True


# ─────────────────────────────────────────────────────────────
# STATS & ANALYTICS (tenant-scoped)
# ─────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def stats(request: Request):
    user = await get_current_user(request)
    db_user = await get_user_by_id(user["user_id"])
    if db_user:
        user = {**user, "role": db_user.get("role", user.get("role","client"))}
    if user["role"] == "admin":
        return await get_screening_stats()
    return await get_stats_for_user(user["user_id"])


@app.get("/api/analytics/skills-gaps")
async def skills_gaps(request: Request):
    user = await get_current_user(request)
    if user["role"] == "admin":
        gaps = await get_skills_gap_frequency()
    else:
        gaps = await get_skills_gaps_for_user(user["user_id"])
    return {"gaps": gaps}


@app.get("/api/analytics/dimension-averages")
async def dimension_averages(request: Request):
    user = await get_current_user(request)
    if user["role"] == "admin":
        dims = await get_dimension_averages()
    else:
        dims = await get_dimension_averages_for_user(user["user_id"])
    return {"dimensions": dims}


# ─────────────────────────────────────────────────────────────
# JOBS (tenant-scoped)
# ─────────────────────────────────────────────────────────────

@app.get("/api/jobs")
async def list_jobs(request: Request):
    user = await get_current_user(request)
    db_user = await get_user_by_id(user["user_id"])
    if db_user:
        user = {**user, "role": db_user.get("role", user.get("role","client"))}
    if user["role"] == "admin":
        jobs = await get_all_jobs()
    else:
        jobs = await get_jobs_for_user(user["user_id"])

    # Pending counts for the Jobs table badge. One grouped aggregation rather
    # than a count per job — the table renders every job at once.
    pending_by_job = {}
    try:
        cursor = db.applications.aggregate([
            {"$match": {"status": {"$in": ["pending", "stored_unscored"]}}},
            {"$group": {"_id": "$job_id", "n": {"$sum": 1}}},
        ])
        async for row in cursor:
            pending_by_job[row["_id"]] = row["n"]
    except Exception:
        pass
    for j in jobs:
        j["_pending"] = pending_by_job.get(str(j.get("_id")), 0)

    return {"jobs": jobs, "count": len(jobs)}


@app.post("/api/jobs")
async def create_job_endpoint(
    request: Request,
    title: str = Form(...),
    department: str = Form(""),
    location: str = Form(""),
    employment_type: str = Form("Full-time"),
    skills: str = Form(""),
    description: str = Form(""),
    min_experience: str = Form(""),
    status: str = Form("active"),
    weights: str = Form(""),   # JSON-encoded dict of {dim_name: float}
):
    user = await get_current_user(request)
    # Parse weights JSON if provided. Invalid JSON → ignore (job will use default weights at score time).
    weights_dict = None
    if weights:
        try:
            import json as _json
            parsed = _json.loads(weights)
            if isinstance(parsed, dict) and parsed:
                weights_dict = parsed
        except Exception:
            weights_dict = None
    job = {
        "title": title, "department": department, "location": location,
        "employment_type": employment_type,
        "skills": [s.strip() for s in skills.split(",") if s.strip()],
        "description": description,
        "min_experience": min_experience,
        "status": status,
        "user_id": user["user_id"], "company": user["company"],
    }
    if weights_dict is not None:
        job["weights"] = weights_dict
    try:
        job_id = await save_job(job)
    except DuplicateJobError as e:
        raise HTTPException(
            status_code=409,
            detail=f"You already have a job titled '{e.title}'. Open the existing one instead of creating a second copy."
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"_id": job_id, **job}


@app.put("/api/jobs/{job_id}")
async def update_job_endpoint(
    request: Request,
    job_id: str,
    description: str = Form(""),
    title: str = Form(""),
    skills: str = Form(""),
    status: str = Form(""),
    department: str = Form(""),
    location: str = Form(""),
    employment_type: str = Form(""),
    min_experience: str = Form(""),
    weights: str = Form(""),
):
    """Update job fields — used to save description and other edits to existing jobs."""
    user = await get_current_user(request)
    from database import db as mongodb
    from bson import ObjectId
    updates = {}
    # Only update fields that were actually submitted (non-empty).
    # Empty string means "field was not in the form", NOT "clear the field".
    if description:     updates["description"] = description
    if title:           updates["title"] = title
    if skills:          updates["skills"] = [s.strip() for s in skills.split(",") if s.strip()]
    if status:          updates["status"] = status
    if department:      updates["department"] = department
    if location:        updates["location"] = location
    if employment_type: updates["employment_type"] = employment_type
    if min_experience:  updates["min_experience"] = min_experience
    if weights:
        try:
            import json as _json
            parsed = _json.loads(weights)
            if isinstance(parsed, dict) and parsed:
                updates["weights"] = parsed
        except Exception:
            pass   # silently ignore bad JSON — caller can retry
    if updates:
        result = await mongodb.jobs.update_one(
            {"_id": ObjectId(job_id), "user_id": user["user_id"]},
            {"$set": updates}
        )
        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail="Job not found.")
    return {"success": True, "updated_fields": list(updates.keys())}


@app.delete("/api/jobs/{job_id}")
async def delete_job_endpoint(request: Request, job_id: str):
    user = await get_current_user(request)
    deleted = await delete_job(job_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {"deleted": True}


# ─────────────────────────────────────────────────────────────
# ADMIN — manage all users
# ─────────────────────────────────────────────────────────────

@app.get("/api/admin/users")
async def admin_list_users(request: Request):
    user = await get_current_user(request)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    users = await get_all_users()
    return {"users": users, "count": len(users)}


@app.post("/api/admin/users/{user_id}/toggle")
async def admin_toggle_user(request: Request, user_id: str):
    user = await get_current_user(request)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    target = await get_user_by_id(user_id)
    if not target:
        raise HTTPException(status_code=404, detail="User not found.")
    new_status = not target.get("active", True)
    await update_user(user_id, {"active": new_status})
    return {"active": new_status}


@app.post("/api/admin/users/create")
async def admin_create_user(
    request: Request,
    company_name: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    plan: str = Form("trial"),
    role: str = Form("client"),
):
    user = await get_current_user(request)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters.")
    try:
        user_id = await create_user(
            email=email,
            hashed_password=hash_password(password),
            company_name=company_name,
            role=role
        )
        await update_user(user_id, {"plan": plan})
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"success": True, "user_id": user_id}


@app.post("/api/admin/users/{user_id}/plan")
async def admin_change_plan(request: Request, user_id: str, plan: str = Form(...)):
    from datetime import datetime
    user = await get_current_user(request)
    # Read fresh role from DB, not the JWT, so a recently-elevated admin isn't blocked.
    db_requester = await get_user_by_id(user["user_id"])
    if not db_requester or db_requester.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    if plan not in ["trial", "starter", "pro", "enterprise"]:
        raise HTTPException(status_code=400, detail="Invalid plan.")
    updates = {"plan": plan}
    # When upgrading from trial to a paid plan, give a fresh monthly quota.
    # When downgrading, DON'T reset — we don't want to hide that they're over the new limit.
    target = await get_user_by_id(user_id)
    if target:
        old_plan = target.get("plan", "trial")
        order = {"trial": 0, "starter": 1, "pro": 2, "enterprise": 3}
        if order.get(plan, 0) > order.get(old_plan, 0):
            updates["screening_count"] = 0
            updates["month_reset_at"] = datetime.utcnow().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    await update_user(user_id, updates)
    return {"plan": plan, "updated_fields": list(updates.keys())}


# ─────────────────────────────────────────────────────────────
# BATCH HISTORY
# ─────────────────────────────────────────────────────────────

@app.get("/api/batch/jobs")
async def list_batch_jobs(request: Request):
    user = await get_current_user(request)
    jobs = await get_all_batch_jobs()
    return {"jobs": jobs, "count": len(jobs)}


@app.get("/api/batch/jobs/{batch_id}")
async def get_batch(request: Request, batch_id: str):
    user = await get_current_user(request)
    job = await get_batch_job(batch_id)
    if not job:
        raise HTTPException(status_code=404, detail="Batch job not found.")
    return job


# ─────────────────────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────────────────────

@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    token = get_token_from_request(request)
    if not token or not decode_token(token):
        return RedirectResponse("/login")
    return read_template("settings.html")


@app.get("/payment/success", response_class=HTMLResponse)
async def payment_success(request: Request, plan: str = "", session_id: str = ""):
    token = get_token_from_request(request)
    if not token or not decode_token(token):
        return RedirectResponse("/login")
    user = decode_token(token)
    if plan and plan in PLANS:
        await update_user_subscription(user["user_id"], plan, {"session_id": session_id})
    return RedirectResponse("/settings?tab=billing&payment=success")


@app.get("/payment/sslcommerz/success")
async def ssl_success(request: Request, plan: str = "", user_id: str = "", val_id: str = "", tran_id: str = ""):
    verification = await verify_sslcommerz_payment(val_id)
    if verification.get("valid") and user_id and plan:
        await update_user_subscription(user_id, plan, {"tran_id": tran_id, "method": "sslcommerz"})
        await save_payment({"user_id": user_id, "plan": plan, "amount": f"৳{PLANS.get(plan, {}).get('bdt_price', 0)}", "method": "SSLCommerz", "status": "paid", "tran_id": tran_id})
    return RedirectResponse("/settings?tab=billing&payment=success")


@app.get("/payment/sslcommerz/fail")
async def ssl_fail():
    return RedirectResponse("/settings?tab=billing&payment=failed")


# ── USER PROFILE & SETTINGS ──

@app.get("/api/auth/me/full")
async def me_full(request: Request):
    user = await get_current_user(request)
    full = await get_full_user(user["user_id"])
    return full or user


@app.post("/api/user/profile")
async def update_profile(
    request: Request,
    company_name: str = Form(""),
    full_name: str = Form(""),
    phone: str = Form(""),
    website: str = Form(""),
    address: str = Form(""),
):
    user = await get_current_user(request)
    await update_user_profile(user["user_id"], {
        "company_name": company_name,
        "full_name": full_name,
        "phone": phone,
        "website": website,
        "address": address,
    })
    return {"success": True}


@app.post("/api/user/change-password")
async def change_password_endpoint(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
):
    user = await get_current_user(request)
    db_user = await get_user_by_id(user["user_id"])
    if not db_user or not verify_password(current_password, db_user.get("password", "")):
        raise HTTPException(status_code=400, detail="Current password is incorrect.")
    if len(new_password) < 8:
        raise HTTPException(status_code=400, detail="New password must be at least 8 characters.")
    from database import db as mongodb
    from bson import ObjectId
    await mongodb.users.update_one(
        {"_id": ObjectId(user["user_id"])},
        {"$set": {"password": hash_password(new_password)}}
    )
    return {"success": True}


@app.post("/api/user/notifications")
async def save_notifications(request: Request, prefs: str = Form(...)):
    import json
    user = await get_current_user(request)
    prefs_dict = json.loads(prefs)
    await update_user_notifications(user["user_id"], prefs_dict)
    return {"success": True}


# ── PAYMENTS ──

@app.get("/api/payments")
async def list_payments(request: Request):
    user = await get_current_user(request)
    payments = await get_payments_for_user(user["user_id"])
    return {"payments": payments}


@app.post("/api/payments/checkout")
async def create_checkout(
    request: Request,
    plan_id: str = Form(...),
    payment_method: str = Form("stripe"),
):
    user = await get_current_user(request)
    db_user = await get_full_user(user["user_id"])

    if payment_method == "stripe":
        result = create_stripe_checkout(
            plan_id=plan_id,
            user_id=user["user_id"],
            email=user["email"],
            company=user["company"],
        )
    else:
        result = await create_sslcommerz_payment(
            plan_id=plan_id,
            user_id=user["user_id"],
            email=user["email"],
            company=user["company"],
            customer_name=db_user.get("full_name", user["company"]) if db_user else user["company"],
            customer_phone=db_user.get("phone", "01700000000") if db_user else "01700000000",
        )

    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error", "Payment failed"))
    return result


@app.post("/api/payments/cancel")
async def cancel_payment(request: Request):
    user = await get_current_user(request)
    db_user = await get_full_user(user["user_id"])
    sub_id = db_user.get("subscription", {}).get("subscription_id") if db_user else None
    if sub_id:
        cancel_stripe_subscription(sub_id)
    return {"success": True}


@app.post("/api/payments/portal")
async def billing_portal(request: Request):
    user = await get_current_user(request)
    db_user = await get_full_user(user["user_id"])
    customer_id = db_user.get("subscription", {}).get("customer_id") if db_user else None
    if not customer_id:
        raise HTTPException(status_code=400, detail="No billing account found.")
    url = create_stripe_portal_session(customer_id)
    if not url:
        raise HTTPException(status_code=400, detail="Could not open billing portal.")
    return {"url": url}


@app.post("/api/payments/stripe/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig = request.headers.get("stripe-signature", "")
    event = verify_stripe_webhook(payload, sig)
    if not event:
        raise HTTPException(status_code=400, detail="Invalid webhook signature")

    if event["type"] == "checkout.session.completed":
        session = event["data"]["object"]
        user_id = session.get("metadata", {}).get("user_id")
        plan_id = session.get("metadata", {}).get("plan_id")
        if user_id and plan_id:
            await update_user_subscription(user_id, plan_id, {
                "session_id": session.get("id"),
                "subscription_id": session.get("subscription"),
                "customer_id": session.get("customer"),
            })
            await save_payment({
                "user_id": user_id,
                "plan": plan_id,
                "amount": f"${PLANS.get(plan_id, {}).get('usd_price', 0)}",
                "method": "Stripe",
                "status": "paid",
                "session_id": session.get("id"),
            })
    return {"received": True}


# ── TEAM ──

@app.post("/api/team/invite")
async def team_invite(request: Request, email: str = Form(...), role: str = Form("screener")):
    user = await get_current_user(request)
    db_user = await get_user_by_id(user["user_id"])
    company = db_user.get("company_name", user["company"]) if db_user else user["company"]
    try:
        invite_id = await invite_team_member(
            owner_user_id=user["user_id"],
            email=email,
            role=role,
            company_name=company,
        )
        # Send invitation email
        from email_service import send_team_invite_email
        email_sent = send_team_invite_email(
            to_email=email,
            invited_by=user["email"],
            company_name=company,
            role=role.capitalize(),
        )
        return {
            "success": True,
            "invite_id": invite_id,
            "email_sent": email_sent,
            "message": f"Invitation sent to {email}" if email_sent else f"Invite saved but email not sent — Gmail not configured in Render environment"
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/team")
async def get_team(request: Request):
    user = await get_current_user(request)
    members = await get_team_members(user["user_id"])
    invites = await get_team_invites(user["user_id"])
    return {"members": members, "invites": invites}


@app.post("/api/admin/migrate-screenings")
async def migrate_screenings(request: Request):
    """Admin tool: assign ALL screenings to the admin user."""
    user = await get_current_user(request)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    from database import db
    # Reassign ALL screenings to admin
    result = await db.screenings.update_many(
        {},
        {"$set": {"user_id": user["user_id"], "company": user["company"]}}
    )
    return {"migrated": result.modified_count, "message": f"Assigned {result.modified_count} screenings to {user['email']}"}


@app.post("/api/admin/transfer-to/{target_email}")
async def transfer_to_user(request: Request, target_email: str):
    """Admin: transfer ALL screenings to a specific user by email."""
    user = await get_current_user(request)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    target = await get_user_by_email(target_email)
    if not target:
        raise HTTPException(status_code=404, detail=f"User {target_email} not found.")
    from database import db
    result = await db.screenings.update_many(
        {},
        {"$set": {"user_id": target["_id"], "company": target["company_name"]}}
    )
    # Update screening counts
    await db.users.update_many({}, {"$set": {"screening_count": 0}})
    count = await db.screenings.count_documents({"user_id": target["_id"]})
    from bson import ObjectId
    await db.users.update_one(
        {"_id": ObjectId(target["_id"])},
        {"$set": {"screening_count": count}}
    )
    return {"transferred": result.modified_count, "to": target_email, "to_id": target["_id"]}


@app.post("/api/admin/migrate-from/{source_user_id}")
async def migrate_from_user(request: Request, source_user_id: str):
    """Admin tool: move screenings from one user to admin."""
    user = await get_current_user(request)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    from database import db
    result = await db.screenings.update_many(
        {"user_id": source_user_id},
        {"$set": {"user_id": user["user_id"], "company": user["company"]}}
    )
    return {"migrated": result.modified_count}


@app.post("/api/user/claim-screenings")
async def claim_my_screenings(request: Request):
    """Let current user claim all unowned screenings."""
    user = await get_current_user(request)
    from database import db
    result = await db.screenings.update_many(
        {"$or": [{"user_id": {"$exists": False}}, {"user_id": None}, {"user_id": ""}]},
        {"$set": {"user_id": user["user_id"], "company": user["company"]}}
    )
    return {"claimed": result.modified_count}


@app.post("/api/admin/transfer-screenings")
async def transfer_screenings(
    request: Request,
    from_user_id: str = Form(""),
    to_user_id: str = Form(""),
    to_email: str = Form(""),
):
    """Transfer all screenings from one user to another."""
    user = await get_current_user(request)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    from database import db
    
    # Find target user by email if user_id not provided
    if not to_user_id and to_email:
        target = await get_user_by_email(to_email)
        if not target:
            raise HTTPException(status_code=404, detail=f"User {to_email} not found.")
        to_user_id = target["_id"]
        to_company = target["company_name"]
    else:
        target = await get_user_by_id(to_user_id)
        to_company = target["company_name"] if target else ""

    # If no from_user_id, transfer from current admin
    if not from_user_id:
        from_user_id = user["user_id"]

    result = await db.screenings.update_many(
        {"user_id": from_user_id},
        {"$set": {"user_id": to_user_id, "company": to_company}}
    )
    return {
        "transferred": result.modified_count,
        "from": from_user_id,
        "to": to_user_id,
        "to_email": to_email or to_company
    }


@app.post("/api/admin/make-admin/{email}")
async def make_admin(request: Request, email: str):
    """Make any user an admin."""
    user = await get_current_user(request)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    target = await get_user_by_email(email)
    if not target:
        raise HTTPException(status_code=404, detail=f"User {email} not found.")
    from database import db
    from bson import ObjectId
    await db.users.update_one(
        {"_id": ObjectId(target["_id"])},
        {"$set": {"role": "admin"}}
    )
    return {"success": True, "message": f"{email} is now admin"}


@app.get("/api/admin/check-users")
async def check_users_screenings(request: Request):
    """Admin: show all users and their actual screening counts in DB."""
    await require_admin(request)
    from database import db as mongodb
    users = []
    async for u in mongodb.users.find({}, {"password":0}):
        uid = str(u["_id"])
        count = await mongodb.screenings.count_documents({"user_id": uid})
        users.append({
            "id": uid,
            "email": u.get("email"),
            "company": u.get("company_name"),
            "role": u.get("role"),
            "db_screening_count": u.get("screening_count", 0),
            "actual_screening_count": count
        })
    # Also count unassigned
    unassigned = await mongodb.screenings.count_documents({
        "$or": [{"user_id": {"$exists": False}}, {"user_id": None}, {"user_id": ""}]
    })
    total = await mongodb.screenings.count_documents({})
    return {"users": users, "unassigned_screenings": unassigned, "total_screenings": total}


@app.get("/api/admin/diagnostics")
async def admin_diagnostics(request: Request):
    """Admin, read-only: the numbers needed before trusting the dedup key.

    Writes nothing — no $set, no update, no insert anywhere in this handler.

    The dedup classification below mirrors dedupKey() in index.html exactly:
    email from parsed_cv.personal.email, else candidate_name + job_id, and no
    key at all when the name is missing or 'unknown'. Keeping the two in sync
    by hand is the weak point — if this endpoint and the dashboard ever
    disagree on distinct_candidates, the definitions have drifted again.
    """
    await require_admin(request)
    from datetime import datetime
    from database import db as mongodb, user_match

    now = datetime.utcnow()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # ── Who holds admin, oldest account first ────────────────────────────
    admins = []
    async for u in mongodb.users.find({"role": "admin"}, {"password": 0}):
        admins.append({
            "id": str(u["_id"]),
            "email": u.get("email"),
            "company": u.get("company_name"),
            "created_at": u.get("created_at"),
            "active": u.get("active", True),
            "plan": u.get("plan"),
        })
    admins.sort(key=lambda a: (a["created_at"] is None, a["created_at"]))

    # ── Every account, with this-month and all-time counts kept apart ────
    # check-users compares a monthly screening_count against an all-time
    # total and labels the second one "actual", which reads as drift when
    # the two are simply different questions. Both are shown here.
    users = []
    async for u in mongodb.users.find({}, {"password": 0}):
        uid = str(u["_id"])
        users.append({
            "id": uid,
            "email": u.get("email"),
            "company": u.get("company_name"),
            "role": u.get("role"),
            "plan": u.get("plan"),
            "created_at": u.get("created_at"),
            "stored_screening_count": u.get("screening_count", 0),
            "month_reset_at": u.get("month_reset_at"),
            "screenings_this_month": await mongodb.screenings.count_documents(
                {**user_match(uid), "created_at": {"$gte": month_start}}
            ),
            "screenings_all_time": await mongodb.screenings.count_documents(user_match(uid)),
        })

    # ── Pending/accepted team invites, so an account's provenance is visible ──
    invites = []
    async for i in mongodb.team_invites.find({}):
        invites.append({
            "email": i.get("email"),
            "owner_user_id": i.get("owner_user_id"),
            "company_name": i.get("company_name"),
            "role": i.get("role"),
            "status": i.get("status"),
            "invited_at": i.get("invited_at"),
        })

    # ── Identity quality across every screening ──────────────────────────
    total = 0
    with_email = 0
    with_usable_name = 0
    name_missing = 0
    name_unknown = 0
    no_identity = 0
    without_job = 0
    keyed_by_email = 0
    keyed_by_name = 0
    keys = set()

    async for s in mongodb.screenings.find(
        {}, {"candidate_name": 1, "job_id": 1, "parsed_cv.personal.email": 1}
    ):
        total += 1
        personal = ((s.get("parsed_cv") or {}).get("personal") or {})
        email = (personal.get("email") or "").strip().lower()
        raw_name = s.get("candidate_name")
        name = (raw_name or "").strip().lower()
        job = s.get("job_id") or ""

        if not job:
            without_job += 1
        if email:
            with_email += 1
        if not name:
            name_missing += 1
        elif name == "unknown":
            name_unknown += 1
        else:
            with_usable_name += 1

        if email:
            keyed_by_email += 1
            keys.add("e:" + email + "|" + job)
        elif name and name != "unknown":
            keyed_by_name += 1
            keys.add("n:" + name + "|" + job)
        else:
            # No identity at all — never merged, each stays its own candidacy.
            no_identity += 1

    distinct = len(keys) + no_identity

    return {
        "generated_at": now,
        "read_only": True,
        "admins": admins,
        "admin_count": len(admins),
        "users": users,
        "team_invites": invites,
        "screening_identity": {
            "total": total,
            "with_email": with_email,
            "with_usable_name": with_usable_name,
            "name_missing": name_missing,
            "name_is_unknown": name_unknown,
            "no_identity_never_merged": no_identity,
            "without_job_id": without_job,
            "keyed_by_email": keyed_by_email,
            "keyed_by_name_and_job": keyed_by_name,
            "distinct_candidates": distinct,
            "duplicates_merged": total - distinct,
        },
    }


@app.post("/api/admin/reset-my-count")
async def reset_my_count(request: Request):
    """Reset current user screening count to 0 (for testing)."""
    user = await get_current_user(request)
    from database import db as mongodb
    from bson import ObjectId as BsonObjId
    from datetime import datetime
    now = datetime.utcnow()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    await mongodb.users.update_one(
        {"_id": BsonObjId(user["user_id"])},
        {"$set": {"screening_count": 0, "month_reset_at": month_start}}
    )
    return {"success": True, "message": "Screening count reset to 0"}


@app.get("/api/admin/fix-counts")
async def fix_all_counts(request: Request):
    """Admin: recalculate and fix screening_count for all users."""
    await require_admin(request)
    from database import db as mongodb
    from bson import ObjectId
    fixed = []
    async for u in mongodb.users.find({}, {"password": 0}):
        uid = str(u["_id"])
        count = await mongodb.screenings.count_documents({"user_id": uid})
        await mongodb.users.update_one(
            {"_id": ObjectId(uid)},
            {"$set": {"screening_count": count}}
        )
        fixed.append({"email": u.get("email"), "correct_count": count})
    return {"fixed": fixed}


@app.get("/api/debug/my-screenings")
async def debug_screenings(request: Request):
    """Admin debug: show what user_id is in token vs what screenings exist.

    The sample below reads across all tenants, so this is admin-only.
    """
    await require_admin(request)
    user = await get_current_user(request)
    from database import db
    # Count screenings by this user_id
    count = await db.screenings.count_documents({"user_id": user["user_id"]})
    # Get a sample
    sample = []
    async for doc in db.screenings.find({}).limit(5):
        sample.append({"_id": str(doc["_id"]), "user_id": doc.get("user_id", "NONE"), "name": doc.get("candidate_name", "?")})
    return {
        "token_user_id": user["user_id"],
        "token_email": user["email"],
        "screenings_matching": count,
        "sample_screenings": sample
    }


# ═══════════════════════════════════════════════════════════════
# PUBLIC API v1 — REST API for third-party integrations
# ═══════════════════════════════════════════════════════════════

async def get_api_user(request: Request) -> tuple[dict, dict]:
    """Authenticate via X-API-Key header. Returns (key_doc, user_doc)."""
    raw_key = request.headers.get("X-API-Key", "")
    if not raw_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header.")
    key_doc = await validate_api_key(raw_key)
    if not key_doc:
        raise HTTPException(status_code=401, detail="Invalid or revoked API key.")
    allowed, reason = await check_rate_limit(key_doc)
    if not allowed:
        raise HTTPException(status_code=429, detail=reason)
    user = await get_user_by_id(key_doc["user_id"])
    if not user:
        raise HTTPException(status_code=401, detail="Account not found.")
    return key_doc, user


@app.get("/api/v1/ping")
async def api_ping(request: Request):
    """Test your API key is working."""
    raw_key = request.headers.get("X-API-Key", "")
    if not raw_key:
        return {"status": "ok", "message": "TopCandidate API v1 is running. Add X-API-Key header to authenticate."}
    key_doc = await validate_api_key(raw_key)
    if not key_doc:
        raise HTTPException(status_code=401, detail="Invalid API key.")
    limits = API_PLAN_LIMITS.get(key_doc.get("plan", "trial"), {})
    return {
        "status": "ok",
        "authenticated": True,
        "key_name": key_doc.get("name"),
        "plan": key_doc.get("plan", "trial"),
        "screens_this_month": key_doc.get("screens_this_month", 0),
        "monthly_limit": limits.get("screens_per_month", 10),
        "screens_remaining": max(0, limits.get("screens_per_month", 10) - key_doc.get("screens_this_month", 0)),
    }


@app.post("/api/v1/screen")
async def api_screen_cv(
    request: Request,
    cv_file: UploadFile = File(...),
    job_description: str = Form(...),
    job_title: str = Form(""),
    candidate_name: str = Form(""),
):
    """
    Screen a single CV against a job description.
    
    **Headers:** X-API-Key: tc_live_...
    
    **Form fields:**
    - cv_file: PDF file (required)
    - job_description: Full job description text (min 50 chars, required)
    - job_title: Job title for labeling (optional)
    - candidate_name: Override candidate name (optional)
    
    **Returns:** Full screening report with score, dimensions, recommendation
    """
    key_doc, user = await get_api_user(request)

    if not cv_file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are supported.")
    if len(job_description.strip()) < 50:
        raise HTTPException(status_code=400, detail="job_description must be at least 50 characters.")

    cv_bytes = await cv_file.read()
    cv_text, error = extract_pdf_text(cv_bytes)
    if error:
        raise HTTPException(status_code=422, detail=f"Could not read PDF: {error}")

    result, error = await run_screening_pipeline(cv_text, job_description.strip(), OPENAI_API_KEY)
    if error:
        raise HTTPException(status_code=500, detail=error)

    if candidate_name:
        result["candidate_name"] = candidate_name

    import base64
    result["user_id"] = user["_id"]
    result["company"] = user.get("company_name", "")
    result["job_title"] = job_title
    result["api_key_id"] = key_doc["_id"]
    result["source"] = "api_v1"
    result["cv_pdf_b64"] = base64.b64encode(cv_bytes).decode()
    result["cv_filename"] = cv_file.filename

    doc_id = await save_screening(result)
    await increment_api_usage(key_doc["_id"])
    await log_api_call(key_doc["_id"], "/api/v1/screen", 200, user["_id"])

    return {
        "id": doc_id,
        "candidate_name": result.get("candidate_name", "Unknown"),
        "current_title": result.get("current_title", ""),
        "years_experience": result.get("years_experience", 0),
        "overall_score": result.get("overall_score", 0),
        "recommendation": result.get("recommendation", "MAYBE"),
        "skills_coverage_pct": result.get("skills_coverage_pct", 0),
        "summary": result.get("summary", ""),
        "key_strengths": result.get("key_strengths", []),
        "critical_gaps": result.get("critical_gaps", []),
        "interview_questions": result.get("interview_questions", []),
        "hiring_risks": result.get("hiring_risks", []),
        "dimensions": result.get("dimensions", []),
        "score_consistency": result.get("score_consistency", {}),
        "report_url": f"{APP_URL}/candidate?id={doc_id}",
    }


@app.get("/api/v1/results")
async def api_list_results(
    request: Request,
    limit: int = 20,
    offset: int = 0,
    job_title: str = "",
    recommendation: str = "",
):
    """
    List your screening results.
    
    **Query params:**
    - limit: Number of results (max 100, default 20)
    - offset: Pagination offset
    - job_title: Filter by job title
    - recommendation: Filter by STRONG HIRE / HIRE / MAYBE / REJECT
    """
    key_doc, user = await get_api_user(request)
    limit = min(limit, 100)

    query = {"user_id": user["_id"], "source": "api_v1"}
    if job_title:
        query["job_title"] = {"$regex": job_title, "$options": "i"}
    if recommendation:
        query["recommendation"] = recommendation.upper()

    cursor = db.screenings.find(query).sort("created_at", -1).skip(offset).limit(limit)
    results = []
    async for doc in cursor:
        results.append({
            "id": str(doc["_id"]),
            "candidate_name": doc.get("candidate_name", "Unknown"),
            "current_title": doc.get("current_title", ""),
            "overall_score": doc.get("overall_score", 0),
            "recommendation": doc.get("recommendation", "MAYBE"),
            "skills_coverage_pct": doc.get("skills_coverage_pct", 0),
            "job_title": doc.get("job_title", ""),
            "years_experience": doc.get("years_experience", 0),
            "screened_at": doc.get("created_at", "").isoformat() if doc.get("created_at") else "",
            "report_url": f"{APP_URL}/candidate?id={str(doc['_id'])}",
        })

    total = await db.screenings.count_documents(query)
    return {
        "results": results,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": (offset + limit) < total,
    }


@app.get("/api/v1/results/{screening_id}")
async def api_get_result(request: Request, screening_id: str):
    """Get full details of a specific screening result."""
    key_doc, user = await get_api_user(request)
    from bson import ObjectId as BsonObjectId
    try:
        doc = await db.screenings.find_one({
            "_id": BsonObjectId(screening_id),
            "user_id": user["_id"]
        })
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid screening ID.")
    if not doc:
        raise HTTPException(status_code=404, detail="Screening not found.")
    doc["_id"] = str(doc["_id"])
    doc.pop("cv_pdf_b64", None)
    return doc


@app.get("/api/v1/usage")
async def api_usage(request: Request):
    """Get your current API usage and limits."""
    key_doc, user = await get_api_user(request)
    limits = API_PLAN_LIMITS.get(key_doc.get("plan", "trial"), {})
    used = key_doc.get("screens_this_month", 0)
    monthly = limits.get("screens_per_month", 10)
    return {
        "plan": key_doc.get("plan", "trial"),
        "key_name": key_doc.get("name"),
        "screens_this_month": used,
        "monthly_limit": monthly,
        "screens_remaining": max(0, monthly - used),
        "screens_total_all_time": key_doc.get("screens_total", 0),
        "last_used_at": key_doc.get("last_used_at", "").isoformat() if key_doc.get("last_used_at") else None,
        "limits": limits,
    }


# ── API KEY MANAGEMENT (for dashboard users) ──

@app.get("/api/keys")
async def list_api_keys(request: Request):
    """List all API keys for the logged-in user."""
    user = await get_current_user(request)
    keys = await get_keys_for_user(user["user_id"])
    return {"keys": keys}


@app.post("/api/keys")
async def create_key(request: Request, name: str = Form(...)):
    """Create a new API key."""
    user = await get_current_user(request)
    db_user = await get_user_by_id(user["user_id"])
    plan = db_user.get("plan", "trial") if db_user else "trial"
    key_doc = await create_api_key(user["user_id"], name, plan)
    return key_doc  # raw_key included here — only time it's shown


@app.delete("/api/keys/{key_id}")
async def delete_key(request: Request, key_id: str):
    """Revoke an API key."""
    user = await get_current_user(request)
    revoked = await revoke_api_key(key_id, user["user_id"])
    if not revoked:
        raise HTTPException(status_code=404, detail="Key not found.")
    return {"success": True}


@app.get("/docs", response_class=HTMLResponse)
async def api_docs(request: Request):
    return read_template("docs.html")


@app.post("/api/payments/manual")
async def manual_payment_request(
    request: Request,
    plan_id: str = Form(...),
    payment_method: str = Form(...),  # "bank" or "bkash" or "nagad"
    transaction_id: str = Form(...),
    amount: str = Form(...),
    screenshot_note: str = Form(""),
):
    """Client submits manual payment proof. Admin reviews and upgrades plan."""
    user = await get_current_user(request)
    from database import db as mongodb
    doc = {
        "user_id": user["user_id"],
        "email": user["email"],
        "company": user["company"],
        "plan_id": plan_id,
        "payment_method": payment_method,
        "transaction_id": transaction_id,
        "amount": amount,
        "note": screenshot_note,
        "status": "pending_review",
        "created_at": __import__("datetime").datetime.utcnow(),
    }
    inserted = await mongodb.manual_payments.insert_one(doc)
    return {
        "success": True,
        "request_id": str(inserted.inserted_id),
        "message": "Payment submitted for review. Your plan will be upgraded within 24 hours after verification."
    }


@app.get("/api/admin/manual-payments")
async def list_manual_payments(request: Request):
    """Admin: list all pending manual payments."""
    user = await get_current_user(request)
    db_user = await get_user_by_id(user["user_id"])
    if not db_user or db_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    from database import db as mongodb
    cursor = mongodb.manual_payments.find({}).sort("created_at", -1).limit(100)
    payments = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        payments.append(doc)
    return {"payments": payments}


@app.post("/api/admin/manual-payments/{payment_id}/approve")
async def approve_manual_payment(request: Request, payment_id: str, plan: str = Form(...)):
    """Admin: approve a manual payment and upgrade user plan."""
    user = await get_current_user(request)
    db_user = await get_user_by_id(user["user_id"])
    if not db_user or db_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    from database import db as mongodb
    from bson import ObjectId
    payment = await mongodb.manual_payments.find_one({"_id": ObjectId(payment_id)})
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found.")
    # Upgrade the user's plan
    await mongodb.users.update_one(
        {"_id": ObjectId(payment["user_id"])},
        {"$set": {"plan": plan, "screening_count": 0}}
    )
    await mongodb.manual_payments.update_one(
        {"_id": ObjectId(payment_id)},
        {"$set": {"status": "approved", "approved_by": user["email"], "approved_at": __import__("datetime").datetime.utcnow()}}
    )
    # Also create a payments history row so the user sees it on their settings page
    await save_payment({
        "user_id": payment["user_id"],
        "plan": plan,
        "amount": payment.get("amount", ""),
        "method": payment.get("payment_method", "manual"),
        "status": "paid",
        "tran_id": payment.get("transaction_id", ""),
    })
    return {"success": True, "message": f"Plan upgraded to {plan} for {payment['email']}"}


@app.post("/api/admin/manual-payments/{payment_id}/reject")
async def reject_manual_payment(request: Request, payment_id: str, status: str = Form("rejected")):
    """Admin: reject a manual payment submission (does NOT upgrade the user)."""
    user = await get_current_user(request)
    db_user = await get_user_by_id(user["user_id"])
    if not db_user or db_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    from database import db as mongodb
    from bson import ObjectId
    result = await mongodb.manual_payments.update_one(
        {"_id": ObjectId(payment_id)},
        {"$set": {
            "status": "rejected",
            "rejected_by": user["email"],
            "rejected_at": __import__("datetime").datetime.utcnow(),
        }}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Payment not found.")
    return {"success": True}


@app.get("/api/jobs/{job_id}/details")
async def get_job_details(request: Request, job_id: str):
    """Get full job details including description for auto-fill on batch page."""
    user = await get_current_user(request)
    db_user = await get_user_by_id(user["user_id"])
    is_admin = bool(db_user and db_user.get("role") == "admin")
    from database import db as mongodb
    from bson import ObjectId as BsonObjId
    # Validate ObjectId format first, separately from the find_one call,
    # so legitimate 404s aren't collapsed into 400s.
    try:
        oid = BsonObjId(job_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid job ID format.")
    query = {"_id": oid}
    if not is_admin:
        query.update(user_match_field("user_id", user["user_id"]))   # tenant isolation
    doc = await mongodb.jobs.find_one(query)
    if not doc:
        raise HTTPException(status_code=404, detail="Job not found.")
    doc["_id"] = str(doc["_id"])
    # Jobs created before public links exist have no token. Mint one on first
    # view so the link is visible immediately, rather than appearing only after
    # the toggle is found. is_public still governs whether the URL resolves.
    if not doc.get("public_token"):
        doc["public_token"] = await ensure_job_token(doc["_id"])
    return doc


@app.post("/api/user/fix-count")
async def user_fix_count(request: Request):
    """Let user sync their own screening count from actual DB."""
    user = await get_current_user(request)
    await sync_screening_count(user["user_id"])
    db_user = await get_user_by_id(user["user_id"])
    return {
        "success": True,
        "screening_count": db_user.get("screening_count", 0) if db_user else 0,
        "plan": db_user.get("plan", "trial") if db_user else "trial",
    }


@app.get("/health")
async def health():
    return {"status": "ok", "version": "5.0.0", "auth": True, "db": "mongodb"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
