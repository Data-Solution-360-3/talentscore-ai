from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Request, Response, Depends
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
from payment_service import (
    create_stripe_checkout, verify_stripe_webhook,
    create_sslcommerz_payment, verify_sslcommerz_payment,
    cancel_stripe_subscription, create_stripe_portal_session,
    STRIPE_PUBLISHABLE_KEY, PLANS
)
from email_service import generate_otp, send_verification_email, send_welcome_email
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
    update_user, increment_screening_count,
    store_otp, verify_otp, delete_pending,
    get_screenings_for_user, get_stats_for_user, get_jobs_for_user,
    get_skills_gaps_for_user, get_dimension_averages_for_user, db,
    save_payment, get_payments_for_user, update_user_subscription,
    invite_team_member, get_team_members, get_team_invites,
    update_user_profile, update_user_notifications, get_full_user
)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect()
    from database import db as mongodb
    from bson import ObjectId

    # Create default admin account if none exists
    admin = await get_user_by_email("admin@talentscore.ai")
    if not admin:
        await create_user(
            email="admin@talentscore.ai",
            hashed_password=hash_password("Admin@123"),
            company_name="TopCandidate",
            role="admin"
        )
        print("[AUTH] Default admin created: admin@talentscore.ai / Admin@123")

    # Always make tarafdersakib08@gmail.com admin (role only, no data migration)
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

    result, error = await run_screening_pipeline(
        cv_text=cv_text,
        jd_text=job_description.strip(),
        api_key=OPENAI_API_KEY
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

    if not OPENAI_API_KEY:
        raise HTTPException(status_code=500, detail="OpenAI API key not configured on server.")
    if len(job_description.strip()) < 50:
        raise HTTPException(status_code=400, detail="Job description too short.")
    if not cv_files:
        raise HTTPException(status_code=400, detail="No files uploaded.")
    if len(cv_files) > 100:
        raise HTTPException(status_code=400, detail="Max 100 CVs per batch.")

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

            results = await run_batch_screening(
                files=files, jd_text=jd_text,
                api_key=OPENAI_API_KEY, on_progress=on_progress,
                extra_fields=extra
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
            try:
                event = await asyncio.wait_for(queue.get(), timeout=120.0)
                yield f"data: {json.dumps(event, default=str)}\n\n"
                if event.get("status") in ("done", "failed"):
                    completed += 1
            except asyncio.TimeoutError:
                yield f"data: {json.dumps({'type':'keepalive'})}\n\n"

        summary = await batch_task
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
        await increment_screening_count(user_id)

    return StreamingResponse(
        event_generator(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )


# ─────────────────────────────────────────────────────────────
# SCREENINGS (tenant-scoped)
# ─────────────────────────────────────────────────────────────

@app.get("/api/screenings")
async def list_screenings(request: Request, limit: int = 500):
    user = await get_current_user(request)
    try:
        db_user = await get_user_by_id(user["user_id"])
        fresh_role = db_user.get("role", "client") if db_user else user.get("role", "client")
    except Exception:
        fresh_role = user.get("role", "client")

    if fresh_role == "admin":
        screenings = await get_all_screenings(limit=limit)
    else:
        screenings = await get_screenings_for_user(user["user_id"], limit=limit)

    return {"screenings": screenings, "count": len(screenings)}


@app.post("/api/admin/fix-user-role")
async def fix_user_role(request: Request, email: str = Form(...), role: str = Form("admin")):
    """Fix user role — accessible without auth for emergency use."""
    from database import db
    from bson import ObjectId
    target = await get_user_by_email(email)
    if not target:
        raise HTTPException(status_code=404, detail=f"User {email} not found.")
    await db.users.update_one(
        {"_id": ObjectId(target["_id"])},
        {"$set": {"role": role}}
    )
    return {"success": True, "email": email, "new_role": role}


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


@app.get("/api/screenings/{screening_id}/cv")
async def get_cv_pdf(request: Request, screening_id: str):
    from bson import ObjectId
    from fastapi.responses import Response as FastResponse
    user = await get_current_user(request)
    doc = await db.screenings.find_one({"_id": ObjectId(screening_id)}, {"cv_pdf_b64": 1, "cv_filename": 1, "user_id": 1})
    if not doc or not doc.get("cv_pdf_b64"):
        raise HTTPException(status_code=404, detail="CV file not found.")
    if user["role"] != "admin" and doc.get("user_id") and doc.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Access denied.")
    pdf_bytes = base64.b64decode(doc["cv_pdf_b64"])
    return FastResponse(content=pdf_bytes, media_type="application/pdf",
                        headers={"Content-Disposition": f"inline; filename={doc.get('cv_filename','cv.pdf')}"})


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
    return {"jobs": jobs, "count": len(jobs)}


@app.post("/api/jobs")
async def create_job_endpoint(
    request: Request,
    title: str = Form(...),
    department: str = Form(""),
    location: str = Form(""),
    employment_type: str = Form("Full-time"),
    skills: str = Form(""),
):
    user = await get_current_user(request)
    job = {
        "title": title, "department": department, "location": location,
        "employment_type": employment_type,
        "skills": [s.strip() for s in skills.split(",") if s.strip()],
        "user_id": user["user_id"], "company": user["company"],
    }
    job_id = await save_job(job)
    return {"_id": job_id, **job}


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
    user = await get_current_user(request)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only.")
    if plan not in ["trial","starter","pro","enterprise"]:
        raise HTTPException(status_code=400, detail="Invalid plan.")
    await update_user(user_id, {"plan": plan})
    return {"plan": plan}


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
    try:
        invite_id = await invite_team_member(
            owner_user_id=user["user_id"],
            email=email,
            role=role,
            company_name=user["company"],
        )
        return {"success": True, "invite_id": invite_id}
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
async def check_users_screenings():
    """Show all users and their actual screening counts in DB."""
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


@app.post("/api/admin/assign-to-email/{email}")
async def assign_screenings_to_email(email: str):
    """Assign ALL unassigned screenings to a specific email."""
    from database import db as mongodb
    from bson import ObjectId
    user = await get_user_by_email(email)
    if not user:
        return {"error": f"User {email} not found"}
    uid = user["_id"]
    result = await mongodb.screenings.update_many(
        {"$or": [{"user_id": {"$exists": False}}, {"user_id": None}, {"user_id": ""}]},
        {"$set": {"user_id": uid}}
    )
    count = await mongodb.screenings.count_documents({"user_id": uid})
    await mongodb.users.update_one({"_id": ObjectId(uid)}, {"$set": {"screening_count": count}})
    return {"assigned": result.modified_count, "total_for_user": count, "user": email}


@app.get("/api/admin/fix-counts")
async def fix_all_counts():
    """Recalculate and fix screening_count for all users."""
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


@app.get("/api/fix-now")
async def fix_now():
    """One-time fix: assign all unowned screenings to tarafdersakib08@gmail.com"""
    from database import db as mongodb
    from bson import ObjectId
    
    # Find tarafdersakib
    user = await get_user_by_email("tarafdersakib08@gmail.com")
    if not user:
        return {"error": "User not found"}
    
    uid = user["_id"]
    
    # Make admin
    await mongodb.users.update_one(
        {"_id": ObjectId(uid)},
        {"$set": {"role": "admin"}}
    )
    
    # Assign ALL unowned screenings
    r1 = await mongodb.screenings.update_many(
        {"$or": [
            {"user_id": {"$exists": False}},
            {"user_id": None},
            {"user_id": ""}
        ]},
        {"$set": {"user_id": uid, "company": user.get("company_name","Data Solution 360")}}
    )
    
    # Count total screenings for this user
    total = await mongodb.screenings.count_documents({"user_id": uid})
    
    # Update screening count
    await mongodb.users.update_one(
        {"_id": ObjectId(uid)},
        {"$set": {"screening_count": total, "role": "admin"}}
    )
    
    return {
        "success": True,
        "user_id": uid,
        "email": "tarafdersakib08@gmail.com",
        "role": "admin",
        "screenings_assigned": r1.modified_count,
        "total_screenings": total,
        "message": f"Done! Now sign out and sign back in at /login"
    }


@app.get("/api/debug/my-screenings")
async def debug_screenings(request: Request):
    """Debug: show what user_id is in token vs what screenings exist."""
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


@app.get("/health")
async def health():
    return {"status": "ok", "version": "5.0.0", "auth": True, "db": "mongodb"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
