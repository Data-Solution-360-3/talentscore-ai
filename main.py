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
    get_screenings_for_user, get_stats_for_user, get_jobs_for_user,
    get_skills_gaps_for_user, get_dimension_averages_for_user, db
)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect()
    # Create default admin account if none exists
    admin = await get_user_by_email("admin@talentscore.ai")
    if not admin:
        await create_user(
            email="admin@talentscore.ai",
            hashed_password=hash_password("Admin@123"),
            company_name="TalentScore AI",
            role="admin"
        )
        print("[AUTH] Default admin created: admin@talentscore.ai / Admin@123")
    yield
    await disconnect()


app = FastAPI(title="TalentScore AI", version="5.0.0", lifespan=lifespan)

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


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    token = get_token_from_request(request)
    if token and decode_token(token):
        return RedirectResponse("/")
    return read_template("login.html")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    token = get_token_from_request(request)
    if not token or not decode_token(token):
        return RedirectResponse("/login")
    return read_template("index.html")


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
    response: Response = None,
):
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters.")
    try:
        user_id = await create_user(
            email=email,
            hashed_password=hash_password(password),
            company_name=company_name,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    token = create_token({"user_id": user_id, "email": email.lower(), "company": company_name, "role": "client"})
    resp = JSONResponse({"success": True, "company": company_name})
    resp.set_cookie("access_token", token, httponly=True, max_age=30*24*3600, samesite="lax")
    return resp


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

    token = create_token({
        "user_id": user["_id"],
        "email": user["email"],
        "company": user["company_name"],
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
    return {
        "user_id": user["user_id"],
        "email": user["email"],
        "company": user["company"],
        "role": user["role"],
    }


# ─────────────────────────────────────────────────────────────
# SINGLE CV SCREENING (no API key needed — uses server key)
# ─────────────────────────────────────────────────────────────

@app.post("/api/screen")
async def screen_endpoint(
    request: Request,
    cv_file: UploadFile = File(...),
    job_description: str = Form(...),
):
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

    # Tag with user/company
    result["user_id"] = user["user_id"]
    result["company"] = user["company"]

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
            results = await run_batch_screening(
                files=files, jd_text=jd_text,
                api_key=OPENAI_API_KEY, on_progress=on_progress
            )
            # Tag each result with user
            for r in results.get("results", []):
                r["user_id"] = user_id
                r["company"] = company
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
async def list_screenings(request: Request, limit: int = 200):
    user = await get_current_user(request)
    if user["role"] == "admin":
        screenings = await get_all_screenings(limit=limit)
    else:
        screenings = await get_screenings_for_user(user["user_id"], limit=limit)
    return {"screenings": screenings, "count": len(screenings)}


@app.get("/api/screenings/{screening_id}")
async def get_screening(request: Request, screening_id: str):
    user = await get_current_user(request)
    doc = await get_screening_by_id(screening_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Not found.")
    if user["role"] != "admin" and doc.get("user_id") != user["user_id"]:
        raise HTTPException(status_code=403, detail="Access denied.")
    return doc


@app.delete("/api/screenings/{screening_id}")
async def delete_screening_endpoint(request: Request, screening_id: str):
    user = await get_current_user(request)
    doc = await get_screening_by_id(screening_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Not found.")
    if user["role"] != "admin" and doc.get("user_id") != user["user_id"]:
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
    if user["role"] != "admin" and doc.get("user_id") != user["user_id"]:
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

@app.get("/health")
async def health():
    return {"status": "ok", "version": "5.0.0", "auth": True, "db": "mongodb"}


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
