from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pathlib import Path
import uvicorn
import asyncio
import json

from scorer import extract_pdf_text, run_screening_pipeline
from batch import run_batch_screening, CONCURRENCY_LIMIT
from database import (
    connect, disconnect,
    save_screening, get_all_screenings, get_screening_by_id,
    get_screening_stats, get_skills_gap_frequency, get_dimension_averages,
    delete_screening,
    save_job, get_all_jobs, delete_job,
    create_batch_job, update_batch_progress, finish_batch_job,
    get_batch_job, get_all_batch_jobs
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect()
    yield
    await disconnect()


app = FastAPI(title="TalentScore AI", version="4.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")


# ─────────────────────────────────────────────────────────────
# FRONTEND
# ─────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    html_path = Path(__file__).parent / "templates" / "index.html"
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()



@app.get("/batch", response_class=HTMLResponse)
async def serve_batch():
    html_path = Path(__file__).parent / "templates" / "batch.html"
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()

# ─────────────────────────────────────────────────────────────
# SINGLE CV SCREENING
# ─────────────────────────────────────────────────────────────

@app.post("/api/screen")
async def screen_endpoint(
    cv_file: UploadFile = File(...),
    job_description: str = Form(...),
    api_key: str = Form(...),
):
    if not cv_file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted.")
    if not job_description or len(job_description.strip()) < 50:
        raise HTTPException(status_code=400, detail="Job description too short.")
    if not api_key or not api_key.strip().startswith("sk-"):
        raise HTTPException(status_code=400, detail="Invalid OpenAI API key.")

    file_bytes = await cv_file.read()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    cv_text, error = extract_pdf_text(file_bytes)
    if error:
        raise HTTPException(status_code=422, detail=error)

    result, error = await run_screening_pipeline(
        cv_text=cv_text,
        jd_text=job_description.strip(),
        api_key=api_key.strip()
    )
    if error:
        raise HTTPException(status_code=500, detail=error)

    # Save screening result
    import base64
    result["cv_pdf_b64"] = base64.b64encode(file_bytes).decode("utf-8")
    result["cv_filename"] = cv_file.filename
    doc_id = await save_screening(result)
    result["_id"] = doc_id
    result.pop("cv_pdf_b64", None)  # Don't send PDF bytes in response
    return result


# ─────────────────────────────────────────────────────────────
# BATCH SCREENING — with Server-Sent Events for live progress
# ─────────────────────────────────────────────────────────────

@app.post("/api/batch/screen")
async def batch_screen_endpoint(
    cv_files: list[UploadFile] = File(...),
    job_description: str = Form(...),
    api_key: str = Form(...),
):
    """
    Batch screen multiple CVs against one job description.
    Returns Server-Sent Events (SSE) stream for live progress updates.
    Each event is a JSON object with type: 'progress' | 'result' | 'done' | 'error'
    """
    # Validate
    if not api_key.strip().startswith("sk-"):
        raise HTTPException(status_code=400, detail="Invalid OpenAI API key.")
    if len(job_description.strip()) < 50:
        raise HTTPException(status_code=400, detail="Job description too short.")
    if not cv_files:
        raise HTTPException(status_code=400, detail="No files uploaded.")
    if len(cv_files) > 100:
        raise HTTPException(status_code=400, detail="Max 100 CVs per batch.")

    # Validate all files are PDFs and read bytes
    files = []
    for f in cv_files:
        if not f.filename.lower().endswith(".pdf"):
            raise HTTPException(status_code=400, detail=f"{f.filename} is not a PDF.")
        file_bytes = await f.read()
        if len(file_bytes) == 0:
            continue  # skip empty files
        files.append((f.filename, file_bytes))

    if not files:
        raise HTTPException(status_code=400, detail="All uploaded files were empty.")

    jd_text = job_description.strip()
    api_key = api_key.strip()

    # Create batch job record in MongoDB
    batch_id = await create_batch_job(total=len(files), jd_preview=jd_text[:200])

    # SSE queue — batch processor pushes events, SSE stream sends them
    queue = asyncio.Queue()

    async def on_progress(index, status, filename, result, error=None):
        """Called by batch processor for each CV update."""
        score = result.get("overall_score") if result else None
        rec   = result.get("recommendation") if result else None

        # Push to SSE queue
        event = {
            "type": "progress",
            "index": index,
            "filename": filename,
            "status": status,
            "score": round(score) if score else None,
            "recommendation": rec,
            "error": error,
            "batch_id": batch_id,
        }
        if result and status == "done":
            event["type"] = "result"
            event["result"] = {
                k: v for k, v in result.items()
                if k not in ("parsed_cv", "parsed_jd")  # keep payload small
            }
        await queue.put(event)

        # Update MongoDB
        await update_batch_progress(
            batch_id=batch_id,
            index=index, status=status,
            filename=filename, score=round(score) if score else None,
            recommendation=rec, error=error
        )

    async def event_generator():
        # Send initial event
        yield f"data: {json.dumps({'type':'start','batch_id':batch_id,'total':len(files),'concurrency':CONCURRENCY_LIMIT})}\n\n"

        # Start batch processing in background
        batch_task = asyncio.create_task(
            run_batch_screening(
                files=files,
                jd_text=jd_text,
                api_key=api_key,
                on_progress=on_progress,
            )
        )

        # Stream progress events until batch is done
        completed = 0
        while completed < len(files):
            try:
                event = await asyncio.wait_for(queue.get(), timeout=120.0)
                yield f"data: {json.dumps(event, default=str)}\n\n"
                if event.get("status") in ("done", "failed"):
                    completed += 1
            except asyncio.TimeoutError:
                yield f"data: {json.dumps({'type':'keepalive'})}\n\n"

        # Get final summary
        summary = await batch_task
        await finish_batch_job(batch_id, summary)

        # Send final done event with ranked results
        done_event = {
            "type": "done",
            "batch_id": batch_id,
            "total": summary["total"],
            "succeeded": summary["succeeded"],
            "failed": summary["failed"],
            "failed_files": summary["failed_files"],
            "ranked": [
                {
                    "rank": r.get("rank"),
                    "filename": r.get("filename"),
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

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        }
    )


# ─────────────────────────────────────────────────────────────
# BATCH HISTORY
# ─────────────────────────────────────────────────────────────

@app.get("/api/batch/jobs")
async def list_batch_jobs():
    jobs = await get_all_batch_jobs()
    return {"jobs": jobs, "count": len(jobs)}


@app.get("/api/batch/jobs/{batch_id}")
async def get_batch(batch_id: str):
    job = await get_batch_job(batch_id)
    if not job:
        raise HTTPException(status_code=404, detail="Batch job not found.")
    return job


# ─────────────────────────────────────────────────────────────
# SCREENINGS
# ─────────────────────────────────────────────────────────────

@app.get("/api/screenings")
async def list_screenings(limit: int = 200):
    screenings = await get_all_screenings(limit=limit)
    return {"screenings": screenings, "count": len(screenings)}


@app.get("/api/screenings/{screening_id}")
async def get_screening(screening_id: str):
    doc = await get_screening_by_id(screening_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Screening not found.")
    return doc


@app.delete("/api/screenings/{screening_id}")
async def delete_screening_endpoint(screening_id: str):
    deleted = await delete_screening(screening_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Screening not found.")
    return {"deleted": True}


# ─────────────────────────────────────────────────────────────
# ANALYTICS
# ─────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def stats():
    return await get_screening_stats()


@app.get("/api/analytics/skills-gaps")
async def skills_gaps():
    return {"gaps": await get_skills_gap_frequency()}


@app.get("/api/analytics/dimension-averages")
async def dimension_averages():
    return {"dimensions": await get_dimension_averages()}


# ─────────────────────────────────────────────────────────────
# JOBS
# ─────────────────────────────────────────────────────────────

@app.get("/api/jobs")
async def list_jobs():
    jobs = await get_all_jobs()
    return {"jobs": jobs, "count": len(jobs)}


@app.post("/api/jobs")
async def create_job(
    title: str = Form(...),
    department: str = Form(""),
    location: str = Form(""),
    employment_type: str = Form("Full-time"),
    skills: str = Form(""),
):
    job = {
        "title": title,
        "department": department,
        "location": location,
        "employment_type": employment_type,
        "skills": [s.strip() for s in skills.split(",") if s.strip()],
    }
    job_id = await save_job(job)
    return {"_id": job_id, **job}


@app.delete("/api/jobs/{job_id}")
async def delete_job_endpoint(job_id: str):
    deleted = await delete_job(job_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Job not found.")
    return {"deleted": True}


# ─────────────────────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "version": "4.0.0", "db": "mongodb", "batch": True}


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)


# ─────────────────────────────────────────────────────────────
# CV FILE STORAGE — store PDF in MongoDB GridFS-style (base64)
# ─────────────────────────────────────────────────────────────

import base64
from database import db

@app.post("/api/screenings/{screening_id}/upload-cv")
async def upload_cv_for_screening(screening_id: str, cv_file: UploadFile = File(...)):
    """Store the actual PDF bytes against a screening record."""
    from bson import ObjectId
    file_bytes = await cv_file.read()
    encoded = base64.b64encode(file_bytes).decode("utf-8")
    await db.screenings.update_one(
        {"_id": ObjectId(screening_id)},
        {"$set": {"cv_pdf_b64": encoded, "cv_filename": cv_file.filename}}
    )
    return {"stored": True}


@app.get("/api/screenings/{screening_id}/cv")
async def get_cv_pdf(screening_id: str):
    """Return the stored PDF for inline viewing."""
    from bson import ObjectId
    from fastapi.responses import Response
    doc = await db.screenings.find_one(
        {"_id": ObjectId(screening_id)},
        {"cv_pdf_b64": 1, "cv_filename": 1}
    )
    if not doc or not doc.get("cv_pdf_b64"):
        raise HTTPException(status_code=404, detail="CV file not found.")
    pdf_bytes = base64.b64decode(doc["cv_pdf_b64"])
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f"inline; filename={doc.get('cv_filename','cv.pdf')}"}
    )


@app.get("/candidate", response_class=HTMLResponse)
async def serve_candidate():
    html_path = Path(__file__).parent / "templates" / "candidate.html"
    with open(html_path, "r", encoding="utf-8") as f:
        return f.read()
