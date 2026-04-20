"""
batch.py — Batch CV screening engine
=====================================
- Accepts up to 100 PDFs at once
- Processes in parallel with a concurrency limit (default: 5 at a time)
  to avoid hitting OpenAI rate limits
- Tracks live progress per CV
- Returns ranked results sorted by score
- Stores each result in MongoDB
"""

import asyncio
from typing import Callable
from scorer import extract_pdf_text, run_screening_pipeline
from database import save_screening


# Max parallel OpenAI calls at once — safe for most OpenAI plans
# Increase to 10 if you're on a higher-tier plan
CONCURRENCY_LIMIT = 5


async def screen_single_cv(
    filename: str,
    file_bytes: bytes,
    jd_text: str,
    api_key: str,
    semaphore: asyncio.Semaphore,
    on_progress: Callable,
    index: int,
) -> dict:
    """
    Screen one CV. Called concurrently for all CVs in the batch.
    Uses a semaphore to limit how many run at the same time.
    """
    async with semaphore:
        # Notify frontend this CV is now being processed
        await on_progress(index, "processing", filename, None)

        # Step 1: Extract text from PDF
        cv_text, error = extract_pdf_text(file_bytes)
        if error:
            await on_progress(index, "failed", filename, None, error=error)
            return {
                "index": index,
                "filename": filename,
                "status": "failed",
                "error": error,
            }

        # Step 2: Run 3-step AI pipeline
        try:
            result, error = await run_screening_pipeline(
                cv_text=cv_text,
                jd_text=jd_text,
                api_key=api_key
            )
            if error:
                await on_progress(index, "failed", filename, None, error=error)
                return {
                    "index": index,
                    "filename": filename,
                    "status": "failed",
                    "error": error,
                }

            # Step 3: Save to MongoDB (include PDF bytes)
            import base64
            doc_id = await save_screening({
                **result,
                "source_file": filename,
                "batch": True,
                "cv_pdf_b64": base64.b64encode(file_bytes).decode("utf-8"),
                "cv_filename": filename
            })
            result["_id"] = doc_id
            result["filename"] = filename
            result["index"] = index
            result["status"] = "done"

            await on_progress(index, "done", filename, result)
            return result

        except Exception as e:
            error_msg = str(e)
            await on_progress(index, "failed", filename, None, error=error_msg)
            return {
                "index": index,
                "filename": filename,
                "status": "failed",
                "error": error_msg,
            }


async def run_batch_screening(
    files: list[tuple[str, bytes]],  # list of (filename, bytes)
    jd_text: str,
    api_key: str,
    on_progress: Callable,
    concurrency: int = CONCURRENCY_LIMIT,
) -> dict:
    """
    Screen a batch of CVs concurrently.

    Args:
        files: List of (filename, file_bytes) tuples
        jd_text: The job description text
        api_key: OpenAI API key
        on_progress: Async callback(index, status, filename, result, error=None)
        concurrency: Max parallel AI calls

    Returns:
        {
            "total": int,
            "succeeded": int,
            "failed": int,
            "results": [...sorted by score descending...],
            "failed_files": [...list of failed filenames...]
        }
    """
    semaphore = asyncio.Semaphore(concurrency)

    # Launch all tasks concurrently (semaphore controls actual parallelism)
    tasks = [
        screen_single_cv(
            filename=filename,
            file_bytes=file_bytes,
            jd_text=jd_text,
            api_key=api_key,
            semaphore=semaphore,
            on_progress=on_progress,
            index=i,
        )
        for i, (filename, file_bytes) in enumerate(files)
    ]

    all_results = await asyncio.gather(*tasks, return_exceptions=False)

    # Separate successes from failures
    succeeded = [r for r in all_results if r.get("status") == "done"]
    failed    = [r for r in all_results if r.get("status") == "failed"]

    # Sort successes by score descending (best candidate first)
    succeeded.sort(key=lambda r: r.get("overall_score", 0), reverse=True)

    # Add rank
    for i, r in enumerate(succeeded):
        r["rank"] = i + 1

    return {
        "total":        len(files),
        "succeeded":    len(succeeded),
        "failed":       len(failed),
        "results":      succeeded,
        "failed_files": [r["filename"] for r in failed],
        "errors":       failed,
    }
