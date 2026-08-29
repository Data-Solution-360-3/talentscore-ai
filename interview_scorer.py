"""
Interview scorer — written-answer segment (Phase 1 of proctored-async).

A SIBLING of scorer.py, not a modification of it. It reuses the CV scorer's
architecture — two passes from opposing angles, a deterministic recompute in
code, a per-dimension audit trail — with a rubric built for interview answers,
which are a different thing from CVs.

Dimensions (0-20 each), per answer:
    Relevance · Depth & Specificity · Clarity & Structure of Thought · Role Competency

The clarity dimension scores CLARITY OF THOUGHT, explicitly not native-English
fluency — the candidates are ESL professionals and penalizing second-language
grammar is the same hidden bias the product has killed everywhere else. The
fairness block in the system prompt is load-bearing; do not trim it.

Non-answers (blank, one word, off-topic filler) are scored near zero and said
plainly — a non-answer is a real recruiter signal, not something to be generous
about. Fully blank answers never reach the model at all: code zero-scores them
deterministically, which is cheaper and exact.

Cost: ONE model call per pass for the whole submission (all answers batched),
so 2 calls per candidate regardless of question count.
"""

import asyncio
import json

from openai import AsyncOpenAI

INTERVIEW_SCORER_VERSION = "written-1.0"
SCORING_MODEL = "gpt-4o"

DIMENSIONS = [
    "Relevance",
    "Depth & Specificity",
    "Clarity & Structure of Thought",
    "Role Competency",
]

# Strict/generous blend — same 60/40 the CV scorer uses, kept as named
# constants so a future re-tune is a deliberate change, not drift.
BLEND_STRICT = 0.6
BLEND_GENEROUS = 0.4

MAX_ANSWER_CHARS = 4000

SYSTEM_PROMPT = """You are assessing a candidate's WRITTEN answers to interview questions, for a job screening. The candidate wrote in English, which is their second or third language. Score only what is asked. Return per-dimension scores 0-20 with a one-sentence justification each, citing specific evidence from the answer. Do not compute an overall score — that is done in code from your dimension scores.

Score these four dimensions (0-20 each) for EVERY answer:

1. Relevance — Does the answer actually address the question that was asked? An answer that is well-written but dodges or misreads the question scores low here.
2. Depth & Specificity — Concrete detail, real examples, evidence, specifics — versus vague generalities that could apply to anyone. Reward "I did X, which caused Y"; do not reward buzzwords with no substance.
3. Clarity & Structure of Thought — Is the REASONING organized and easy to follow? Does one point lead to the next? This measures the structure of the thinking, not the polish of the English.
4. Role Competency — Does the answer show relevant knowledge, judgment, or skill for the role?

---
CRITICAL — FAIRNESS FOR SECOND-LANGUAGE CANDIDATES. Read before scoring:

You are scoring the QUALITY OF THE CANDIDATE'S THINKING, not their command of English.

- Judge "Clarity & Structure" as the coherence and organization of the ideas — is the answer easy to follow as an argument?
- DO NOT lower any score for, and treat as completely irrelevant:
  - grammar, tense, article, or preposition mistakes
  - spelling errors or typos
  - limited vocabulary, awkward phrasing, or non-native idiom
  - short, simple sentences or plain word choice
- A candidate who makes a sharp, well-organized point in broken English must score HIGHER than one who writes fluent, grammatically perfect English that rambles or says nothing.
- If you notice yourself lowering a score because the English "sounds off" rather than because the thinking is unclear — stop. That is bias, not assessment. Re-read the answer for its substance and score that.
---
NON-ANSWERS — a separate rule from the fairness rule above:

If an answer is blank, a single word, off-topic filler, copy-pasted boilerplate that ignores the question, or otherwise a non-answer: score it near zero (0-2) on ALL dimensions and say so plainly in the reasoning — do NOT invent merit that is not in the text. A non-answer is a real signal the recruiter needs, not something to be generous about.

These two rules do not conflict: rough English with real substance scores HIGH; emptiness — fluent or not — scores near ZERO.
---

Return strict JSON:
{"answers": [{"index": <0-based>, "dimensions": [{"name": "<one of the four>", "score": <0-20>, "reason": "<one sentence citing evidence>"}], "summary": "<one neutral sentence>"}]}"""

STRICT_ANGLE = (
    "Adopt a STRICT gatekeeper perspective for this pass: award scores only for "
    "quality that is clearly evidenced in the text. Unsupported claims earn nothing."
)
GENEROUS_ANGLE = (
    "Adopt an UPSIDE-FOCUSED hiring manager perspective for this pass: give fair "
    "credit for genuine potential and partially-developed points, while still "
    "scoring non-answers near zero."
)


def _is_blank(text: str) -> bool:
    return len((text or "").strip()) < 2


def _blank_result(index: int) -> dict:
    return {
        "index": index,
        "dimensions": [
            {"name": d, "score": 0, "reason": "Blank or empty answer — scored zero deterministically, no model call."}
            for d in DIMENSIONS
        ],
        "summary": "No answer was given.",
    }


def _build_user_prompt(angle: str, job_title: str, qa_pairs: list) -> str:
    blocks = []
    for i, (q, a) in enumerate(qa_pairs):
        blocks.append(f"ANSWER {i} — QUESTION: {q}\nCANDIDATE'S ANSWER:\n\"\"\"\n{(a or '')[:MAX_ANSWER_CHARS]}\n\"\"\"")
    return (
        f"{angle}\n\nROLE BEING SCREENED FOR: {job_title or 'not specified'}\n\n"
        + "\n\n".join(blocks)
        + "\n\nScore every answer listed above. Use the exact dimension names given in the instructions."
    )


async def _pass_call(client: AsyncOpenAI, angle: str, temperature: float,
                     job_title: str, qa_pairs: list) -> dict:
    resp = await client.chat.completions.create(
        model=SCORING_MODEL,
        temperature=temperature,
        max_tokens=2500,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_prompt(angle, job_title, qa_pairs)},
        ],
    )
    return json.loads(resp.choices[0].message.content)


def _dims_by_index(pass_result: dict, n: int) -> dict:
    """index -> {dimension name -> (score, reason)}, defensively parsed."""
    out = {}
    for a in (pass_result or {}).get("answers", []):
        try:
            idx = int(a.get("index"))
        except Exception:
            continue
        if not (0 <= idx < n):
            continue
        dims = {}
        for d in a.get("dimensions", []):
            name = str(d.get("name", "")).strip()
            # Tolerate minor name drift by matching on the first word.
            match = next((D for D in DIMENSIONS if D.lower().split()[0] == name.lower().split()[0]), None) if name else None
            if match is None:
                continue
            try:
                score = max(0.0, min(20.0, float(d.get("score", 0))))
            except Exception:
                score = 0.0
            dims[match] = (score, str(d.get("reason", ""))[:400])
        out[idx] = {"dims": dims, "summary": str(a.get("summary", ""))[:300]}
    return out


async def score_written_answers(qa_pairs: list, api_key: str,
                                job_title: str = "") -> tuple[dict | None, str | None]:
    """Score a full written submission. Returns (result, error).

    The overall numbers are RECOMPUTED IN CODE from the blended per-dimension
    scores — never taken from the model. Same honest-architecture rule as the
    CV scorer: a model that returned a confident total would have it thrown away.
    """
    n = len(qa_pairs)
    if n == 0:
        return None, "No answers to score."

    # Blank answers are deterministic zeros and are excluded from the model call.
    blank_idx = {i for i, (_, a) in enumerate(qa_pairs) if _is_blank(a)}
    live_pairs = [(i, qa_pairs[i]) for i in range(n) if i not in blank_idx]

    strict_by, generous_by = {}, {}
    if live_pairs:
        client = AsyncOpenAI(api_key=api_key)
        sub = [qa for _, qa in live_pairs]
        try:
            strict_raw, generous_raw = await asyncio.gather(
                _pass_call(client, STRICT_ANGLE, 0.2, job_title, sub),
                _pass_call(client, GENEROUS_ANGLE, 0.3, job_title, sub),
            )
        except Exception as e:
            return None, f"Scoring call failed: {str(e)[:200]}"
        # Map the model's sub-indices back to original answer indices.
        remap = {si: oi for si, (oi, _) in enumerate(live_pairs)}
        strict_by = {remap[k]: v for k, v in _dims_by_index(strict_raw, len(sub)).items() if k in remap}
        generous_by = {remap[k]: v for k, v in _dims_by_index(generous_raw, len(sub)).items() if k in remap}

    answers = []
    for i in range(n):
        if i in blank_idx:
            b = _blank_result(i)
            blended = {d: 0.0 for d in DIMENSIONS}
            audit = {"strict": None, "generous": None, "blank": True}
            dims_out = b["dimensions"]
            summary = b["summary"]
        else:
            s = strict_by.get(i, {"dims": {}, "summary": ""})
            g = generous_by.get(i, {"dims": {}, "summary": ""})
            blended, dims_out = {}, []
            for d in DIMENSIONS:
                ss, sr = s["dims"].get(d, (0.0, "not scored"))
                gs, gr = g["dims"].get(d, (0.0, "not scored"))
                bl = round(ss * BLEND_STRICT + gs * BLEND_GENEROUS, 1)
                blended[d] = bl
                dims_out.append({"name": d, "score": bl, "strict": ss, "generous": gs,
                                 "reason": sr or gr})
            summary = s["summary"] or g["summary"]
            audit = {"strict": {d: s["dims"].get(d, (None,))[0] for d in DIMENSIONS},
                     "generous": {d: g["dims"].get(d, (None,))[0] for d in DIMENSIONS},
                     "blank": False}
        # Deterministic recompute: 4 dims x 20 = 80 max -> 0..100.
        overall = round(sum(blended.values()) / 80 * 100)
        answers.append({"index": i, "question": qa_pairs[i][0],
                        "dimensions": dims_out, "overall": max(0, min(100, overall)),
                        "summary": summary, "audit": audit})

    segment = round(sum(a["overall"] for a in answers) / n)
    return {
        "scorer_version": INTERVIEW_SCORER_VERSION,
        "model": SCORING_MODEL,
        "blend": {"strict": BLEND_STRICT, "generous": BLEND_GENEROUS},
        "answers": answers,
        "segment_score": max(0, min(100, segment)),
        "blank_answers": sorted(blank_idx),
    }, None
