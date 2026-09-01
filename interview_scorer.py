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


# ── Bangla (BETA) scoring addenda — appended to the base prompt ONLY when the
#    interview language is Bangla, so English scoring stays byte-identical (the
#    fairness gate keeps passing). Same philosophy: substance over language. ──
_BN_TEXT_ADDENDUM = (
    "\n\nLANGUAGE — BANGLA (BETA). The candidate wrote in BANGLA (Bengali), their own "
    "language. Read and score the Bangla directly. Every fairness rule above applies "
    "IDENTICALLY to Bangla: you are scoring the substance and clarity of the THINKING, "
    "never the polish of the language. Do not lower any score for regional word choice, "
    "spelling, or grammar. Rough Bangla that makes a sharp, well-organized point must score "
    "HIGHER than fluent Bangla that rambles or says nothing. Emptiness — fluent or not — "
    "still scores near zero.")
_BN_SPOKEN_ADDENDUM = (
    "\n\nLANGUAGE — BANGLA (BETA). The candidate spoke BANGLA, and this transcript was "
    "produced by machine speech recognition of BANGLA, which is materially LESS reliable "
    "than English — expect more garbled phrases, wrong words, and dropped words. Treat "
    "every garble as MACHINE error, never the candidate's fault: read past transcription "
    "noise for the intended meaning and never lower a score because the transcript reads "
    "rough. Score the substance of the thinking, not language polish. If a passage is too "
    "garbled to interpret, do NOT invent a weakness — note the uncertainty and score only "
    "what is legible. A sharp point in rough Bangla beats fluent emptiness; a genuine "
    "non-answer still floors near zero.")


def _sys(base: str, language: str, spoken: bool = False) -> str:
    """Base prompt for English (unchanged); base + Bangla addendum for 'bn'."""
    if (language or "en").lower() != "bn":
        return base
    return base + (_BN_SPOKEN_ADDENDUM if spoken else _BN_TEXT_ADDENDUM)


async def _pass_call(client: AsyncOpenAI, angle: str, temperature: float,
                     job_title: str, qa_pairs: list, language: str = "en") -> dict:
    resp = await client.chat.completions.create(
        model=SCORING_MODEL,
        temperature=temperature,
        max_tokens=2500,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": _sys(SYSTEM_PROMPT, language)},
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
                                job_title: str = "", language: str = "en"
                                ) -> tuple[dict | None, str | None]:
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
                _pass_call(client, STRICT_ANGLE, 0.2, job_title, sub, language),
                _pass_call(client, GENEROUS_ANGLE, 0.3, job_title, sub, language),
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


# ─────────────────────────────────────────────────────────────
# SCENARIO scorer — the written section as ONE coherent response.
# The candidate saw a single workplace scenario and typed answers to 2-4
# questions about it. Scoring them separately would punish natural
# cross-referencing ("as I said above, I'd tell the manager first"), so the
# SET gets one score, with the scenario given to the model as the context
# the answers must actually engage with.

SCENARIO_SCORER_VERSION = "scenario-1.0"

SCENARIO_SYSTEM_PROMPT = """You are assessing a candidate's WRITTEN answers to a scenario exercise, for a job screening. The candidate was shown ONE short workplace scenario and typed answers to a few questions about it. The candidate wrote in English, which is their second or third language.

Evaluate the answers TOGETHER, as one coherent response to the scenario. An answer may build on an earlier one — that is normal and good, not repetition. Score only what is asked. Return per-dimension scores 0-20 for the SET AS A WHOLE, with a one-sentence justification each citing specific evidence from the answers. Do not compute an overall score — that is done in code from your dimension scores.

Score these four dimensions (0-20 each) for the set:

1. Relevance — Do the answers actually engage with THIS scenario and address the questions asked about it? Generic advice that ignores the situation described scores low here, however polished.
2. Depth & Specificity — Concrete actions, steps, and judgments grounded in the scenario's details — versus vague generalities that fit any situation.
3. Clarity & Structure of Thought — Is the REASONING organized and easy to follow across the answers? Does the candidate's approach to the situation hang together? This measures the structure of the thinking, not the polish of the English.
4. Role Competency — Do the answers show the knowledge, judgment, and priorities you would want from someone in this role facing this situation?

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

If an individual answer is blank, a single word, off-topic filler, or otherwise a non-answer: say so plainly in that answer's note and weigh the set down accordingly — do NOT invent merit that is not in the text. If ALL the answers are non-answers, score near zero (0-2) on ALL dimensions. A non-answer is a real signal the recruiter needs, not something to be generous about.

These two rules do not conflict: rough English with real substance scores HIGH; emptiness — fluent or not — scores near ZERO.
---

Return strict JSON:
{"dimensions": [{"name": "<one of the four>", "score": <0-20>, "reason": "<one sentence citing evidence>"}], "per_answer_notes": ["<one short neutral sentence per answer, in order>"], "summary": "<one neutral sentence on the set>"}"""


def _build_scenario_prompt(angle: str, job_title: str, scenario: str, qa_pairs: list) -> str:
    blocks = []
    for i, (q, a) in enumerate(qa_pairs):
        blocks.append(f"QUESTION {i+1}: {q}\nCANDIDATE'S TYPED ANSWER:\n\"\"\"\n{(a or '')[:MAX_ANSWER_CHARS]}\n\"\"\"")
    return (
        f"{angle}\n\nROLE BEING SCREENED FOR: {job_title or 'not specified'}\n\n"
        f"THE SCENARIO THE CANDIDATE WAS SHOWN:\n\"\"\"\n{(scenario or '')[:1200]}\n\"\"\"\n\n"
        + "\n\n".join(blocks)
        + "\n\nScore the set of answers as one response to the scenario. Use the exact dimension names given in the instructions."
    )


def _scenario_dims(raw: dict) -> tuple[dict, list, str]:
    """{dimension -> (score, reason)}, per-answer notes, summary — defensively parsed."""
    dims = {}
    for d in (raw or {}).get("dimensions", []):
        name = str(d.get("name", "")).strip()
        match = next((D for D in DIMENSIONS if D.lower().split()[0] == name.lower().split()[0]), None) if name else None
        if match is None:
            continue
        try:
            score = max(0.0, min(20.0, float(d.get("score", 0))))
        except Exception:
            score = 0.0
        dims[match] = (score, str(d.get("reason", ""))[:400])
    notes = [str(x)[:300] for x in (raw or {}).get("per_answer_notes", [])][:6]
    return dims, notes, str((raw or {}).get("summary", ""))[:300]


async def score_scenario_answers(scenario: str, qa_pairs: list, api_key: str,
                                 job_title: str = "", language: str = "en"
                                 ) -> tuple[dict | None, str | None]:
    """Score the scenario answer SET. Returns (result, error).

    Same honest-architecture rules as every other scorer: strict/generous
    dual pass with the blend and overall recomputed in code, and an all-blank
    set is a deterministic zero with no model call.
    """
    if not qa_pairs:
        return None, "No scenario answers to score."
    blank_idx = sorted(i for i, (_, a) in enumerate(qa_pairs) if _is_blank(a))

    if len(blank_idx) == len(qa_pairs):
        dims_out = [{"name": d, "score": 0, "strict": 0, "generous": 0,
                     "reason": "Every answer was blank — scored zero deterministically, no model call."}
                    for d in DIMENSIONS]
        return {
            "scorer_version": SCENARIO_SCORER_VERSION, "model": SCORING_MODEL,
            "blend": {"strict": BLEND_STRICT, "generous": BLEND_GENEROUS},
            "scenario": (scenario or "")[:1200],
            "qa": [{"question": q, "answer": (a or "")[:MAX_ANSWER_CHARS]} for q, a in qa_pairs],
            "dimensions": dims_out, "overall": 0,
            "per_answer_notes": ["No answer was given." for _ in qa_pairs],
            "summary": "No answers were given to the scenario.",
            "audit": {"strict": None, "generous": None, "all_blank": True},
            "blank_answers": blank_idx,
        }, None

    client = AsyncOpenAI(api_key=api_key)

    async def _call(angle: str, temperature: float) -> dict:
        resp = await client.chat.completions.create(
            model=SCORING_MODEL, temperature=temperature, max_tokens=1600,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _sys(SCENARIO_SYSTEM_PROMPT, language)},
                {"role": "user", "content": _build_scenario_prompt(angle, job_title, scenario, qa_pairs)},
            ],
        )
        return json.loads(resp.choices[0].message.content)

    try:
        strict_raw, generous_raw = await asyncio.gather(
            _call(STRICT_ANGLE, 0.2), _call(GENEROUS_ANGLE, 0.3))
    except Exception as e:
        return None, f"Scenario scoring call failed: {str(e)[:200]}"

    s_dims, s_notes, s_sum = _scenario_dims(strict_raw)
    g_dims, g_notes, g_sum = _scenario_dims(generous_raw)
    blended, dims_out = {}, []
    for d in DIMENSIONS:
        ss, sr = s_dims.get(d, (0.0, "not scored"))
        gs, gr = g_dims.get(d, (0.0, "not scored"))
        bl = round(ss * BLEND_STRICT + gs * BLEND_GENEROUS, 1)
        blended[d] = bl
        dims_out.append({"name": d, "score": bl, "strict": ss, "generous": gs, "reason": sr or gr})
    overall = max(0, min(100, round(sum(blended.values()) / 80 * 100)))

    return {
        "scorer_version": SCENARIO_SCORER_VERSION, "model": SCORING_MODEL,
        "blend": {"strict": BLEND_STRICT, "generous": BLEND_GENEROUS},
        "scenario": (scenario or "")[:1200],
        "qa": [{"question": q, "answer": (a or "")[:MAX_ANSWER_CHARS]} for q, a in qa_pairs],
        "dimensions": dims_out, "overall": overall,
        "per_answer_notes": s_notes or g_notes,
        "summary": s_sum or g_sum,
        "audit": {"strict": {d: s_dims.get(d, (None,))[0] for d in DIMENSIONS},
                  "generous": {d: g_dims.get(d, (None,))[0] for d in DIMENSIONS},
                  "all_blank": False},
        "blank_answers": blank_idx,
    }, None


# ─────────────────────────────────────────────────────────────
# SPOKEN interview scorer (L4) — whole-conversation, not per-answer.
# A live follow-up answer ("yes, exactly — it was the cache") is legitimately
# three words BECAUSE of context; per-exchange scoring would punish the very
# conversational quality the live interview is built for. One set of four
# dimension scores for the interview, every score anchored to a quoted piece
# of transcript evidence so the recruiter can verify each claim.
# ─────────────────────────────────────────────────────────────

SPOKEN_SCORER_VERSION = "spoken-1.0"
MAX_TRANSCRIPT_CHARS = 16000   # raised from 8k: a 10-question job-based
# interview would otherwise be silently truncated before scoring — which would
# quietly break the fairness story for long interviews.

SPOKEN_SYSTEM_PROMPT = """You are assessing the transcript of a LIVE SPOKEN screening interview conducted by an AI interviewer. The candidate spoke English as their second or third language. The transcript was produced by machine speech recognition of accented English and may contain transcription errors — garbled phrases, wrong homophones, dropped words. Score only what is asked. Return per-dimension scores 0-20 with a one-sentence justification each, quoting a short piece of evidence from the transcript for every score. Do not compute an overall score — that is done in code from your dimension scores.

Score these four dimensions (0-20) for the interview as a whole:

1. Relevance — Did the answers address the questions actually asked? Dodging or generic redirection scores low.
2. Depth & Specificity — Concrete examples, real events, specifics ("I did X, which caused Y") versus vague generalities that could apply to anyone.
3. Clarity & Structure of Thought — Is the REASONING organized and followable as spoken conversation? This measures the structure of the thinking, never the polish of the English.
4. Role Competency — Does the conversation show relevant knowledge, judgment, or skill for the role?

---
LENGTH NORMALIZATION — the interview's question count must not move the score:

You are scoring the QUALITY of the answers given, never their quantity. Interviews come in different lengths (6 questions, 12 questions) set by the recruiter — that is not the candidate's doing. A short interview with excellent answers scores exactly as high as a long interview with excellent answers. Answering more questions adequately earns no extra credit, and having been asked fewer questions is no penalty. Judge the consistent pattern of quality across whatever was asked, as a rate — not an accumulation.
---
SPOKEN LANGUAGE — read before scoring:
- Fillers ("um", "you know"), false starts, self-corrections, and repetition are normal speech, not disorganized thinking. Never penalize them.
- Short conversational answers to follow-up questions are normal — judge them in the context of the question asked, not against essay length.
- If a phrase reads as garbled or nonsensical, treat it charitably as likely transcription error, not candidate error. Never penalize what the machine may have misheard.
---
CRITICAL — FAIRNESS FOR SECOND-LANGUAGE CANDIDATES. Read before scoring:

You are scoring the quality of the candidate's thinking, not their command of English.

- Judge "Clarity & Structure" as the coherence and organization of the ideas — is the line of thinking easy to follow?
- DO NOT lower any score for, and treat as completely irrelevant: grammar, tense, article, or preposition mistakes; limited vocabulary, awkward phrasing, or non-native idiom; short, simple sentences or plain word choice; accent-driven phrasing.
- A candidate who makes a sharp, well-organized point in broken English must score HIGHER than one who speaks fluent, polished English that rambles or says nothing.
- If you notice yourself lowering a score because the English "sounds off" rather than because the thinking is unclear — stop. That is bias, not assessment. Re-read the exchange for its substance and score that.
---
NON-ANSWERS — a separate rule from the fairness rule above:

If the candidate's answers are consistently blank, single-word, evasive, off-topic filler, or otherwise non-answers: score near zero (0-2) on the affected dimensions and say so plainly in the reasoning — do NOT invent merit that is not in the transcript. A non-answer is a real signal the recruiter needs, not something to be generous about.

These two rules do not conflict: rough English with real substance scores HIGH; emptiness — fluent or not — scores near ZERO.
---
Return strict JSON:
{"dimensions": [{"name": "<one of the four>", "score": <0-20>, "reason": "<one sentence>", "evidence": "<short transcript quote>"}], "summary": "<one neutral sentence>"}"""


def _render_transcript(transcript: list) -> str:
    lines = []
    for t in transcript:
        role = "Interviewer" if (t or {}).get("role") == "ai" else "Candidate"
        text = str((t or {}).get("text", "")).strip()
        if text:
            lines.append(f"{role}: {text[:600]}")
    return "\n".join(lines)[-MAX_TRANSCRIPT_CHARS:]


def _spoken_zero(reason: str) -> dict:
    return {
        "scorer_version": SPOKEN_SCORER_VERSION,
        "model": SCORING_MODEL,
        "blend": {"strict": BLEND_STRICT, "generous": BLEND_GENEROUS},
        "dimensions": [{"name": d, "score": 0.0, "strict": 0.0, "generous": 0.0,
                        "reason": reason, "evidence": ""} for d in DIMENSIONS],
        "overall": 0,
        "summary": reason,
        "audit": {"deterministic_zero": True},
    }


async def _spoken_pass(client: AsyncOpenAI, angle: str, temperature: float,
                       job_title: str, convo: str, language: str = "en") -> dict:
    resp = await client.chat.completions.create(
        model=SCORING_MODEL,
        temperature=temperature,
        max_tokens=1800,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": _sys(SPOKEN_SYSTEM_PROMPT, language, spoken=True)},
            {"role": "user", "content":
                f"{angle}\n\nROLE BEING SCREENED FOR: {job_title or 'not specified'}\n\n"
                f"INTERVIEW TRANSCRIPT:\n\"\"\"\n{convo}\n\"\"\"\n\n"
                "Score the four dimensions for this interview as a whole."},
        ],
    )
    return json.loads(resp.choices[0].message.content)


def _spoken_dims(raw: dict) -> dict:
    """dimension name -> (score, reason, evidence), defensively parsed."""
    out = {}
    for d in (raw or {}).get("dimensions", []):
        name = str(d.get("name", "")).strip()
        match = next((D for D in DIMENSIONS if D.lower().split()[0] == name.lower().split()[0]), None) if name else None
        if match is None:
            continue
        try:
            score = max(0.0, min(20.0, float(d.get("score", 0))))
        except Exception:
            score = 0.0
        out[match] = (score, str(d.get("reason", ""))[:400], str(d.get("evidence", ""))[:300])
    return out


async def score_spoken_interview(transcript: list, api_key: str,
                                 job_title: str = "", language: str = "en"
                                 ) -> tuple[dict | None, str | None]:
    """Score a live-interview transcript. Returns (result, error).

    Same honest architecture as every scorer in this product: two passes from
    opposing angles, blended 60/40, and the overall number RECOMPUTED IN CODE
    from the blended dimension scores — never taken from the model.
    """
    candidate_text = " ".join(
        str((t or {}).get("text", "")).strip()
        for t in transcript if (t or {}).get("role") == "you")
    if not candidate_text.strip():
        # No candidate speech at all — deterministic zero, no model call.
        return _spoken_zero("The candidate said nothing scorable in this interview."), None

    convo = _render_transcript(transcript)
    client = AsyncOpenAI(api_key=api_key)
    try:
        strict_raw, generous_raw = await asyncio.gather(
            _spoken_pass(client, STRICT_ANGLE, 0.2, job_title, convo, language),
            _spoken_pass(client, GENEROUS_ANGLE, 0.3, job_title, convo, language),
        )
    except Exception as e:
        return None, f"Scoring call failed: {str(e)[:200]}"

    s, g = _spoken_dims(strict_raw), _spoken_dims(generous_raw)
    dims_out, blended = [], {}
    for d in DIMENSIONS:
        ss, sr, se = s.get(d, (0.0, "not scored", ""))
        gs, gr, ge = g.get(d, (0.0, "not scored", ""))
        bl = round(ss * BLEND_STRICT + gs * BLEND_GENEROUS, 1)
        blended[d] = bl
        dims_out.append({"name": d, "score": bl, "strict": ss, "generous": gs,
                         "reason": sr or gr, "evidence": se or ge})
    overall = max(0, min(100, round(sum(blended.values()) / 80 * 100)))
    return {
        "scorer_version": SPOKEN_SCORER_VERSION,
        "model": SCORING_MODEL,
        "blend": {"strict": BLEND_STRICT, "generous": BLEND_GENEROUS},
        "dimensions": dims_out,
        "overall": overall,
        "summary": str((strict_raw or {}).get("summary") or (generous_raw or {}).get("summary") or "")[:300],
        "audit": {"strict": {d: s.get(d, (None,))[0] for d in DIMENSIONS},
                  "generous": {d: g.get(d, (None,))[0] for d in DIMENSIONS}},
    }, None
