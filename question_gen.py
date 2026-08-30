"""Job-based interview question generation.

One GPT-4o call per job, ON DEMAND (never per candidate): the same approved
set is used verbatim for every applicant — fairness by design, and required
for fair scoring. The 70/30 spoken/written split is ENFORCED IN CODE: the
model's format_hint is advice; deterministic adjustment makes the ratio exact,
so two generations with the same hints always land on the same mix.
"""

import json

from openai import AsyncOpenAI

GEN_MODEL = "gpt-4o"
GEN_VERSION = "qgen-1.0"

WRITTEN_RATIO = 0.3   # ~30% of questions answered in writing

GENERATION_PROMPT = """You are writing screening interview questions for a specific job. You are given
the job description. Produce exactly {n} questions.

RULES
- Every question must be answerable from the candidate's own experience and
  relevant to THIS role as described. Draw on the actual skills, duties, and
  context in the description — no generic filler that fits any job.
- Write in simple, clear English. Many candidates speak English as a second or
  third language: short sentences, no idioms, no cultural references. A question
  that is hard to parse measures English, not competence.
- One thing per question. No two-part questions.
- NEVER ask about, or fish for: age, religion, marital or family status,
  pregnancy or family plans, health or disability, ethnicity, political views,
  or anything else a recruiter could not lawfully ask. If the job description
  invites such a question, ignore that part of it.
- No trick questions, no riddles, no "sell me this pen".
- For each question, set format_hint to "written" only when a composed,
  structured answer genuinely reveals more than talking (walkthroughs of
  processes, explanations of designs or decisions); otherwise "spoken".

Return JSON: {{"questions": [{{"text": "...", "format_hint": "spoken"|"written"}}, ...]}}"""


def enforce_split(questions: list[dict], n: int) -> list[dict]:
    """Force exactly round(n * WRITTEN_RATIO) typed questions, deterministically.

    Keeps the model's hints where possible; when the count is off, demotes
    (typed->spoken) or promotes (spoken->typed) starting from the END of the
    list, so the earliest — usually most central — questions keep their
    suggested format.
    """
    target = round(n * WRITTEN_RATIO)
    typed_idx = [i for i, q in enumerate(questions) if q["mode"] == "typed"]
    if len(typed_idx) > target:
        # demote the surplus, keeping the earliest suggested ones typed
        for i in typed_idx[target:]:
            questions[i]["mode"] = "spoken"
    elif len(typed_idx) < target:
        need = target - len(typed_idx)
        for i in range(len(questions) - 1, -1, -1):
            if need <= 0:
                break
            if questions[i]["mode"] == "spoken":
                questions[i]["mode"] = "typed"
                need -= 1
    return questions


async def generate_interview_questions(jd_text: str, n: int, api_key: str,
                                       job_title: str = "") -> tuple[list | None, str | None]:
    """Returns ([{"text","mode"}], None) or (None, error)."""
    n = max(4, min(15, int(n)))
    jd = (jd_text or "").strip()
    if len(jd) < 40:
        return None, "The job description is too short to generate questions from."

    client = AsyncOpenAI(api_key=api_key)
    try:
        resp = await client.chat.completions.create(
            model=GEN_MODEL,
            temperature=0.4,
            max_tokens=2000,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": GENERATION_PROMPT.format(n=n)},
                {"role": "user", "content":
                    f"JOB TITLE: {job_title or 'not specified'}\n\n"
                    f"JOB DESCRIPTION:\n\"\"\"\n{jd[:8000]}\n\"\"\""},
            ],
        )
        raw = json.loads(resp.choices[0].message.content)
    except Exception as e:
        return None, f"Generation call failed: {str(e)[:200]}"

    out = []
    for q in (raw or {}).get("questions", [])[:n]:
        text = str((q or {}).get("text", "")).strip()[:300]
        if not text:
            continue
        out.append({"text": text,
                    "mode": "typed" if (q or {}).get("format_hint") == "written" else "spoken"})
    if len(out) < 4:
        return None, f"The model returned only {len(out)} usable questions — try again."
    return enforce_split(out, len(out)), None
