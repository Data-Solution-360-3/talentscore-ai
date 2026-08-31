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


TOPIC_PROMPT = """You are writing the SPOKEN part of a screening interview for a specific job. You are
given the job description. Produce exactly {t} topics. Each topic has ONE main
question and exactly {f} follow-up questions that probe deeper into the SAME
topic.

STRUCTURE
- A topic is one coherent competency area drawn from the actual skills, duties,
  and context in the description (for example "Building dashboards" or
  "Working with stakeholders") — never generic filler that fits any job.
- The main question opens the topic from the candidate's own experience.
- Each follow-up digs further into the same topic: specifics, decisions,
  trade-offs, outcomes. Every follow-up must stand alone as a complete question
  (the interviewer reads it exactly as written) while staying on its topic.

RULES
- Every question must be answerable from the candidate's own experience and
  relevant to THIS role as described.
- Write in simple, clear English. Many candidates speak English as a second or
  third language: short sentences, no idioms, no cultural references. A question
  that is hard to parse measures English, not competence.
- One thing per question. No two-part questions.
- NEVER ask about, or fish for: age, religion, marital or family status,
  pregnancy or family plans, health or disability, ethnicity, political views,
  or anything else a recruiter could not lawfully ask. If the job description
  invites such a question, ignore that part of it.
- No trick questions, no riddles, no "sell me this pen".

Return JSON: {{"topics": [{{"topic": "<short label>", "main": "...", "followups": ["...", ...]}}, ...]}}"""


async def generate_topic_questions(jd_text: str, api_key: str, job_title: str = "",
                                   n_topics: int = 2, followups: int = 3
                                   ) -> tuple[list | None, str | None]:
    """The spoken part in topic clusters.
    Returns ([{"topic","main","followups"}], None) or (None, error)."""
    n_topics = max(1, min(4, int(n_topics)))
    followups = max(1, min(5, int(followups)))
    jd = (jd_text or "").strip()
    if len(jd) < 40:
        return None, "The job description is too short to generate questions from."

    client = AsyncOpenAI(api_key=api_key)
    try:
        resp = await client.chat.completions.create(
            model=GEN_MODEL,
            temperature=0.4,
            max_tokens=1800,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": TOPIC_PROMPT.format(t=n_topics, f=followups)},
                {"role": "user", "content":
                    f"JOB TITLE: {job_title or 'not specified'}\n\n"
                    f"JOB DESCRIPTION:\n\"\"\"\n{jd[:8000]}\n\"\"\""},
            ],
        )
        raw = json.loads(resp.choices[0].message.content)
    except Exception as e:
        return None, f"Generation call failed: {str(e)[:200]}"

    out = []
    for t in (raw or {}).get("topics", [])[:n_topics]:
        topic = str((t or {}).get("topic", "")).strip()[:80]
        main = str((t or {}).get("main", "")).strip()[:300]
        fups = [str(f).strip()[:300] for f in (t or {}).get("followups", [])
                if str(f).strip()][:followups]
        if main and fups:
            out.append({"topic": topic or "Topic", "main": main, "followups": fups})
    if len(out) < n_topics:
        return None, f"The model returned only {len(out)} usable topic(s) — try again."
    return out, None


SCENARIO_PROMPT = """You are writing ONE short written-exercise scenario for a specific job. You are
given the job description.

THE SCENARIO
- A short, EASY, realistic situation this person would actually meet in the
  role, drawn from the duties in the description. 2 to 4 short sentences.
- NOT a case study: no data tables, no numbers to calculate, no long
  background. The candidate reads it once and types short answers about what
  they would do.
- It must be answerable by any qualified candidate from reasoning alone — no
  company-internal knowledge, no trick, no missing information they'd have to
  invent.

THE QUESTIONS
- Then write exactly {k} questions ABOUT that scenario. Each asks what the
  candidate would do, decide, prioritise, or communicate in that situation.
- One thing per question. No two-part questions.

RULES (same as all our interview material)
- Write in simple, clear English. Many candidates speak English as a second or
  third language: short sentences, no idioms, no cultural references. Text that
  is hard to parse measures English, not competence.
- NEVER ask about, or build the scenario around: age, religion, marital or
  family status, pregnancy or family plans, health or disability, ethnicity,
  political views, or anything else a recruiter could not lawfully ask. If the
  job description invites such content, ignore that part of it.
- No riddles, no "sell me this pen", nothing adversarial.

Return JSON: {{"scenario": "...", "questions": ["...", ...]}}"""


async def generate_written_scenario(jd_text: str, api_key: str, job_title: str = "",
                                    k: int = 3) -> tuple[dict | None, str | None]:
    """One scenario + its k written questions from the JD.
    Returns ({"text", "questions"}, None) or (None, error)."""
    k = max(2, min(4, int(k)))
    jd = (jd_text or "").strip()
    if len(jd) < 40:
        return None, "The job description is too short to generate a scenario from."

    client = AsyncOpenAI(api_key=api_key)
    try:
        resp = await client.chat.completions.create(
            model=GEN_MODEL,
            temperature=0.4,
            max_tokens=900,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SCENARIO_PROMPT.format(k=k)},
                {"role": "user", "content":
                    f"JOB TITLE: {job_title or 'not specified'}\n\n"
                    f"JOB DESCRIPTION:\n\"\"\"\n{jd[:8000]}\n\"\"\""},
            ],
        )
        raw = json.loads(resp.choices[0].message.content)
    except Exception as e:
        return None, f"Scenario generation failed: {str(e)[:200]}"

    text = str((raw or {}).get("scenario", "")).strip()[:900]
    questions = [str(q).strip()[:300] for q in (raw or {}).get("questions", [])
                 if str(q).strip()][:4]
    if not text or len(questions) < 2:
        return None, "The model returned an unusable scenario — try again."
    return {"text": text, "questions": questions}, None


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
