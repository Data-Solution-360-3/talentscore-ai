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

# The one language rule injected into every generation prompt. English is the
# default and unchanged. Bangla (BETA) tells the model to write the entire set
# in Bengali script — questions are then asked + shown in Bangla, and the
# candidate answers in Bangla (typed answers are exact; spoken is transcript-BETA).
LANG_RULE = {
    "en": ("Write in simple, clear English. Many candidates speak English as a second or "
           "third language: short sentences, no idioms, no cultural references. A question "
           "that is hard to parse measures English, not competence."),
    "bn": ("Write ENTIRELY in simple, clear Bangla (Bengali script). Use short sentences and "
           "everyday words; no idioms, no cultural references, and avoid English loanwords "
           "where a common Bangla word exists. Every piece of text you output — topics, "
           "questions, and any scenario — must be in Bangla. A question that is hard to parse "
           "measures language, not competence."),
}


def _lang(language: str) -> str:
    return LANG_RULE.get((language or "en").lower(), LANG_RULE["en"])

GENERATION_PROMPT = """You are writing screening interview questions for a specific job. You are given
the job description. Produce exactly {n} questions.

RULES
- Every question must be answerable from the candidate's own experience and
  relevant to THIS role as described. Draw on the actual skills, duties, and
  context in the description — no generic filler that fits any job.
- {lang_rule}
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
- {lang_rule}
- One thing per question. No two-part questions.
- NEVER ask about, or fish for: age, religion, marital or family status,
  pregnancy or family plans, health or disability, ethnicity, political views,
  or anything else a recruiter could not lawfully ask. If the job description
  invites such a question, ignore that part of it.
- No trick questions, no riddles, no "sell me this pen".

Return JSON: {{"topics": [{{"topic": "<short label>", "main": "...", "followups": ["...", ...]}}, ...]}}"""


async def generate_topic_questions(jd_text: str, api_key: str, job_title: str = "",
                                   n_topics: int = 2, followups: int = 3,
                                   language: str = "en", role_hint: str = "",
                                   usage_out: list | None = None
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
                {"role": "system", "content": TOPIC_PROMPT.format(t=n_topics, f=followups, lang_rule=_lang(language))},
                {"role": "user", "content":
                    f"JOB TITLE: {job_title or 'not specified'}\n"
                    + (f"ROLE TYPE: {role_hint}\n" if role_hint else "")
                    + f"\nJOB DESCRIPTION:\n\"\"\"\n{jd[:8000]}\n\"\"\""},
            ],
        )
        if usage_out is not None:   # cost observability — measurement only
            usage_out.append(getattr(resp, "usage", None))
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


SCENARIO_PROMPT = """You are writing ONE substantial business case for a specific job interview. You
are given the job description.

THE CASE
- A RICH, REALISTIC business situation this person would actually face in the
  role — a real problem with meaningful context: the business setting, what
  went wrong or what is being decided, the people involved, and concrete
  specifics (numbers, timelines, constraints are welcome). 150-250 words.
- Genuinely challenging and role-appropriate: a strong candidate should have
  to think, weigh trade-offs, and justify choices — not recite a definition.
- Still self-contained: answerable by any qualified candidate from the case
  text plus professional reasoning alone. No company-internal knowledge, no
  tricks, no information they would have to invent. The case stays on screen
  while they answer.

THE MULTIPLE-CHOICE QUESTIONS ({m} of them, FIRST)
- Write exactly {m} multiple-choice questions ABOUT the case, each with 4
  plausible options and exactly ONE clearly best answer. Distractors must be
  believable choices a weaker candidate might pick — never jokes or filler.
- Test judgment about the case (what to check first, which conclusion the
  facts support, the right trade-off) — not vocabulary.

THE WRITTEN QUESTIONS ({k} of them, AFTER the multiple-choice)
- Write exactly {k} open questions ABOUT the case. Each asks what the
  candidate would do, decide, prioritise, analyse, or communicate.
- One thing per question. No two-part questions. Independent of each other.

RULES (same as all our interview material)
- {lang_rule}
- NEVER ask about, or build the scenario around: age, religion, marital or
  family status, pregnancy or family plans, health or disability, ethnicity,
  political views, or anything else a recruiter could not lawfully ask. If the
  job description invites such content, ignore that part of it.
- No riddles, no "sell me this pen", nothing adversarial.

Return JSON:
{{"scenario": "...",
  "mcq": [{{"question": "...", "options": ["...", "...", "...", "..."], "correct": <0-based index>}}, ...],
  "questions": ["...", ...]}}"""


# ── Screening-MCQ pipeline (drafter → critic → refiner, all gpt-4o) ─────
# Generation QUALITY only: the output shape, grading, storage, and the
# recruiter review/approve gate are untouched. Cost-bounded by construction:
# at most FOUR batched calls total (draft, critic, refine-failed, re-check),
# never per-question calls, never a second refine round.

def _mcq_lang(language: str) -> str:
    """MCQ-LOCAL language rule. Deliberately NOT the shared _lang(): for 'bn'
    the MCQs must read as natural BANGLISH — the way Bangladeshi professionals
    actually write — never a formal-Bengali dictionary translation. The
    interview generators keep their own behavior (out of scope here)."""
    if (language or "en").lower() == "bn":
        return (
            "Write in NATURAL BANGLA-ENGLISH MIX (Banglish), exactly the way a "
            "Bangladeshi professional writes at work: the SENTENCE language "
            "(connectives, verbs, framing) in Bangla script, but ALL technical "
            "terms, tool names, and standard English professional words stay in "
            "ENGLISH — dashboard, SQL query, report, stakeholder, deadline, "
            "data, analysis, meeting, follow-up, KPI, refresh, filter. NEVER "
            "translate these into formal Bengali (no বিশ্লেষণ for analysis, no "
            "উপাত্ত for data). Example of the CORRECT register: "
            "\"আপনি একটা monthly sales report তৈরি করছেন। Manager আগামীকাল সকালে "
            "presentation-এ regional trends দেখাতে চান — কোন approach টা best?\" "
            "WRONG register (reject-worthy): pure English sentences, or "
            "over-translated formal Bengali like \"তথ্য বিশ্লেষণের জন্য কোন "
            "পদ্ধতি ব্যবহার করবেন\".")
    return "Write everything in clear, professional ENGLISH — no other language."


def _mcq_lang_checks(language: str) -> str:
    """Critic criteria for the language register — only active for 'bn'."""
    if (language or "en").lower() == "bn":
        return (
            "   - WRONG LANGUAGE REGISTER: this set must be natural BANGLISH "
            "(Bangla sentence language + English technical/professional terms). "
            "FAIL a question that is (a) written in pure English, OR (b) "
            "over-translated into formal Bengali — technical terms like "
            "dashboard, SQL query, report, data, analysis, stakeholder must "
            "appear in ENGLISH, not as Bengali translations (বিশ্লেষণ, উপাত্ত "
            "etc.). It must read like a real BD professional wrote it.\n")
    return ""


_MCQ_FAIRNESS_RULES = """- {lang_rule}
- NEVER ask about, or write questions around: age, religion, marital or family
  status, pregnancy, health or disability, ethnicity, political views, or
  anything a recruiter could not lawfully ask."""

MCQ_DRAFT_PROMPT = """You are writing a knowledge screen for a specific job. Draft {n}
multiple-choice questions grounded in the ACTUAL day-to-day work this job
description describes, in a MIXED STRUCTURE:

STRUCTURE (both kinds, mixed)
- SCENARIO GROUPS: {n_scen} realistic role scenarios — each a substantial
  paragraph (a concrete situation with context: what's happening, who's
  involved, real numbers/constraints where natural) — followed by 3-4 MCQs
  ABOUT that scenario. The questions must genuinely depend on the scenario's
  details, not merely sit next to it.
- STANDALONE questions: the remainder — each self-framed as its own mini
  work situation.

WHAT MAKES A QUESTION GOOD HERE
- Frame EVERY question as a realistic work SCENARIO for this role: a
  situation the person is in, then what they should do, conclude, check
  first, or prioritize. NEVER a bare recall stem — no "which
  term/clause/function/feature does X", no definitions, no vocabulary.
  A knowledgeable-but-thoughtless person must not be able to ace it on
  memorized facts alone; the answer must require applying knowledge to the
  situation.
- Exactly 4 options, exactly ONE defensibly best answer.
- THE DISTRACTORS ARE THE CRAFT: each wrong option must be something a person
  with PARTIAL knowledge would genuinely pick — a real, common misconception,
  a plausible-but-inferior approach, or a right-sounding answer for a subtly
  different situation. A distractor a layperson can eliminate on sight is a
  failure. No joke options, no obviously-absurd options, and no
  universally-bad-behavior options ("ignore it", "do nothing", "assume it
  will resolve itself") — every option must be a choice a
  reasonable-but-imperfect professional might actually make, ESPECIALLY in
  judgment and soft-skills questions.
- No two options may overlap in meaning, and the correct one must not be the
  longest, most detailed, or most hedged option (length/format must not leak
  the answer).
- Self-contained: answerable from professional knowledge of the role plus the
  question (and its scenario, for grouped ones). No company-internal facts.
{fairness}

Return JSON:
{{"scenarios": [{{"scenario": "...paragraph...",
                "mcq": [{{"question": "...", "options": ["...", "...", "...", "..."], "correct": <0-based index>}}, ...]}}, ...],
  "standalone": [{{"question": "...", "options": ["...", "...", "...", "..."], "correct": <0-based index>}}, ...]}}"""

MCQ_CRITIC_PROMPT = """You are reviewing screening MCQs for a specific job. You are NOT given the
answer key. Some questions belong to a SCENARIO (shown above them) — judge
those WITH their scenario. For EACH question, do two things IN ORDER:

1. SOLVE IT BLIND: pick the single best option yourself (best_index), and
   honestly note HOW you got there.
2. JUDGE IT against these criteria — fail it if ANY apply:
   - OBVIOUS: you could pick the correct answer CONFIDENTLY without any real
     role knowledge — from wording, option shape or length, general common
     sense, or one option simply standing out. The test is your own step 1:
     if solving it felt easy and did NOT require weighing the situation with
     role knowledge, FAIL it as OBVIOUS. A good question makes even you
     hesitate between two defensible-looking options before role judgment
     settles it.
   - WEAK DISTRACTOR: any option a layperson (no role knowledge) could
     eliminate immediately. This INCLUDES universally-bad-behavior options —
     "ignore it", "do nothing", "assume they'll figure it out", "skip the
     check", "move on" — which nobody trying to pass would ever pick.
     Judgment and soft-skills questions are the usual offenders: every
     option must be a choice a reasonable-but-imperfect professional might
     actually make. The test, option by option: would at least SOME real
     candidates with partial knowledge pick it? If any option would attract
     nobody, FAIL.
   - NOT GROUNDED: not clearly about this specific role's real work as
     described in the job description.
   - TRIVIA / RECALL: answerable by memorized definition, terminology, or
     tool/keyword/clause/function knowledge ALONE. Any stem of the form
     "which term/clause/function/feature does X" FAILS — even when the
     distractors are plausible. The test: could a knowledgeable-but-
     thoughtless person ace it on recall, without weighing the situation?
     If yes, FAIL.
   - NO SCENARIO: the stem does not put the candidate in a realistic work
     situation for THIS role (a decision to make, a trade-off to weigh, a
     problem to diagnose, something to check or prioritize FIRST).
     Calibration: "While analyzing a dataset you notice missing values in a
     key column — what is the best FIRST step?" PASSES (a situation, tempting
     shortcuts as distractors, judgment required). "Which SQL concept would
     you use to retrieve data from two related tables?" FAILS (recall dressed
     as a task).
   - AMBIGUOUS: two options overlap, or more than one option is defensibly
     correct, or none clearly is.
   - DETACHED (scenario questions only): the question does not actually
     depend on its scenario's details — it could be answered identically
     with the scenario deleted.
{lang_checks}
Be strict: a question that merely "seems fine" but tests recall instead of
applied judgment must FAIL. When you are unsure between pass and fail, FAIL.

Return JSON:
{{"reviews": [{{"i": <index in the list>, "best_index": <0-3>, "pass": true|false,
"reasons": ["short, specific reasons — empty when pass"]}}, ...]}}"""

MCQ_REFINE_PROMPT = """You are fixing screening MCQs that failed review, for a specific job.
For each item you get the question, its options, the intended correct index,
and the reviewer's SPECIFIC reasons. Rewrite each question to FIX those
reasons while keeping it grounded in this job's real work. A question shown
WITH a scenario must stay about THAT scenario (revise the question, not the
scenario). You may rewrite
the stem, any option, or replace the question entirely with a better one on
the same topic area. A question failed for TRIVIA/RECALL or NO SCENARIO must
come back CONVERTED into a realistic work scenario on the same topic — a
situation, then what to do / conclude / check first — never a lightly
reworded recall stem. Same bar as before:
- 4 options, ONE defensibly best answer, distractors = genuine
  partial-knowledge misconceptions, nothing a layperson can eliminate,
  no universally-bad-behavior options ("ignore it", "do nothing"),
  no length/format leak, no recall-only stems, no ambiguity.
{fairness}

Return JSON (same order as given):
{{"mcq": [{{"question": "...", "options": ["...", "...", "...", "..."], "correct": <0-based index>}}, ...]}}"""


def _mcq_shape_ok(m) -> bool:
    try:
        return (isinstance(m, dict) and str(m.get("question", "")).strip()
                and len([o for o in (m.get("options") or []) if str(o).strip()]) == 4
                and 0 <= int(m.get("correct")) < 4)
    except Exception:
        return False


async def _gen_json(client, system: str, user: str, temperature: float,
                    max_tokens: int, usage_out: list | None) -> dict:
    resp = await client.chat.completions.create(
        model=GEN_MODEL, temperature=temperature, max_tokens=max_tokens,
        response_format={"type": "json_object"},
        messages=[{"role": "system", "content": system},
                  {"role": "user", "content": user}])
    if usage_out is not None:   # cost observability — measurement only
        usage_out.append(getattr(resp, "usage", None))
    return json.loads(resp.choices[0].message.content) or {}


async def generate_screening_mcqs(jd_text: str, api_key: str, n: int = 12,
                                  job_title: str = "", language: str = "en",
                                  role_hint: str = "", usage_out: list | None = None
                                  ) -> tuple[list | None, str | None]:
    """Mixed-structure MCQ DRAFT set via the drafter→critic→refiner loop.

    Structure: scenario groups (a paragraph + 3-4 MCQs about it) + standalone
    questions, carried as a FLAT list where grouped items share an optional
    "scenario" string — grading, storage, and review stay index-based and
    untouched. The critic BLIND-SOLVES every question (never sees the key):
    a key mismatch fails it, and so does the critic finding it EASY without
    role knowledge. Language: pure English, or natural Banglish for 'bn'
    (critic rejects over-translation). Bounded: draft(target+8) → critic →
    up to THREE refine+re-check rounds (the infinite-loop rail, not a cost
    limit), stopping early once the target passes. Draft only — the
    recruiter still reviews, edits, and approves before anything goes live.
    Returns (mcq list, None) or (None, error)."""
    n = max(5, min(20, int(n)))
    jd = (jd_text or "").strip()
    if len(jd) < 40:
        return None, "The job description is too short to generate questions from."
    client = AsyncOpenAI(api_key=api_key)
    job_ctx = (f"JOB TITLE: {job_title or 'not specified'}\n"
               + (f"ROLE TYPE: {role_hint}\n" if role_hint else "")
               + f"\nJOB DESCRIPTION:\n\"\"\"\n{jd[:8000]}\n\"\"\"")
    fairness = _MCQ_FAIRNESS_RULES.format(lang_rule=_mcq_lang(language))
    critic_prompt = MCQ_CRITIC_PROMPT.format(lang_checks=_mcq_lang_checks(language))

    def _fmt(items, with_keys=False, reasons=None):
        lines, last_scen = [], None
        for i, m in enumerate(items):
            scen = m.get("scenario")
            if scen and scen != last_scen:
                lines.append(f"SCENARIO (for the following questions):\n{scen}")
            last_scen = scen
            block = (f"[{i}] {m['question']}\n" + "\n".join(
                f"   ({oi}) {o}" for oi, o in enumerate(m["options"])))
            if with_keys:
                block += f"\n   intended correct: ({m['correct']})"
            if reasons is not None:
                block += "\n   reviewer reasons: " + "; ".join(reasons[i])
            lines.append(block)
        return "\n\n".join(lines)

    # ── 1) DRAFT target+8, mixed structure, flattened with scenario refs ──
    n_scen = 3 if n >= 12 else 2
    try:
        raw = await _gen_json(client,
                              MCQ_DRAFT_PROMPT.format(n=n + 8, n_scen=n_scen,
                                                      fairness=fairness),
                              job_ctx, 0.5, 6000, usage_out)
    except Exception as e:
        return None, f"Generation call failed: {str(e)[:200]}"
    drafts = []
    for g in (raw.get("scenarios") or [])[:n_scen + 1]:
        scen = str((g or {}).get("scenario") or "").strip()[:1200]
        if not scen:
            continue
        for m in (g.get("mcq") or [])[:4]:
            if _mcq_shape_ok(m):
                m["scenario"] = scen
                drafts.append(m)
    for m in (raw.get("standalone") or []):
        if _mcq_shape_ok(m):
            drafts.append(m)
    drafts = drafts[:n + 8]
    if len(drafts) < 3:
        return None, "Drafting produced too few valid questions — try again."

    # ── 2) CRITIC: blind-solve + judge (batched; the key is never sent) ──
    async def _critic(items):
        raw = await _gen_json(client, critic_prompt,
                              job_ctx + "\n\nQUESTIONS TO REVIEW:\n" + _fmt(items),
                              0.0, 3000, usage_out)
        verdicts = {}
        for r in (raw.get("reviews") or []):
            try:
                verdicts[int(r.get("i"))] = r
            except Exception:
                continue
        passed, failed = [], []
        for i, m in enumerate(items):
            v = verdicts.get(i)
            if v is None:
                failed.append((m, ["not reviewed"]))
                continue
            reasons = [str(x)[:160] for x in (v.get("reasons") or [])][:4]
            try:
                key_match = int(v.get("best_index")) == int(m["correct"])
            except Exception:
                key_match = False
            if not key_match:
                reasons.append("blind solver chose a different option — key is wrong or the question is ambiguous")
            if bool(v.get("pass")) and key_match:
                passed.append(m)
            else:
                failed.append((m, reasons or ["failed review"]))
        return passed, failed

    try:
        passed, failed = await _critic(drafts)
    except Exception as e:
        return None, f"Review call failed: {str(e)[:200]}"

    # ── 3) REFINE loop: up to 3 rounds, stop early at target. The round cap
    #      is the never-infinite rail; each round revises ONLY that round's
    #      failures and re-checks them. Fail-soft throughout: trouble in a
    #      round never loses questions that already passed. ──
    for _round in range(3):
        if not failed or len(passed) >= n:
            break
        items = [m for m, _ in failed]
        rs = [r for _, r in failed]
        try:
            raw = await _gen_json(client,
                                  MCQ_REFINE_PROMPT.format(fairness=fairness),
                                  job_ctx + "\n\nQUESTIONS TO FIX:\n"
                                  + _fmt(items, with_keys=True, reasons=rs),
                                  0.4, 4000, usage_out)
            refined = []
            got = [m for m in (raw.get("mcq") or []) if _mcq_shape_ok(m)]
            for i, m in enumerate(got[:len(items)]):
                # The scenario is reattached FROM THE ORIGINAL — the refiner
                # revises questions, never the scenario, and we don't trust
                # it to echo the paragraph back byte-perfect.
                if items[i].get("scenario"):
                    m["scenario"] = items[i]["scenario"]
                refined.append(m)
            if not refined:
                break
            re_passed, failed = await _critic(refined)
            passed.extend(re_passed)
        except Exception as e:
            print(f"[MCQ-GEN] refine round {_round + 1} failed (continuing with passers): {str(e)[:120]}")
            break

    # ── Assemble: scenario groups stay CONTIGUOUS (consecutive items sharing
    #    a scenario string are one group downstream), standalones follow. ──
    groups: dict = {}
    order: list = []
    for m in passed:
        key = m.get("scenario") or f"__solo_{id(m)}"
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(m)
    final = []
    for key in order:
        if len(final) >= n:
            break
        final.extend(groups[key][:max(0, n - len(final))])
    return final, None


async def generate_written_scenario(jd_text: str, api_key: str, job_title: str = "",
                                    k: int = 3, language: str = "en", role_hint: str = "",
                                    usage_out: list | None = None
                                    ) -> tuple[dict | None, str | None]:
    """One rich business case + its questions from the JD. `k` is the TOTAL
    case-question count: when k >= 4 that is 2 MCQ + (k-2) written; smaller k
    stays all-written (no room for MCQs). Correct answers are stored with the
    job for server-side grading and NEVER reach the interview model or page.
    Returns ({"text", "questions", "mcq"?}, None) or (None, error)."""
    k = max(2, min(8, int(k)))
    mcq_n = 2 if k >= 4 else 0
    wk = k - mcq_n
    jd = (jd_text or "").strip()
    if len(jd) < 40:
        return None, "The job description is too short to generate a scenario from."

    client = AsyncOpenAI(api_key=api_key)
    try:
        resp = await client.chat.completions.create(
            model=GEN_MODEL,
            temperature=0.4,
            max_tokens=1700,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SCENARIO_PROMPT.format(k=wk, m=mcq_n, lang_rule=_lang(language))},
                {"role": "user", "content":
                    f"JOB TITLE: {job_title or 'not specified'}\n"
                    + (f"ROLE TYPE: {role_hint}\n" if role_hint else "")
                    + f"\nJOB DESCRIPTION:\n\"\"\"\n{jd[:8000]}\n\"\"\""},
            ],
        )
        if usage_out is not None:   # cost observability — measurement only
            usage_out.append(getattr(resp, "usage", None))
        raw = json.loads(resp.choices[0].message.content)
    except Exception as e:
        return None, f"Scenario generation failed: {str(e)[:200]}"

    text = str((raw or {}).get("scenario", "")).strip()[:3000]
    questions = [str(q).strip()[:300] for q in (raw or {}).get("questions", [])
                 if str(q).strip()][:max(wk, 2)]
    mcq = []
    for m in ((raw or {}).get("mcq") or [])[:mcq_n]:
        if not isinstance(m, dict):
            continue
        q = str(m.get("question", "")).strip()[:300]
        opts = [str(o).strip()[:200] for o in (m.get("options") or []) if str(o).strip()][:5]
        try:
            c = int(m.get("correct"))
        except Exception:
            continue
        # Exactly FOUR options — malformed MCQs are dropped rather than
        # shipped; the recruiter sees fewer MCQs and can regenerate or add.
        if q and len(opts) == 4 and 0 <= c < 4:
            mcq.append({"q": q, "options": opts, "correct": c})
    if not text or len(questions) < 2:
        return None, "The model returned an unusable scenario — try again."
    out = {"text": text, "questions": questions}
    if mcq:
        out["mcq"] = mcq
    return out, None


async def generate_interview_questions(jd_text: str, n: int, api_key: str,
                                       job_title: str = "", language: str = "en"
                                       ) -> tuple[list | None, str | None]:
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
                {"role": "system", "content": GENERATION_PROMPT.format(n=n, lang_rule=_lang(language))},
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
