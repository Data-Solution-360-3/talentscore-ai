"""Job-based interview question generation.

One GPT-4o call per job, ON DEMAND (never per candidate): the same approved
set is used verbatim for every applicant — fairness by design, and required
for fair scoring. The 70/30 spoken/written split is ENFORCED IN CODE: the
model's format_hint is advice; deterministic adjustment makes the ratio exact,
so two generations with the same hints always land on the same mix.
"""

import asyncio
import json
import re

from openai import AsyncOpenAI

GEN_MODEL = "gpt-4o"
GEN_VERSION = "qgen-2.4"   # 2.4: exam item-writing standard (functional distractors, homogeneity, tell words, one defensible key) + viva marking rubric + verification-hole fixes (2026-09-15); 2.3: knowledge-not-judgment MCQs + dimension selector + structural gate; 2.2: adversarial naive-guesser MCQ critic; 2.1: seniority calibration

# ── Seniority calibration (2026-09-14): shifts GENERATION difficulty only —
# grading, storage, fairness floors untouched. Injected into job_ctx (all
# three agents read it) + a bidirectional critic clause: too easy AND too
# hard for the stated level both FAIL, and the refine loop fixes in place.
SENIORITY_CALIBRATION = {
    "junior": {
        "label": "JUNIOR (entry level, 0-2 years)",
        "draft": ("Test FUNDAMENTALS and their straightforward application in routine, "
                  "first-year work situations. The best answer must be reachable from solid "
                  "basics correctly applied; distractors are classic beginner misconceptions. "
                  "Do NOT demand architecture choices, org-level strategy, or judgment that "
                  "only years of experience builds."),
        "critic": ("it demands system/architecture design, organizational strategy, or "
                   "seasoned judgment beyond a capable entry-level professional — TOO HARD "
                   "for a junior screen."),
    },
    "mid": {
        "label": "MID-LEVEL (2-5 years, works independently)",
        "draft": ("Test APPLIED JUDGMENT on real trade-offs: diagnosing familiar breakage, "
                  "choosing the right standard approach for a concrete situation, knowing "
                  "why the common shortcut fails. Beyond textbook basics, but not "
                  "system-design or strategic scope."),
        "critic": ("it is answerable from textbook fundamentals alone (TOO EASY for "
                   "mid-level), or demands architecture/strategy scope (TOO HARD)."),
    },
    "senior": {
        "label": "SENIOR (5+ years, owns hard problems)",
        "draft": ("Test DEPTH: ambiguity, edge cases, what breaks at scale, why NOT the "
                  "obvious option, weighing two defensible approaches and the cost of each. "
                  "Prefer 'why' and 'what goes wrong' over 'what/how'. Routine application "
                  "questions are below this level."),
        "critic": ("it is routine application any 2-year practitioner handles confidently — "
                   "TOO EASY for a senior screen (a senior set of junior questions FAILS)."),
    },
    "lead": {
        "label": "LEAD / PRINCIPAL (sets direction, leads others)",
        "draft": ("Test STRATEGIC, ARCHITECTURAL and LEADERSHIP judgment: direction-setting "
                  "under constraints, cross-team consequences, prioritization calls, "
                  "standards and mentoring decisions, when to rebuild vs. patch. Hands-on "
                  "task mechanics alone are below this level."),
        "critic": ("it tests hands-on task mechanics instead of direction-setting judgment — "
                   "TOO EASY for a lead-level screen."),
    },
}


def _seniority(level: str) -> dict:
    return SENIORITY_CALIBRATION.get((level or "mid").strip().lower(),
                                     SENIORITY_CALIBRATION["mid"])


def _seniority_ctx(level: str) -> str:
    s = _seniority(level)
    return (f"\nSENIORITY LEVEL: {s['label']}\n"
            f"CALIBRATION — every question must sit at exactly this level:\n{s['draft']}\n")


def _seniority_check(level: str) -> str:
    s = _seniority(level)
    return (f"   - MISCALIBRATED for the stated seniority level ({s['label']}) — "
            f"BOTH directions fail: {s['critic']}\n")

WRITTEN_RATIO = 0.3   # ~30% of questions answered in writing

# The one language rule injected into every generation prompt. English is the
# default and unchanged. Bangla (BETA) = natural BANGLISH (the same register the
# MCQ generator and the instruction pages use): Bangla sentence language,
# technical/professional terms kept in English — never formal-Bengali
# dictionary translations. Candidates answer in the same mix.
LANG_RULE = {
    "en": ("Write in simple, clear English. Many candidates speak English as a second or "
           "third language: short sentences, no idioms, no cultural references. A question "
           "that is hard to parse measures English, not competence."),
    "bn": ("Write in NATURAL BANGLA-ENGLISH MIX (Banglish) — the way Bangladeshi "
           "professionals actually speak at work: the sentence language (connectives, "
           "verbs, framing) in Bangla script, but ALL technical terms, tool names, and "
           "standard English professional words in ENGLISH — dashboard, pivot table, "
           "SQL query, data model, report, stakeholder, deadline, KPI. NEVER translate "
           "these into formal Bengali (no বিশ্লেষণ for analysis, no উপাত্ত for data). "
           "Correct register example: \"আপনার team-এর জন্য আপনি একটা sales dashboard "
           "design করেছিলেন — data model টা কীভাবে সাজিয়েছিলেন, step by step বলুন।\" "
           "Keep sentences short and words simple — a question that is hard to parse "
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


TOPIC_DRAFT_PROMPT = """You are writing the SPOKEN part of a screening interview for a specific job. You are
given the job description. Produce exactly {t} topics. Each topic has ONE main
question and exactly {f} follow-up questions that probe deeper into the SAME
topic.

STRUCTURE
- A topic is one coherent competency area drawn from the actual skills, duties,
  and context in the description (for example "Building dashboards" or
  "Working with stakeholders") — never generic filler that fits any job.
- The main question opens the topic from the candidate's own experience.
- Each follow-up digs further into the same topic and must stand alone as a
  complete question (the interviewer reads it exactly as written).

WHAT MAKES A QUESTION DEEP ENOUGH — every question must pass this bar
- It must force the candidate to DEMONSTRATE real experience: walk through how
  they built, designed, or fixed something; what they decided and why; what
  broke and how they found it; what trade-off they took; what the result was.
- Name the SPECIFIC tools, techniques, and situations of THIS role. Not
  "do you know Power BI?" but "walk me through how you would design a data
  model in Power BI when two tables have a many-to-many relationship — what
  exactly do you build, and what goes wrong if you skip it?"
- BANNED: yes/no questions; "do you know / have you used X"; anything a
  one-line generic answer fully satisfies ("I would communicate with the
  team"); definition or vocabulary asks; questions that fit any job.
- Deep does NOT mean long or complicated wording. Keep the words simple and
  the sentence short — the depth is in what the ANSWER must contain, not in
  the question's language. A nervous or second-language candidate must
  understand the question instantly.
- EXAMINABLE STANDARD: for every question you write it must be OBVIOUS what a
  strong answer contains versus a weak one, so two different reviewers would
  rank the same answer the same way. A question whose answers cannot be ranked
  — one that invites opinion or preference rather than demonstrable competence
  — has no place in an assessment. Write questions that can be MARKED.

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


TOPIC_CRITIC_PROMPT = """You are reviewing SPOKEN interview questions for a specific job, as a harsh
quality bar. Spoken answers are open-ended — this is where genuine reasoning
depth can and MUST be tested, so be demanding. Judge EVERY question
independently against one standard: answering it well must require real,
role-specific experience AND reasoning that someone who only read about the
topic could not fake.

FAIL a question for ANY of these:
   - SHALLOW: a candidate can fully answer it with one generic sentence, or
     with yes/no ("do you know X", "have you worked with X").
   - GENERIC: it would fit almost any job — nothing in it names this role's
     actual tools, duties, or situations.
   - NO SPECIFIC DEMONSTRATION: the best answer does not force the candidate
     to WALK THROUGH concrete real work — what they actually built, decided,
     diagnosed, or measured; HOW exactly; WHY that way and not the obvious
     alternative; what BROKE and how they found it; what TRADE-OFF they took
     and what it cost; what the RESULT was. A question answerable with
     opinions, generalities, or "what one should do in theory" FAILS.
   - BOOK-ANSWERABLE: someone who studied the topic but never DID it could
     answer as fully as a practitioner. If reciting knowledge suffices — with
     no lived specifics required — FAIL.
   - WRONG DEPTH FOR THE LEVEL: too easy for the stated seniority. A question
     a junior could fully answer FAILS for a senior/lead role — senior/lead
     questions must probe trade-offs, edge cases, what fails at scale, or
     cross-team / strategic reasoning; junior questions must still demand a
     concrete demonstration of the fundamentals in practice, not theory.
   - NO DEFENSIBLE ANSWER STANDARD: you cannot articulate what separates a
     STRONG answer from a WEAK one for this question — there is no examinable
     bar, so two reviewers would score the same answer differently. A question
     that invites opinion or preference rather than demonstrable competence
     FAILS. (State the strong-vs-weak split to yourself before you pass it.)
   - TWO-PART: it asks more than one thing.
   - UNLAWFUL/UNFAIR: it touches age, religion, family, health, ethnicity,
     politics, or anything a recruiter could not lawfully ask.
   - HARD TO PARSE: the wording itself is long, idiomatic, or convoluted —
     depth must live in the required ANSWER, never in the sentence. (A
     nervous or second-language candidate must grasp the question instantly.)
{level_check}{lang_checks}
When genuinely unsure whether a question forces real demonstrated reasoning,
FAIL it — a shallow spoken question wastes the interview's most valuable
minutes.

Return JSON: {{"reviews": [{{"id": "<the bracketed id exactly as given>",
"pass": true|false, "reasons": ["<short reason>", ...]}}, ...]}} — one review
per question, every id covered."""


TOPIC_REFINE_PROMPT = """You are deepening interview questions that failed review, for a specific job.
For each question you get its id, its topic, the current text, and the
reviewer's reasons. Rewrite EACH question so it passes review:
- force a demonstration of real experience — how exactly, what they decided,
  what broke, what trade-off, what the result was — grounded in THIS role's
  actual tools and situations from the job description;
- stay on the SAME topic, and keep it ONE question (no two-part questions);
- keep the wording simple and the sentence short — the depth belongs in the
  required answer, not the question's language.
{fairness}

Return JSON: {{"fixes": [{{"id": "<same id>", "text": "<rewritten question>"}},
...]}} — every id you were given, exactly once each."""


# MARKING STANDARD (qgen-2.4). Written like an examiner's marking scheme so the
# recruiter reviews spoken answers against a defensible bar instead of a vibe.
# ADVISORY ONLY: this text is recruiter-facing review metadata and is NEVER sent
# to any scorer — interview scoring, grading and the fairness floor are untouched.
TOPIC_RUBRIC_PROMPT = """You are writing the MARKING STANDARD for spoken interview questions, the way an
examiner writes a marking scheme. For EACH question you are given (with its id
and topic), state what separates a strong answer from a weak one for THIS role
at THIS seniority.

For each question give:
- "strong": what a STRONG answer actually contains — the specific things the
  candidate must name, walk through, or justify (the concrete decision and why,
  the trade-off and its cost, how they diagnosed it, what the result was).
  Ground it in THIS role's real tools and situations. Not generic praise.
- "weak": what a WEAK answer looks like — the generic, theoretical or recited
  version someone gives when they have READ about the topic but never done it.

Two or three lines each, plain language, concrete. This is a reviewer's guide:
it is never shown to the candidate and never used to compute a score.

Return JSON: {{"rubrics": [{{"id": "<the bracketed id exactly as given>",
"strong": "...", "weak": "..."}}, ...]}} — one per question, every id covered."""


async def generate_topic_questions(jd_text: str, api_key: str, job_title: str = "",
                                   n_topics: int = 2, followups: int = 3,
                                   language: str = "en", role_hint: str = "",
                                   seniority: str = "mid",
                                   usage_out: list | None = None
                                   ) -> tuple[list | None, str | None]:
    """Deep spoken topic clusters via the drafter -> critic -> refiner loop
    (the same bounded pattern as the MCQ multi-agent). The critic fails
    shallow / generic / no-depth questions (and, for 'bn', over-translation
    out of the Banglish register); failures are refined IN PLACE — the topic
    structure and slot counts never change — and re-checked, up to FOUR
    rounds. After the cap the latest refined text stands, FLAGGED (bounded
    best-effort, never an infinite loop). Nothing unverified ships looking
    verified: a critic failure flags every slot, and a mid-refine failure flags
    exactly the slots whose text was replaced but not re-checked.

    Finally an advisory MARKING STANDARD ("rubric": {slot: {strong, weak}}) is
    generated over the FINAL text — recruiter-facing review metadata, never sent
    to a scorer, so scoring/grading/fairness are untouched. Draft only — the
    recruiter still reviews, edits, and approves before anything reaches a
    candidate. Returns ([{"topic","main","followups"}], None) or (None, error)."""
    n_topics = max(1, min(4, int(n_topics)))
    followups = max(1, min(5, int(followups)))
    jd = (jd_text or "").strip()
    if len(jd) < 40:
        return None, "The job description is too short to generate questions from."

    client = AsyncOpenAI(api_key=api_key)
    job_ctx = (f"JOB TITLE: {job_title or 'not specified'}\n"
               + (f"ROLE TYPE: {role_hint}\n" if role_hint else "")
               + _seniority_ctx(seniority)
               + f"\nJOB DESCRIPTION:\n\"\"\"\n{jd[:8000]}\n\"\"\"")
    lang_rule = _lang(language)
    critic_prompt = TOPIC_CRITIC_PROMPT.format(lang_checks=_mcq_lang_checks(language),
                                               level_check=_seniority_check(seniority))

    # ── 1) DRAFT the full structure in one call ──
    try:
        raw = await _gen_json(client,
                              TOPIC_DRAFT_PROMPT.format(t=n_topics, f=followups,
                                                        lang_rule=lang_rule),
                              job_ctx, 0.4, 2200, usage_out)
    except Exception as e:
        return None, f"Generation call failed: {str(e)[:200]}"
    topics = []
    for t in (raw or {}).get("topics", [])[:n_topics]:
        topic = str((t or {}).get("topic", "")).strip()[:80]
        main = str((t or {}).get("main", "")).strip()[:300]
        fups = [str(f).strip()[:300] for f in (t or {}).get("followups", [])
                if str(f).strip()][:followups]
        if main and fups:
            topics.append({"topic": topic or "Topic", "main": main, "followups": fups})
    if len(topics) < n_topics:
        return None, f"The model returned only {len(topics)} usable topic(s) — try again."

    # Stable slot ids ("t0.main", "t0.f1", ...) — the structure is fixed, so
    # critique and refinement address questions in place, never add/drop slots.
    def _slots():
        out = []
        for ti, t in enumerate(topics):
            out.append((f"t{ti}.main", t["topic"], t["main"]))
            for fi, f in enumerate(t["followups"]):
                out.append((f"t{ti}.f{fi}", t["topic"], f))
        return out

    def _set(qid: str, text: str):
        ti, part = qid.split(".")
        t = topics[int(ti[1:])]
        if part == "main":
            t["main"] = text
        else:
            t["followups"][int(part[1:])] = text

    async def _critic(ids):
        cur = {qid: (topic, text) for qid, topic, text in _slots()}
        listing = "\n".join(f"[{qid}] (topic: {cur[qid][0]}) {cur[qid][1]}"
                            for qid in ids)
        raw = await _gen_json(client, critic_prompt,
                              job_ctx + "\n\nQUESTIONS TO REVIEW:\n" + listing,
                              0.0, 1800, usage_out)
        verdicts = {str((r or {}).get("id")): r for r in (raw.get("reviews") or [])
                    if isinstance(r, dict)}
        failed = []
        for qid in ids:
            v = verdicts.get(qid)
            reasons = [str(x)[:160] for x in ((v or {}).get("reasons") or [])][:4]
            if v is None or not bool(v.get("pass")):
                failed.append((qid, reasons or ["not reviewed — treated as failed"]))
        return failed

    # ── 2) CRITIC everything. Fail-soft: a broken critic call returns the
    #      uncritiqued draft rather than nothing (the recruiter still reviews). ──
    try:
        failed = await _critic([qid for qid, _, _ in _slots()])
    except Exception as e:
        # VERIFICATION HOLE (fixed 2026-09-15): this used to return a COMPLETELY
        # uncritiqued draft carrying no flags at all — which reads to the
        # recruiter as "reviewed and clean". Nothing unverified may ship looking
        # verified, so every slot is flagged for review instead.
        print(f"[VIVA-GEN] critic call failed — returning draft with ALL slots "
              f"flagged unverified: {str(e)[:120]}")
        for t in topics:
            t["review_flags"] = ["main"] + [f"f{fi}" for fi in range(len(t["followups"]))]
        return topics, None

    # ── 3) REFINE loop: up to 4 rounds, failures rewritten in place and
    #      re-checked. The round cap is the never-infinite rail. ──
    fairness = _MCQ_FAIRNESS_RULES.format(lang_rule=lang_rule)
    for _round in range(4):
        if not failed:
            break
        cur = {qid: (topic, text) for qid, topic, text in _slots()}
        listing = "\n".join(
            f"[{qid}] (topic: {cur[qid][0]}) {cur[qid][1]}\n   reviewer reasons: " + "; ".join(rs)
            for qid, rs in failed)
        fixed_ids = []
        try:
            raw = await _gen_json(client, TOPIC_REFINE_PROMPT.format(fairness=fairness),
                                  job_ctx + "\n\nQUESTIONS TO FIX:\n" + listing,
                                  0.4, 2200, usage_out)
            valid = {qid for qid, _ in failed}
            for fx in (raw.get("fixes") or []):
                qid = str((fx or {}).get("id", ""))
                text = str((fx or {}).get("text", "")).strip()[:300]
                if qid in valid and text:
                    _set(qid, text)
                    fixed_ids.append(qid)
            if not fixed_ids:
                break
            failed = await _critic(fixed_ids)
        except Exception as e:
            # VERIFICATION HOLE (fixed 2026-09-15): _set() above may already have
            # REPLACED question text before this failure, so that new text was
            # never re-checked — it used to ship behind the previous round's
            # stale verdicts. Flag exactly the slots we touched as unverified.
            print(f"[VIVA-GEN] refine round {_round + 1} failed — flagging "
                  f"{len(fixed_ids)} unverified slot(s): {str(e)[:120]}")
            _touched = set(fixed_ids)
            failed = ([(qid, ["refined but not re-verified — please review"])
                       for qid in fixed_ids]
                      + [(q, r) for q, r in failed if q not in _touched])
            break
    # ── 4) FLAG, don't silently ship (Option A, 2026-09-14). The topic
    #      structure is FIXED (each interview's phase-sequencing depends on
    #      exact slot counts), so a still-shallow question CANNOT be dropped
    #      like an MCQ. Instead it is MARKED on the draft — the viva editor
    #      shows the recruiter exactly which questions the AI could not deepen,
    #      so nothing shallow reaches a candidate looking verified. Slot names
    #      match the qids: "main", "f0", "f1", ... ──
    if failed:
        for qid, _rs in failed:
            try:
                ti_str, part = qid.split(".")
                ti = int(ti_str[1:])
                topics[ti].setdefault("review_flags", [])
                if part not in topics[ti]["review_flags"]:
                    topics[ti]["review_flags"].append(part)
            except Exception:
                continue
        print(f"[VIVA-GEN] {len(failed)} question(s) still flagged for recruiter "
              "review after refinement: " + ", ".join(qid for qid, _ in failed))

    # ── 5) MARKING STANDARD (qgen-2.4, advisory). ONE call over the FINAL text,
    #      so the rubric always matches the question the recruiter will actually
    #      see — generating it at draft time would go stale the moment refinement
    #      rewrote a question. Recruiter-facing review metadata ONLY: it is never
    #      sent to a scorer, so interview scoring, grading and the fairness floor
    #      are untouched. Fail-soft: a missing rubric is a lost nicety, never a
    #      failed generation. ──
    try:
        slots = _slots()
        listing = "\n".join(f"[{qid}] (topic: {topic}) {text}" for qid, topic, text in slots)
        rraw = await _gen_json(client, TOPIC_RUBRIC_PROMPT,
                               job_ctx + "\n\nQUESTIONS:\n" + listing,
                               0.3, 2200, usage_out)
        by_id = {}
        for r in (rraw.get("rubrics") or []):
            qid = str((r or {}).get("id", ""))
            strong = str((r or {}).get("strong", "")).strip()[:600]
            weak = str((r or {}).get("weak", "")).strip()[:600]
            if qid and (strong or weak):
                by_id[qid] = {"strong": strong, "weak": weak}
        for qid, _topic, _text in slots:
            if qid not in by_id:
                continue
            try:
                ti_str, part = qid.split(".")
                topics[int(ti_str[1:])].setdefault("rubric", {})[part] = by_id[qid]
            except Exception:
                continue
    except Exception as e:
        print(f"[VIVA-GEN] rubric pass skipped (questions unaffected): {str(e)[:120]}")
    return topics, None


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

# ── MCQ DIMENSIONS (qgen-2.3): the recruiter picks WHICH kinds of question to
# test per job; generation splits the set roughly EVENLY across the chosen ones.
# The whole point of 2.3 is to move OFF pure workplace-judgment ("what would you
# do") — which is guessable by common sense — and ONTO knowledge/technical/
# reasoning questions, where four close options can be separated ONLY by real
# knowledge. Judgment stays available but minimized (off by default) because it
# is the guessable kind. These tags are advisory display metadata; grading is
# unchanged and index-based.
MCQ_DIMENSIONS = {
    "domain": {
        "label": "DOMAIN KNOWLEDGE",
        "guide": ("Do they actually KNOW this field? Test facts, core concepts, and the "
                  "correct method or standard for a specific task in THIS role. The right "
                  "answer is a fact or an established best method — not an opinion. Every "
                  "distractor is a real misconception, an outdated/kind-of-right method, or "
                  "the correct answer to a subtly different question — something a "
                  "half-informed practitioner genuinely believes."),
    },
    "technical": {
        "label": "TECHNICAL SKILL",
        "guide": ("If the role uses a concrete tool, language, or process (SQL, Excel, a "
                  "framework, a formula, a standard workflow), TEST IT DIRECTLY with a "
                  "question only someone skilled could answer: read this query/formula and "
                  "say what it returns, pick the one that does X correctly, spot the bug, "
                  "choose the right function for the job. Distractors are plausible-but-wrong "
                  "syntax, near-miss functions, or approaches that look right but fail on the "
                  "specifics. A crisp technical item needs no work-story wrapper."),
    },
    "reasoning": {
        "label": "REASONING / PROBLEM-SOLVING",
        "guide": ("Give a concrete problem and make them WORK IT THROUGH — apply role "
                  "knowledge across a step or two to reach the answer. The right option "
                  "follows from correct reasoning about the specifics; each distractor is the "
                  "result of a SPECIFIC reasoning error (a wrong assumption, a skipped step, "
                  "the right idea misapplied) — never merely the 'less careful' option."),
    },
    "judgment": {
        "label": "JUDGMENT (situational — use sparingly)",
        "guide": ("A situational call — but write one ONLY when all four options are genuinely "
                  "close, each a choice a competent professional could defend, separated by a "
                  "real trade-off a knowledgeable person weighs (not by which sounds most "
                  "diligent). If you cannot make the options that close, write a domain, "
                  "technical, or reasoning question instead. Never a 'which sounds "
                  "professional' question."),
    },
}
DIMENSION_ORDER = ["domain", "technical", "reasoning", "judgment"]
DEFAULT_DIMENSIONS = ["domain", "technical", "reasoning"]   # knowledge-heavy; judgment OFF


def _validate_dimensions(dims) -> list:
    """Clamp a recruiter dimension selection to the known set, preserving the
    canonical order. Empty/garbage -> the knowledge-heavy default (no judgment)."""
    if not dims:
        return list(DEFAULT_DIMENSIONS)
    want = {str(x).strip().lower() for x in dims}
    out = [d for d in DIMENSION_ORDER if d in want]
    return out or list(DEFAULT_DIMENSIONS)


def _dimensions_block(dims) -> str:
    """Per-dimension drafting guidance for the SELECTED mix, injected into the
    drafter. Also tells the drafter to spread questions ~evenly and tag each."""
    picked = _validate_dimensions(dims)
    lines = ["DIMENSIONS TO TEST — spread the questions roughly EVENLY across these, and "
             "tag EACH question with its dimension (the \"dimension\" field):"]
    for d in picked:
        info = MCQ_DIMENSIONS[d]
        lines.append(f'- {info["label"]} — tag "{d}": {info["guide"]}')
    if "judgment" not in picked:
        lines.append("Do NOT write pure situational-judgment questions — they are not in the "
                     "selected mix.")
    return "\n".join(lines)


MCQ_DRAFT_PROMPT = """You are writing a knowledge screen for a specific job. Draft {n}
multiple-choice questions grounded in the ACTUAL day-to-day work this job
description describes, in a MIXED STRUCTURE:

{dimensions}

STRUCTURE (both kinds, mixed)
- SCENARIO GROUPS: {n_scen} realistic role scenarios — each a substantial
  paragraph (a concrete situation with context: what's happening, who's
  involved, real numbers/constraints/data where natural) — followed by 3-4
  questions whose answers genuinely DEPEND on the scenario's specifics, not
  merely sit next to it.
- STANDALONE questions: the remainder — each self-contained: a crisp technical
  item, a domain-knowledge question, or a short reasoning problem. These need
  NOT be wrapped in a work story — a direct "which query returns X", "what does
  this metric indicate", or "read this and compute Y" is good.

THE HARD RULE — applies to EVERY question and EVERY option, no exceptions:
- TEST REAL KNOWLEDGE, NOT COMMON SENSE. A smart person with ZERO knowledge of
  this role must NOT be able to pick the right answer better than random chance.
  If common sense, elimination, or "which option sounds most professional /
  thorough / careful / kind" can find the answer, the question is BROKEN. This
  is the single most important rule — everything below serves it.
- Exactly 4 options, exactly ONE correct (or clearly-best) answer.
- ALL FOUR options must be close and plausible to someone with PARTIAL
  knowledge. No strawmen, no joke or absurd options, no universally-bad options
  ("ignore it", "do nothing", "never contact them again", "fabricate the
  data"), nothing eliminable on sight. Every distractor is a real misconception,
  a near-miss method/function, a plausible-but-wrong computation, or the correct
  answer to a subtly different question — something a half-trained person
  genuinely believes.
- NO SOCIAL-DESIRABILITY TELL: the options must NOT differ in how conscientious,
  responsible, or thorough they SOUND. The wrong ones are wrong on the MERITS (a
  fact, a method, a syntax, a computation), never on tone. If exactly one option
  reads as "the responsible choice", the question is broken — rewrite it as a
  knowledge/technical/reasoning question instead.
- BANNED "GENERIC GOOD PRACTICE" ANSWERS: the correct answer must be a specific,
  field-dependent fact/method — NEVER a piece of universal workplace advice that
  everyone knows without the field. Do NOT let the right answer be "set up
  automated tests", "document it / use version control", "communicate clearly
  with stakeholders", "present to non-technical people with charts", "double-
  check / validate the data", "standardize the formats", "test before you
  deploy". A layperson picks these on sight — they measure nothing.
- BANNED STEMS: do NOT ask "what is the best practice", "what is a/the crucial
  (or first, or best) step", "how do you ensure / how can you ensure", "how
  should you", "what should you do to". These invite generic good-practice
  answers. Ask instead for a determinate knowledge answer: "what does this
  query/formula return", "which clause/function does X", "given these numbers,
  what is [the value / the cause]", "which statement about X is correct".
- PARALLEL OPTIONS (length must not leak): all four options must be about the
  SAME length and the same grammatical shape. The correct one must NOT be the
  longest, most detailed, most qualified, or the only one with an added clause
  ("...considering data types and formats"). If you are tempted to append a
  qualifier to the right answer to make it "more complete", STOP — that is a
  tell; trim it so all four read as equally terse and plausible.
- No two options may overlap in meaning.
- BARE VOCABULARY IS BANNED, APPLICATION IS REQUIRED: never "what does term X
  mean" / "which word defines Y". But making the candidate APPLY a fact, read a
  query, compute a result, or pick the correct method FOR A SPECIFIC CASE is
  exactly what you want — do that.
- STAY IN THE SELECTED DIMENSIONS: every question must be a real instance of one
  of the dimensions listed above. If a question would only fit "judgment" and
  judgment is not in the selected list, do NOT write it — write a domain,
  technical, or reasoning question on the same topic instead.
- Self-contained: answerable from real knowledge of the role plus the question
  (and its scenario, for grouped ones). No company-internal facts.

EXAM ITEM-WRITING STANDARD — write these the way a professional certification-
exam item writer would. Each of these is a published item-flaw to eliminate:
- FUNCTIONAL DISTRACTORS: every wrong option must encode ONE specific, nameable
  error a partially-competent candidate actually makes — a named misconception,
  the right method for a DIFFERENT case, a classic wrong-tool or wrong-step
  mistake. You must be able to say in one line WHY each distractor is wrong and
  why someone would still pick it. An option nobody would ever choose is a
  wasted option and breaks the item.
- HOMOGENEOUS OPTIONS: the four options must match each other in LENGTH,
  grammatical form, specificity, and structure — all noun phrases, or all
  clauses, or all code/formula snippets. The key must be indistinguishable from
  the distractors by FORM alone.
- NO TELL WORDS: never "All of the above", "None of the above", or "Both A and
  B". No absolute qualifiers (always, never, every, all, none) — a test-wise
  reader scores those as wrong on sight. No grammatical clue: every option must
  fit the stem's grammar equally (a/an, singular/plural, tense).
- ONE DEFENSIBLE KEY: exactly one option is unambiguously best and a subject
  expert would agree without debate. If two options could both be defended,
  tighten the stem until only one survives.
- SCENARIO-DEPENDENT (grouped items): a question attached to a scenario must be
  UNANSWERABLE without reading it — the answer must turn on that scenario's
  specific details (its numbers, its constraint, what already happened). If the
  question still works with the scenario deleted, rewrite it.
- SELF-CONTAINED STEM: the stem (plus its scenario) poses one complete, clear
  question. No trick phrasing, no double negatives, no window dressing.
- NO CROSS-CLUEING: no item may reveal or imply the answer to another item in
  this set, and no option may restate another item's key.
{fairness}

Return JSON (tag every question with its "dimension" — one of: domain, technical, reasoning, judgment):
{{"scenarios": [{{"scenario": "...paragraph...",
                "mcq": [{{"question": "...", "options": ["...", "...", "...", "..."], "correct": <0-based index>, "dimension": "domain|technical|reasoning|judgment"}}, ...]}}, ...],
  "standalone": [{{"question": "...", "options": ["...", "...", "...", "..."], "correct": <0-based index>, "dimension": "domain|technical|reasoning|judgment"}}, ...]}}"""

# The GUESSER runs as its OWN call with NO job context (v3, 2026-09-14): an
# in-critic "pretend you know nothing" pass proved contaminated — the model
# had the JD in front of it and systematically under-reported how guessable
# its questions were, while an independent zero-context call guessed the
# same questions confidently. Honest ignorance can't be role-played; it has
# to be structural.
MCQ_NAIVE_GUESS_PROMPT = """You are a smart test-taker with ZERO knowledge of any profession. You will see
multiple-choice questions (some with a scenario). You know NOTHING about the
job they belong to. GAME each one: pick the option a clever layperson would
choose using only test-taking tells — the most professional/thorough/kind-
sounding option, eliminating careless or absurd options, length/specificity,
the middle ground. Rate how confident that guess feels.

Return JSON: {"guesses": [{"i": <index>, "pick": <0-3>,
"confidence": "low"|"medium"|"high"}, ...]} — one entry per question."""

# TELL-DETECTOR (qgen-2.3, the GATE — replaces the naive-guesser as the gate).
# An LLM "naive guesser" cannot validate KNOWLEDGE questions: it already knows
# the field, so it answers SQL/stats questions correctly via KNOWLEDGE, not via
# a tell (measured on a real Data-Analyst set: 93% "guessed", nearly all of them
# genuine knowledge questions). That is a false positive, not a weak question.
# So we detect the ACTUAL defect — a WORDING TELL, which a no-knowledge reader
# could follow — and drop a question only when the wording pulls that reader to
# the CORRECT option (a leak). Questions whose four options read as equally
# plausible, separable only by real knowledge, have no tell and PASS. The
# naive-guesser above is kept only for the proof harness's contrast.
MCQ_TELL_DETECTOR_PROMPT = """You are auditing multiple-choice questions for WORDING TELLS. Imagine a person
with ZERO knowledge of the field is taking the test. ASSUME YOU KNOW NOTHING
about the topic — do NOT use any real knowledge to decide which option is
correct. Judge ONLY the WRITING of the options.

For each question, decide which option (if any) a no-knowledge reader would be
PULLED toward using ONLY these surface tells:
- one option SOUNDS more thorough / careful / professional / complete /
  responsible than the others;
- one option is noticeably LONGER, more detailed, or more hedged;
- the OTHER options sound careless, dismissive, extreme, absurd, or clearly wrong.
If all four options read as equally plausible and NO wording tell points to any
one of them — so only real field knowledge could choose — the pull is "none".

Return JSON: {"tells": [{"i": <index>, "pick": <0-3, or -1 if none>,
"pull": "none"|"weak"|"medium"|"strong"}, ...]} — one per question. Base "pick"
on WORDING ALONE, never on whether you think it is factually correct."""

MCQ_CRITIC_PROMPT = """You are reviewing screening MCQs for a specific job. You are NOT given the
answer key. Some questions belong to a SCENARIO (shown above them) — judge
those WITH their scenario. For EACH question: first solve it properly with
full role knowledge and pick the single best option (best_index); then decide
pass/fail.

FAIL a question when ANY of these clearly applies (name the reason):
   - COMMON-SENSE ANSWERABLE: a smart person with ZERO knowledge of this role
     could pick the best option using general reasoning, elimination, or
     social-desirability ("which sounds most professional / thorough / careful").
     If real DOMAIN, TECHNICAL, or REASONING knowledge is NOT required to choose,
     FAIL. This is the most important check — a screening question that common
     sense can answer measures nothing. Watch for GENERIC-GOOD-PRACTICE answers
     that everyone knows without the field: "present to non-technical people with
     a summary and charts", "double-check the data for accuracy", "communicate
     clearly with stakeholders", "verify before acting", "document your work",
     "test before deploying". If the correct answer is one of these and the
     distractors are just less-careful versions, FAIL — the field is decoration.
   - STRAWMAN OPTION: any option NO competent professional would EVER pick,
     even on a lazy day — "do nothing", "ignore it", "never contact them
     again", "call them every day", "fabricate the data", joke options, or
     options from a different profession. A distractor must be a real
     misconception or a plausible-but-inferior approach someone could
     genuinely choose. Apply it to EVERY option.
   - SOCIAL-DESIRABILITY TELL: exactly one option reads as the diligent /
     careful / professional choice while the others read careless, dismissive,
     or extreme — the tone leaks the answer. Every option must be defensible on
     the MERITS; the wrong ones are wrong because of a fact, method, or
     computation, never because they sound less responsible.
   - LENGTH / DETAIL TELL: your best_index option is noticeably longer, more
     detailed, more hedged, or the only one carrying an extra qualifying clause
     ("...considering data types and formats") — a no-knowledge reader picks the
     most detailed option and is right. The four options must read as equally
     terse and parallel; FAIL when the answer stands out by length or detail.
   - BARE VOCABULARY: a pure definition/vocabulary ask — "what does term X
     mean", "which word/key defines Y" — with no application at all. (A question
     that makes them APPLY a fact, read a query/formula, compute a result, or
     pick the correct method FOR A SPECIFIC CASE is GOOD — do NOT fail it just
     because answering requires knowing something. A crisp, self-contained
     technical question with no work-story is fine.)
   - NON-FUNCTIONAL DISTRACTOR: before deciding, name to yourself the SPECIFIC
     error each non-best option encodes — a real misconception, the right answer
     for a different case, a classic wrong step. If you cannot name a genuine
     error for some option (nobody would ever pick it, or it is filler), FAIL.
   - HETEROGENEOUS OPTIONS: the four options do not match in length, grammatical
     form, specificity, or structure — so FORM alone separates the key from the
     rest (e.g. one full clause among three short noun phrases).
   - TELL WORDS: contains "All of the above", "None of the above", "Both A and
     B", an absolute qualifier (always / never / every / all / none) in an
     option, or a grammatical clue where only one option fits the stem (a/an,
     singular/plural, tense).
   - MULTIPLE DEFENSIBLE KEYS: more than one option can be defended as best by a
     subject expert, or no option clearly is.
   - SCENARIO-INDEPENDENT: the question belongs to a SCENARIO but can be answered
     correctly WITHOUT reading it — the scenario is decoration, not evidence.
     (Applies only to questions shown under a scenario.)
   - CROSS-CLUED: another question or option in this set gives away this one's
     answer, or this item restates another item's key.
   - AMBIGUOUS or WRONG KEY: two options overlap or are synonymous, or the key
     does not hold.
   - NOT GROUNDED: not about this role's real work at all.
   - NEAR-DUPLICATE: it asks essentially the same thing as an EARLIER
     question in this set — fail the later one.
{level_check}{lang_checks}
Otherwise PASS. A good question has four options a competent person could
each defend, and needs real role knowledge or careful reasoning (NOT common
sense) to choose between them — such a question DESERVES a pass. Judge each
criterion on its own test; do not fail a solid knowledge/technical question out
of general strictness. (A separate zero-knowledge guesser is also run over the
survivors as a hard gate, so you do not have to catch every guessable question
yourself — focus on the defects above.)

Return JSON:
{{"reviews": [{{"i": <index in the list>, "best_index": <0-3>,
"pass": true|false,
"reasons": ["short, specific reasons — empty when pass"]}}, ...]}}"""

MCQ_REFINE_PROMPT = """You are fixing screening MCQs that failed review, for a specific job.
For each item you get the question, its options, the intended correct index,
and the reviewer's SPECIFIC reasons. Rewrite each question to FIX those
reasons while keeping it grounded in this job's real work. A question shown
WITH a scenario must stay about THAT scenario (revise the question, not the
scenario). You may rewrite
the stem, any option, or replace the question entirely with a better one on
the same topic area. A question failed as COMMON-SENSE ANSWERABLE, GUESSABLE, or
a SOCIAL-DESIRABILITY tell must come back as a genuine DOMAIN, TECHNICAL, or
REASONING question on the same topic — one that requires real knowledge (a fact,
a method, reading a query/formula, a computation, a multi-step deduction) — with
its DISTRACTORS rebuilt as real misconceptions, NOT the stem lightly reworded. A
question failed for BARE VOCABULARY must come back as an APPLICATION question on
the same topic (apply the fact / read the query / compute the result), never a
reworded definition. Same bar as before:
- 4 options, ONE defensibly best answer, distractors = genuine
  partial-knowledge misconceptions or near-miss methods, nothing a layperson can
  eliminate, no universally-bad options ("ignore it", "do nothing", "never
  contact them again"), no length/format leak, no ambiguity.
- BEAT THE GUESSER: a zero-knowledge test-gamer picks the most
  professional-sounding option, eliminates careless-sounding ones, and takes the
  middle ground. Your rewrite must defeat that — a smart layperson must not beat
  chance. EVERY option must be defensible on the MERITS, with the wrong ones
  wrong because of a fact/method/computation, never because of tone. The
  right answer must NOT differ from the others in how conscientious it sounds.
- EXAM ITEM-WRITING STANDARD (the rewrite must satisfy all of it): every
  distractor encodes ONE specific, nameable error someone actually makes;
  the four options are HOMOGENEOUS in length, grammar, specificity and
  structure; NO "All/None of the above", no absolute qualifiers (always/never/
  every), no grammatical clue; exactly ONE defensible key an expert agrees on
  without debate; a scenario-attached question must be UNANSWERABLE without its
  scenario; the stem is self-contained with no double negatives; and it must not
  give away, or repeat, another item's answer.
{fairness}

Return JSON (same order as given; keep each question's "dimension"):
{{"mcq": [{{"question": "...", "options": ["...", "...", "...", "..."], "correct": <0-based index>, "dimension": "domain|technical|reasoning|judgment"}}, ...]}}"""


# Dismissive "do-nothing" distractors — the classic non-functional option a
# test-wise reader eliminates on sight ("Ignore it as a one-time event"), which
# quietly turns a 4-option item into a 3-option one. Both the drafter rule and
# the critic's STRAWMAN check are meant to stop these and mostly do, but a
# measured run still carried one in roughly a third of items — so the structural
# gate uses this as a deterministic backstop. Only the LEADING verb is matched,
# so a legitimate key that merely contains "ignore" mid-sentence is untouched.
_STRAWMAN_PROBE = re.compile(
    r"^\s*(ignore\b|do nothing\b|leave (it|them|the .{0,30}) as\b|"
    r"keep (it|them) as\b|assume it('s| is| will)\b|report it as is\b|"
    r"never contact\b|fabricat\w*\b)", re.I)


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
                                  role_hint: str = "", seniority: str = "mid",
                                  dimensions: list | None = None,
                                  usage_out: list | None = None
                                  ) -> tuple[list | None, str | None]:
    """Mixed-structure MCQ DRAFT set via the drafter→critic→refiner loop.

    Structure: scenario groups (a paragraph + 3-4 MCQs about it) + standalone
    questions, carried as a FLAT list where grouped items share an optional
    "scenario" string — grading, storage, and review stay index-based and
    untouched. The critic BLIND-SOLVES every question (never sees the key):
    a key mismatch fails it, and so does the critic finding it EASY without
    role knowledge. Language: pure English, or natural Banglish for 'bn'
    (critic rejects over-translation). Bounded: draft(~2x) → critic → up to
    FOUR refine+re-check rounds (the infinite-loop rail, not a cost limit),
    stopping early once the target passes, then a knowledge-free structural gate.

    `dimensions` (qgen-2.3) is the recruiter's chosen kinds of question —
    any subset of {domain, technical, reasoning, judgment}; the drafter spreads
    the set evenly across them and tags each. Default (empty) is the
    knowledge-heavy {domain, technical, reasoning} — judgment off. Guess-proofing
    lives in the DRAFTER (terse parallel options, banned generic-good-practice
    answers and stems) and the expert CRITIC (social-desirability + length-tell
    checks); the final gate is a DETERMINISTIC length-leak drop — no LLM
    "guesser", which can't validate knowledge questions because it knows the
    field. Draft only — the recruiter still reviews, edits, and approves before
    anything goes live. Returns (mcq list, None) or (None, error)."""
    n = max(5, min(20, int(n)))
    jd = (jd_text or "").strip()
    if len(jd) < 40:
        return None, "The job description is too short to generate questions from."
    client = AsyncOpenAI(api_key=api_key)
    job_ctx = (f"JOB TITLE: {job_title or 'not specified'}\n"
               + (f"ROLE TYPE: {role_hint}\n" if role_hint else "")
               + _seniority_ctx(seniority)
               + f"\nJOB DESCRIPTION:\n\"\"\"\n{jd[:8000]}\n\"\"\"")
    fairness = _MCQ_FAIRNESS_RULES.format(lang_rule=_mcq_lang(language))
    dims_block = _dimensions_block(dimensions)   # per-dimension guidance for the drafter
    critic_prompt = MCQ_CRITIC_PROMPT.format(lang_checks=_mcq_lang_checks(language),
                                             level_check=_seniority_check(seniority))

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

    # ── 1) DRAFT ~2x target, mixed structure, flattened with scenario refs.
    #      Heavy over-generation (2026-09-14): the strict critic drops ~half,
    #      so drafting ~2x keeps the final set near the requested count. It's
    #      one call — cost scales with tokens, not a round-trip. ──
    n_scen = 4 if n >= 12 else 2
    draft_target = min(40, n * 2)
    try:
        raw = await _gen_json(client,
                              MCQ_DRAFT_PROMPT.format(n=draft_target, n_scen=n_scen,
                                                      fairness=fairness, dimensions=dims_block),
                              job_ctx, 0.5, 9000, usage_out)
    except Exception as e:
        return None, f"Generation call failed: {str(e)[:200]}"
    drafts = []
    for g in (raw.get("scenarios") or [])[:n_scen + 2]:
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
    drafts = drafts[:draft_target]   # over-generate: the strict bar drops some
    if len(drafts) < 3:
        return None, "Drafting produced too few valid questions — try again."

    # ── 2) CRITIC — the EXPERT judge only, ONE call per round (sees the JD;
    #      the key is never sent). The independent naive-guesser used to run
    #      in PARALLEL here every round — 2 calls x 5 rounds = generation
    #      routinely exceeded the request timeout (502). It now runs ONCE at
    #      the end as a final filter (below), which is far cheaper and enough:
    #      the expert critic's strawman / uniquely-virtuous / trivia rules
    #      already catch most guessable questions each round. ──
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

    # ── 3) REFINE loop: up to 4 rounds, stops EARLY the moment `n` clean
    #      questions exist (extra rounds only run when yield is still short,
    #      so they cost time only when needed). Batched + carry-forward so
    #      rounds accumulate toward n. Fail-soft: trouble never loses passers. ──
    for _round in range(4):
        if not failed or len(passed) >= n:
            break
        # Refine at most REFINE_BATCH failures per round so the model's JSON
        # output isn't truncated (which silently dropped questions and starved
        # yield). Failures beyond the batch are CARRIED FORWARD, not lost, so
        # rounds accumulate toward n instead of shrinking.
        REFINE_BATCH = 12
        batch = failed[:REFINE_BATCH]
        carry = failed[REFINE_BATCH:]
        items = [m for m, _ in batch]
        rs = [r for _, r in batch]
        try:
            raw = await _gen_json(client,
                                  MCQ_REFINE_PROMPT.format(fairness=fairness),
                                  job_ctx + "\n\nQUESTIONS TO FIX:\n"
                                  + _fmt(items, with_keys=True, reasons=rs),
                                  0.4, 6000, usage_out)
            refined = []
            got = [m for m in (raw.get("mcq") or []) if _mcq_shape_ok(m)]
            for i, m in enumerate(got[:len(items)]):
                # The scenario is reattached FROM THE ORIGINAL — the refiner
                # revises questions, never the scenario, and we don't trust
                # it to echo the paragraph back byte-perfect. The dimension tag
                # is advisory; keep the original if the refiner dropped it.
                if items[i].get("scenario"):
                    m["scenario"] = items[i]["scenario"]
                if not m.get("dimension") and items[i].get("dimension"):
                    m["dimension"] = items[i]["dimension"]
                refined.append(m)
            # Any batch item the refiner didn't return is still a failure —
            # carry it too, so nothing is silently dropped.
            unreturned = [(m, r) for (m, r) in batch[len(refined):]]
            if refined:
                re_passed, re_failed = await _critic(refined)
                passed.extend(re_passed)
                failed = re_failed + unreturned + carry
            else:
                failed = unreturned + carry
                if not carry:
                    break
        except Exception as e:
            print(f"[MCQ-GEN] refine round {_round + 1} failed (continuing with passers): {str(e)[:120]}")
            break

    # ── 4) STRUCTURAL LENGTH-LEAK GATE (qgen-2.3, knowledge-free). NO LLM
    #      "guesser" can validate knowledge questions — it knows the field and
    #      flags everything (proved: naive-guesser AND tell-detector both marked
    #      ~15/15, including a clean 4-equal-length JOIN question). So the
    #      automated gate checks only STRUCTURE, which needs no knowledge — the
    #      two mechanical tells a no-knowledge reader actually follows:
    #        (a) LENGTH/DETAIL LEAK: the key is a clear length outlier, so
    #            "pick the most detailed one" wins;
    #        (b) STRAWMAN DISTRACTOR: a wrong option is a dismissive do-nothing
    #            ("Ignore it as a one-time event"), eliminated on sight — which
    #            turns a 4-option item into a 3-option one.
    #      Items with four roughly-equal-length, all-plausible options trip
    #      neither and pass, so yield holds. Deterministic, no API call. The
    #      expert critic (which CAN judge social-desirability per question) plus
    #      the recruiter's review handle what structure cannot see. Fail-soft:
    #      never drop below the usable floor. ──
    def _strawman_distractor(m):
        """True when a WRONG option is a dismissive do-nothing strawman. The key
        itself is exempt — 'ignore NULLs in this average' can be correct."""
        try:
            ci = int(m["correct"])
        except Exception:
            return False
        return any(_STRAWMAN_PROBE.match(str(o))
                   for j, o in enumerate(m.get("options") or []) if j != ci)

    def _length_leak(m):
        opts = [str(o) for o in (m.get("options") or [])]
        if len(opts) != 4:
            return False
        try:
            ci = int(m["correct"])
        except Exception:
            return False
        lengths = [len(o) for o in opts]
        others = [lengths[j] for j in range(4) if j != ci]
        med = sorted(others)[len(others) // 2]   # median of the distractor lengths
        # Correct option is a clear length outlier: the longest AND materially
        # longer than a typical distractor (guards short terse sets from tripping).
        return lengths[ci] == max(lengths) and lengths[ci] >= max(med * 1.6, med + 25)

    if passed:
        n_len = sum(1 for m in passed if _length_leak(m))
        n_straw = sum(1 for m in passed if not _length_leak(m) and _strawman_distractor(m))
        kept = [m for m in passed
                if not _length_leak(m) and not _strawman_distractor(m)]
        dropped = len(passed) - len(kept)
        if dropped and (len(kept) >= min(n, 5) or len(kept) >= 3):
            print(f"[MCQ-GEN] structural gate dropped {dropped} question(s) "
                  f"(length-leak {n_len}, strawman distractor {n_straw})")
            passed = kept

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
    # If the strict bar left too few to be a usable set, say so plainly —
    # a clear "try again" beats shipping a 2-question draft.
    if len(final) < 3:
        return None, ("The quality bar rejected too many questions this time — "
                      "please try Generate again (each run drafts fresh questions).")
    return final, None


async def generate_written_scenario(jd_text: str, api_key: str, job_title: str = "",
                                    k: int = 3, language: str = "en", role_hint: str = "",
                                    seniority: str = "mid",
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
                    + _seniority_ctx(seniority)
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
