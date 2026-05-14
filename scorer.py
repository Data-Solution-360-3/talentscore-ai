"""
CV Screener — 3-Step Pipeline (v2: accuracy hardening)
======================================================
Step 1A: Structure the CV into a clean JSON profile (GPT call 1)
Step 1B: Parse the JD into structured requirements (GPT call 2)
  → 1A and 1B run in parallel.
Step 1C: Compute deterministic tenure/job-hopping signals from parsed CV (no GPT, free).
Step 2:  Score with TWO different perspectives (strict + upside) and reconcile.

What changed from v1:
- Tenure analysis catches frequent job switching (a trust signal, per product requirement).
- Hard-requirement gaps now apply a numeric penalty after scoring, not just dim downgrades.
- Skill normalization fixes false negatives from JS/JavaScript-style nomenclature mismatches.
- CV truncation raised 5000 → 15000 chars (catches metrics on page 3+ of senior CVs).
- Two-pass scoring now uses different angles, not identical temp-0 runs.
- Self-contradiction validator: re-parses if current_title doesn't match years_experience.
"""

import json
import os
import re
import tempfile
import asyncio
import pdfplumber
from openai import AsyncOpenAI


# ─────────────────────────────────────────────────────────────
# PDF EXTRACTION
# ─────────────────────────────────────────────────────────────

def extract_pdf_text(file_bytes: bytes) -> tuple[str, str | None]:
    try:
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name

        text = ""
        with pdfplumber.open(tmp_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        os.unlink(tmp_path)
        text = text.strip()

        if not text or len(text) < 80:
            return "", "PDF appears to be empty or image-based. Please use a text-based PDF."
        return text, None
    except Exception as e:
        return "", f"Could not read PDF: {str(e)}"


# ─────────────────────────────────────────────────────────────
# SKILL NORMALIZATION
# ─────────────────────────────────────────────────────────────
# Common nomenclature variants. The map is intentionally curated and
# conservative — if we don't know a synonym, we leave it alone (better
# to miss a match than to incorrectly equate two different things).
SKILL_SYNONYMS = {
    # languages
    "javascript": ["js", "java script"],
    "typescript": ["ts"],
    "python": ["py", "python3"],
    "c#": ["csharp", "c sharp", "dotnet c#"],
    "c++": ["cpp", "cplusplus"],
    "go": ["golang"],
    "objective-c": ["objc", "objective c"],
    # frameworks / libs
    "node.js": ["node", "nodejs"],
    "react": ["react.js", "reactjs"],
    "vue": ["vue.js", "vuejs"],
    "angular": ["angular.js", "angularjs"],
    "next.js": ["nextjs", "next js"],
    "tensorflow": ["tf"],
    "pytorch": ["torch"],
    "scikit-learn": ["sklearn", "scikit learn"],
    # data / cloud
    "postgresql": ["postgres", "psql"],
    "mongodb": ["mongo"],
    "amazon web services": ["aws"],
    "google cloud platform": ["gcp", "google cloud"],
    "microsoft azure": ["azure"],
    "kubernetes": ["k8s"],
    # concepts
    "machine learning": ["ml"],
    "artificial intelligence": ["ai"],
    "natural language processing": ["nlp"],
    "deep learning": ["dl"],
    "continuous integration": ["ci", "ci/cd", "cicd"],
    "version control": ["git"],
    "user experience": ["ux", "ux design"],
    "user interface": ["ui", "ui design"],
    # roles
    "software engineer": ["software developer", "swe"],
    "data scientist": ["ds"],
}


def _normalize_skill(s: str) -> str:
    """Reduce a skill string to a canonical key. Lowercase, strip punctuation/extra
    whitespace, and resolve known synonyms to a single canonical form."""
    if not s:
        return ""
    key = re.sub(r"[^\w\s.+/-]", " ", s.lower())
    key = re.sub(r"\s+", " ", key).strip()
    for canonical, aliases in SKILL_SYNONYMS.items():
        if key == canonical or key in aliases:
            return canonical
    return key


def normalize_skill_list(skills: list) -> set:
    return {_normalize_skill(s) for s in (skills or []) if s}


# ─────────────────────────────────────────────────────────────
# STEP 1A — STRUCTURE THE CV
# ─────────────────────────────────────────────────────────────

CV_STRUCTURE_PROMPT = """You are an expert CV parser. Extract every fact present in the CV into structured JSON.
Rules:
- Do not invent facts. If a field isn't in the CV, use null or an empty list.
- For each work_experience entry, you MUST estimate `years` (float, 1 decimal) from the date range.
  If only "Present" is given, assume the current year. If only one year is shown ("2022"), use 0.5.
- `total_years_experience` = sum of all work_experience.years entries, rounded to nearest 0.5.
  Do NOT just guess based on graduation year; sum from actual roles.
- Sort work_experience newest-first.
Respond with valid JSON only, no markdown."""


def build_cv_parse_prompt(cv_text: str) -> str:
    return f"""Parse this CV into a structured JSON profile.

CV TEXT:
\"\"\"
{cv_text[:15000]}
\"\"\"

Respond ONLY with this JSON structure:
{{
  "personal": {{
    "name": "<full name or Unknown>",
    "email": "<email or null>",
    "phone": "<phone or null>",
    "location": "<city/country or null>",
    "linkedin": "<linkedin url or null>"
  }},
  "current_title": "<most recent job title>",
  "total_years_experience": <float, sum of all roles in years, rounded to nearest 0.5>,
  "summary": "<professional summary if present, else null>",
  "work_experience": [
    {{
      "title": "<job title>",
      "company": "<company name>",
      "start": "<YYYY or YYYY-MM or null>",
      "end": "<YYYY or YYYY-MM or 'Present' or null>",
      "years": <float, length of this role rounded to 0.1>,
      "responsibilities": ["<key responsibility>"],
      "achievements": ["<quantified achievement if any>"],
      "technologies": ["<tech/tool used in this role>"]
    }}
  ],
  "education": [
    {{
      "degree": "<degree title>",
      "field": "<field of study>",
      "institution": "<university/college>",
      "year": "<graduation year or null>"
    }}
  ],
  "skills": {{
    "technical": ["<skill>"],
    "soft": ["<soft skill>"],
    "languages": ["<programming language>"],
    "tools": ["<tool or platform>"],
    "frameworks": ["<framework>"]
  }},
  "certifications": ["<cert name and issuer>"],
  "languages_spoken": ["<language>"],
  "notable_projects": ["<project name and brief description>"]
}}"""


# ─────────────────────────────────────────────────────────────
# STEP 1B — PARSE THE JOB DESCRIPTION
# ─────────────────────────────────────────────────────────────

JD_PARSE_PROMPT = """You are an expert job description analyst. Extract requirements into structured JSON.
CRITICAL — separating REQUIRED vs PREFERRED:
- Put in `required_skills` ONLY skills explicitly marked as required, must-have, essential, mandatory,
  or listed in "Requirements"/"Qualifications" without "preferred"/"nice-to-have" framing.
- Put in `hard_requirements` any pass/fail criteria: visa status, location/relocation, security
  clearance, specific certifications that are non-negotiable, language fluency requirements.
- If the JD is vague, err on the side of `preferred_skills`. A false "required" causes good
  candidates to be rejected; a false "preferred" only causes a minor scoring miss.
Respond with valid JSON only, no markdown."""


def build_jd_parse_prompt(jd_text: str) -> str:
    return f"""Parse this job description into structured requirements JSON.

JOB DESCRIPTION:
\"\"\"
{jd_text[:6000]}
\"\"\"

Respond ONLY with this JSON structure:
{{
  "role_title": "<exact job title>",
  "seniority_level": "<Junior | Mid | Senior | Lead | Manager | Director>",
  "department": "<department or team>",
  "industry": "<industry sector>",
  "employment_type": "<Full-time | Part-time | Contract | Remote>",
  "required_skills": ["<must-have skill>"],
  "preferred_skills": ["<nice-to-have skill>"],
  "required_technologies": ["<required tech/tool>"],
  "required_experience_years": <minimum integer years required, 0 if not specified>,
  "required_education": "<e.g. Bachelor's in CS or equivalent, or null>",
  "required_certifications": ["<required cert if any>"],
  "key_responsibilities": ["<main responsibility>"],
  "soft_skills_required": ["<soft skill>"],
  "domain_knowledge_required": ["<specific domain knowledge>"],
  "nice_to_have": ["<bonus qualification>"],
  "hard_requirements": ["<pass/fail criterion like visa status, language fluency, clearance>"]
}}"""


# ─────────────────────────────────────────────────────────────
# STEP 1C — TENURE ANALYSIS (deterministic, no GPT)
# ─────────────────────────────────────────────────────────────

def analyze_tenure(cv_profile: dict) -> dict:
    """Compute job-hopping signals from parsed work_experience.

    Returns a dict the scorer can use directly. We don't ask GPT to compute these —
    arithmetic on parsed dates is more reliable than asking a model to count, and
    we want this to be exactly reproducible across runs.

    Definitions (industry-standard rule of thumb):
    - "Short stint" = role under 12 months
    - "Job hopper" = 3+ short stints in last 5 jobs, OR average tenure < 18 months
    - "Stable"     = average tenure >= 30 months and at most 1 short stint
    """
    roles = cv_profile.get("work_experience", []) or []
    if not roles:
        return {
            "total_roles": 0,
            "avg_tenure_years": None,
            "short_stints": 0,
            "current_role_years": None,
            "stability": "Unknown",
            "trust_flag": False,
            "notes": "No work history found in CV.",
        }

    years_list = []
    for r in roles:
        y = r.get("years")
        try:
            y = float(y) if y is not None else 0.0
        except (TypeError, ValueError):
            y = 0.0
        years_list.append(max(0.0, y))

    total_roles = len(years_list)
    avg_tenure = sum(years_list) / total_roles if total_roles else 0.0
    short_stints = sum(1 for y in years_list if 0 < y < 1.0)
    current_role_years = years_list[0] if years_list else 0.0

    # Look only at the most recent 5 roles for "recent pattern" — early-career
    # internships shouldn't haunt a candidate forever.
    recent = years_list[:5]
    recent_short = sum(1 for y in recent if 0 < y < 1.0)
    avg_recent = sum(recent) / len(recent) if recent else 0.0

    if avg_recent < 1.5 or recent_short >= 3:
        stability = "Frequent switcher"
        trust_flag = True
        notes = (
            f"{recent_short} role(s) under 12 months in last {len(recent)} positions. "
            f"Average tenure {avg_recent:.1f} yrs. May indicate job-hopping — verify "
            f"reasons in interview."
        )
    elif avg_recent < 2.5 or recent_short >= 2:
        stability = "Moderate"
        trust_flag = False
        notes = (
            f"Average recent tenure {avg_recent:.1f} yrs across {len(recent)} roles. "
            f"Reasonable but worth probing."
        )
    else:
        stability = "Stable"
        trust_flag = False
        notes = (
            f"Average tenure {avg_recent:.1f} yrs across recent roles. "
            f"Shows commitment."
        )

    return {
        "total_roles": total_roles,
        "avg_tenure_years": round(avg_tenure, 1),
        "avg_recent_tenure_years": round(avg_recent, 1),
        "short_stints": short_stints,
        "current_role_years": round(current_role_years, 1),
        "stability": stability,
        "trust_flag": trust_flag,
        "notes": notes,
    }


# ─────────────────────────────────────────────────────────────
# HARD-REQUIREMENT GAP DETECTION
# ─────────────────────────────────────────────────────────────

def detect_hard_gaps(cv_profile: dict, jd: dict) -> list:
    """Return a list of unmet hard requirements with penalty points.

    We're intentionally conservative — only flag a hard gap when we can detect it
    with high confidence from structured data. Anything ambiguous gets handled by
    the LLM scorer instead.
    """
    gaps = []

    # 1. Years of experience cliff
    req_years = jd.get("required_experience_years") or 0
    cv_years = cv_profile.get("total_years_experience") or 0
    try:
        cv_years = float(cv_years)
    except (TypeError, ValueError):
        cv_years = 0.0
    try:
        req_years = float(req_years)
    except (TypeError, ValueError):
        req_years = 0.0
    if req_years >= 3 and cv_years < req_years * 0.5:
        gaps.append({
            "requirement": f"{int(req_years)}+ years experience (candidate has {cv_years:.1f})",
            "kind": "experience",
            "penalty": 18,
        })
    elif req_years >= 2 and cv_years < req_years - 1.5:
        gaps.append({
            "requirement": f"{int(req_years)}+ years experience (candidate has {cv_years:.1f})",
            "kind": "experience",
            "penalty": 10,
        })

    # 2. Required-skills coverage cliff
    req_skills_norm = normalize_skill_list(jd.get("required_skills", []))
    req_tech_norm = normalize_skill_list(jd.get("required_technologies", []))
    cv_skills = []
    sk = cv_profile.get("skills", {}) or {}
    for bucket in ("technical", "languages", "tools", "frameworks"):
        cv_skills.extend(sk.get(bucket, []) or [])
    # Also pick up tech mentioned per role — many CVs only list tech inside role descriptions
    for r in cv_profile.get("work_experience", []) or []:
        cv_skills.extend(r.get("technologies", []) or [])
    cv_skills_norm = normalize_skill_list(cv_skills)

    all_required = req_skills_norm | req_tech_norm
    if all_required:
        matched = all_required & cv_skills_norm
        coverage = len(matched) / len(all_required)
        missing = sorted(all_required - cv_skills_norm)
        if coverage < 0.3 and len(all_required) >= 3:
            gaps.append({
                "requirement": f"Required skills (matched {len(matched)}/{len(all_required)})",
                "kind": "skills",
                "penalty": 15,
                "missing": missing[:6],
            })
        elif coverage < 0.5 and len(all_required) >= 4:
            gaps.append({
                "requirement": f"Required skills (matched {len(matched)}/{len(all_required)})",
                "kind": "skills",
                "penalty": 8,
                "missing": missing[:6],
            })

    return gaps


# ─────────────────────────────────────────────────────────────
# STEP 2 — MATCH & SCORE
# ─────────────────────────────────────────────────────────────

SCORING_SYSTEM_PROMPT = """You are a senior technical recruiter with 15+ years of experience.
You score candidates strictly and consistently using ONLY the structured data provided.

Calibration anchors — use these as fixed reference points:
- 90+ overall: Exceptional fit. All required skills, exceeds experience bar, quantified impact, stable tenure.
- 75-89:       Strong fit. Hits ~80%+ of required skills, meets experience bar, some quantified achievements.
- 60-74:       Partial fit. Missing some required skills OR experience slightly short OR mixed tenure.
- 45-59:       Weak fit. Multiple missing requirements OR significant experience gap.
- Below 45:    Poor fit. Lacks core requirements.

Score each dimension 0-20 against the criteria in the prompt. Cite specific evidence from the CV
(skill name, company name, year count) — generic feedback like "good experience" is not acceptable.

Respond with valid JSON only."""


STRICT_ANGLE = """Adopt a STRICT gatekeeper perspective for this scoring pass:
- A skill on the CV is "matched" only if it appears as a primary skill, in a role description,
  or in a notable project. A skill listed once in a long skills wall with no work evidence
  counts as PARTIAL match (give half credit on Skills Match).
- Experience years should be from actual roles, not student/internship time unless directly relevant.
- If the CV exaggerates (e.g. "expert in X" but X only appears once in a 6-month role), call it out.
"""

UPSIDE_ANGLE = """Adopt an UPSIDE-FOCUSED hiring manager perspective for this scoring pass:
- Recognize transferable skills. Python experience can transfer to a Go role with ramp-up.
  React experience transfers to Vue. SQL on MySQL transfers to PostgreSQL.
- Strong fundamentals (CS degree, top company in past) can compensate for a missing specific tool.
- Quantified achievements anywhere should boost Achievement & Impact, even if the role title
  doesn't perfectly match.
"""


def build_scoring_prompt(cv_profile: dict, jd_requirements: dict, tenure: dict, angle: str) -> str:
    return f"""{angle}

STRUCTURED JOB REQUIREMENTS:
{json.dumps(jd_requirements, indent=2)}

STRUCTURED CANDIDATE PROFILE:
{json.dumps(cv_profile, indent=2)}

PRE-COMPUTED TENURE SIGNALS (use these for the Stability & Tenure dimension; do not recompute):
{json.dumps(tenure, indent=2)}

DIMENSION CRITERIA (each 0-20):
- Skills Match (25%): How many of jd.required_skills + jd.required_technologies are found in the CV?
  20 = ALL required matched with evidence. 15 = 80% matched. 10 = ~50%. 5 = <30%. 0 = none.
- Experience Relevance (20%): Years AND domain relevance vs jd.required_experience_years.
  20 = meets/exceeds years AND domain matches. 10 = close on years OR adjacent domain. 0 = neither.
- Education & Certifications (10%): Compare cv.education + cv.certifications vs jd.required_education + jd.required_certifications.
  20 = meets or exceeds. 10 = related field. 5 = unrelated but has degree. 0 = no relevant education.
- Achievement & Impact (20%): Count quantified achievements (numbers, %, $, scale) across work_experience.
  20 = 5+ quantified achievements. 12 = 2-3. 5 = 1 vague. 0 = no metrics anywhere.
- Role Alignment (15%): Does career trajectory point toward this role?
  20 = same/adjacent role with progression. 12 = related field. 5 = pivot. 0 = unrelated.
- Stability & Tenure (10%): Use the pre-computed tenure signals.
  20 = "Stable". 12 = "Moderate". 5 = "Frequent switcher" with valid context. 0 = severe job hopping.

Compute overall_score as weighted sum of dimension scores (each dim is 0-20):
  overall = (skills*0.25 + experience*0.20 + education*0.10 + achievement*0.20 + alignment*0.15 + stability*0.10) * 5

Respond ONLY with this JSON:
{{
  "candidate_name": "<from cv_profile.personal.name>",
  "current_title": "<from cv_profile.current_title>",
  "years_experience": "<cv_profile.total_years_experience>",
  "overall_score": <integer 0-100, computed via the weighted formula above>,
  "recommendation": "<STRONG HIRE | HIRE | MAYBE | REJECT>",
  "recommendation_reason": "<1 precise sentence citing specific evidence>",
  "summary": "<3-4 sentences referencing specific CV facts vs JD requirements>",
  "dimensions": [
    {{"name": "Skills Match",              "score": <0-20>, "weight": 0.25, "feedback": "<cite specific matched/missing skills>", "matched_skills": ["<exact name>"], "missing_skills": ["<exact name>"]}},
    {{"name": "Experience Relevance",      "score": <0-20>, "weight": 0.20, "feedback": "<cite years and domain>", "matched_skills": [], "missing_skills": []}},
    {{"name": "Education & Certifications","score": <0-20>, "weight": 0.10, "feedback": "<cite degree/certs>", "matched_skills": [], "missing_skills": []}},
    {{"name": "Achievement & Impact",      "score": <0-20>, "weight": 0.20, "feedback": "<cite specific metrics or note their absence>", "matched_skills": [], "missing_skills": []}},
    {{"name": "Role Alignment",            "score": <0-20>, "weight": 0.15, "feedback": "<cite title progression>", "matched_skills": [], "missing_skills": []}},
    {{"name": "Stability & Tenure",        "score": <0-20>, "weight": 0.10, "feedback": "<reference the tenure signals>", "matched_skills": [], "missing_skills": []}}
  ],
  "key_strengths": ["<specific strength with evidence>", "<strength 2>", "<strength 3>"],
  "critical_gaps": ["<specific gap referencing JD requirement>", "<gap 2>", "<gap 3>"],
  "interview_questions": [
    "<targeted question probing a specific gap or verifying a claim>",
    "<question 2>",
    "<question 3>"
  ],
  "hiring_risks": ["<specific risk based on CV data, or 'None identified'>"],
  "skills_coverage_pct": <integer 0-100, percentage of required_skills found in CV>
}}"""


# ─────────────────────────────────────────────────────────────
# ASYNC GPT HELPER
# ─────────────────────────────────────────────────────────────

async def gpt_json_call(client: AsyncOpenAI, system: str, user: str,
                        temperature: float = 0.0, max_tokens: int = 2500) -> dict:
    response = await client.chat.completions.create(
        model="gpt-4o",
        max_tokens=max_tokens,
        temperature=temperature,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ]
    )
    return json.loads(response.choices[0].message.content)


# ─────────────────────────────────────────────────────────────
# RECONCILE TWO RUNS + APPLY HARD-GAP PENALTIES
# ─────────────────────────────────────────────────────────────

def reconcile_scores(strict: dict, upside: dict, hard_gaps: list, tenure: dict) -> dict:
    """Combine the strict + upside perspective runs and apply hard-gap penalties.
    The two perspectives disagreeing is INFORMATION — it tells us the candidate is
    borderline. Blend (strict slightly weighted) and then subtract hard-gap penalties."""

    # 60/40 toward strict — generosity is a known LLM failure mode.
    def blend(a, b):
        try:
            return round((float(a) * 0.6 + float(b) * 0.4), 1)
        except (TypeError, ValueError):
            return 0

    result = dict(strict)

    overall_strict = strict.get("overall_score", 0)
    overall_upside = upside.get("overall_score", 0)
    blended = blend(overall_strict, overall_upside)

    # Average dimension scores
    dims_strict = {d["name"]: d for d in strict.get("dimensions", [])}
    dims_upside = {d["name"]: d for d in upside.get("dimensions", [])}
    averaged_dims = []
    for dim in strict.get("dimensions", []):
        name = dim["name"]
        ds = dims_strict.get(name, {}).get("score", 0)
        du = dims_upside.get(name, {}).get("score", 0)
        avg_dim = dict(dim)
        avg_dim["score"] = blend(ds, du)
        if "matched_skills" in dim:
            a_set = set(dims_strict.get(name, {}).get("matched_skills", []) or [])
            b_set = set(dims_upside.get(name, {}).get("matched_skills", []) or [])
            avg_dim["matched_skills"] = sorted(a_set | b_set)
        if "missing_skills" in dim:
            # Only call a skill "missing" if BOTH runs agree it's missing.
            # Lowers false negatives caused by one run failing to spot a match.
            a_set = set(dims_strict.get(name, {}).get("missing_skills", []) or [])
            b_set = set(dims_upside.get(name, {}).get("missing_skills", []) or [])
            avg_dim["missing_skills"] = sorted(a_set & b_set)
        averaged_dims.append(avg_dim)
    result["dimensions"] = averaged_dims

    # Apply hard-gap penalties. Cap at 35 — per user spec: "lower significantly,
    # don't auto-reject". 35 is enough to flip HIRE → REJECT but won't nuke a
    # genuinely promising adjacent candidate.
    total_penalty = min(sum(g.get("penalty", 0) for g in hard_gaps), 35)
    final_score = max(0, min(100, int(round(blended - total_penalty))))
    result["overall_score"] = final_score

    # Merge critical_gaps — prepend hard gaps for visibility
    gpt_gaps = result.get("critical_gaps", []) or []
    hard_gap_lines = []
    for g in hard_gaps:
        line = f"[{g['kind'].upper()}] {g['requirement']}"
        if g.get("missing"):
            line += f" — missing: {', '.join(g['missing'])}"
        hard_gap_lines.append(line)
    result["critical_gaps"] = hard_gap_lines + [g for g in gpt_gaps if g not in hard_gap_lines]

    # Skills coverage
    cov_a = strict.get("skills_coverage_pct", 0)
    cov_b = upside.get("skills_coverage_pct", 0)
    result["skills_coverage_pct"] = blend(cov_a, cov_b)
    if not result["skills_coverage_pct"]:
        all_matched, all_missing = set(), set()
        for d in averaged_dims:
            all_matched.update(d.get("matched_skills", []) or [])
            all_missing.update(d.get("missing_skills", []) or [])
        total = len(all_matched) + len(all_missing)
        if total > 0:
            result["skills_coverage_pct"] = round(len(all_matched) / total * 100)

    # Recompute recommendation from final score (after penalty)
    if final_score >= 80:
        result["recommendation"] = "STRONG HIRE"
    elif final_score >= 65:
        result["recommendation"] = "HIRE"
    elif final_score >= 48:
        result["recommendation"] = "MAYBE"
    else:
        result["recommendation"] = "REJECT"

    # Surface tenure as first-class data
    result["tenure_analysis"] = tenure
    risks = result.get("hiring_risks", []) or []
    if tenure.get("trust_flag"):
        msg = f"Tenure: {tenure.get('notes', '')}"
        if msg not in risks:
            risks = [msg] + [r for r in risks if r != "None identified"]
    result["hiring_risks"] = risks

    # Diagnostic metadata
    diff = abs(float(overall_strict) - float(overall_upside))
    result["score_consistency"] = {
        "strict_pass":     overall_strict,
        "upside_pass":     overall_upside,
        "difference":      round(diff, 1),
        "penalty_applied": total_penalty,
        "confidence":      "High" if diff <= 5 else "Medium" if diff <= 12 else "Low",
    }

    return result


# ─────────────────────────────────────────────────────────────
# SELF-CONTRADICTION VALIDATOR
# ─────────────────────────────────────────────────────────────

def cv_is_self_consistent(cv: dict) -> tuple[bool, str]:
    """Catch the GPT failure where current_title says 'Senior Manager' but
    total_years_experience is 1. One cheap retry beats poisoning all of scoring."""
    title = (cv.get("current_title") or "").lower()
    try:
        years = float(cv.get("total_years_experience") or 0)
    except (TypeError, ValueError):
        years = 0.0
    senior_words = ("senior", "lead", "principal", "head of", "director", "vp", "chief", "manager")
    if years < 3 and any(w in title for w in senior_words):
        return False, f"Title '{cv.get('current_title')}' suggests seniority but only {years} years experience parsed."
    return True, ""


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

async def run_screening_pipeline(cv_text: str, jd_text: str, api_key: str) -> tuple[dict, str | None]:
    """Full screening pipeline. Returns (result_dict, error_str_or_None)."""
    try:
        client = AsyncOpenAI(api_key=api_key)

        # ── STEP 1A + 1B in parallel ──
        cv_profile, jd_requirements = await asyncio.gather(
            gpt_json_call(client, CV_STRUCTURE_PROMPT, build_cv_parse_prompt(cv_text)),
            gpt_json_call(client, JD_PARSE_PROMPT,    build_jd_parse_prompt(jd_text))
        )

        # ── Self-consistency: one retry if the model contradicted itself ──
        ok, reason = cv_is_self_consistent(cv_profile)
        if not ok:
            retry_prompt = build_cv_parse_prompt(cv_text) + (
                f"\n\nIMPORTANT: Your previous parse had a contradiction: {reason}\n"
                f"Re-parse carefully. Sum years from actual roles in work_experience."
            )
            cv_profile = await gpt_json_call(client, CV_STRUCTURE_PROMPT, retry_prompt)

        # ── STEP 1C: deterministic tenure analysis (no GPT) ──
        tenure = analyze_tenure(cv_profile)

        # ── STEP 2: score with two perspectives in parallel ──
        strict_prompt = build_scoring_prompt(cv_profile, jd_requirements, tenure, STRICT_ANGLE)
        upside_prompt = build_scoring_prompt(cv_profile, jd_requirements, tenure, UPSIDE_ANGLE)
        strict_result, upside_result = await asyncio.gather(
            gpt_json_call(client, SCORING_SYSTEM_PROMPT, strict_prompt, temperature=0.2),
            gpt_json_call(client, SCORING_SYSTEM_PROMPT, upside_prompt, temperature=0.3),
        )

        # ── Detect hard-requirement gaps deterministically ──
        hard_gaps = detect_hard_gaps(cv_profile, jd_requirements)

        # ── Reconcile, apply penalties, finalize ──
        final = reconcile_scores(strict_result, upside_result, hard_gaps, tenure)

        # ── Attach parsed structures for transparency in the candidate report ──
        final["parsed_cv"] = cv_profile
        final["parsed_jd"] = jd_requirements

        # ── Validate score range ──
        final["overall_score"] = max(0, min(100, int(final["overall_score"])))

        return final, None

    except Exception as e:
        err = str(e)
        if "authentication" in err.lower() or "api key" in err.lower():
            return {}, "Invalid OpenAI API key. Please check and try again."
        if "rate limit" in err.lower():
            return {}, "OpenAI rate limit hit. Please wait a moment and try again."
        if "quota" in err.lower():
            return {}, "OpenAI quota exceeded. Please check your billing."
        return {}, f"Screening failed: {err}"
