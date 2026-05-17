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
# DIMENSION WEIGHTS
# ─────────────────────────────────────────────────────────────
# The six scoring dimensions. Weights must sum to 1.0 — we normalize on read.
# Per-job custom weights let recruiters tune what matters for each role:
# a sales role weighs Achievement & Impact heavier than Education; a junior
# dev role does the opposite. Without per-job weights, we use the balanced default.

DIMENSION_NAMES = [
    "Skills Match",
    "Experience Relevance",
    "Education & Certifications",
    "Achievement & Impact",
    "Role Alignment",
    "Stability & Tenure",
]

DEFAULT_WEIGHTS = {
    "Skills Match":               0.25,
    "Experience Relevance":       0.20,
    "Education & Certifications": 0.10,
    "Achievement & Impact":       0.20,
    "Role Alignment":             0.15,
    "Stability & Tenure":         0.10,
}

# Curated presets — chosen to reflect what experienced recruiters actually
# emphasize for these role types. Each preset must sum to 1.0.
# Recruiters pick one as a starting point, then fine-tune with sliders.
WEIGHT_PRESETS = {
    "balanced": {
        "Skills Match": 0.25, "Experience Relevance": 0.20, "Education & Certifications": 0.10,
        "Achievement & Impact": 0.20, "Role Alignment": 0.15, "Stability & Tenure": 0.10,
    },
    # Sales / BD roles: hire on track record of hitting numbers, not credentials.
    "sales": {
        "Skills Match": 0.10, "Experience Relevance": 0.20, "Education & Certifications": 0.05,
        "Achievement & Impact": 0.35, "Role Alignment": 0.20, "Stability & Tenure": 0.10,
    },
    # Senior engineering: deep skill match + demonstrated impact + relevant trajectory.
    "engineering_senior": {
        "Skills Match": 0.30, "Experience Relevance": 0.20, "Education & Certifications": 0.05,
        "Achievement & Impact": 0.20, "Role Alignment": 0.15, "Stability & Tenure": 0.10,
    },
    # Junior engineering / entry-level: weigh education + raw skills, light on experience.
    "engineering_junior": {
        "Skills Match": 0.30, "Experience Relevance": 0.10, "Education & Certifications": 0.25,
        "Achievement & Impact": 0.10, "Role Alignment": 0.20, "Stability & Tenure": 0.05,
    },
    # Manager / leadership: track record, stability, alignment matter more than raw skills.
    "manager": {
        "Skills Match": 0.10, "Experience Relevance": 0.25, "Education & Certifications": 0.10,
        "Achievement & Impact": 0.25, "Role Alignment": 0.20, "Stability & Tenure": 0.10,
    },
    # Customer-facing operations / support: experience + alignment + soft signals.
    "operations": {
        "Skills Match": 0.15, "Experience Relevance": 0.25, "Education & Certifications": 0.10,
        "Achievement & Impact": 0.20, "Role Alignment": 0.20, "Stability & Tenure": 0.10,
    },
    # Data / analytics: skills heavy with education credit (often degree-gated).
    "data_analytics": {
        "Skills Match": 0.30, "Experience Relevance": 0.20, "Education & Certifications": 0.15,
        "Achievement & Impact": 0.15, "Role Alignment": 0.15, "Stability & Tenure": 0.05,
    },
    # Creative / design: portfolio impact > credentials.
    "creative": {
        "Skills Match": 0.20, "Experience Relevance": 0.15, "Education & Certifications": 0.05,
        "Achievement & Impact": 0.30, "Role Alignment": 0.20, "Stability & Tenure": 0.10,
    },
}


def normalize_weights(weights: dict | None) -> dict:
    """Return a clean weights dict that always:
       - contains all 6 dimensions (missing ones get default)
       - sums to exactly 1.0 (rescaled if user input doesn't)
       - has no negative or NaN values

    If `weights` is None/empty/invalid, returns DEFAULT_WEIGHTS unchanged.
    This is the gate: every caller of scoring goes through this so we never
    hit a divide-by-zero or weights summing to 1.04 from JS floating point."""
    if not weights or not isinstance(weights, dict):
        return dict(DEFAULT_WEIGHTS)

    cleaned = {}
    for name in DIMENSION_NAMES:
        v = weights.get(name, DEFAULT_WEIGHTS[name])
        try:
            v = float(v)
        except (TypeError, ValueError):
            v = DEFAULT_WEIGHTS[name]
        cleaned[name] = max(0.0, v)   # clamp negatives to 0

    total = sum(cleaned.values())
    if total <= 0:
        # All zero (or all junk) — fall back to default rather than divide by zero
        return dict(DEFAULT_WEIGHTS)
    # Rescale so they sum to exactly 1.0
    return {k: v / total for k, v in cleaned.items()}


def weighted_overall(dim_scores: dict, weights: dict) -> float:
    """Compute overall_score (0-100) from per-dimension scores (each 0-20) using weights.
    The scoring rubric in the prompt asks for dim scores 0-20; final formula is:
       overall = sum(dim_score * weight) * 5
    Multiplying by 5 maps the 0-20 weighted sum to a 0-100 scale."""
    return sum(dim_scores.get(name, 0) * weights[name] for name in DIMENSION_NAMES) * 5


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

    Important: "frequent switcher" is only a meaningful label for a candidate
    who has had enough career to demonstrate the pattern. A fresh grad with
    1 internship + 1 first job isn't job-hopping — they're just starting out.
    We require BOTH a minimum total career (~2 yrs) AND a minimum role count (3)
    before the trust flag can fire.
    """
    roles = cv_profile.get("work_experience", []) or []
    if not roles:
        return {
            "total_roles": 0,
            "avg_tenure_years": None,
            "total_career_years": 0.0,
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

    total_roles  = len(years_list)
    total_career = sum(years_list)
    avg_tenure   = total_career / total_roles if total_roles else 0.0
    short_stints = sum(1 for y in years_list if 0 < y < 1.0)
    current_role_years = years_list[0] if years_list else 0.0

    # Use the most recent 5 roles for "recent pattern" — early-career internships
    # shouldn't haunt a candidate forever.
    recent = years_list[:5]
    recent_short = sum(1 for y in recent if 0 < y < 1.0)
    avg_recent   = sum(recent) / len(recent) if recent else 0.0

    # ── Minimum career threshold for the "job hopper" pattern to be meaningful ──
    # If a candidate has under ~2 years of total career, or fewer than 3 roles,
    # there isn't enough data to call them a "frequent switcher" — they're just
    # early-career. We label these neutrally and never raise the trust flag.
    MIN_CAREER_YEARS_FOR_FLAG = 2.0
    MIN_ROLES_FOR_FLAG = 3

    if total_career < MIN_CAREER_YEARS_FOR_FLAG or total_roles < MIN_ROLES_FOR_FLAG:
        stability = "Early career"
        trust_flag = False
        notes = (
            f"Total career {total_career:.1f} yrs across {total_roles} role(s). "
            f"Insufficient history to assess long-term tenure pattern."
        )
        return {
            "total_roles":             total_roles,
            "total_career_years":      round(total_career, 1),
            "avg_tenure_years":        round(avg_tenure, 1),
            "avg_recent_tenure_years": round(avg_recent, 1),
            "short_stints":            short_stints,
            "current_role_years":      round(current_role_years, 1),
            "stability":               stability,
            "trust_flag":              trust_flag,
            "notes":                   notes,
        }

    # ── Pattern classification (only reached when candidate has enough career history) ──
    # Heuristic ratio: how much of the candidate's career has been spent in <1yr stints?
    # If half their career is short stints, that's a real pattern regardless of average.
    short_stint_total = sum(y for y in years_list if 0 < y < 1.0)
    short_ratio = short_stint_total / total_career if total_career > 0 else 0

    if avg_recent < 1.5 or recent_short >= 3 or short_ratio > 0.5:
        stability = "Frequent switcher"
        trust_flag = True
        notes = (
            f"{recent_short} role(s) under 12 months in last {len(recent)} positions. "
            f"Average recent tenure {avg_recent:.1f} yrs across {total_career:.1f} yrs "
            f"total career. May indicate job-hopping — verify reasons in interview."
        )
    elif avg_recent < 2.5 or recent_short >= 2:
        stability = "Moderate"
        trust_flag = False
        notes = (
            f"Average recent tenure {avg_recent:.1f} yrs across {len(recent)} roles "
            f"({total_career:.1f} yrs total career). Reasonable but worth probing."
        )
    else:
        stability = "Stable"
        trust_flag = False
        notes = (
            f"Average tenure {avg_recent:.1f} yrs across recent roles "
            f"({total_career:.1f} yrs total career). Shows commitment."
        )

    return {
        "total_roles":             total_roles,
        "total_career_years":      round(total_career, 1),
        "avg_tenure_years":        round(avg_tenure, 1),
        "avg_recent_tenure_years": round(avg_recent, 1),
        "short_stints":            short_stints,
        "current_role_years":      round(current_role_years, 1),
        "stability":               stability,
        "trust_flag":              trust_flag,
        "notes":                   notes,
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
# CV AUTHENTICITY DETECTION
# ─────────────────────────────────────────────────────────────
# Lexical-only detector for ChatGPT-written CVs. No extra GPT call — just text
# pattern matching on the raw CV text. The signals chosen here are heuristics
# that, individually, are weak; together they form a useful flag. We surface
# the result as a *separate* signal (does NOT lower the score) because (a) it's
# not reliable enough to penalize, and (b) recruiters want the data, not the
# decision — many candidates legitimately use AI to polish their CVs.

# Phrases that GPT/Claude love and human CVs rarely contain organically.
# Each match = +1 to the AI-likelihood score (lowercase comparison).
_AI_CLICHES = [
    "leveraged", "leverage", "spearheaded", "spearhead", "results-oriented",
    "results-driven", "proven track record", "synergize", "synergy", "synergies",
    "championed", "orchestrated", "spearheading", "results oriented",
    "strategic initiatives", "cross-functional", "stakeholder management",
    "value proposition", "drove revenue", "drove growth", "drove success",
    "demonstrated proficiency", "demonstrated expertise", "in-depth knowledge",
    "comprehensive understanding", "deep understanding of", "deep expertise",
    "actionable insights", "data-driven decision", "robust solutions",
    "innovative solutions", "cutting-edge", "best practices", "best-in-class",
    "world-class", "transformative", "transformational", "passionate about",
    "deeply passionate", "thrive in", "enthusiastic about",
    "elevated", "optimized workflows", "streamlined operations",
    "collaborated cross-functionally", "fostering collaboration",
    "delivered exceptional", "exceptional results", "exceeded expectations consistently",
]


def detect_cv_authenticity(cv_text: str) -> dict:
    """Estimate likelihood that a CV was AI-written. Returns a dict with:
       - ai_likelihood_pct: int 0-100
       - confidence: Low | Medium | High
       - signals: list of which heuristics fired (for transparency)
       - is_flagged: bool — convenience for UI ("show the badge?")

    This is intentionally a separate signal from the match score. We don't want
    to penalize AI-polished CVs (many strong candidates use AI); we just want
    recruiters to know which CVs deserve extra interview scrutiny."""
    if not cv_text or len(cv_text) < 200:
        return {
            "ai_likelihood_pct": 0,
            "confidence": "Low",
            "signals": [],
            "is_flagged": False,
            "notes": "CV too short to analyze.",
        }

    text  = cv_text.lower()
    words = re.findall(r"\b[a-z]+\b", text)
    word_count = len(words)
    if word_count < 80:
        return {
            "ai_likelihood_pct": 0,
            "confidence": "Low",
            "signals": [],
            "is_flagged": False,
            "notes": "CV too short to analyze.",
        }

    signals = []
    score   = 0   # raw points, will be converted to percent at the end

    # ── 1. AI cliché density (per 1000 words) ──
    # Genuine human CVs typically have 0-2 of these phrases. AI CVs often have 8+.
    cliche_hits = sum(1 for phrase in _AI_CLICHES if phrase in text)
    cliche_density = cliche_hits / (word_count / 1000)   # hits per 1000 words
    if cliche_density >= 8:
        signals.append(f"Very high AI-cliché density ({cliche_hits} matches, {cliche_density:.1f}/1k words)")
        score += 35
    elif cliche_density >= 4:
        signals.append(f"Elevated AI-cliché density ({cliche_hits} matches)")
        score += 18
    elif cliche_density >= 2:
        signals.append(f"Some AI-style phrasing ({cliche_hits} matches)")
        score += 8

    # ── 2. Average sentence length variance ──
    # Humans write with bursty rhythm (3-word sentences next to 25-word ones).
    # LLM output is much more uniform. We measure the coefficient of variation
    # of sentence lengths; very low CoV is a giveaway.
    sentences = [s.strip() for s in re.split(r"[.!?\n]+", cv_text) if s.strip()]
    sent_lens = [len(s.split()) for s in sentences if len(s.split()) >= 3]
    if len(sent_lens) >= 8:
        mean = sum(sent_lens) / len(sent_lens)
        if mean > 0:
            var  = sum((x - mean) ** 2 for x in sent_lens) / len(sent_lens)
            std  = var ** 0.5
            cov  = std / mean
            if cov < 0.35:
                signals.append(f"Unusually uniform sentence rhythm (CoV={cov:.2f})")
                score += 15
            elif cov < 0.5:
                signals.append(f"Low sentence rhythm variance (CoV={cov:.2f})")
                score += 6

    # ── 3. Bullet-point parallelism ──
    # AI CVs tend to start every bullet with a strong action verb in identical
    # gerund/past-tense form. Real CVs have more inconsistency (people mix
    # "Built X", "Was responsible for Y", "Achieved Z").
    lines = [l.strip() for l in cv_text.split("\n") if l.strip()]
    bullet_lines = [l for l in lines if l[:3] in ("•  ", "•\t", "- ", "* ", "– ") or re.match(r"^[•\-*–]\s", l)]
    if len(bullet_lines) >= 5:
        first_words = [re.sub(r"^[•\-*–]\s*", "", l).split(" ", 1)[0].lower() for l in bullet_lines]
        # How many END with an "ed" past-tense verb (the AI-CV signature)
        ed_count = sum(1 for w in first_words if w.endswith("ed") and len(w) > 4)
        ed_ratio = ed_count / len(bullet_lines)
        if ed_ratio > 0.85:
            signals.append(f"Nearly all bullets start with past-tense verbs ({ed_count}/{len(bullet_lines)})")
            score += 12
        elif ed_ratio > 0.7:
            signals.append(f"Heavy past-tense bullet parallelism ({ed_count}/{len(bullet_lines)})")
            score += 5

    # ── 4. Suspiciously perfect formatting ──
    # Em dashes (—), curly quotes, en dashes in dates. These come for free from
    # GPT but require effort in Word. Each is a weak signal; together they add up.
    has_em_dash  = "—" in cv_text
    has_en_dash  = "–" in cv_text
    has_curly_q  = "" in cv_text or "" in cv_text
    typography_score = sum([has_em_dash, has_en_dash, has_curly_q])
    if typography_score >= 2:
        signals.append("AI-style punctuation (em/en dashes, curly quotes)")
        score += 6

    # ── Final score & confidence ──
    score = max(0, min(100, score))
    # Confidence reflects how many independent signals fired, not just the magnitude.
    # 1 signal at 35 points is less reliable than 3 signals at 12 each.
    n_signals = len(signals)
    if n_signals >= 3 and score >= 40:
        confidence = "High"
    elif n_signals >= 2 and score >= 25:
        confidence = "Medium"
    else:
        confidence = "Low"

    return {
        "ai_likelihood_pct": score,
        "confidence":        confidence,
        "signals":           signals,
        "is_flagged":        score >= 40 and confidence in ("Medium", "High"),
        "notes":             ("This CV shows multiple patterns common in AI-written text. "
                              "Verify specific claims in the interview." if score >= 40
                              else "No strong indicators of AI authorship."),
    }


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


def build_scoring_prompt(cv_profile: dict, jd_requirements: dict, tenure: dict,
                          angle: str, weights: dict | None = None) -> str:
    """Build the scoring prompt with per-job weights baked in.

    Weights influence three things in the prompt:
      1. The percentage shown next to each DIMENSION CRITERIA line — so the
         model knows what's emphasized for THIS role.
      2. The overall_score formula at the bottom — the model is asked to
         compute the score using THESE weights, not generic defaults.
      3. The `weight` field inside each dimension entry in the JSON output —
         keeps the downstream UI consistent with what was actually used.

    We still compute the final overall_score ourselves in reconcile_scores() to
    guard against the model getting the arithmetic slightly wrong, but giving
    the model the right weights still helps because it adjusts which dimensions
    it scrutinizes most.
    """
    w = normalize_weights(weights)

    def pct(name): return f"{int(round(w[name] * 100))}%"

    return f"""{angle}

STRUCTURED JOB REQUIREMENTS:
{json.dumps(jd_requirements, indent=2)}

STRUCTURED CANDIDATE PROFILE:
{json.dumps(cv_profile, indent=2)}

PRE-COMPUTED TENURE SIGNALS (use these for the Stability & Tenure dimension; do not recompute):
{json.dumps(tenure, indent=2)}

DIMENSION CRITERIA (each 0-20). Note the weight percentages — for THIS role they reflect
what the hiring team has prioritized. A high-weight dimension deserves extra scrutiny:
- Skills Match ({pct("Skills Match")}): How many of jd.required_skills + jd.required_technologies are found in the CV?
  20 = ALL required matched with evidence. 15 = 80% matched. 10 = ~50%. 5 = <30%. 0 = none.
- Experience Relevance ({pct("Experience Relevance")}): Years AND domain relevance vs jd.required_experience_years.
  20 = meets/exceeds years AND domain matches. 10 = close on years OR adjacent domain. 0 = neither.
- Education & Certifications ({pct("Education & Certifications")}): Compare cv.education + cv.certifications vs jd.required_education + jd.required_certifications.
  20 = meets or exceeds. 10 = related field. 5 = unrelated but has degree. 0 = no relevant education.
- Achievement & Impact ({pct("Achievement & Impact")}): Count quantified achievements (numbers, %, $, scale) across work_experience.
  20 = 5+ quantified achievements. 12 = 2-3. 5 = 1 vague. 0 = no metrics anywhere.
- Role Alignment ({pct("Role Alignment")}): Does career trajectory point toward this role?
  20 = same/adjacent role with progression. 12 = related field. 5 = pivot. 0 = unrelated.
- Stability & Tenure ({pct("Stability & Tenure")}): Use the pre-computed tenure signals.
  20 = "Stable". 15 = "Early career" (insufficient history — give benefit of doubt, this is not a negative signal).
  12 = "Moderate". 5 = "Frequent switcher" with valid context. 0 = severe job hopping.

Compute overall_score as weighted sum of dimension scores (each dim is 0-20):
  overall = (skills*{w["Skills Match"]:.2f} + experience*{w["Experience Relevance"]:.2f} + education*{w["Education & Certifications"]:.2f}
             + achievement*{w["Achievement & Impact"]:.2f} + alignment*{w["Role Alignment"]:.2f} + stability*{w["Stability & Tenure"]:.2f}) * 5

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
    {{"name": "Skills Match",              "score": <0-20>, "weight": {w["Skills Match"]:.2f}, "feedback": "<cite specific matched/missing skills>", "matched_skills": ["<exact name>"], "missing_skills": ["<exact name>"]}},
    {{"name": "Experience Relevance",      "score": <0-20>, "weight": {w["Experience Relevance"]:.2f}, "feedback": "<cite years and domain>", "matched_skills": [], "missing_skills": []}},
    {{"name": "Education & Certifications","score": <0-20>, "weight": {w["Education & Certifications"]:.2f}, "feedback": "<cite degree/certs>", "matched_skills": [], "missing_skills": []}},
    {{"name": "Achievement & Impact",      "score": <0-20>, "weight": {w["Achievement & Impact"]:.2f}, "feedback": "<cite specific metrics or note their absence>", "matched_skills": [], "missing_skills": []}},
    {{"name": "Role Alignment",            "score": <0-20>, "weight": {w["Role Alignment"]:.2f}, "feedback": "<cite title progression>", "matched_skills": [], "missing_skills": []}},
    {{"name": "Stability & Tenure",        "score": <0-20>, "weight": {w["Stability & Tenure"]:.2f}, "feedback": "<reference the tenure signals>", "matched_skills": [], "missing_skills": []}}
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

def reconcile_scores(strict: dict, upside: dict, hard_gaps: list, tenure: dict,
                     weights: dict | None = None) -> dict:
    """Combine the strict + upside perspective runs and apply hard-gap penalties.
    The two perspectives disagreeing is INFORMATION — it tells us the candidate is
    borderline. Blend (strict slightly weighted) and then subtract hard-gap penalties.

    Weights affect this in two ways:
      1. The per-dim scores are blended as before.
      2. The overall_score is RECOMPUTED from the blended per-dim scores using the
         actual weights — we don't trust the model's arithmetic. This is what
         actually makes the score reflect the custom weights, not just the prompt.
    """
    w = normalize_weights(weights)

    # 60/40 toward strict — generosity is a known LLM failure mode.
    def blend(a, b):
        try:
            return round((float(a) * 0.6 + float(b) * 0.4), 1)
        except (TypeError, ValueError):
            return 0

    result = dict(strict)

    overall_strict = strict.get("overall_score", 0)
    overall_upside = upside.get("overall_score", 0)

    # Average dimension scores
    dims_strict = {d["name"]: d for d in strict.get("dimensions", [])}
    dims_upside = {d["name"]: d for d in upside.get("dimensions", [])}
    averaged_dims = []
    blended_dim_scores = {}     # for the deterministic overall recomputation
    for dim in strict.get("dimensions", []):
        name = dim["name"]
        ds = dims_strict.get(name, {}).get("score", 0)
        du = dims_upside.get(name, {}).get("score", 0)
        avg_dim = dict(dim)
        avg_dim["score"] = blend(ds, du)
        # Overwrite the weight to the canonical one so downstream UI is consistent
        avg_dim["weight"] = w.get(name, avg_dim.get("weight", 0))
        blended_dim_scores[name] = avg_dim["score"]
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

    # KEY STEP: recompute overall from blended per-dim scores using the *actual* weights.
    # This is what makes per-job custom weights truly take effect:
    #   - the model saw the weights in its prompt and emphasized accordingly
    #   - but we don't trust its multiplication; we redo the math ourselves here
    weighted_pre_penalty = weighted_overall(blended_dim_scores, w)
    final_score = max(0, min(100, int(round(weighted_pre_penalty - total_penalty))))
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

    # ── Score consistency (computed early so recommendation logic can use it) ──
    diff = abs(float(overall_strict) - float(overall_upside))
    confidence_level = "High" if diff <= 5 else "Medium" if diff <= 12 else "Low"

    # ── Recommendation: score-based, with confidence-based downgrade ──
    # The base recommendation comes from the final blended score after hard-gap penalty.
    # BUT: if the two passes disagreed wildly (confidence Low), don't make a confident
    # call — downgrade HIRE/STRONG HIRE to MAYBE so the recruiter takes a closer look.
    # We don't upgrade REJECT — even if the passes disagree, a low score is a low score
    # and the recruiter shouldn't be falsely lured into reviewing a clear miss.
    if final_score >= 80:
        base_rec = "STRONG HIRE"
    elif final_score >= 65:
        base_rec = "HIRE"
    elif final_score >= 48:
        base_rec = "MAYBE"
    else:
        base_rec = "REJECT"

    if confidence_level == "Low" and base_rec in ("STRONG HIRE", "HIRE"):
        result["recommendation"] = "MAYBE"
        result["recommendation_note"] = (
            f"Passes disagreed significantly ({overall_strict:.0f} vs {overall_upside:.0f}). "
            f"Downgraded from {base_rec} for manual review."
        )
    elif confidence_level == "Low" and base_rec == "MAYBE":
        result["recommendation"] = "MAYBE"
        result["recommendation_note"] = (
            f"Borderline candidate, passes disagree ({overall_strict:.0f} vs {overall_upside:.0f}). "
            f"Manual review essential."
        )
    else:
        result["recommendation"] = base_rec
        # Clear any stale note from a previous run
        result.pop("recommendation_note", None)

    # Surface tenure as first-class data
    result["tenure_analysis"] = tenure
    risks = result.get("hiring_risks", []) or []
    if tenure.get("trust_flag"):
        msg = f"Tenure: {tenure.get('notes', '')}"
        if msg not in risks:
            risks = [msg] + [r for r in risks if r != "None identified"]
    result["hiring_risks"] = risks

    # Diagnostic metadata
    result["score_consistency"] = {
        "strict_pass":     overall_strict,
        "upside_pass":     overall_upside,
        "difference":      round(diff, 1),
        "penalty_applied": total_penalty,
        "confidence":      confidence_level,
    }

    # ── Score breakdown: visible math, so recruiters can see WHY the score is what it is ──
    # Each dimension contribution = dim_score (0-20) × weight × 5
    # Summing those gives the pre-penalty score; subtract hard-gap penalty for final.
    # Stored as a list of {name, score, weight, contribution} so the UI can render
    # a formula like "Skills 16×0.25 + Experience 14×0.20 + ... = 70.5, −10 hard gap, = 60.5".
    breakdown_rows = []
    for name in DIMENSION_NAMES:
        dim_score = blended_dim_scores.get(name, 0)
        wt        = w[name]
        contrib   = dim_score * wt * 5    # contribution to the 0-100 scale
        breakdown_rows.append({
            "name":         name,
            "dim_score":    round(dim_score, 1),
            "weight":       round(wt, 3),
            "weight_pct":   int(round(wt * 100)),
            "contribution": round(contrib, 1),
        })
    breakdown_subtotal = round(sum(r["contribution"] for r in breakdown_rows), 1)
    result["score_breakdown"] = {
        "rows":               breakdown_rows,
        "subtotal":           breakdown_subtotal,
        "hard_gap_penalty":   total_penalty,
        "final":              final_score,
        "hard_gap_details":   [
            {"requirement": g.get("requirement"), "penalty": g.get("penalty"), "kind": g.get("kind")}
            for g in hard_gaps
        ],
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

async def run_screening_pipeline(cv_text: str, jd_text: str, api_key: str,
                                  weights: dict | None = None) -> tuple[dict, str | None]:
    """Full screening pipeline. Returns (result_dict, error_str_or_None).

    `weights` is an optional per-job override of the dimension weights. If None,
    DEFAULT_WEIGHTS is used (which matches the v1 behavior — backward compatible)."""
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

        # ── STEP 2: score with two perspectives in parallel, using job-specific weights ──
        strict_prompt = build_scoring_prompt(cv_profile, jd_requirements, tenure, STRICT_ANGLE, weights)
        upside_prompt = build_scoring_prompt(cv_profile, jd_requirements, tenure, UPSIDE_ANGLE, weights)
        strict_result, upside_result = await asyncio.gather(
            gpt_json_call(client, SCORING_SYSTEM_PROMPT, strict_prompt, temperature=0.2),
            gpt_json_call(client, SCORING_SYSTEM_PROMPT, upside_prompt, temperature=0.3),
        )

        # ── Detect hard-requirement gaps deterministically ──
        hard_gaps = detect_hard_gaps(cv_profile, jd_requirements)

        # ── Authenticity check (deterministic, no GPT) ──
        # Operates on the raw CV text (not the parsed structure) so we catch
        # writing-style signals before the parser smooths them out.
        authenticity = detect_cv_authenticity(cv_text)

        # ── Reconcile (applies the same weights to deterministic overall recomputation) ──
        final = reconcile_scores(strict_result, upside_result, hard_gaps, tenure, weights)

        # ── Attach parsed structures + extras (so the UI can display the breakdown) ──
        final["parsed_cv"] = cv_profile
        final["parsed_jd"] = jd_requirements
        final["weights_used"] = normalize_weights(weights)
        final["authenticity"] = authenticity

        # If authenticity is strongly flagged, surface it as a hiring risk so it
        # shows up in the existing risks list (without lowering the score).
        if authenticity.get("is_flagged"):
            risks = final.get("hiring_risks", []) or []
            msg = f"CV authenticity: {authenticity['ai_likelihood_pct']}% AI-likelihood. {authenticity.get('notes', '')}"
            if not any("AI-likelihood" in r for r in risks):
                risks = [msg] + [r for r in risks if r != "None identified"]
                final["hiring_risks"] = risks

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
