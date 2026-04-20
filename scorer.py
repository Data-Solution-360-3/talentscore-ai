"""
CV Screener — 3-Step Pipeline
==============================
Step 1A: Structure the CV into a clean JSON profile (GPT call 1)
Step 1B: Parse the JD into structured requirements (GPT call 2)
  → Steps 1A and 1B run in PARALLEL to save time
Step 2:  Match structured CV vs structured JD and score (GPT call 3, run twice, averaged)

This approach is far more accurate than a single raw-text prompt.
"""

import json
import os
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
# STEP 1A — STRUCTURE THE CV
# ─────────────────────────────────────────────────────────────

CV_STRUCTURE_PROMPT = """You are an expert CV parser. Extract all information from the CV text into a structured JSON profile.
Be thorough and precise. Do not invent information — only extract what is explicitly stated.
Respond with valid JSON only, no markdown."""

def build_cv_parse_prompt(cv_text: str) -> str:
    return f"""Parse this CV into a structured JSON profile.

CV TEXT:
\"\"\"
{cv_text[:5000]}
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
  "total_years_experience": <integer, estimated total years>,
  "summary": "<professional summary if present, else null>",
  "work_experience": [
    {{
      "title": "<job title>",
      "company": "<company name>",
      "duration": "<e.g. Jan 2021 - Mar 2023>",
      "years": <float, estimated years in this role>,
      "responsibilities": ["<key responsibility 1>", "<key responsibility 2>"],
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
    "technical": ["<skill1>", "<skill2>"],
    "soft": ["<soft skill1>"],
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

JD_PARSE_PROMPT = """You are an expert job description analyst. Extract all requirements from the JD into structured JSON.
Be precise about what is required vs preferred. Respond with valid JSON only, no markdown."""

def build_jd_parse_prompt(jd_text: str) -> str:
    return f"""Parse this job description into structured requirements JSON.

JOB DESCRIPTION:
\"\"\"
{jd_text[:3000]}
\"\"\"

Respond ONLY with this JSON structure:
{{
  "role_title": "<exact job title>",
  "seniority_level": "<Junior | Mid | Senior | Lead | Manager | Director>",
  "department": "<department or team>",
  "industry": "<industry sector>",
  "employment_type": "<Full-time | Part-time | Contract | Remote>",
  "required_skills": ["<must-have skill 1>", "<must-have skill 2>"],
  "preferred_skills": ["<nice-to-have skill>"],
  "required_technologies": ["<required tech/tool>"],
  "required_experience_years": <minimum integer years required, 0 if not specified>,
  "required_education": "<e.g. Bachelor's in CS or equivalent, or null>",
  "required_certifications": ["<required cert if any>"],
  "key_responsibilities": ["<main responsibility>"],
  "soft_skills_required": ["<communication>", "<leadership>"],
  "domain_knowledge_required": ["<specific domain knowledge>"],
  "nice_to_have": ["<bonus qualification>"]
}}"""


# ─────────────────────────────────────────────────────────────
# STEP 2 — MATCH & SCORE
# ─────────────────────────────────────────────────────────────

SCORING_SYSTEM_PROMPT = """You are a senior technical recruiter with 15+ years of experience making accurate, consistent hiring decisions.
You receive a structured CV profile and structured JD requirements — both already parsed.
Your job is to score the candidate objectively and precisely against the role.
Be strict and consistent. A score of 80+ means genuinely strong fit. 50-79 means partial fit. Below 50 means poor fit.
Respond with valid JSON only."""

def build_scoring_prompt(cv_profile: dict, jd_requirements: dict) -> str:
    return f"""Score this candidate against the job requirements.

STRUCTURED JOB REQUIREMENTS:
{json.dumps(jd_requirements, indent=2)}

STRUCTURED CANDIDATE PROFILE:
{json.dumps(cv_profile, indent=2)}

Score across 6 dimensions (each 0-20). Be strict and data-driven.

SCORING RULES:
- Skills Match: Compare cv.skills vs jd.required_skills and required_technologies. Count exact and partial matches.
- Experience: Compare cv.total_years_experience vs jd.required_experience_years. Consider domain relevance.
- Education: Compare cv.education vs jd.required_education. Give full marks if exceeded.
- Achievement: Look for numbers, metrics, scale in cv.work_experience achievements. No metrics = lower score.
- Role Alignment: Does career trajectory logically lead to this role? Title progression matters.
- Presentation: Quality of responsibilities/achievements descriptions. Vague bullets = lower score.

Respond ONLY with this JSON:
{{
  "candidate_name": "<from cv_profile.personal.name>",
  "current_title": "<from cv_profile.current_title>",
  "years_experience": "<cv_profile.total_years_experience>",
  "overall_score": <weighted average: skills*0.25 + experience*0.25 + education*0.10 + achievement*0.20 + alignment*0.15 + presentation*0.05, scaled to 100>,
  "recommendation": "<STRONG HIRE | HIRE | MAYBE | REJECT>",
  "recommendation_reason": "<1 precise sentence citing specific evidence>",
  "summary": "<3-4 sentences referencing specific CV facts vs JD requirements>",
  "dimensions": [
    {{
      "name": "Skills Match",
      "score": <0-20>,
      "weight": 0.25,
      "feedback": "<cite specific matched/missing skills by name>",
      "matched_skills": ["<exact skill from JD found in CV>"],
      "missing_skills": ["<required skill NOT found in CV>"]
    }},
    {{
      "name": "Experience Relevance",
      "score": <0-20>,
      "weight": 0.25,
      "feedback": "<cite years and domain specifics>",
      "matched_skills": [],
      "missing_skills": []
    }},
    {{
      "name": "Education & Certifications",
      "score": <0-20>,
      "weight": 0.10,
      "feedback": "<cite specific degree and certs>",
      "matched_skills": [],
      "missing_skills": []
    }},
    {{
      "name": "Achievement & Impact",
      "score": <0-20>,
      "weight": 0.20,
      "feedback": "<cite specific metrics or note their absence>",
      "matched_skills": [],
      "missing_skills": []
    }},
    {{
      "name": "Role Alignment",
      "score": <0-20>,
      "weight": 0.15,
      "feedback": "<cite title progression and industry match>",
      "matched_skills": [],
      "missing_skills": []
    }},
    {{
      "name": "Presentation & Clarity",
      "score": <0-20>,
      "weight": 0.05,
      "feedback": "<note quality of CV writing>",
      "matched_skills": [],
      "missing_skills": []
    }}
  ],
  "key_strengths": ["<specific strength with evidence>", "<strength 2>", "<strength 3>"],
  "critical_gaps": ["<specific gap referencing JD requirement>", "<gap 2>", "<gap 3>"],
  "interview_questions": [
    "<targeted question probing a specific gap or verifying a claim>",
    "<question 2>",
    "<question 3>"
  ],
  "hiring_risks": ["<specific risk based on CV data, or None identified>"],
  "skills_coverage_pct": <integer 0-100, percentage of required_skills found in CV>
}}"""


# ─────────────────────────────────────────────────────────────
# ASYNC GPT HELPER
# ─────────────────────────────────────────────────────────────

async def gpt_json_call(client: AsyncOpenAI, system: str, user: str, temperature: float = 0.0) -> dict:
    response = await client.chat.completions.create(
        model="gpt-4o",
        max_tokens=2000,
        temperature=temperature,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ]
    )
    return json.loads(response.choices[0].message.content)


# ─────────────────────────────────────────────────────────────
# AVERAGE TWO SCORE RUNS FOR CONSISTENCY
# ─────────────────────────────────────────────────────────────

def average_scores(result_a: dict, result_b: dict) -> dict:
    """Average the numeric scores from two runs for consistency."""
    result = result_a.copy()

    # Average overall score
    score_a = result_a.get("overall_score", 0)
    score_b = result_b.get("overall_score", 0)
    result["overall_score"] = round((score_a + score_b) / 2)

    # Average dimension scores
    dims_a = {d["name"]: d for d in result_a.get("dimensions", [])}
    dims_b = {d["name"]: d for d in result_b.get("dimensions", [])}

    averaged_dims = []
    for dim in result_a.get("dimensions", []):
        name = dim["name"]
        score_a_dim = dims_a.get(name, {}).get("score", 0)
        score_b_dim = dims_b.get(name, {}).get("score", 0)
        avg_dim = dim.copy()
        avg_dim["score"] = round((score_a_dim + score_b_dim) / 2, 1)
        averaged_dims.append(avg_dim)

    result["dimensions"] = averaged_dims

    # Recalculate recommendation based on averaged score
    score = result["overall_score"]
    if score >= 82:
        result["recommendation"] = "STRONG HIRE"
    elif score >= 68:
        result["recommendation"] = "HIRE"
    elif score >= 50:
        result["recommendation"] = "MAYBE"
    else:
        result["recommendation"] = "REJECT"

    # Add consistency metadata
    score_diff = abs(score_a - score_b)
    result["score_consistency"] = {
        "run_1": score_a,
        "run_2": score_b,
        "difference": score_diff,
        "confidence": "High" if score_diff <= 5 else "Medium" if score_diff <= 10 else "Low"
    }

    return result


# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────

async def run_screening_pipeline(cv_text: str, jd_text: str, api_key: str) -> tuple[dict, str | None]:
    """
    Full 3-step screening pipeline:
    - Step 1A + 1B run in parallel (structure CV + parse JD)
    - Step 2 runs twice in parallel (score with averaged results)
    """
    try:
        client = AsyncOpenAI(api_key=api_key)

        # ── STEP 1: Parse CV and JD in parallel ──
        cv_profile, jd_requirements = await asyncio.gather(
            gpt_json_call(client, CV_STRUCTURE_PROMPT, build_cv_parse_prompt(cv_text)),
            gpt_json_call(client, JD_PARSE_PROMPT, build_jd_parse_prompt(jd_text))
        )

        # ── STEP 2: Score twice in parallel and average ──
        scoring_prompt = build_scoring_prompt(cv_profile, jd_requirements)
        result_a, result_b = await asyncio.gather(
            gpt_json_call(client, SCORING_SYSTEM_PROMPT, scoring_prompt, temperature=0.0),
            gpt_json_call(client, SCORING_SYSTEM_PROMPT, scoring_prompt, temperature=0.0)
        )

        # ── Average the two runs ──
        final_result = average_scores(result_a, result_b)

        # ── Attach parsed structures for transparency ──
        final_result["parsed_cv"] = cv_profile
        final_result["parsed_jd"] = jd_requirements

        # ── Validate score range ──
        final_result["overall_score"] = max(0, min(100, int(final_result["overall_score"])))

        return final_result, None

    except Exception as e:
        err = str(e)
        if "authentication" in err.lower() or "api key" in err.lower():
            return {}, "Invalid OpenAI API key. Please check and try again."
        if "rate limit" in err.lower():
            return {}, "OpenAI rate limit hit. Please wait a moment and try again."
        if "quota" in err.lower():
            return {}, "OpenAI quota exceeded. Please check your billing."
        return {}, f"Screening failed: {err}"
