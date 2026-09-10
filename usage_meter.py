"""
usage_meter.py — chat-completions token accounting (Batch: cost observability)
==============================================================================
Pure measurement: summarize the `response.usage` objects the OpenAI SDK already
returns on every chat call into one stored, owner-only summary. NOTHING here
influences a call — no model, prompt, temperature, or scoring change. The USD
figure is an ESTIMATE from the named rate constants below and is labelled as
such wherever it is shown; OpenAI changes rates, so bump CHAT_RATES_VERSION
when editing them. Sibling of main.py's _RT_RATES_BY_FAMILY (Realtime audio),
which stays where it is — this module covers the text-completions side.

No prompt contents are ever recorded — token counts, model, and a purpose tag
only.
"""

# USD per 1M tokens. Verified against published OpenAI pricing 2026-09-10.
_CHAT_RATES = {
    "gpt-4o-mini": {"in": 0.15, "cached_in": 0.075, "out": 0.60},
    "gpt-4o":      {"in": 2.50, "cached_in": 1.25,  "out": 10.0},
}
CHAT_RATES_VERSION = "2026-09-10"


def _rate_for(model: str) -> dict:
    m = str(model or "")
    # longest-prefix match so "gpt-4o-mini" never falls into "gpt-4o"
    for key in sorted(_CHAT_RATES, key=len, reverse=True):
        if m.startswith(key):
            return _CHAT_RATES[key]
    return _CHAT_RATES["gpt-4o"]   # unknown model: price at the dearer rate


def _n(v) -> int:
    try:
        return max(0, min(2_000_000_000, int(v)))
    except Exception:
        return 0


def summarize_chat_usage(model: str, usages: list) -> dict:
    """Fold a list of SDK usage objects (or dicts) into one summary.

    Accepts both attribute-style (CompletionUsage) and dict-style entries, and
    tolerates None entries — a call that somehow returned no usage still counts
    as a call, it just adds zero tokens.
    """
    calls = in_tok = cached = out_tok = 0
    for u in usages or []:
        calls += 1
        if u is None:
            continue
        get = (lambda k, _u=u: _u.get(k)) if isinstance(u, dict) else \
              (lambda k, _u=u: getattr(_u, k, None))
        in_tok += _n(get("prompt_tokens"))
        out_tok += _n(get("completion_tokens"))
        det = get("prompt_tokens_details")
        if det is not None:
            dget = (lambda k, _d=det: _d.get(k)) if isinstance(det, dict) else \
                   (lambda k, _d=det: getattr(_d, k, None))
            cached += _n(dget("cached_tokens"))
    cached = min(cached, in_tok)
    r = _rate_for(model)
    usd = ((in_tok - cached) * r["in"] + cached * r["cached_in"]
           + out_tok * r["out"]) / 1_000_000.0
    return {
        "calls": calls,
        "input_tokens": in_tok,
        "cached_input_tokens": cached,
        "output_tokens": out_tok,
        "est_usd": round(usd, 5),
        "model": str(model or ""),
        "rates_version": CHAT_RATES_VERSION,
    }


def merge_usage_summaries(parts: dict) -> dict:
    """Combine named component summaries ({name: summary}) into one total.
    Skips None components; keeps the per-component breakdown alongside."""
    total_in = total_cached = total_out = calls = 0
    usd = 0.0
    comps = {}
    for name, s in (parts or {}).items():
        if not isinstance(s, dict):
            continue
        comps[name] = s
        calls += _n(s.get("calls"))
        total_in += _n(s.get("input_tokens"))
        total_cached += _n(s.get("cached_input_tokens"))
        total_out += _n(s.get("output_tokens"))
        try:
            usd += float(s.get("est_usd") or 0)
        except Exception:
            pass
    return {
        "calls": calls,
        "input_tokens": total_in,
        "cached_input_tokens": total_cached,
        "output_tokens": total_out,
        "est_usd": round(usd, 5),
        "components": comps,
        "rates_version": CHAT_RATES_VERSION,
    }
