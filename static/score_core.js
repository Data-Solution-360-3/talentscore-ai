// THE single source of truth for which score a surface shows. (item 6)
// Every page that renders a candidate score loads this file and calls ovOf —
// index.html (table, pipeline, dashboard, KPI, CSV, email composer),
// candidate.html (headline, attempts pills), admin.html (screenings table).
// Duplicating this rule inline is how two screens end up disagreeing; don't.
//
// Rule: the headline is overall_combined (CV x 0.35 + interview x 0.65),
// computed server-side when the interview scores. Until then the headline is
// the CV score with pending=true — "interview pending" is a STATE, never to
// be presented as a low score, and CV-only is never silently labeled Overall.
function ovOf(r){
  // cv sources by surface: r.score (batch rows), r.cv_score (attempts API),
  // r.overall_score (screening rows).
  const cv = r.score || (r.cv_score != null ? Math.round(r.cv_score)
                                            : Math.round(r.overall_score || 0));
  if(r.overall_combined != null && r.interview_score != null)
    return {s: Math.round(r.overall_combined), cv,
            iv: Math.round(r.interview_score), pending: false};
  return {s: cv, cv, iv: null, pending: true};
}

// Honest-uncertainty helper (item 7): the review flags the scorer attached
// (thin CV, pass disagreement, boundary score) plus the interview-side flag
// (very short transcript). Returns [] when the score is solid — old rows
// without the fields are simply unflagged, never back-filled.
function reviewFlagsOf(r){
  const flags = Array.isArray(r.review_flags) ? r.review_flags.slice() : [];
  if(r.interview_review_flag) flags.push(String(r.interview_review_flag));
  return flags;
}
