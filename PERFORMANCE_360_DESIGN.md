# 360° Performance Management — DESIGN FOR REVIEW (no code built)

Status: **awaiting owner approval.** Nothing in this document exists in code.
It touches employee auth and multi-user data, so per your instruction the
security design gets reviewed awake before a line is written.

---

## 1. The 360° model

One review cycle produces, per reviewed employee ("subject"), up to four
perspectives:

| Perspective | Who | When it exists |
|---|---|---|
| Self | the subject | always |
| Manager | the subject's manager | when `manager_id` is set |
| Peers | 2–4 colleagues | assigned per cycle (default: same department) |
| Subordinates | direct reports | only for subjects who manage others (reverse `manager_id`) |

Same competencies, same scale, four vantage points — combined into one
performance picture but **never silently blended** (see §6; same philosophy
as CV/interview scores).

**New data requirement:** `employees.manager_id` (nullable, points at
another employee in the same tenant). Set by the admin on the Employees
page. This single field derives the manager AND subordinate relationships;
without it those two perspectives simply don't exist for that person —
honest absence, not fake data.

## 2. Review cycles

Collection `review_cycles` (tenant-scoped):
`{user_id, name ("Q1 2027"), period: quarterly|annual, start/end dates,
competency_set (snapshotted at launch), scale, weights, status:
draft → active → closed, created_at}`

Admin flow, mirroring the interview-questions discipline (draft → review →
approve/launch):

1. **Create draft cycle** — pick period, included employees (default: all
   active), competency set, weights.
2. **Assignment matrix generated as a DRAFT**: self ×1; manager from
   `manager_id`; peers auto-proposed (same department, excluding
   manager/subordinates, capped at 3, random among eligible); subordinates =
   everyone whose `manager_id` is the subject (capped at 5, random if more).
3. **Admin reviews and edits the matrix** — add/remove any reviewer pair.
   Nothing launches unseen, same rule as AI-generated questions.
4. **Launch** — assignments become visible in the employee portal; email/
   portal notice "You have N reviews to complete by {date}".
5. **Close** — manual or on end-date. Late submissions blocked; completion
   rate recorded. Closing computes and freezes the aggregates (§6).

One `review_assignments` row per (cycle, reviewer, subject, relation):
`{user_id, cycle_id, reviewer_employee_id, subject_employee_id,
relation: self|manager|peer|subordinate, status: pending|submitted,
submitted_at, answers}` — **this row is the entire access-control story**
(§5).

## 3. Competencies and scale

Reuse the add-your-own pattern (like job questions):

- A tenant-level default set the admin edits: e.g. Job Knowledge, Quality of
  Work, Communication, Teamwork, Ownership & Reliability, plus
  **Leadership** auto-included only for subjects with subordinates.
- 4–8 competencies per cycle; each is `{name, description}`; the cycle
  snapshots the set at launch (mid-cycle edits can't corrupt comparability —
  same lesson as editing approved interview questions).
- **Scale: 1–5 per competency** with anchored labels (1 = Needs significant
  improvement … 5 = Consistently exceptional), plus one optional free-text
  "evidence / example" per competency and one overall comment. Free text is
  where 360° value lives; the form nudges for a concrete example.
- Optional later: AI drafting of role-specific competency descriptions from
  the JD — same generate → review → approve flow, never unreviewed. Not in
  v1.

## 4. The employee portal experience

Extends the existing employee login (the `/api/me/*` surface used for leave
and attendance) — **no new auth system**:

- New portal section "My reviews": list of pending assignments — subject's
  **name, role title, department, relation, due date. Nothing else.**
- Opening one shows the review form only: competencies, scale, evidence
  boxes. Draft saved locally (sessionStorage, like typed interview answers);
  submit is final (status → submitted, answers frozen).
- Self-assessment is the same form aimed at yourself.
- After submitting: "Thanks — submitted." No visibility into anyone's
  results. What a subject eventually sees of their own results is an admin
  decision per cycle (v1 default: **nothing in-portal**; the admin shares
  offline. A later "share summary with employee" toggle is listed in §9.)

## 5. Security design (the hard part)

The boundary to preserve: **an employee reviewing a peer sees ONLY the
review form — never the peer's salary, leave, attendance, scores, or
anything else.** Design rules:

1. **The assignment row IS the ACL.** Every portal review endpoint takes an
   `assignment_id`, loads the row, and verifies
   `reviewer_employee_id == the logged-in employee` AND
   `cycle status == active`. The subject is **derived from the row** —
   subject ids are never free request parameters, so there is nothing to
   probe or enumerate. (Same pattern as the interview token being the
   credential.)
2. **Dedicated minimal endpoints, no reuse of admin readers:**
   - `GET /api/me/reviews` → my pending/submitted assignments; subject
     projected to `{name, role_title, department}` at the query — the
     employee document is never serialized whole.
   - `GET /api/me/reviews/{assignment_id}` → form payload: the projection
     above + the cycle's competency snapshot.
   - `POST /api/me/reviews/{assignment_id}` → answers (validated: every
     score 1–5, text capped, only pending assignments accept writes; one
     submission, no edits after).
   Nothing under `/api/me/*` ever touches `salary_structures`, `payslips`,
   `leave_requests` (others'), `screenings`, or `interview_sessions`.
3. **Tenant scoping unchanged:** every query also filters `user_id` with
   the same `user_match` discipline; an employee login is already bound to
   one tenant's employee record.
4. **Aggregates are admin-only.** Results endpoints live under the
   admin-gated surface (`require_admin`), like payroll. The portal never
   exposes another person's scores, raw or aggregated.
5. **Anonymity guarantee for raw feedback:** peer and subordinate answers
   are stored with the reviewer id (needed for completion tracking and
   abuse investigation) but every admin-facing results view shows them
   **aggregated per relation** (peer avg, subordinate avg) with individual
   free-text comments listed **without attribution**, shuffled. Where a
   relation has n=1 (a single peer), the UI labels it "1 peer — effectively
   attributable" instead of pretending anonymity. No pseudo-anonymity lies.
6. **Rate limits** on portal review endpoints (same `rate_limit_allows`
   helper), and audit entries (`review_audit`) for submissions and admin
   matrix edits — actor, timestamp, before/after for edits.
7. **Smoke locks** (day one): every `/api/me/reviews*` route 401s without
   an employee session; an assignment id belonging to another reviewer
   404s identically to a nonexistent one (probe-resistant, like closed
   links); admin results routes 401 on credential-free clients.

## 6. Aggregation — separate first, weighted second

Consistent with the product's no-silent-blend philosophy:

- **Per competency, per relation:** mean of submitted 1–5 scores → shown as
  four side-by-side values (self / manager / peer / subordinate), with n for
  each. Gaps (e.g. self 4.8 vs peers 3.1) are the actual 360° signal and get
  a visible "perception gap" marker when |self − others| ≥ 1.5.
- **Headline rating per subject** (for ranking/KPIs): weighted mean of
  relation means, normalized to 0–100:
  `manager 0.40 · peers 0.30 · subordinates 0.20 · self 0.10` (defaults,
  editable per cycle). Missing relations **re-normalize the remaining
  weights** — a person with no subordinates isn't penalized. Small-sample
  honesty: n(total reviewers) < 3 → the headline carries the same
  "limited data" tag the KPI page uses.
- Stored frozen on cycle close: `performance_reviews` summary doc per
  subject `{cycle_id, employee_id, per_competency, per_relation, headline,
  n, completion}` — append-only history across cycles.

## 7. What it unlocks

- KPI page "Coming soon" cards go LIVE: **Avg performance rating** (mean
  headline, latest closed cycle), **High performer %** (headline ≥ 80 —
  threshold configurable), plus **review completion rate** per cycle.
- Trend once ≥ 2 closed cycles: rating over time per employee and
  company-wide (same chart kit).
- Later (explicitly out of v1): quality-of-hire — join a hired candidate's
  CV/interview scores to their first performance headline; that's the
  missing "Quality of hire" recruitment KPI.

## 8. Integration with what exists

| Piece | Change |
|---|---|
| `employees` | + `manager_id` (admin-set, Employees page dropdown) |
| Employee portal | + "My reviews" section on the existing login |
| HRM dashboard | + cycle status chip (active cycle, completion %) |
| KPI page | performance cards switch from Coming-soon to LIVE |
| Payroll | none in v1 (no performance-pay linkage until you ask) |
| New collections | `review_cycles`, `review_assignments`, `performance_reviews`, `review_audit` — all tenant-scoped, indexed on (user_id, cycle_id) |

Build order when approved: manager_id + Employees UI → cycles + matrix
(admin) → portal form endpoints + smoke gates → results/aggregation →
KPI wiring. Each step deployable and smoke-gated alone.

## 9. Decisions I need from you (morning list)

1. **Peer selection default** — same-department auto-propose (cap 3), or
   admin hand-picks every pair?
2. **Does the subject ever see results in-portal?** v1 default: no.
3. **Headline weights** — 40/30/20/10 ok? Should self be excluded (0) from
   the headline entirely?
4. **Anonymity floor** — is the "1 peer — effectively attributable" label
   acceptable, or should peer review require n ≥ 2 to launch?
5. **Cycle cadence default** — quarterly or annual?
6. **Scale** — 1–5 anchored, or 1–10?
7. **Language** — portal forms English-only for v1 (matching the product),
   Bangla later with the same Phase-2 plan as interviews?
