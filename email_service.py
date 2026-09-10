"""
email_service.py — transactional email via Resend (HTTPS API)
=============================================================
The droplet's outbound SMTP is blocked at the network level (DigitalOcean
default), so Gmail SMTP never worked in production. All mail now goes
through Resend's HTTPS API (port 443 — always reachable).

Requires:
  RESEND_API_KEY = re_...          (resend.com → API Keys)
  RESEND_FROM    = optional sender, default "TopCandidate <onboarding@resend.dev>"

IMPORTANT — sender domain: until topcandidate.pro is verified in Resend
(DNS records under Domains), the onboarding@resend.dev sender can only
deliver to the Resend account owner's own address. Verify the domain and
set RESEND_FROM="TopCandidate <no-reply@topcandidate.pro>" to email
candidates and team invitees.

Without RESEND_API_KEY the service degrades exactly as before: OTPs are
printed to the server log and signup proceeds (dev mode); candidate emails
and invites report a clear "not configured" error instead of pretending.
"""

import os
import random
import string

import httpx
from dotenv import load_dotenv

load_dotenv()

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
APP_NAME       = "TopCandidate"
APP_URL        = os.getenv("APP_URL", "https://topcandidate.pro")
RESEND_FROM    = os.getenv("RESEND_FROM", f"{APP_NAME} <onboarding@resend.dev>")


def generate_otp(length: int = 6) -> str:
    return ''.join(random.choices(string.digits, k=length))


def _resend_send(to_email: str, subject: str, html: str | None = None,
                 text: str | None = None, reply_to: str = "") -> tuple[bool, str]:
    """One transport for every email. Returns (ok, resend_id_or_error).
    The returned id is Resend's delivery id — checkable in their dashboard,
    so 'sent' here means accepted for delivery, not merely 'no exception'."""
    if not RESEND_API_KEY:
        return False, "RESEND_API_KEY not set"
    payload = {"from": RESEND_FROM, "to": [to_email], "subject": subject}
    if html:
        payload["html"] = html
    if text:
        payload["text"] = text
    if reply_to:
        payload["reply_to"] = reply_to
    try:
        r = httpx.post("https://api.resend.com/emails", json=payload,
                       headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
                       timeout=15.0)
        if r.status_code in (200, 201):
            eid = str((r.json() or {}).get("id", ""))
            print(f"[EMAIL] Resend accepted for {to_email} — id {eid} — {subject[:60]}")
            return True, eid
        return False, f"Resend {r.status_code}: {r.text[:200]}"
    except Exception as e:
        return False, f"Resend request failed: {str(e)[:200]}"


def send_verification_email(to_email: str, company_name: str, otp: str) -> bool:
    """Send OTP verification email. Returns True if sent successfully."""
    if not RESEND_API_KEY:
        print(f"[EMAIL] Resend not configured. OTP for {to_email}: {otp}")
        return True  # Allow development without email

    subject = f"Verify your {APP_NAME} account — {otp}"

    html = f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:'Inter',Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:40px 20px">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,.08)">
        <!-- Header -->
        <tr><td style="background:linear-gradient(135deg,#639922,#128A40);padding:32px 40px;text-align:center">
          <img src="{APP_URL}/static/logo-mark-512.png" width="46" height="46" alt="TopCandidate" style="display:block;margin:0 auto 12px;border:0">
          <div style="font-size:24px;font-weight:800;color:#ffffff;letter-spacing:-.5px">
            TopCandidate<span style="color:#fb923c">.pro</span>
          </div>
          <div style="font-size:13px;color:rgba(255,255,255,.6);margin-top:4px">by LinkX360</div>
        </td></tr>
        <!-- Body -->
        <tr><td style="padding:40px">
          <h2 style="font-size:22px;font-weight:700;color:#1f2937;margin:0 0 12px">Verify your email address</h2>
          <p style="font-size:15px;color:#4B5563;line-height:1.7;margin:0 0 28px">
            Hi <strong>{company_name}</strong>, welcome to {APP_NAME}!
            Enter the verification code below to activate your account.
          </p>
          <!-- OTP Box -->
          <div style="background:#F4FAF0;border:2px dashed #CFE3B4;border-radius:12px;padding:28px;text-align:center;margin:0 0 28px">
            <div style="font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:#547F1E;margin-bottom:8px">Your verification code</div>
            <div style="font-size:42px;font-weight:900;letter-spacing:10px;color:#1f2937;font-family:monospace">{otp}</div>
            <div style="font-size:12px;color:#6B7280;margin-top:8px">This code expires in <strong>15 minutes</strong></div>
          </div>
          <p style="font-size:14px;color:#6B7280;line-height:1.6;margin:0">
            If you didn't create an account on {APP_NAME}, you can safely ignore this email.
          </p>
        </td></tr>
        <!-- Footer -->
        <tr><td style="background:#F7FAF5;padding:20px 40px;border-top:1px solid #E7EAEE">
          <p style="font-size:12px;color:#6B7280;text-align:center;margin:0">
            © 2026 TopCandidate by <a href="https://linkx360.com" style="color:#F57C2E;text-decoration:none">LinkX360</a> · Dhaka, Bangladesh
          </p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    ok, info = _resend_send(to_email, subject, html=html)
    if not ok:
        print(f"[EMAIL] Failed to send to {to_email}: {info}")
    return ok


def send_welcome_email(to_email: str, company_name: str) -> bool:
    """Send welcome email after verification."""
    if not RESEND_API_KEY:
        return True

    subject = f"Welcome to {APP_NAME} — You're all set!"
    html = f"""
<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:40px 20px">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:16px;overflow:hidden">
        <tr><td style="background:linear-gradient(135deg,#639922,#128A40);padding:32px 40px;text-align:center">
          <img src="{APP_URL}/static/logo-mark-512.png" width="46" height="46" alt="TopCandidate" style="display:block;margin:0 auto 12px;border:0">
          <div style="font-size:24px;font-weight:800;color:#ffffff">TopCandidate<span style="color:#fb923c">.pro</span></div>
        </td></tr>
        <tr><td style="padding:40px">
          <h2 style="font-size:22px;font-weight:700;color:#1f2937;margin:0 0 12px">🎉 Welcome, {company_name}!</h2>
          <p style="font-size:15px;color:#4B5563;line-height:1.7;margin:0 0 24px">
            Your account is verified and ready. Start screening candidates faster than ever with our AI-powered 3-step pipeline.
          </p>
          <a href="{APP_URL}" style="display:inline-block;background:#F57C2E;color:#fff;padding:13px 28px;border-radius:10px;text-decoration:none;font-weight:700;font-size:15px">
            Go to dashboard →
          </a>
        </td></tr>
        <tr><td style="background:#F7FAF5;padding:20px 40px;border-top:1px solid #E7EAEE">
          <p style="font-size:12px;color:#6B7280;text-align:center;margin:0">© 2026 TopCandidate by LinkX360</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    ok, info = _resend_send(to_email, subject, html=html)
    if not ok:
        print(f"[EMAIL] Welcome email failed: {info}")
    return ok


def send_team_invite_email(to_email: str, invited_by: str, company_name: str,
                           role: str, accept_link: str) -> tuple[bool, str]:
    """Send team invitation email. Returns (ok, resend_id_or_error).

    accept_link is the tokenized /join URL built by the invite route — the
    ONLY working entry to the accept flow. The old hardcoded /login?invite=1
    CTA sent invitees to a password prompt they couldn't answer (A1 bug).
    """
    if not RESEND_API_KEY:
        print(f"[EMAIL] Resend not configured. Team invite for {to_email} from {company_name}")
        return False, "RESEND_API_KEY not set"

    subject = f"You're invited to join {company_name} on {APP_NAME}"
    register_url = accept_link

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:'Inter',Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:40px 20px">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;border:1px solid #e5e7eb">
        <tr><td style="background:linear-gradient(135deg,#639922,#128A40);padding:28px 32px;text-align:center">
          <img src="{APP_URL}/static/logo-mark-512.png" width="46" height="46" alt="TopCandidate" style="display:block;margin:0 auto 10px;border:0">
          <h1 style="margin:0;color:#fff;font-size:22px;font-weight:800">TopCandidate<span style="color:#fde68a">.pro</span></h1>
          <p style="margin:4px 0 0;color:rgba(255,255,255,.8);font-size:13px">by LinkX360</p>
        </td></tr>
        <tr><td style="padding:32px">
          <h2 style="margin:0 0 12px;font-size:20px;color:#111;font-weight:700">You've been invited! 🎉</h2>
          <p style="margin:0 0 16px;font-size:14px;color:#555;line-height:1.7">
            <strong>{invited_by}</strong> has invited you to join <strong>{company_name}</strong>'s workspace on {APP_NAME} as a <strong>{role}</strong>.
          </p>
          <p style="margin:0 0 24px;font-size:14px;color:#555;line-height:1.7">
            {APP_NAME} uses AI to screen and rank CVs instantly — helping your team hire smarter and faster.
          </p>
          <table cellpadding="0" cellspacing="0" style="margin:0 auto 24px">
            <tr><td style="background:#F57C2E;border-radius:8px;padding:12px 28px;text-align:center">
              <a href="{register_url}" style="color:#fff;font-size:15px;font-weight:700;text-decoration:none">Accept invitation →</a>
            </td></tr>
          </table>
          <p style="margin:0 0 8px;font-size:12px;color:#999;text-align:center">
            Or copy this link: <a href="{register_url}" style="color:#F57C2E">{register_url}</a>
          </p>
          <p style="margin:0;font-size:12px;color:#999;text-align:center">
            This invitation link expires in 7 days.
          </p>
        </td></tr>
        <tr><td style="background:#f9fafb;padding:16px 32px;text-align:center;border-top:1px solid #e5e7eb">
          <p style="margin:0;font-size:11px;color:#aaa">© 2026 {APP_NAME} by LinkX360 · Dhaka, Bangladesh</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    ok, info = _resend_send(to_email, subject, html=html)
    if not ok:
        print(f"[EMAIL] Failed to send invite to {to_email}: {info}")
    return ok, info


def send_interview_invite_email(to_email: str, candidate_name: str, company: str,
                                job_title: str, link: str, deadline_str: str,
                                days: int, reply_to: str = "",
                                reminder: bool = False,
                                language: str = "en") -> tuple[bool, str]:
    """Automated AI-interview invite (funnel Part 2). Returns (ok, resend_id).

    HONEST by construction: says what it is (a live AI interview, ~12 min),
    the real deadline, and that a HUMAN reviews the results. No overclaiming.
    `reminder=True` reuses the same body with a reminder subject/lead."""
    if not RESEND_API_KEY:
        print(f"[EMAIL] Resend not configured. Interview invite for {to_email} ({job_title})")
        return False, "RESEND_API_KEY not set"

    first = (candidate_name or "").strip().split(" ")[0] or "there"
    subject = (f"Reminder: your interview for {job_title} at {company} — closes {deadline_str}"
               if reminder else
               f"Interview invitation — {job_title} at {company}")
    lead = (f"A quick reminder: your interview link below closes on <strong>{deadline_str}</strong> — "
            f"it takes about 12 minutes, whenever suits you before then."
            if reminder else
            f"Good news — after reviewing your application, <strong>{company}</strong> would like to "
            f"invite you to the next step for the <strong>{job_title}</strong> role.")
    lang_line = ("The interview is in English (Bangla is available in beta)."
                 if language != "bn" else
                 "The interview is available in Bangla (beta) and English.")

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:'Inter',Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:40px 20px">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;border:1px solid #e5e7eb">
        <tr><td style="background:linear-gradient(135deg,#639922,#128A40);padding:24px 32px;text-align:center">
          <h1 style="margin:0;color:#fff;font-size:20px;font-weight:800">{company}</h1>
          <p style="margin:4px 0 0;color:rgba(255,255,255,.85);font-size:12px">via TopCandidate.pro</p>
        </td></tr>
        <tr><td style="padding:30px 32px">
          <h2 style="margin:0 0 12px;font-size:19px;color:#111;font-weight:700">Hi {first},</h2>
          <p style="margin:0 0 16px;font-size:14px;color:#555;line-height:1.7">{lead}</p>
          <p style="margin:0 0 16px;font-size:14px;color:#555;line-height:1.7">
            It's a <strong>live AI interview</strong> — about <strong>12 minutes</strong> of spoken and
            written questions, taken on this link at any time that suits you.
            Please complete it <strong>within {days} days (by {deadline_str})</strong>.
            {lang_line}
          </p>
          <table cellpadding="0" cellspacing="0" style="margin:0 auto 20px">
            <tr><td style="background:#F57C2E;border-radius:8px;padding:12px 28px;text-align:center">
              <a href="{link}" style="color:#fff;font-size:15px;font-weight:700;text-decoration:none">Start your interview →</a>
            </td></tr>
          </table>
          <p style="margin:0 0 6px;font-size:12px;color:#999;text-align:center">
            Or copy this link: <a href="{link}" style="color:#F57C2E">{link}</a>
          </p>
          <p style="margin:14px 0 0;font-size:12px;color:#777;line-height:1.6">
            <strong>What to expect:</strong> find a quiet spot with a working microphone; a camera is
            requested for periodic still snapshots (never continuous video). The AI conducts the
            conversation and a <strong>human at {company} reviews the results and makes every
            hiring decision</strong> — the AI never decides anything on its own.
            Details: <a href="{APP_URL}/privacy" style="color:#F57C2E">privacy policy</a>.
          </p>
        </td></tr>
        <tr><td style="background:#f9fafb;padding:14px 32px;text-align:center;border-top:1px solid #e5e7eb">
          <p style="margin:0;font-size:11px;color:#aaa">© 2026 {APP_NAME} by LinkX360 · Dhaka, Bangladesh</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""
    ok, info = _resend_send(to_email, subject, html=html, reply_to=reply_to)
    if not ok:
        print(f"[EMAIL] Failed to send interview invite to {to_email}: {info}")
    return ok, info


# ─────────────────────────────────────────────────────────────
# CANDIDATE COMMUNICATION (interview invite, rejection, offer)
# ─────────────────────────────────────────────────────────────

# Variables available in every candidate template. Templates use {curly} style.
# We list these here so the UI can show the recruiter what placeholders exist.
TEMPLATE_VARIABLES = [
    ("{candidate_name}", "The candidate's full name"),
    ("{job_title}",      "The job they applied for"),
    ("{company_name}",   "Your company name"),
    ("{recruiter_name}", "Your name (the sender)"),
    ("{score}",          "Their CV score (0-100) — the CV screening only, not the combined overall"),
]

DEFAULT_TEMPLATES = {
    "interview": {
        "subject": "Interview invitation — {job_title} at {company_name}",
        "body": (
            "Hi {candidate_name},\n\n"
            "Thank you for applying for the {job_title} role at {company_name}. "
            "We were impressed with your application and would like to invite you to "
            "the next stage of our process.\n\n"
            "Please reply with a few times that work for you next week for a 30-minute "
            "conversation, and we'll get something on the calendar.\n\n"
            "Looking forward to speaking with you.\n\n"
            "Best regards,\n"
            "{recruiter_name}\n"
            "{company_name}"
        ),
    },
    "rejection": {
        "subject": "Update on your application — {job_title}",
        "body": (
            "Hi {candidate_name},\n\n"
            "Thank you for your interest in the {job_title} role at {company_name} "
            "and for taking the time to apply.\n\n"
            "After careful review, we've decided to move forward with other candidates "
            "whose background more closely matches what we're looking for in this position. "
            "This was a difficult decision — we received many strong applications.\n\n"
            "We genuinely appreciate your interest in {company_name} and wish you the best "
            "in your job search.\n\n"
            "Best regards,\n"
            "{recruiter_name}\n"
            "{company_name}"
        ),
    },
    "offer": {
        "subject": "Offer — {job_title} at {company_name}",
        "body": (
            "Hi {candidate_name},\n\n"
            "We're delighted to offer you the {job_title} position at {company_name}!\n\n"
            "We were very impressed throughout the interview process and believe you'd be "
            "a great addition to our team. The formal offer document is attached / will be "
            "sent separately with full details on compensation, benefits, and start date.\n\n"
            "Please review and let us know if you have any questions. We'd love to have "
            "your decision by [DATE].\n\n"
            "Congratulations, and we hope to welcome you onboard soon.\n\n"
            "Best regards,\n"
            "{recruiter_name}\n"
            "{company_name}"
        ),
    },
}


def _esc(v: str) -> str:
    """Minimal HTML-escape for values interpolated into email HTML."""
    return (str(v or "")
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _demo_shell(title_html: str, body_html: str) -> str:
    """Shared green-header shell for the demo-request emails (brand-consistent)."""
    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:'Inter',Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:40px 20px">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 4px 20px rgba(0,0,0,.08)">
        <tr><td style="background:linear-gradient(135deg,#639922,#128A40);padding:30px 40px;text-align:center">
          <img src="{APP_URL}/static/logo-mark-512.png" width="44" height="44" alt="TopCandidate" style="display:block;margin:0 auto 10px;border:0">
          <div style="font-size:22px;font-weight:800;color:#ffffff">TopCandidate<span style="color:#fb923c">.pro</span></div>
        </td></tr>
        <tr><td style="padding:36px 40px">
          <h2 style="font-size:20px;font-weight:800;color:#1f2937;margin:0 0 14px">{title_html}</h2>
          {body_html}
        </td></tr>
        <tr><td style="background:#F7FAF5;padding:18px 40px;border-top:1px solid #E7EAEE">
          <p style="font-size:12px;color:#6B7280;text-align:center;margin:0">© 2026 TopCandidate by LinkX360 · Dhaka, Bangladesh</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""


def send_demo_admin_notification(admin_email: str, req: dict) -> tuple[bool, str]:
    """Notify the admin (super-admin) that a new demo request arrived.
    Returns (ok, id_or_error) — the caller records this on the lead so a failed
    send is visibly 'pending', never silently lost."""
    rows = ""
    for label, key in [("Name", "name"), ("Company", "company"), ("Email", "email"),
                       ("Phone", "phone"), ("Hires for", "roles"), ("Wants", "message")]:
        val = _esc(req.get(key, "") or "—")
        rows += (f'<tr><td style="padding:7px 0;font-size:13px;color:#6B7280;font-weight:700;'
                 f'width:110px;vertical-align:top">{label}</td>'
                 f'<td style="padding:7px 0;font-size:14px;color:#1f2937;white-space:pre-wrap">{val}</td></tr>')
    body = (f'<p style="font-size:14px;color:#4B5563;line-height:1.7;margin:0 0 18px">'
            f'A new demo request just came in from the landing page.</p>'
            f'<table width="100%" cellpadding="0" cellspacing="0" style="border-top:1px solid #E7EAEE">{rows}</table>'
            f'<p style="font-size:13px;color:#6B7280;margin:20px 0 0">Open your admin → Demo Requests to update its status.</p>')
    html = _demo_shell("New demo request 🎯", body)
    return _resend_send(admin_email, f"New demo request — {req.get('company') or req.get('name') or 'Lead'}",
                        html=html, reply_to=(req.get("email") or ""))


def send_demo_confirmation(to_email: str, name: str) -> tuple[bool, str]:
    """Confirm to the requester that we received their demo request.
    Returns (ok, id_or_error)."""
    body = (f'<p style="font-size:15px;color:#4B5563;line-height:1.7;margin:0 0 16px">'
            f'Hi <strong>{_esc(name) or "there"}</strong>, thanks for requesting a demo of '
            f'<strong>TopCandidate.pro</strong>.</p>'
            f'<p style="font-size:15px;color:#4B5563;line-height:1.7;margin:0 0 16px">'
            f'We\'ve got your details and someone from our team will be in touch shortly to '
            f'schedule a time that works for you.</p>'
            f'<p style="font-size:13px;color:#6B7280;line-height:1.6;margin:18px 0 0">'
            f'Didn\'t request this? You can safely ignore this email.</p>')
    html = _demo_shell("Thanks — we'll be in touch 👋", body)
    return _resend_send(to_email, "Thanks for requesting a demo — TopCandidate.pro", html=html)


def substitute_template(template_str: str, variables: dict) -> str:
    """Replace {placeholders} in a template. Unknown placeholders are left alone
    so the recruiter notices and can correct them, rather than silently producing
    'Hi ,' if candidate_name is missing."""
    if not template_str:
        return ""
    result = template_str
    for key, val in (variables or {}).items():
        if val is None:
            continue
        result = result.replace("{" + key + "}", str(val))
    return result


def send_candidate_email(to_email: str, subject: str, body_text: str,
                          reply_to: str = "") -> tuple[bool, str]:
    """Send a candidate email through Resend. Returns (success, error_message).

    Plain text first — recruiter emails should feel personal, not marketing-y —
    with a minimal HTML version so inboxes that prefer HTML render cleanly."""
    if not RESEND_API_KEY:
        return False, ("Email service not configured. Set RESEND_API_KEY in the "
                       "server's .env to enable candidate emails.")
    if not to_email or "@" not in to_email:
        return False, "Invalid candidate email address."
    if not subject or not subject.strip():
        return False, "Email subject is required."
    if not body_text or not body_text.strip():
        return False, "Email body is required."

    html_body = body_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html_body = html_body.replace("\n", "<br>\n")
    html_wrapper = f"""<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;font-size:14px;line-height:1.6;color:#222;max-width:600px">
{html_body}
</body></html>"""

    ok, info = _resend_send(to_email, subject, html=html_wrapper,
                            text=body_text, reply_to=reply_to)
    if ok:
        return True, ""
    if "403" in info and "testing" in info.lower():
        return False, ("Resend is in test mode: until the topcandidate.pro domain is "
                       "verified in Resend, emails can only be sent to the account "
                       "owner's address. Verify the domain, then set RESEND_FROM.")
    return False, f"Send failed: {info}"
