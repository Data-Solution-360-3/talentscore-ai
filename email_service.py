"""
email_service.py — Gmail SMTP email service
============================================
Sends verification OTP emails via Gmail SMTP.
Requires:
  GMAIL_USER = your Gmail address
  GMAIL_APP_PASSWORD = Gmail App Password (not your regular password)
  
To get Gmail App Password:
  1. Go to Google Account → Security
  2. Enable 2-Step Verification
  3. Go to App Passwords → Generate
  4. Use that 16-char password here
"""

import smtplib
import random
import string
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv

load_dotenv()

GMAIL_USER     = os.getenv("GMAIL_USER", "")
GMAIL_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
APP_NAME       = "TopCandidate"
APP_URL        = os.getenv("APP_URL", "https://topcandidate.pro")


def generate_otp(length: int = 6) -> str:
    return ''.join(random.choices(string.digits, k=length))


def send_verification_email(to_email: str, company_name: str, otp: str) -> bool:
    """Send OTP verification email. Returns True if sent successfully."""
    if not GMAIL_USER or not GMAIL_PASSWORD:
        print(f"[EMAIL] SMTP not configured. OTP for {to_email}: {otp}")
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
        <tr><td style="background:linear-gradient(135deg,#1e1b4b,#5b5ef4);padding:32px 40px;text-align:center">
          <div style="font-size:24px;font-weight:800;color:#ffffff;letter-spacing:-.5px">
            TopCandidate<span style="color:#fb923c">.pro</span>
          </div>
          <div style="font-size:13px;color:rgba(255,255,255,.6);margin-top:4px">by LinkX360</div>
        </td></tr>
        <!-- Body -->
        <tr><td style="padding:40px">
          <h2 style="font-size:22px;font-weight:700;color:#0a0b1e;margin:0 0 12px">Verify your email address</h2>
          <p style="font-size:15px;color:#4a5270;line-height:1.7;margin:0 0 28px">
            Hi <strong>{company_name}</strong>, welcome to {APP_NAME}! 
            Enter the verification code below to activate your account.
          </p>
          <!-- OTP Box -->
          <div style="background:#f1f3ff;border:2px dashed #c7d2fe;border-radius:12px;padding:28px;text-align:center;margin:0 0 28px">
            <div style="font-size:13px;font-weight:600;text-transform:uppercase;letter-spacing:.1em;color:#6366f1;margin-bottom:8px">Your verification code</div>
            <div style="font-size:42px;font-weight:900;letter-spacing:10px;color:#1e1b4b;font-family:monospace">{otp}</div>
            <div style="font-size:12px;color:#8892aa;margin-top:8px">This code expires in <strong>15 minutes</strong></div>
          </div>
          <p style="font-size:14px;color:#8892aa;line-height:1.6;margin:0">
            If you didn't create an account on {APP_NAME}, you can safely ignore this email.
          </p>
        </td></tr>
        <!-- Footer -->
        <tr><td style="background:#f8f9ff;padding:20px 40px;border-top:1px solid #e8eaf5">
          <p style="font-size:12px;color:#8892aa;text-align:center;margin:0">
            © 2026 TopCandidate by <a href="https://linkx360.com" style="color:#5b5ef4;text-decoration:none">LinkX360</a> · Dhaka, Bangladesh
          </p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"{APP_NAME} <{GMAIL_USER}>"
        msg["To"]      = to_email
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, to_email, msg.as_string())
        print(f"[EMAIL] Verification sent to {to_email}")
        return True
    except Exception as e:
        print(f"[EMAIL] Failed to send to {to_email}: {e}")
        return False


def send_welcome_email(to_email: str, company_name: str) -> bool:
    """Send welcome email after verification."""
    if not GMAIL_USER or not GMAIL_PASSWORD:
        return True

    subject = f"Welcome to {APP_NAME} — You're all set!"
    html = f"""
<!DOCTYPE html>
<html>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:40px 20px">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:16px;overflow:hidden">
        <tr><td style="background:linear-gradient(135deg,#1e1b4b,#5b5ef4);padding:32px 40px;text-align:center">
          <div style="font-size:24px;font-weight:800;color:#ffffff">TopCandidate<span style="color:#fb923c">.pro</span></div>
        </td></tr>
        <tr><td style="padding:40px">
          <h2 style="font-size:22px;font-weight:700;color:#0a0b1e;margin:0 0 12px">🎉 Welcome, {company_name}!</h2>
          <p style="font-size:15px;color:#4a5270;line-height:1.7;margin:0 0 24px">
            Your account is verified and ready. Start screening candidates faster than ever with our AI-powered 3-step pipeline.
          </p>
          <a href="{APP_URL}" style="display:inline-block;background:#5b5ef4;color:#fff;padding:13px 28px;border-radius:10px;text-decoration:none;font-weight:700;font-size:15px">
            Go to dashboard →
          </a>
        </td></tr>
        <tr><td style="background:#f8f9ff;padding:20px 40px;border-top:1px solid #e8eaf5">
          <p style="font-size:12px;color:#8892aa;text-align:center;margin:0">© 2026 TopCandidate by LinkX360</p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"{APP_NAME} <{GMAIL_USER}>"
        msg["To"]      = to_email
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, to_email, msg.as_string())
        return True
    except Exception as e:
        print(f"[EMAIL] Welcome email failed: {e}")
        return False


def send_team_invite_email(to_email: str, invited_by: str, company_name: str, role: str) -> bool:
    """Send team invitation email."""
    if not GMAIL_USER or not GMAIL_PASSWORD:
        print(f"[EMAIL] SMTP not configured. Team invite for {to_email} from {company_name}")
        return False  # Return False so we show proper error to user

    subject = f"You're invited to join {company_name} on {APP_NAME}"
    register_url = f"{APP_URL}/login?invite=1&email={to_email}&company={company_name}"

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:'Inter',Arial,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f5f5;padding:40px 20px">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;border:1px solid #e5e7eb">
        <tr><td style="background:linear-gradient(135deg,#5b5ef4,#fb923c);padding:28px 32px;text-align:center">
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
            <tr><td style="background:#5b5ef4;border-radius:8px;padding:12px 28px;text-align:center">
              <a href="{register_url}" style="color:#fff;font-size:15px;font-weight:700;text-decoration:none">Accept invitation →</a>
            </td></tr>
          </table>
          <p style="margin:0;font-size:12px;color:#999;text-align:center">
            Or copy this link: <a href="{register_url}" style="color:#5b5ef4">{register_url}</a>
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

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"{APP_NAME} <{GMAIL_USER}>"
        msg["To"] = to_email
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, to_email, msg.as_string())
        return True
    except Exception as e:
        print(f"[EMAIL] Failed to send invite to {to_email}: {e}")
        return False


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
    ("{score}",          "Their AI match score (0-100)"),
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
    """Send a candidate email through Gmail SMTP. Returns (success, error_message).

    Plain text only — recruiter emails should feel personal, not marketing-y.
    We wrap the body in a minimal HTML version too so it renders cleanly
    in inboxes that prefer HTML."""
    if not GMAIL_USER or not GMAIL_PASSWORD:
        return False, ("Email service not configured. Set GMAIL_USER and GMAIL_APP_PASSWORD "
                       "in your Render environment variables to enable candidate emails.")
    if not to_email or "@" not in to_email:
        return False, "Invalid candidate email address."
    if not subject or not subject.strip():
        return False, "Email subject is required."
    if not body_text or not body_text.strip():
        return False, "Email body is required."

    # Plain-text MIME part (preferred for personal email) + a simple HTML fallback
    # so inboxes that auto-prefer HTML don't show monospace.
    html_body = body_text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html_body = html_body.replace("\n", "<br>\n")
    html_wrapper = f"""<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;font-size:14px;line-height:1.6;color:#222;max-width:600px">
{html_body}
</body></html>"""

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = f"{APP_NAME} <{GMAIL_USER}>"
        msg["To"]      = to_email
        if reply_to:
            # Replies from the candidate go to the recruiter's real address, not the Gmail bot.
            msg["Reply-To"] = reply_to
        msg.attach(MIMEText(body_text, "plain"))
        msg.attach(MIMEText(html_wrapper, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(GMAIL_USER, to_email, msg.as_string())
        print(f"[EMAIL] Candidate email sent to {to_email} — subject: {subject[:60]}")
        return True, ""
    except smtplib.SMTPAuthenticationError:
        return False, ("Gmail authentication failed. The app password may be wrong "
                       "or expired. Regenerate it at https://myaccount.google.com/apppasswords")
    except smtplib.SMTPRecipientsRefused:
        return False, f"Email address {to_email} was rejected by the mail server."
    except Exception as e:
        return False, f"Send failed: {str(e)}"
