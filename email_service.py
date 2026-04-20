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
APP_NAME       = "TalentScore AI"
APP_URL        = os.getenv("APP_URL", "https://talentscore-ai.onrender.com")


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
            TalentScore<span style="color:#fb923c">AI</span>
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
            © 2026 TalentScore AI by <a href="https://linkx360.com" style="color:#5b5ef4;text-decoration:none">LinkX360</a> · Dhaka, Bangladesh
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
          <div style="font-size:24px;font-weight:800;color:#ffffff">TalentScore<span style="color:#fb923c">AI</span></div>
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
          <p style="font-size:12px;color:#8892aa;text-align:center;margin:0">© 2026 TalentScore AI by LinkX360</p>
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
