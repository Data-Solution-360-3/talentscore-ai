"""
payment_service.py — Stripe + SSLCommerz payment integration
=============================================================
Stripe: International card payments
SSLCommerz: Bangladesh local payment gateway

Environment variables needed:
  STRIPE_SECRET_KEY        = sk_live_... or sk_test_...
  STRIPE_WEBHOOK_SECRET    = whsec_...
  SSLCOMMERZ_STORE_ID      = your store ID
  SSLCOMMERZ_STORE_PASS    = your store password
  SSLCOMMERZ_SANDBOX       = true (for testing) / false (for live)
  APP_URL                  = https://topcandidate.pro
"""

import os
import httpx
import stripe
from dotenv import load_dotenv

load_dotenv()

# ── STRIPE ──
STRIPE_SECRET_KEY     = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PUBLISHABLE_KEY = os.getenv("STRIPE_PUBLISHABLE_KEY", "")
APP_URL               = os.getenv("APP_URL", "https://topcandidate.pro")

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY

# ── SSLCOMMERZ ──
SSL_STORE_ID   = os.getenv("SSLCOMMERZ_STORE_ID", "")
SSL_STORE_PASS = os.getenv("SSLCOMMERZ_STORE_PASS", "")
SSL_SANDBOX    = os.getenv("SSLCOMMERZ_SANDBOX", "true").lower() == "true"
SSL_BASE_URL   = "https://sandbox.sslcommerz.com" if SSL_SANDBOX else "https://securepay.sslcommerz.com"

# ── PLAN CONFIG ──
PLANS = {
    "starter": {
        "name": "Starter",
        "usd_price": 29,
        "bdt_price": 3200,
        "stripe_price_id": os.getenv("STRIPE_PRICE_STARTER", ""),
        "screenings": 100,
        "batch_limit": 20,
    },
    "pro": {
        "name": "Pro",
        "usd_price": 79,
        "bdt_price": 8500,
        "stripe_price_id": os.getenv("STRIPE_PRICE_PRO", ""),
        "screenings": 500,
        "batch_limit": 100,
    },
    "enterprise": {
        "name": "Enterprise",
        "usd_price": 0,  # custom
        "bdt_price": 0,
        "stripe_price_id": "",
        "screenings": 999999,
        "batch_limit": 100,
    },
}


# ─────────────────────────────────────────────────────────────
# STRIPE FUNCTIONS
# ─────────────────────────────────────────────────────────────

def create_stripe_checkout(plan_id: str, user_id: str, email: str, company: str) -> dict:
    """
    Create a Stripe Checkout Session for subscription.
    Returns {success, url, session_id} or {success: False, error}
    """
    if not STRIPE_SECRET_KEY:
        return {"success": False, "error": "Stripe not configured. Contact admin."}

    plan = PLANS.get(plan_id)
    if not plan:
        return {"success": False, "error": "Invalid plan."}

    if not plan["stripe_price_id"]:
        return {"success": False, "error": f"Stripe price not configured for {plan_id} plan."}

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{
                "price": plan["stripe_price_id"],
                "quantity": 1,
            }],
            customer_email=email,
            metadata={
                "user_id": user_id,
                "plan_id": plan_id,
                "company": company,
            },
            success_url=f"{APP_URL}/payment/success?session_id={{CHECKOUT_SESSION_ID}}&plan={plan_id}",
            cancel_url=f"{APP_URL}/app?payment=cancelled",
            subscription_data={
                "metadata": {
                    "user_id": user_id,
                    "plan_id": plan_id,
                }
            },
        )
        return {"success": True, "url": session.url, "session_id": session.id}
    except stripe.error.StripeError as e:
        return {"success": False, "error": str(e)}


def verify_stripe_webhook(payload: bytes, sig_header: str) -> dict | None:
    """Verify Stripe webhook signature and return event."""
    if not STRIPE_WEBHOOK_SECRET:
        return None
    try:
        return stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except Exception:
        return None


def get_stripe_subscription(subscription_id: str) -> dict | None:
    """Get subscription details from Stripe."""
    try:
        sub = stripe.Subscription.retrieve(subscription_id)
        return {
            "id": sub.id,
            "status": sub.status,
            "current_period_end": sub.current_period_end,
            "cancel_at_period_end": sub.cancel_at_period_end,
        }
    except Exception:
        return None


def cancel_stripe_subscription(subscription_id: str) -> bool:
    """Cancel at period end."""
    try:
        stripe.Subscription.modify(subscription_id, cancel_at_period_end=True)
        return True
    except Exception:
        return False


def create_stripe_portal_session(customer_id: str) -> str | None:
    """Create Stripe billing portal session for self-service."""
    try:
        session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=f"{APP_URL}/app?tab=billing",
        )
        return session.url
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# SSLCOMMERZ FUNCTIONS
# ─────────────────────────────────────────────────────────────

async def create_sslcommerz_payment(
    plan_id: str,
    user_id: str,
    email: str,
    company: str,
    customer_name: str = "Customer",
    customer_phone: str = "01700000000",
) -> dict:
    """
    Initiate SSLCommerz payment.
    Returns {success, url} or {success: False, error}
    """
    if not SSL_STORE_ID or not SSL_STORE_PASS:
        return {"success": False, "error": "SSLCommerz not configured. Contact admin."}

    plan = PLANS.get(plan_id)
    if not plan:
        return {"success": False, "error": "Invalid plan."}

    tran_id = f"TSC_{user_id[:8]}_{plan_id}_{int(__import__('time').time())}"

    payload = {
        "store_id": SSL_STORE_ID,
        "store_passwd": SSL_STORE_PASS,
        "total_amount": plan["bdt_price"],
        "currency": "BDT",
        "tran_id": tran_id,
        "success_url": f"{APP_URL}/payment/sslcommerz/success?plan={plan_id}&user_id={user_id}&tran_id={tran_id}",
        "fail_url": f"{APP_URL}/payment/sslcommerz/fail",
        "cancel_url": f"{APP_URL}/app?payment=cancelled",
        "ipn_url": f"{APP_URL}/api/payments/sslcommerz/ipn",
        "product_name": f"TopCandidate - {plan['name']} Plan",
        "product_category": "Software",
        "product_profile": "general",
        "cus_name": customer_name or company,
        "cus_email": email,
        "cus_add1": "Dhaka",
        "cus_city": "Dhaka",
        "cus_country": "Bangladesh",
        "cus_phone": customer_phone,
        "ship_name": customer_name or company,
        "ship_add1": "Dhaka",
        "ship_city": "Dhaka",
        "ship_country": "Bangladesh",
        "shipping_method": "NO",
        "num_of_item": 1,
        "emi_option": 0,
        "value_a": user_id,
        "value_b": plan_id,
        "value_c": company,
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            res = await client.post(
                f"{SSL_BASE_URL}/gwprocess/apitest/api.php",
                data=payload
            )
            data = res.json()
            if data.get("status") == "SUCCESS":
                return {"success": True, "url": data["GatewayPageURL"], "tran_id": tran_id}
            else:
                return {"success": False, "error": data.get("failedreason", "SSLCommerz error")}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def verify_sslcommerz_payment(val_id: str) -> dict:
    """Verify SSLCommerz payment after redirect."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            res = await client.get(
                f"{SSL_BASE_URL}/validator/api/validationserverAPI.php",
                params={
                    "val_id": val_id,
                    "store_id": SSL_STORE_ID,
                    "store_passwd": SSL_STORE_PASS,
                    "format": "json"
                }
            )
            data = res.json()
            return {
                "valid": data.get("status") == "VALID",
                "amount": data.get("amount"),
                "currency": data.get("currency"),
                "tran_id": data.get("tran_id"),
                "val_id": val_id,
            }
    except Exception as e:
        return {"valid": False, "error": str(e)}
