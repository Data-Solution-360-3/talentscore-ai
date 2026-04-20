"""
auth.py — Authentication system
================================
- JWT tokens for session management
- bcrypt password hashing
- Company-based data isolation
"""

from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import HTTPException, Request, Depends
from fastapi.responses import RedirectResponse
import os

SECRET_KEY = os.getenv("SECRET_KEY", "talentscore-secret-key-change-in-production-2024")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


def create_token(data: dict) -> str:
    payload = data.copy()
    payload["exp"] = datetime.utcnow() + timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None


def get_token_from_request(request: Request) -> Optional[str]:
    """Get JWT token from cookie or Authorization header."""
    # Try cookie first
    token = request.cookies.get("access_token")
    if token:
        return token
    # Try Authorization header
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        return auth[7:]
    return None


async def get_current_user(request: Request) -> dict:
    """Get current logged-in user. Raises 401 if not authenticated."""
    token = get_token_from_request(request)
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    payload = decode_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return payload


async def get_current_user_optional(request: Request) -> Optional[dict]:
    """Get current user without raising error — returns None if not authenticated."""
    try:
        return await get_current_user(request)
    except HTTPException:
        return None


def require_auth(request: Request) -> dict:
    """Dependency for protected routes — redirects to login if not authenticated."""
    token = get_token_from_request(request)
    if not token:
        raise HTTPException(status_code=302, headers={"Location": "/login"})
    payload = decode_token(token)
    if not payload:
        raise HTTPException(status_code=302, headers={"Location": "/login"})
    return payload
