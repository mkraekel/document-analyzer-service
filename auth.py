"""
JWT-basierte Authentifizierung fuer das Dashboard.

Konfiguration ueber Environment-Variablen:
  DASHBOARD_USER      - Login-Username (default: admin)
  DASHBOARD_PASSWORD   - Login-Passwort (PFLICHT in Production)
  JWT_SECRET           - Secret fuer Token-Signierung (PFLICHT in Production)
  JWT_EXPIRY_HOURS     - Token-Gueltigkeitsdauer (default: 24)
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = int(os.getenv("JWT_EXPIRY_HOURS", "24"))

DASHBOARD_USER = os.getenv("DASHBOARD_USER", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")

if JWT_SECRET == "dev-secret-change-in-production":
    logger.warning("JWT_SECRET nicht gesetzt – verwende unsicheren Default. "
                    "Setze JWT_SECRET in Production!")

if not DASHBOARD_PASSWORD:
    logger.warning("DASHBOARD_PASSWORD nicht gesetzt – Login ist deaktiviert bis "
                    "ein Passwort gesetzt wird.")

# ── Password Hashing ────────────────────────────────────────────────
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# ── Models ───────────────────────────────────────────────────────────
class LoginRequest(BaseModel):
    username: str
    password: str

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: str

# ── Token Creation ───────────────────────────────────────────────────
def create_access_token(username: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS)
    payload = {
        "sub": username,
        "exp": expire,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

# ── Token Verification ──────────────────────────────────────────────
_bearer_scheme = HTTPBearer(auto_error=False)

async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> str:
    """
    Dependency: Extrahiert und validiert JWT aus dem Authorization-Header.
    Gibt den Username zurueck oder wirft 401.
    """
    if not credentials:
        raise HTTPException(status_code=401, detail="Nicht autorisiert")

    try:
        payload = jwt.decode(
            credentials.credentials,
            JWT_SECRET,
            algorithms=[JWT_ALGORITHM],
        )
        username: str = payload.get("sub", "")
        if not username:
            raise HTTPException(status_code=401, detail="Ungueltiger Token")
        return username
    except JWTError:
        raise HTTPException(status_code=401, detail="Token abgelaufen oder ungueltig")

# ── Login Handler ────────────────────────────────────────────────────
def authenticate_user(username: str, password: str) -> Optional[str]:
    """
    Prueft Credentials. Gibt den Username zurueck bei Erfolg, None bei Fehler.
    Unterstuetzt:
      1. Klartext-Passwort aus DASHBOARD_PASSWORD (einfaches Setup)
      2. Bcrypt-Hash in DASHBOARD_PASSWORD (sicherer)
    """
    if not DASHBOARD_PASSWORD:
        return None

    if username != DASHBOARD_USER:
        return None

    # Bcrypt-Hash erkennen ($2b$...)
    if DASHBOARD_PASSWORD.startswith("$2"):
        if pwd_context.verify(password, DASHBOARD_PASSWORD):
            return username
        return None

    # Klartext-Vergleich
    if password == DASHBOARD_PASSWORD:
        return username

    return None
