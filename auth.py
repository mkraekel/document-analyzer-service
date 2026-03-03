"""
JWT-basierte Authentifizierung fuer das Dashboard.

Konfiguration ueber Environment-Variablen:
  DASHBOARD_USER      - Login-Username (default: admin)
  DASHBOARD_PASSWORD   - Login-Passwort (PFLICHT in Production)
  JWT_SECRET           - Secret fuer Token-Signierung (PFLICHT in Production)
  JWT_EXPIRY_HOURS     - Token-Gueltigkeitsdauer (default: 24)
"""

import os
import sys
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Request, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from passlib.context import CryptContext
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)

# ── Config ───────────────────────────────────────────────────────────
JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = int(os.getenv("JWT_EXPIRY_HOURS", "24"))

DASHBOARD_USER = os.getenv("DASHBOARD_USER", "admin")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD", "")

# ── Startup-Absicherung ─────────────────────────────────────────────
if JWT_SECRET == "dev-secret-change-in-production":
    logger.critical("FATAL: JWT_SECRET ist der unsichere Default! "
                    "Setze JWT_SECRET als Environment-Variable.")
    sys.exit(1)

if not DASHBOARD_PASSWORD:
    logger.critical("FATAL: DASHBOARD_PASSWORD nicht gesetzt! "
                    "Setze DASHBOARD_PASSWORD als Environment-Variable.")
    sys.exit(1)


# ── JWT Auth Middleware (global) ─────────────────────────────────────
# Pfade die KEIN Token brauchen:
# Komplett oeffentliche Pfade (kein Auth noetig):
_PUBLIC_PATHS = {
    "/health",
    "/api/auth/login",
    "/favicon.ico",
    "/dashboard",
}

# Pfad-Prefixe die KEIN Token brauchen:
_PUBLIC_PREFIXES = (
    "/app/",
    "/app",
)

# API-Key fuer n8n / externe Services (Alternative zu JWT)
_N8N_API_KEY = os.getenv("N8N_API_KEY", "")

if not _N8N_API_KEY:
    logger.warning("N8N_API_KEY nicht gesetzt! X-API-Key Auth ist deaktiviert. "
                    "Setze N8N_API_KEY als Environment-Variable.")


class JWTAuthMiddleware(BaseHTTPMiddleware):
    """
    Globale Middleware: Prueft Auth fuer ALLE Requests.
    Akzeptiert: JWT Bearer Token ODER X-API-Key Header.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Komplett oeffentliche Pfade durchlassen
        if path in _PUBLIC_PATHS or path.startswith(_PUBLIC_PREFIXES):
            return await call_next(request)

        # OPTIONS Requests fuer CORS Preflight durchlassen
        if request.method == "OPTIONS":
            return await call_next(request)

        # X-API-Key pruefen (fuer n8n und externe Services)
        api_key = request.headers.get("x-api-key", "")
        if api_key:
            logger.info(f"[AUTH] X-API-Key received for {path} (key_len={len(api_key)}, expected_len={len(_N8N_API_KEY)}, match={api_key == _N8N_API_KEY})")
        if api_key and _N8N_API_KEY and api_key == _N8N_API_KEY:
            request.state.user = "n8n"
            return await call_next(request)

        # Token extrahieren
        auth_header = request.headers.get("authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={"detail": "Nicht autorisiert – Bearer Token oder X-API-Key erforderlich"},
            )

        token = auth_header[7:]  # "Bearer " abschneiden

        # Token validieren
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
            username = payload.get("sub", "")
            if not username:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Ungueltiger Token"},
                )
            # Username im Request State speichern fuer Downstream
            request.state.user = username
        except JWTError:
            return JSONResponse(
                status_code=401,
                content={"detail": "Token abgelaufen oder ungueltig"},
            )

        return await call_next(request)

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
    Akzeptiert auch X-API-Key Auth (bereits von Middleware validiert).
    Gibt den Username zurueck oder wirft 401.
    """
    # Middleware hat bereits via X-API-Key authentifiziert
    if hasattr(request.state, "user") and request.state.user:
        return request.state.user

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
