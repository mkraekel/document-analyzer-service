# ── Stage 1: Build React Dashboard ───────────────────────────────
FROM node:22-slim AS frontend

WORKDIR /frontend
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ .
RUN npm run build

# ── Stage 2: Python Service ─────────────────────────────────────
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .
COPY static/ static/

# React Dashboard Build aus Stage 1
COPY --from=frontend /frontend/dist dashboard/dist/

COPY start.sh .
RUN chmod +x start.sh

EXPOSE 8000

ENTRYPOINT ["./start.sh"]
