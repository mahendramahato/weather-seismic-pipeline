# Small Python image; the API needs FastAPI, Uvicorn, DuckDB and boto3.
FROM python:3.12-slim

WORKDIR /app

# Dependencies first so code-only changes reuse this cached layer.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# 0.0.0.0 = accept connections from other containers (Caddy), not just
# from inside this container. Port 8000 is only reachable on Docker's
# internal network — Caddy is the only way in from outside.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
