# Small official Python image; the producer only needs requests + confluent-kafka.
FROM python:3.12-slim

WORKDIR /app

# Install dependencies BEFORE copying the code: Docker caches each step, so a
# code-only change reuses this layer and rebuilds in seconds.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# -u = unbuffered output, so print() lines appear in `docker logs` immediately.
CMD ["python", "-u", "producer.py"]
