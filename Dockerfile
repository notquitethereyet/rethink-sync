# Use Python 3.12 slim image
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app \
    LOG_LEVEL=INFO \
    PORT=8080

RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:$PORT/health', timeout=5)" || exit 1

# ✅ FIXED: Shell form allows $PORT to be expanded
# CMD uvicorn main:app --host 0.0.0.0 --port ${PORT}
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port $PORT"]

