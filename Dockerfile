# PySparkAssist — API image only (ingest runs locally; mount ./data into /app/data).
FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.title="PySparkAssist"
LABEL org.opencontainers.image.description="RAG PySpark learning assistant"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-runtime.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements-runtime.txt

COPY pysparkassist ./pysparkassist
COPY frontend ./frontend

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=15s --start-period=600s --retries=5 \
    CMD curl -sf http://127.0.0.1:8000/api/health || exit 1

CMD ["python", "-m", "uvicorn", "pysparkassist.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
