# PySparkAssist — production image (Python package dir remains pysparkassist).
FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.title="PySparkAssist"
LABEL org.opencontainers.image.description="RAG PySpark learning assistant"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    libgomp1 \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    libatspi2.0-0 \
    libxshmfence1 \
    fonts-liberation \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

RUN python -m playwright install-deps chromium \
    && python -m playwright install chromium

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

COPY pysparkassist ./pysparkassist
COPY frontend ./frontend

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV WORKDIR=/app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=15s --start-period=900s --retries=5 \
    CMD curl -sf http://127.0.0.1:8000/api/health || exit 1

ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["python", "-m", "uvicorn", "pysparkassist.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
