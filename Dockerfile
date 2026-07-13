# API container — includes embeddings.
FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.title="PySparkAssist"
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_PROJECT_ENVIRONMENT=/usr/local
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

COPY pyproject.toml uv.lock README.md ./
COPY pysparkassist ./pysparkassist
RUN uv sync --frozen --no-dev --extra ml

COPY frontend ./frontend

ENV PYTHONUNBUFFERED=1

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=15s --start-period=300s --retries=5 \
    CMD curl -sf http://127.0.0.1:8000/api/health || exit 1

CMD ["python", "-m", "uvicorn", "pysparkassist.api.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
