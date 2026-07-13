# PySparkAssist

RAG chatbot for learning PySpark. Ingests official docs and Spark Python examples, embeds chunks into Qdrant, answers via Groq with citations. Embeddings stay local; only generation hits Groq.

Ingest (host) fills Qdrant + a SQLite entity index. The API embeds queries, retrieves chunks (dense search, optional entity boost), streams SSE to a plain HTML frontend.

## Production

Deployed via [wee-deployment-scripts](https://github.com/Aanjney/wee-deployment-scripts) compose stack. Generic compose layout, Caddy, cron auto-deploy: see that repo's README (Type 2 section).

**Paths on host:**

| Path | Purpose |
| --- | --- |
| `~/services/pysparkassist/.env` | Secrets and config |
| `~/services/pysparkassist/data/` | SQLite entity index, manifests, cached embedding weights |
| `~/services/pysparkassist/qdrant_data/` | Qdrant storage |

**Network:** `pysparkassist` and `qdrant` on the `edge` compose network. Caddy proxies `pysparkassist.duckdns.org` → `pysparkassist:8000`.

### Setup

```bash
git clone https://github.com/Aanjney/pysparkassist.git ~/services/pysparkassist
cd ~/services/pysparkassist
cp env.example .env
# Edit .env — at minimum GROQ_API_KEY
```

Install uv if needed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source "$HOME/.local/bin/env"
```

One-time ingest on the host **before first API start** (Qdrant must be up — start it via compose first):

```bash
cd ~/wee-deployment-scripts
docker compose up -d qdrant

cd ~/services/pysparkassist
uv sync --extra ml --extra ingest
uv run python -m playwright install chromium
uv run python -m pysparkassist.ingest run
```

Start or update the stack:

```bash
cd ~/wee-deployment-scripts
docker compose up -d --build pysparkassist
docker compose exec caddy caddy reload --config /etc/caddy/Caddyfile
```

Auto-deploy: `DEPLOY_SERVICE=pysparkassist DEPLOY_REPO=~/services/pysparkassist ~/wee-deployment-scripts/deploy.sh` (cron setup in wee-deployment-scripts `env.template`).

Health check (public URL or exec into container):

```bash
curl -s https://pysparkassist.duckdns.org/api/health
docker compose exec pysparkassist curl -sf http://127.0.0.1:8000/api/health
```

Qdrant image pinned in compose: `qdrant/qdrant:v1.18.2`.

## Local dev

Same repo layout. `.env` with `QDRANT_URL=http://localhost:6333` for host-side tools.

```bash
uv sync --extra ml --extra ingest --extra dev
uv run python -m playwright install chromium
uv run python -m pysparkassist.ingest run
```

Run API locally (Qdrant at localhost:6333):

```bash
uv run python -m pysparkassist
```

## Retrieval evals

Golden questions under `pysparkassist/evals/data/`. Retrieval only — no Groq calls:

```bash
uv run python -m pysparkassist.evals.run --modes dense_only,dense_entity_boost --k 8
```

Reports in `eval_reports/` (JSON + markdown).

## Configuration

See `env.example`. Key vars: `GROQ_API_KEY`, `GROQ_MODEL`, `EMBEDDING_MODEL`, `QDRANT_URL`, `DATA_DIR`. Compose overrides `QDRANT_URL` to `http://qdrant:6333` inside the API container.

## License

See [LICENSE](LICENSE).
