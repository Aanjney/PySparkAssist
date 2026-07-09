# PySparkAssist

A RAG chatbot for learning PySpark. It scrapes official docs and Spark Python examples, chunks and embeds them into Qdrant, and answers questions with a small Groq model that has to cite what it retrieved. Embeddings and vectors stay on your box; only generation hits Groq.

Longer design notes live in [`docs/superpowers/specs/2026-07-08-pysparkassist-production-rag-design.md`](docs/superpowers/specs/2026-07-08-pysparkassist-production-rag-design.md) if you want the full story.

**How it fits together:** ingest runs on the host and fills Qdrant + a SQLite entity index; the API container embeds queries, retrieves chunks (dense search, with an optional entity boost), and streams SSE to a plain HTML frontend.

---

## Get it running

You'll need [uv](https://docs.astral.sh/uv/), Docker, and a Groq API key. On the VPS, Qdrant and the API are standalone `docker run` containers; Caddy in [`wee-deployment-scripts`](https://github.com/Aanjney/wee-deployment-scripts) handles TLS and proxies to the API.

### 1. Clone and configure

```bash
git clone https://github.com/Aanjney/pysparkassist.git
cd pysparkassist
cp env.example .env
# Set GROQ_API_KEY (and tweak models/paths if you care)
```

Install uv if you don't have it:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source "$HOME/.local/bin/env"   # if `uv` isn't on PATH yet
```

Host deps for ingest and evals:

```bash
uv sync --extra ml --extra ingest --extra dev
```

### 2. Qdrant + ingest

Create a Docker network once, then start Qdrant:

```bash
docker network create pysparkassist

docker run -d \
  --name qdrant \
  --network pysparkassist \
  -p 127.0.0.1:6333:6333 \
  -v pysparkassist_qdrant:/qdrant/storage \
  --restart unless-stopped \
  qdrant/qdrant:latest
```

Ingest runs on the host. `.env` should have `QDRANT_URL=http://localhost:6333` for this step:

```bash
uv run python -m playwright install chromium
uv run python -m pysparkassist.ingest run
```

That crawl takes a while the first time. Go get coffee.

### 3. API container

Build and run the API. Mount `./data` so the entity index and manifest from ingest are visible inside the container:

```bash
docker build -t pysparkassist:local .

docker run -d \
  --name pysparkassist \
  --network pysparkassist \
  -p 127.0.0.1:8000:8000 \
  -v "$(pwd)/data:/app/data" \
  --env-file .env \
  -e QDRANT_URL=http://qdrant:6333 \
  --restart unless-stopped \
  pysparkassist:local
```

Health check: `curl -s http://127.0.0.1:8000/api/health`

### 4. Caddy (public HTTPS)

In `wee-deployment-scripts`, the Caddyfile proxies `pysparkassist.duckdns.org` → `pysparkassist:8000`. For that to resolve, attach the API container to Caddy's compose network (usually `edge_default`):

```bash
docker network connect edge_default pysparkassist
```

Then from `wee-deployment-scripts`:

```bash
docker compose up -d caddy
docker compose exec caddy caddy reload --config /etc/caddy/Caddyfile
```

**Where things live:** ingest on the host (uv), Qdrant + API via `docker run` on network `pysparkassist`, Caddy via compose. Qdrant data is in the `pysparkassist_qdrant` volume; SQLite and manifests sit under `./data`.

---

## Retrieval evals

There's a golden question set under `pysparkassist/evals/data/`. Run it against your ingested index — no Groq calls, just retrieval:

```bash
uv run python -m pysparkassist.evals.run --modes dense_only,dense_entity_boost --k 8
```

Reports land in `eval_reports/` (JSON + markdown). Compares plain vector search vs the entity-boost path so you can see if the SQLite index is worth the extra moving parts.

---

## Configuration

See `env.example`. The important bits: `GROQ_API_KEY`, `GROQ_MODEL`, `EMBEDDING_MODEL`, `QDRANT_URL` (localhost for host tools, `http://qdrant:6333` inside the API container), and `DATA_DIR` for the entity index and cached embedding weights.

---

## License

See [LICENSE](LICENSE).
