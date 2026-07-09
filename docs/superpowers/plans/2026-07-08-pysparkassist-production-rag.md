# PySparkAssist Production RAG Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor PySparkAssist into a production-minded RAG portfolio app with Qdrant-as-service, deterministic ingest, typed retrieval contracts, evals, and clean deployment.

**Architecture:** Offline ingest writes manifest + Qdrant vectors + SQLite entity index. API uses thin routes → ChatService → Retriever (dense_only / dense_entity_boost) → Groq SSE. Evals compare retrieval modes on golden questions.

**Tech Stack:** Python 3.12, FastAPI, Qdrant (Docker service), SQLite entity index, sentence-transformers, Groq, Alpine/Tailwind frontend.

**Design spec:** `docs/superpowers/specs/2026-07-08-pysparkassist-production-rag-design.md`

**Worktree:** `.worktrees/production-rag` on branch `feature/production-rag`

---

### Task 1: README and terminology

**Files:**
- Modify: `README.md`

- [ ] Rename "knowledge graph" → "entity-aware retrieval boost" throughout README
- [ ] Add architecture summary, eval mention, Qdrant-as-service tradeoff
- [ ] Confident tone; remove self-deprecating lines

---

### Task 2: Qdrant service migration

**Files:**
- Modify: `pysparkassist/config.py`, `env.example`
- Modify: `pysparkassist/api/app.py`
- Modify: `pysparkassist/ingest/embedder.py` (use QDRANT_URL)
- Modify: `pysparkassist/retrieval/searcher.py`
- Modify: `Dockerfile`
- Modify: `/home/aanjney/wee-deployment-scripts/docker-compose.yml`, `.env`, `deploy.sh`

- [ ] Add `QDRANT_URL` setting; remove embedded `qdrant_path` as primary
- [ ] Add `qdrant` service to compose with volume
- [ ] API connects via HTTP client
- [ ] Unify health checks on `/api/health`
- [ ] Tests for config parsing

---

### Task 3: Deterministic ingest and manifest

**Files:**
- Create: `pysparkassist/ingest/manifest.py`, `pysparkassist/ingest/indexer.py`
- Modify: `pysparkassist/ingest/embedder.py`, `cli.py`, `entities.py`

- [ ] Stable chunk IDs as Qdrant point IDs
- [ ] Derive vector dim from model
- [ ] Full ingest recreates collection + rebuilds SQLite
- [ ] Write/read/validate IngestManifest
- [ ] Batch commits for entity graph
- [ ] Tests for manifest and chunk ID stability

---

### Task 4: Typed RAG contracts and ChatService

**Files:**
- Create: `pysparkassist/retrieval/models.py`, `relevance.py`, `retriever.py`
- Create: `pysparkassist/chat/service.py`, `chat/schemas.py`
- Modify: `pysparkassist/api/routes.py`, `query_processor.py`, `context_builder.py`, `searcher.py`

- [ ] Typed QueryAnalysis, RetrievedChunk, RetrievalResult
- [ ] Retriever with dense_only and dense_entity_boost modes
- [ ] Relevance policy extracted from routes
- [ ] Thin routes calling ChatService
- [ ] Unit tests for relevance and retriever merge

---

### Task 5: Retrieval evals

**Files:**
- Create: `pysparkassist/evals/data/golden_questions.jsonl`, `run.py`, `metrics.py`, `report.py`
- Modify: `README.md`, `.gitignore` for `eval_reports/`

- [ ] 30+ golden questions
- [ ] Compare dense_only vs dense_entity_boost
- [ ] hit@k, MRR, entity_match_rate, abstention_accuracy
- [ ] Write report to eval_reports/

---

### Task 6: Dependency and deployment hardening

**Files:**
- Create: `requirements-runtime.lock` or pin exact versions
- Modify: `wee-deployment-scripts` deploy health URL, image tags

- [ ] Pin runtime deps
- [ ] BUILD_VERSION or git SHA in image tag
- [ ] Fix DEPLOY_HEALTH_URL to `/api/health`
- [ ] Add `.gitignore` for deploy repo `.env`

---

### Task 7: Frontend CDN pin

**Files:**
- Modify: `frontend/index.html`

- [ ] Pin Alpine, marked, DOMPurify, highlight.js versions

---

### Task 8: Deslop pass

- [ ] Review diff vs main; remove AI slop, extra comments, unnecessary abstractions
