# PySparkAssist Production RAG Design

## Goal

Turn PySparkAssist into a production-minded portfolio RAG application that demonstrates strong command of retrieval-augmented generation, full-stack application structure, deployment tradeoffs, and evaluation-driven engineering.

The finished project should remain small enough to understand, run, and maintain, but mature enough that a recruiter or engineer can see:

- A clear RAG pipeline with explicit data contracts.
- Deterministic ingestion and index lifecycle management.
- Measurable retrieval quality through evals.
- A clean FastAPI service boundary.
- A simple, intentional frontend.
- A Docker deployment model that avoids embedded database file-lock problems.
- Honest architecture tradeoffs documented in the README.

## Product Definition

PySparkAssist is a learner-facing assistant for PySpark. It scrapes PySpark documentation and Spark Python examples, indexes them, retrieves relevant context for user questions, and streams cited answers from Groq.

Core behavior to preserve:

- Answer PySpark and Apache Spark questions using retrieved documentation and examples.
- Stream responses over SSE.
- Show source cards and require inline citation labels in model responses.
- Keep the frontend lightweight and easy to inspect.
- Keep ingestion separate from the request path.
- Preserve local-first operation: the application should run from Docker Compose without managed cloud search infrastructure.

Core behavior to improve:

- Retrieval quality must be measurable.
- Ingestion must be repeatable and safe to rerun.
- Qdrant should run as its own service instead of embedded local storage.
- The entity graph should be renamed and treated as an entity-aware retrieval boost, not a full knowledge graph.
- The API route should stop owning the whole RAG pipeline.

## Non-Goals

This design deliberately avoids:

- Building a full RAGOps platform with dashboards, workflow orchestration, and tracing infrastructure.
- Adding user accounts, billing, teams, or admin panels.
- Adding agentic multi-hop planning.
- Adding a frontend build system unless the current no-bundler approach becomes a blocker.
- Optimizing for arbitrary coding-assistant behavior outside the PySpark learning domain.

The project should look production-minded, not enterprise-bloated.

## Design Principles

1. **Make the RAG path inspectable.** A reviewer should be able to follow query analysis, retrieval, context construction, prompting, and streaming without reading unrelated framework glue.

2. **Prefer measured improvements over buzzwords.** Entity-aware retrieval stays only because it is useful to compare against dense-only retrieval. The eval suite decides whether it earns its complexity.

3. **Version the data plane.** The embedding model, chunking parameters, source versions, index schema, and code revision used for ingest should be recorded in a manifest.

4. **Keep production dependencies boring.** Qdrant should be a service. The API should talk to it over HTTP. Runtime config should come from settings, not hardcoded paths.

5. **Use small tests and evals where they matter.** This project does not need a massive test harness. It does need checks around retrieval, manifest consistency, relevance policy, and health/deployment contracts.

## Current Architecture Problems

### Embedded Qdrant Creates Operational Drag

The current app uses Qdrant local storage under `data/qdrant`, mounted into the API container. That keeps setup simple, but it creates file-lock and deployment problems. It also forces stop-first deployments and makes scaling impossible without redesign.

The simpler production-shaped design is to run Qdrant as a Compose service and connect via `QDRANT_URL`.

### Ingest Is Not Deterministic Enough

The current ingest path creates or upserts into Qdrant, uses sequential point IDs, hardcodes vector dimension to `768`, and does not clearly reset stale vectors or graph state on full reindex.

For a RAG app, this is a core correctness problem. Search quality depends on the index being a faithful representation of the current corpus and embedding model.

### RAG Contracts Are Too Loose

Several module boundaries pass `dict` metadata or loosely-shaped histories. This makes the pipeline harder to test and harder to explain. The project should use typed models for the data shapes that cross boundaries.

### Chat Route Owns Too Much

The current `/api/chat` path performs request validation, rate limiting, query processing, search, context building, off-topic handling, prompt creation, Groq streaming, usage persistence, and SSE event formatting.

The route should be thin. RAG orchestration belongs in a service layer.

### Entity Graph Is Overmarketed

The SQLite graph is useful as an entity-aware boost, but it is not a production knowledge graph. It uses heuristic extraction, curated relationships, and co-occurrence edges. That can be valuable, but only if framed honestly and evaluated.

### No Evals Means No Proof

The README currently explains the RAG design but does not prove it. A production-ready RAG portfolio project needs a small golden dataset and a repeatable eval command.

## Target Architecture

```text
pysparkassist/
  api/
    app.py                 # FastAPI app factory and lifecycle
    routes.py              # Thin HTTP/SSE endpoints
    rate_limiter.py
    groq_limits_store.py

  chat/
    service.py             # Query -> retrieve -> prompt -> stream orchestration
    schemas.py             # Chat request/history/output event contracts

  retrieval/
    models.py              # QueryAnalysis, RetrievedChunk, RetrievalResult, Source
    query_processor.py     # Embedding + entity/domain analysis
    retriever.py           # Dense retrieval + entity-aware boost
    context_builder.py     # Context text and source card construction
    relevance.py           # Named relevance/off-topic policy

  generation/
    prompt.py              # Prompt construction
    groq_client.py         # Groq streaming and error mapping

  ingest/
    scraper.py             # Fetch docs/examples
    chunker.py             # Structure-aware chunking
    entities.py            # Entity extraction and SQLite entity index
    indexer.py             # Qdrant collection management and embedding writes
    manifest.py            # Ingest manifest write/read/validate
    cli.py                 # Ingest commands

  evals/
    data/golden_questions.jsonl
    run.py                 # Eval runner
    metrics.py             # hit@k, MRR, recall, mode comparison
    report.py              # Markdown/JSON report writer

frontend/
  index.html
  app.js
  styles.css
```

This structure preserves the existing package shape but gives each concern a clear home.

## Data And Contracts

### `QueryAnalysis`

Represents the interpreted user query.

Fields:

- `query`: original user query.
- `embedding`: dense query vector.
- `entities`: matched PySpark entities.
- `domain_relevant`: whether query text matches Spark/data terms.

### `RetrievedChunk`

Represents one retrieved chunk independent of Qdrant internals.

Fields:

- `chunk_id`: stable ID derived from source identity and content hash.
- `content`: chunk text.
- `score`: retrieval score after the selected retrieval mode.
- `source`: normalized source metadata.
- `matched_by`: `dense`, `entity_boost`, or future retrieval modes.
- `entity_names`: entities attached at ingest time.

### `RetrievalResult`

Represents the full retrieval output for one query.

Fields:

- `query_analysis`
- `chunks`
- `mode`: `dense_only` or `dense_entity_boost`.
- `top_score`
- `debug`: optional scores and retrieved IDs for eval/reporting.

### `IngestManifest`

Written after a successful ingest.

Fields:

- `schema_version`
- `created_at`
- `git_commit`
- `embedding_model`
- `embedding_dimension`
- `collection_name`
- `chunker_version`
- `source_versions`
- `chunk_count`
- `entity_count`
- `relationship_count`

The API startup should validate the manifest before accepting chat traffic. If the manifest is missing or incompatible with settings, startup should fail loudly.

## Ingestion Design

### Source Collection

Keep the current sources:

- PySpark API docs for selected versions.
- Spark Python examples.

Improve source metadata:

- Every scraped doc gets `source_url`, `doc_version`, `title`, and `source_type=documentation`.
- Every example gets `file_path`, `example_category`, and `source_type=code_example`.

### Chunking

Keep structure-aware chunking:

- Markdown chunks split by headings and paragraphs.
- Python examples split by AST function definitions, with whole-file fallback.

Target baseline:

- Documentation chunks should remain coherent sections, usually in the 512-1024 token range.
- Code chunks should remain function-level where possible.

Do not add semantic chunking yet. It adds moving parts before evals prove chunking is the bottleneck.

### Entity-Aware Index

Rename “knowledge graph” to “entity-aware retrieval boost.”

The SQLite entity index stores:

- Known PySpark classes/modules/methods found in chunks.
- Curated relationships between important PySpark concepts.
- Co-occurrence edges where useful.

Extraction should be tightened:

- Keep known PySpark class/module matching.
- Keep AST-aware method extraction for Python code where possible.
- Avoid treating every `.word(` pattern as meaningful without filtering.

Writes should be transactional:

- Full ingest clears/rebuilds entity tables.
- Graph/index writes commit per batch or per ingest phase, not per helper call.

### Qdrant Indexing

Qdrant runs as a service.

Configuration:

- `QDRANT_URL=http://qdrant:6333` in Docker.
- Local dev can use `http://localhost:6333`.
- Collection name remains `pyspark_docs`.

Indexing behavior:

- Derive embedding dimension from the loaded model.
- Recreate or sync the collection during full ingest.
- Use stable point IDs based on chunk identity, not sequential counters.
- Store normalized payloads matching `RetrievedChunk` fields.
- Batch embedding and upload operations.

This makes re-ingest predictable and avoids stale vectors.

## Retrieval Design

### Phase 1 Retrieval Modes

Implement two first-class retrieval modes:

1. `dense_only`
   - Query embedding searches Qdrant.
   - Returns top candidates by dense similarity.

2. `dense_entity_boost`
   - Run dense search.
   - Match query entities.
   - Expand related entities through SQLite.
   - Retrieve or boost chunks containing matching entities.
   - Merge results deterministically.

The entity boost must be explainable in eval output: which entities matched, which related entities were used, and which chunks changed rank.

### Future Retrieval Hooks

Design, but do not immediately implement:

- Sparse/BM25 vector support in Qdrant using named vectors.
- Reciprocal Rank Fusion for dense+sparse hybrid search.
- Cross-encoder reranking.

These should be future modes that plug into the same `Retriever` interface. They should not be added until the baseline evals show where retrieval fails.

## Relevance And Abstention

Move off-topic handling out of `api/routes.py` into `retrieval/relevance.py`.

Inputs:

- `QueryAnalysis`
- `RetrievalResult`
- chat history summary signal if needed

Outputs:

- `should_answer`
- `reason`: `in_domain`, `low_relevance`, `out_of_domain`, or `no_context`
- `user_message` for abstention cases

The current hidden second threshold should become named config. The design should avoid scattered magic values in the route.

## Chat Service Design

`ChatService` owns the application flow:

1. Analyze query.
2. Retrieve context.
3. Classify relevance.
4. Build prompt messages.
5. Stream Groq events.
6. Attach sources and usage metadata.

The route owns only:

- HTTP request validation.
- IP rate limiting.
- Calling `ChatService`.
- Returning `EventSourceResponse`.

This makes the RAG flow testable without FastAPI.

## Eval Design

### Golden Dataset

Create `pysparkassist/evals/data/golden_questions.jsonl`.

Start with 30-50 hand-curated rows.

Each row:

```json
{
  "id": "dataframe-select-basic",
  "question": "How do I select columns from a PySpark DataFrame?",
  "expected_entities": ["DataFrame", "select", "Column"],
  "expected_source_contains": ["DataFrame.select"],
  "answer_notes": "Should explain selecting one or more columns and include PySpark syntax.",
  "should_answer": true
}
```

Include categories:

- Basic DataFrame operations.
- Joins and aggregations.
- Schema and data types.
- File I/O.
- Caching and persistence.
- Structured streaming.
- ML pipeline examples.
- Off-topic/adversarial queries that should abstain.

### Retrieval Metrics

The first eval runner should avoid LLM-as-judge complexity.

Metrics:

- `hit@k`: whether any expected source/entity appears in top-k.
- `mrr`: rank of first expected hit.
- `entity_match_rate`: expected entities matched by query analysis or retrieved chunks.
- `abstention_accuracy`: correct answer/abstain decision.

Compare:

- `dense_only`
- `dense_entity_boost`

The report should show aggregate metrics and per-question diffs where entity boost helped or hurt.

### Generation Smoke Evals

Keep generation evals lightweight initially.

Checks:

- Response contains citation labels.
- Response avoids fabricated URLs.
- Off-topic questions get the abstention message.
- Groq errors map to user-safe codes.

Do not gate every commit on live Groq calls. Live generation evals should be manually runnable or marked integration-only.

### Eval Reports

Write reports to `eval_reports/`.

Include:

- Timestamp.
- Git commit.
- manifest metadata.
- retrieval mode comparison table.
- failing questions with retrieved chunk IDs.

This becomes the project’s strongest proof of production readiness.

## API Design

Endpoints:

- `GET /api/health`
  - Returns app readiness.
  - Should validate Qdrant connectivity and manifest compatibility, or expose shallow/deep health separately.

- `GET /api/limits`
  - Returns cached Groq usage.

- `POST /api/chat`
  - Streams SSE events.

Optional later endpoint:

- `POST /api/retrieve`
  - Debug-only retrieval endpoint for local eval/demo use.
  - Should be disabled or protected in public deployment if added.

## Frontend Design

Keep the no-bundler frontend for now. It is appropriate for the project if presented as an intentional constraint.

Required cleanup:

- Pin CDN dependency versions or vendor assets.
- Keep DOMPurify.
- Keep SSE streaming.
- Keep source cards.
- Keep usage display if it does not distract from the RAG story.

Split `frontend/app.js` only when editing that area meaningfully. A reasonable split is:

- `markdown.js`
- `sse.js`
- `usage.js`
- `app.js`

Do not introduce React/Vite just for optics.

## Deployment Design

Use Docker Compose with separate services:

- `pysparkassist-api`
- `qdrant`
- `caddy` in the edge deployment repo

Qdrant:

- Uses official Qdrant image.
- Persists data in a named volume or host bind mount.
- Exposes `6333` only inside the Compose network unless local dev needs host access.

API:

- Connects to Qdrant through `QDRANT_URL`.
- Mounts or reads only the data it still owns, such as SQLite entity index, manifest, and Groq limits cache.
- Uses the same canonical health URL everywhere: `/api/health`.

Deployment improvements:

- Remove Qdrant local file-lock dependence from the API container.
- Stop-first deploy should no longer be required for Qdrant.
- Keep rollback simple by tagging images with Git SHA.
- Health checks should fail on incompatible manifest/index state.

## Dependency Strategy

Keep direct dependencies understandable:

- FastAPI
- uvicorn
- qdrant-client
- sentence-transformers
- groq
- pydantic-settings
- sse-starlette
- crawl4ai and Playwright for ingest

Improve reproducibility:

- Add a locked production requirements file.
- Keep runtime and ingest dependency groups separate.
- Avoid adding LangChain/LlamaIndex unless they remove more code than they add. For this project, they likely obscure the architecture rather than improve it.

## Documentation Design

README should be rewritten around:

- What the app does.
- Architecture diagram.
- Why Qdrant as a service.
- Why entity-aware boost instead of a full knowledge graph.
- Ingest lifecycle and manifest.
- Eval methodology and current results.
- Deployment flow.
- Tradeoffs and known ceilings.

The tone should be confident and honest. Avoid self-deprecating tradeoff language. The tradeoff is not weakness; it is architecture judgment.

## Implementation Phases

### Phase 1: Rename And Document The Architecture

Outcome:

- README reflects the new product story.
- “Knowledge graph” becomes “entity-aware retrieval boost.”
- Design tradeoffs are explicit.

### Phase 2: Qdrant Service Migration

Outcome:

- Qdrant runs as a separate service.
- API uses `QDRANT_URL`.
- Local and Docker config are consistent.
- Health checks use `/api/health`.

### Phase 3: Deterministic Ingest And Manifest

Outcome:

- Full ingest creates a clean index state.
- Qdrant collection schema derives vector dimension from the model.
- Chunk IDs are stable.
- Manifest is written and validated.

### Phase 4: Typed RAG Contracts And Chat Service

Outcome:

- Route is thin.
- Chat orchestration is testable without FastAPI.
- Retrieval outputs are typed.
- Relevance policy is named and tested.

### Phase 5: Retrieval Evals

Outcome:

- Golden dataset exists.
- Eval command compares dense-only and dense+entity modes.
- Report proves whether entity boost helps.

### Phase 6: Dependency And Deployment Hardening

Outcome:

- Production dependencies are locked.
- Docker image and deployment are reproducible.
- Deploy health contract is consistent.

### Phase 7: Frontend And Demo Polish

Outcome:

- CDN dependencies are pinned or vendored.
- UI remains simple and polished.
- README includes eval results and screenshots if useful.

## Success Criteria

The project is successful when:

- A new developer can run ingest, run evals, and start the app from documented commands.
- The app starts only when Qdrant and the ingest manifest are compatible.
- Retrieval evals produce a clear dense-only vs dense+entity comparison.
- The README explains why each major tradeoff was chosen.
- The code has a thin API layer and a readable RAG service layer.
- Deployment no longer depends on embedded Qdrant file locks.
- The frontend remains easy to inspect and does not dominate the architecture.

## Sources Considered

This design follows current small-production RAG guidance:

- Structure-aware chunking before semantic chunking.
- Hybrid/boosted retrieval only when measured.
- RRF/sparse/rerank as future modes, not premature dependencies.
- Golden datasets and stage-specific evals.
- Versioned indexes and manifests.
- Per-request retrieval traceability.

References used during design:

- Qdrant hybrid search and Universal Query API documentation.
- IBM RAG evaluation guidance.
- 2025 RAGOps guidance on versioning, observability, and data lifecycle.
- Current RAG production articles emphasizing chunking, hybrid retrieval, reranking, and golden evals.
