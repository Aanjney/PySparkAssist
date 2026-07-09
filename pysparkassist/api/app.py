import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from groq import AsyncGroq

from pysparkassist.config import get_settings
from pysparkassist.api.routes import router
from pysparkassist.api.rate_limiter import RateLimiter
from pysparkassist.api.groq_limits_store import load_groq_limits, save_groq_limits
from pysparkassist.generation.groq_client import fetch_rate_limits_from_groq
from pysparkassist.ingest.manifest import load_manifest, validate_manifest

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    if os.environ.get("SKIP_MANIFEST_CHECK") != "1":
        manifest_path = Path(settings.data_dir) / "manifest.json"
        if not manifest_path.is_file():
            logger.error("Ingest manifest missing at %s", manifest_path)
            raise RuntimeError(f"Ingest manifest missing: {manifest_path}")
        manifest = load_manifest(manifest_path)
        validate_manifest(manifest, settings)
        app.state.manifest_ok = True
        logger.info(
            "Manifest OK: %d chunks, model=%s, dim=%d",
            manifest.chunk_count,
            manifest.embedding_model,
            manifest.embedding_dimension,
        )
    else:
        app.state.manifest_ok = False
        logger.warning("SKIP_MANIFEST_CHECK=1 — manifest validation skipped")

    logger.info("Loading embedding model: %s", settings.embedding_model)

    from sentence_transformers import SentenceTransformer
    from qdrant_client import QdrantClient
    from pysparkassist.ingest.entities import EntityGraph
    from pysparkassist.chat.service import ChatService
    from pysparkassist.retrieval.query_processor import QueryProcessor
    from pysparkassist.retrieval.retriever import Retriever

    local_model_path = Path("data/models") / settings.embedding_model.replace("/", "_")
    if local_model_path.exists():
        logger.info("Loading model from local cache: %s", local_model_path)
        model = SentenceTransformer(str(local_model_path))
    else:
        logger.info("Downloading model (first time only)...")
        model = SentenceTransformer(settings.embedding_model)
        local_model_path.parent.mkdir(parents=True, exist_ok=True)
        model.save(str(local_model_path))
        logger.info("Model saved to %s", local_model_path)
    qdrant = QdrantClient(url=settings.qdrant_url)
    graph = EntityGraph(settings.sqlite_path)
    app.state.qdrant = qdrant

    groq_client = AsyncGroq(api_key=settings.groq_api_key)

    app.state.settings = settings
    app.state.groq_client = groq_client
    app.state.limiter = RateLimiter(max_requests=settings.rate_limit_rpm, window_seconds=60)

    def on_usage(usage_dict: dict) -> None:
        app.state.groq_limits = usage_dict
        save_groq_limits(settings.groq_limits_path, usage_dict)

    app.state.chat_service = ChatService(
        settings=settings,
        query_processor=QueryProcessor(model=model, graph=graph),
        retriever=Retriever(client=qdrant, graph=graph, settings=settings),
        groq_client=groq_client,
        on_usage=on_usage,
    )

    loaded = load_groq_limits(settings.groq_limits_path)
    app.state.groq_limits = loaded

    if settings.groq_limits_startup_probe:
        fresh_stats = await fetch_rate_limits_from_groq(groq_client)
        if fresh_stats is not None and (
            fresh_stats.remaining_requests is not None or fresh_stats.remaining_tokens is not None
        ):
            usage_dict = fresh_stats.to_dict()
            app.state.groq_limits = usage_dict
            save_groq_limits(settings.groq_limits_path, usage_dict)
            logger.info("Groq rate limits refreshed from models.list probe")
        elif loaded is None:
            logger.info("No cached Groq limits and probe failed or returned empty headers")
    elif loaded is None:
        logger.info("Groq limits startup probe disabled; no cache file yet")

    logger.info("PySparkAssist ready")
    yield

    await groq_client.close()
    graph.close()
    qdrant.close()


def create_app() -> FastAPI:
    app = FastAPI(title="PySparkAssist", version="0.1.0", lifespan=lifespan)
    app.include_router(router, prefix="/api")

    frontend_dir = Path(__file__).parent.parent.parent / "frontend"
    if frontend_dir.exists():
        app.mount("/", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")

    return app
