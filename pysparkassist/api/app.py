import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from pysparkassist.config import get_settings
from pysparkassist.api.routes import router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logger.info("Loading embedding model: %s", settings.embedding_model)

    from sentence_transformers import SentenceTransformer
    from qdrant_client import QdrantClient
    from pysparkassist.ingest.entities import EntityGraph
    from pysparkassist.retrieval.query_processor import QueryProcessor
    from pysparkassist.retrieval.searcher import Searcher

    model = SentenceTransformer(settings.embedding_model)
    qdrant = QdrantClient(path=settings.qdrant_path)
    graph = EntityGraph(settings.sqlite_path)

    app.state.settings = settings
    app.state.query_processor = QueryProcessor(model=model, graph=graph)
    app.state.searcher = Searcher(client=qdrant, graph=graph)

    logger.info("PySparkAssist ready")
    yield

    graph.close()
    qdrant.close()


def create_app() -> FastAPI:
    app = FastAPI(title="PySparkAssist", version="0.1.0", lifespan=lifespan)
    app.include_router(router, prefix="/api")

    frontend_dir = Path(__file__).parent.parent.parent / "frontend"
    if frontend_dir.exists():
        app.mount("/", StaticFiles(directory=str(frontend_dir), html=True), name="frontend")

    return app
