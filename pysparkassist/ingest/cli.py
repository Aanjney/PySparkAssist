import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

from qdrant_client import QdrantClient

from pysparkassist.config import get_settings
from pysparkassist.ingest.chunker import Chunk, chunk_markdown, chunk_python_file
from pysparkassist.ingest.entities import EntityGraph
from pysparkassist.ingest.graph_builder import build_graph
from pysparkassist.ingest.indexer import embed_and_store
from pysparkassist.ingest.manifest import build_manifest, write_manifest
from pysparkassist.ingest.scraper import scrape_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_QDRANT_START = "  docker run -d --name qdrant --network pysparkassist -p 127.0.0.1:6333:6333 -v pysparkassist_qdrant:/qdrant/storage qdrant/qdrant:latest"


def require_qdrant(url: str) -> None:
    try:
        QdrantClient(url=url, timeout=5).get_collections()
    except Exception:
        logger.error("Qdrant is not reachable at %s", url)
        print(f"Start Qdrant with:\n{_QDRANT_START}", file=sys.stderr)
        sys.exit(1)


def load_chunks_from_raw(raw_dir: Path) -> list[Chunk]:
    chunks: list[Chunk] = []

    docs_dir = raw_dir / "docs"
    if docs_dir.exists():
        for md_file in docs_dir.rglob("*.md"):
            meta_file = md_file.with_suffix(".json")
            metadata = json.loads(meta_file.read_text()) if meta_file.exists() else {}
            content = md_file.read_text(encoding="utf-8")
            doc_chunks = chunk_markdown(
                content,
                source_url=metadata.get("url", ""),
                doc_version=metadata.get("version", "unknown"),
            )
            chunks.extend(doc_chunks)

    examples_dir = raw_dir / "examples"
    if examples_dir.exists():
        for py_file in examples_dir.rglob("*.py"):
            content = py_file.read_text(encoding="utf-8")
            category = py_file.parent.name or "general"
            py_chunks = chunk_python_file(content, file_path=str(py_file.relative_to(examples_dir)), category=category)
            chunks.extend(py_chunks)

    return chunks


def _manifest_path(settings) -> Path:
    return Path(settings.data_dir) / "manifest.json"


def cmd_scrape(args: argparse.Namespace) -> None:
    settings = get_settings()
    asyncio.run(scrape_all(Path(settings.raw_data_path)))


def cmd_chunk(args: argparse.Namespace) -> None:
    settings = get_settings()
    chunks = load_chunks_from_raw(Path(settings.raw_data_path))
    logger.info("Created %d chunks from raw data", len(chunks))


def cmd_embed(args: argparse.Namespace) -> None:
    settings = get_settings()
    require_qdrant(settings.qdrant_url)
    chunks = load_chunks_from_raw(Path(settings.raw_data_path))
    stats = embed_and_store(
        chunks,
        qdrant_url=settings.qdrant_url,
        sqlite_path=settings.sqlite_path,
        model_name=settings.embedding_model,
    )
    logger.info("Embedding complete: %d chunks stored", stats["chunk_count"])


def cmd_build_graph(args: argparse.Namespace) -> None:
    settings = get_settings()
    graph = EntityGraph(settings.sqlite_path)
    result = build_graph(graph)
    logger.info("Graph built: %s", result)
    graph.close()


def _write_run_manifest(settings, embed_stats: dict, relationship_count: int) -> None:
    manifest = build_manifest(
        embedding_model=settings.embedding_model,
        embedding_dimension=embed_stats["embedding_dimension"],
        source_versions=embed_stats["source_versions"],
        chunk_count=embed_stats["chunk_count"],
        entity_count=embed_stats["entity_count"],
        relationship_count=relationship_count,
    )
    write_manifest(_manifest_path(settings), manifest)


def cmd_run(args: argparse.Namespace) -> None:
    logger.info("Starting full ingestion pipeline")
    settings = get_settings()
    require_qdrant(settings.qdrant_url)
    cmd_scrape(args)
    chunks = load_chunks_from_raw(Path(settings.raw_data_path))
    embed_stats = embed_and_store(
        chunks,
        qdrant_url=settings.qdrant_url,
        sqlite_path=settings.sqlite_path,
        model_name=settings.embedding_model,
    )
    graph = EntityGraph(settings.sqlite_path)
    build_graph(graph)
    relationship_count = graph.relationship_count()
    graph.close()
    _write_run_manifest(settings, embed_stats, relationship_count)
    logger.info("Ingestion pipeline complete")


def main() -> None:
    parser = argparse.ArgumentParser(description="PySparkAssist ingestion pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run", help="Run full pipeline (scrape + chunk + embed + build-graph)")
    sub.add_parser("scrape", help="Scrape PySpark docs and examples")
    sub.add_parser("chunk", help="Chunk scraped content (dry run, no storage)")
    sub.add_parser("embed", help="Chunk + embed + store in Qdrant and SQLite")
    sub.add_parser("build-graph", help="Build entity relationships from co-occurrence + curated seed")

    args = parser.parse_args()
    commands = {"run": cmd_run, "scrape": cmd_scrape, "chunk": cmd_chunk, "embed": cmd_embed, "build-graph": cmd_build_graph}
    commands[args.command](args)


if __name__ == "__main__":
    main()
