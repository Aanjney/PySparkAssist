import argparse
import asyncio
import json
import logging
from pathlib import Path

from pysparkassist.config import get_settings
from pysparkassist.ingest.scraper import scrape_all, clone_spark_examples, scrape_pyspark_docs, PYSPARK_DOC_ROOTS
from pysparkassist.ingest.chunker import chunk_markdown, chunk_python_file, Chunk
from pysparkassist.ingest.embedder import embed_and_store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_chunks_from_raw(raw_dir: Path) -> list[Chunk]:
    """Load all scraped content and chunk it."""
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


def cmd_scrape(args: argparse.Namespace) -> None:
    settings = get_settings()
    raw_dir = Path("data/raw")
    asyncio.run(scrape_all(raw_dir))


def cmd_chunk(args: argparse.Namespace) -> None:
    raw_dir = Path("data/raw")
    chunks = load_chunks_from_raw(raw_dir)
    logger.info("Created %d chunks from raw data", len(chunks))


def cmd_embed(args: argparse.Namespace) -> None:
    settings = get_settings()
    raw_dir = Path("data/raw")
    chunks = load_chunks_from_raw(raw_dir)
    count = embed_and_store(
        chunks,
        qdrant_path=settings.qdrant_path,
        sqlite_path=settings.sqlite_path,
        model_name=settings.embedding_model,
    )
    logger.info("Embedding complete: %d chunks stored", count)


def cmd_run(args: argparse.Namespace) -> None:
    """Run the full pipeline: scrape -> chunk -> embed."""
    logger.info("Starting full ingestion pipeline")
    cmd_scrape(args)
    cmd_embed(args)
    logger.info("Ingestion pipeline complete")


def main() -> None:
    parser = argparse.ArgumentParser(description="PySparkAssist ingestion pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run", help="Run full pipeline (scrape + chunk + embed)")
    sub.add_parser("scrape", help="Scrape PySpark docs and examples")
    sub.add_parser("chunk", help="Chunk scraped content (dry run, no storage)")
    sub.add_parser("embed", help="Chunk + embed + store in Qdrant and SQLite")

    args = parser.parse_args()
    commands = {"run": cmd_run, "scrape": cmd_scrape, "chunk": cmd_chunk, "embed": cmd_embed}
    commands[args.command](args)


if __name__ == "__main__":
    main()
