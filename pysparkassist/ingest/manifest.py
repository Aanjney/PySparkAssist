import json
import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field

from pysparkassist.config import COLLECTION_NAME, Settings

logger = logging.getLogger(__name__)

MANIFEST_SCHEMA_VERSION = "1"
CHUNKER_VERSION = "1"


class IngestManifest(BaseModel):
    schema_version: str = MANIFEST_SCHEMA_VERSION
    created_at: str
    git_commit: str
    embedding_model: str
    embedding_dimension: int
    collection_name: str
    chunker_version: str = CHUNKER_VERSION
    source_versions: dict[str, str] = Field(default_factory=dict)
    chunk_count: int
    entity_count: int
    relationship_count: int


def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL, text=True
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def write_manifest(path: str | Path, manifest: IngestManifest) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(manifest.model_dump_json(indent=2), encoding="utf-8")
    logger.info("Wrote ingest manifest to %s", p)


def load_manifest(path: str | Path) -> IngestManifest:
    return IngestManifest.model_validate_json(Path(path).read_text(encoding="utf-8"))


def validate_manifest(manifest: IngestManifest, settings: Settings) -> None:
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION:
        raise ValueError(
            f"manifest schema_version {manifest.schema_version!r} != {MANIFEST_SCHEMA_VERSION!r}"
        )
    if manifest.embedding_model != settings.embedding_model:
        raise ValueError(
            f"manifest embedding_model {manifest.embedding_model!r} != settings {settings.embedding_model!r}"
        )
    if manifest.collection_name != COLLECTION_NAME:
        raise ValueError(
            f"manifest collection_name {manifest.collection_name!r} != {COLLECTION_NAME!r}"
        )


def build_manifest(
    *,
    embedding_model: str,
    embedding_dimension: int,
    source_versions: dict[str, str],
    chunk_count: int,
    entity_count: int,
    relationship_count: int,
    collection_name: str = COLLECTION_NAME,
) -> IngestManifest:
    return IngestManifest(
        created_at=datetime.now(timezone.utc).isoformat(),
        git_commit=_git_commit(),
        embedding_model=embedding_model,
        embedding_dimension=embedding_dimension,
        collection_name=collection_name,
        source_versions=source_versions,
        chunk_count=chunk_count,
        entity_count=entity_count,
        relationship_count=relationship_count,
    )
