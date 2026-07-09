import pytest

from pysparkassist.config import get_settings
from pysparkassist.ingest.manifest import (
    MANIFEST_SCHEMA_VERSION,
    IngestManifest,
    build_manifest,
    load_manifest,
    validate_manifest,
    write_manifest,
)


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _settings(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    monkeypatch.setenv("GROQ_MODEL", "test-model")
    monkeypatch.setenv("EMBEDDING_MODEL", "BAAI/bge-base-en-v1.5")
    return get_settings()


def test_write_load_manifest(tmp_path) -> None:
    path = tmp_path / "manifest.json"
    manifest = IngestManifest(
        created_at="2026-07-08T12:00:00+00:00",
        git_commit="abc123",
        embedding_model="BAAI/bge-base-en-v1.5",
        embedding_dimension=768,
        collection_name="pyspark_docs",
        source_versions={"docs": "3.5.1"},
        chunk_count=42,
        entity_count=10,
        relationship_count=5,
    )
    write_manifest(path, manifest)
    loaded = load_manifest(path)
    assert loaded == manifest


def test_validate_manifest_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(monkeypatch)
    manifest = build_manifest(
        embedding_model=settings.embedding_model,
        embedding_dimension=768,
        source_versions={},
        chunk_count=1,
        entity_count=1,
        relationship_count=0,
    )
    validate_manifest(manifest, settings)


def test_validate_manifest_rejects_model_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(monkeypatch)
    manifest = build_manifest(
        embedding_model="other/model",
        embedding_dimension=768,
        source_versions={},
        chunk_count=1,
        entity_count=1,
        relationship_count=0,
    )
    with pytest.raises(ValueError, match="embedding_model"):
        validate_manifest(manifest, settings)


def test_validate_manifest_rejects_schema_version(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(monkeypatch)
    manifest = build_manifest(
        embedding_model=settings.embedding_model,
        embedding_dimension=768,
        source_versions={},
        chunk_count=1,
        entity_count=1,
        relationship_count=0,
    )
    manifest.schema_version = "999"
    with pytest.raises(ValueError, match="schema_version"):
        validate_manifest(manifest, settings)


def test_build_manifest_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(monkeypatch)
    manifest = build_manifest(
        embedding_model=settings.embedding_model,
        embedding_dimension=768,
        source_versions={"docs": "3.5"},
        chunk_count=10,
        entity_count=3,
        relationship_count=2,
    )
    assert manifest.schema_version == MANIFEST_SCHEMA_VERSION
    assert manifest.collection_name == "pyspark_docs"
    assert manifest.created_at
    assert manifest.git_commit
