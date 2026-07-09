import pytest

from pysparkassist.config import Settings, get_settings


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _set_required_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GROQ_API_KEY", "test-key")
    monkeypatch.setenv("GROQ_MODEL", "test-model")
    monkeypatch.setenv("EMBEDDING_MODEL", "test-embed")


def test_qdrant_url_default(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.delenv("QDRANT_URL", raising=False)
    settings = get_settings()
    assert settings.qdrant_url == "http://localhost:6333"


def test_qdrant_url_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("QDRANT_URL", "http://qdrant:6333")
    settings = get_settings()
    assert settings.qdrant_url == "http://qdrant:6333"


def test_paths_derived_from_data_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required_env(monkeypatch)
    monkeypatch.setenv("DATA_DIR", "/tmp/pysparkassist-data")
    settings = Settings(_env_file=None)
    assert settings.sqlite_path == "/tmp/pysparkassist-data/graph.db"
    assert settings.groq_limits_path == "/tmp/pysparkassist-data/groq_limits.json"
    assert settings.raw_data_path == "/tmp/pysparkassist-data/raw"
