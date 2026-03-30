import os
import pytest

os.environ.setdefault("GROQ_API_KEY", "test_key")


@pytest.fixture
def tmp_data_dir(tmp_path):
    """Provides a temporary data directory for tests."""
    qdrant_dir = tmp_path / "qdrant"
    qdrant_dir.mkdir()
    sqlite_path = tmp_path / "graph.db"
    return {"qdrant_path": str(qdrant_dir), "sqlite_path": str(sqlite_path)}
