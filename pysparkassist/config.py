from functools import lru_cache
from pathlib import Path
from typing import Self

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

COLLECTION_NAME = "pyspark_docs"

_env_file = Path(".env")
_settings_kw: dict = {}
if _env_file.is_file():
    _settings_kw["env_file"] = _env_file
    _settings_kw["env_file_encoding"] = "utf-8"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(**_settings_kw)

    groq_api_key: str
    groq_model: str
    groq_temperature: float = 0.3
    groq_max_tokens: int = 2048

    data_dir: str = "./data"
    qdrant_url: str = "http://localhost:6333"
    sqlite_path: str | None = None
    groq_limits_path: str | None = None
    raw_data_path: str | None = None
    embedding_model: str

    rate_limit_rpm: int = 20
    relevance_threshold: float = 0.35
    off_topic_score_threshold: float = 0.55
    retrieval_mode: str = "dense_entity_boost"
    groq_limits_startup_probe: bool = True

    @model_validator(mode="after")
    def _derive_paths(self) -> Self:
        base = Path(self.data_dir)
        if self.sqlite_path is None:
            object.__setattr__(self, "sqlite_path", str(base / "graph.db"))
        if self.groq_limits_path is None:
            object.__setattr__(self, "groq_limits_path", str(base / "groq_limits.json"))
        if self.raw_data_path is None:
            object.__setattr__(self, "raw_data_path", str(base / "raw"))
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
