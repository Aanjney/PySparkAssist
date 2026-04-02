from pathlib import Path

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

    qdrant_path: str = "./data/qdrant"
    sqlite_path: str = "./data/graph.db"
    groq_limits_path: str = "./data/groq_limits.json"
    raw_data_path: str = "./data/raw"
    embedding_model: str

    rate_limit_rpm: int = 20
    relevance_threshold: float = 0.35
    # One GET /openai/v1/models on each process start to avoid stale limits after restart (uses 1 RPD).
    groq_limits_startup_probe: bool = True


def get_settings() -> Settings:
    return Settings()
