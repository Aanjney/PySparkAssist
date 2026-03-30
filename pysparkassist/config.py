from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    groq_api_key: str
    groq_model: str = "llama-3.3-70b-versatile"

    qdrant_path: str = "./data/qdrant"
    sqlite_path: str = "./data/graph.db"
    embedding_model: str = "BAAI/bge-base-en-v1.5"

    rate_limit_rpm: int = 20
    relevance_threshold: float = 0.35

    @property
    def qdrant_dir(self) -> Path:
        return Path(self.qdrant_path)

    @property
    def sqlite_file(self) -> Path:
        return Path(self.sqlite_path)


def get_settings() -> Settings:
    return Settings()
