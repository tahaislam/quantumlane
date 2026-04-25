"""API service config."""
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QL_", env_file=".env", extra="ignore")

    postgres_dsn: str = Field(
        default="postgresql://quantumlane:quantumlane@postgres:5432/quantumlane"
    )
    postgres_pool_size: int = Field(default=5, ge=1, le=20)

    cors_origins: list[str] = Field(
        default=["http://localhost:8080", "https://quantumlane.io"],
        description="Allowed CORS origins for the website to call the API.",
    )

    rate_limit_per_minute: int = Field(default=60, ge=1)
    environment: str = Field(default="development")


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
