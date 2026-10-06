from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    api_token: SecretStr = Field(min_length=24)
    database_path: Path = Path("data/jobs.sqlite3")
    global_concurrency: int = Field(default=2, ge=1, le=8)
    item_timeout_seconds: float = Field(default=180, gt=0)
    retention_hours: int = Field(default=50, ge=1)
    max_active_jobs: int = Field(default=100, ge=1)
    cleanup_interval_seconds: float = Field(default=300, gt=0)
    captcha_api_key: SecretStr = SecretStr("")
    stparts_storage_state: Path | None = None
    headless: bool = True
