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
    # RuCaptcha is the documented provider; its /in.php and /res.php API is shared with 2captcha.
    captcha_api_url: str = "https://rucaptcha.com"
    captcha_max_attempts: int = Field(default=2, ge=1, le=5)
    captcha_solve_timeout_seconds: float = Field(default=90, gt=0)
    captcha_poll_interval_seconds: float = Field(default=5, gt=0)
    captcha_wait_seconds: float = Field(default=30, gt=0)
    stparts_storage_state: Path | None = None
    headless: bool = True
    diagnostics_enabled: bool = True
    diagnostics_max_samples: int = Field(default=20, ge=1, le=200)
