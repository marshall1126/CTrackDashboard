from __future__ import annotations

import os
from enum import Enum
from pathlib import Path

from dotenv import load_dotenv
from logger import get_logger

logger = get_logger(__name__)


class EnvKey(str, Enum):
    SUPABASE_ANON_KEY = "SUPABASE_ANON_KEY"
    SUPABASE_SERVICE_ROLE_KEY = "SUPABASE_SERVICE_ROLE_KEY"
    SUPABASE_URL = "SUPABASE_URL"
    DATABASE_URL = "DATABASE_URL"
    OPENAI_API_KEY = "OPENAI_API_KEY"
    NEON_DATABASE_URL_POOLER = "NEON_DATABASE_URL_POOLER"

_keydict: dict[EnvKey, str] = {}
_loaded = False

def _project_root() -> Path:
    # env.py lives in analysis_scripts/, so parent is project root
    return Path(__file__).resolve().parent


def load_env(*, override_dotenv: bool = False) -> None:
    """
    Load env vars once.
    - Local dev: reads .env from project root.
    - Railway: variables already exist; .env likely absent and that's OK.
    """
    global _loaded
    if _loaded:
        return

    if os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_GIT_COMMIT_SHA"):
        logger.info("Running on Railway – using platform env vars")
    else:
        env_path = _project_root() / ".env"
        loaded = load_dotenv(dotenv_path=env_path, override=override_dotenv)

        # This log is safe (no secret values)
        logger.info("dotenv loaded=%s path=%s exists=%s", loaded, env_path, env_path.exists())

    missing: list[str] = []
    for k in EnvKey:
        val = os.getenv(k.value)  # type: ignore[attr-defined]
        if not val:
            missing.append(k.value) # type: ignore[attr-defined]
        else:
            _keydict[k] = val

    if missing:
        raise RuntimeError("Missing required environment variables: " + ", ".join(missing))

    _loaded = True

def get(key: EnvKey) -> str:
    if not _loaded:
        load_env()
    return _keydict[key]

if __name__ == "__main__":
    load_env()
    print("DATABASE_URL:", get(EnvKey.DATABASE_URL))
    