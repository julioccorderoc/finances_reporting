import json
import os
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "finances.db"
OUTPUT_DIR = PROJECT_ROOT / "output"
DATA_DIR = PROJECT_ROOT / "data"
CARACAS_TZ = ZoneInfo("America/Caracas")

# EPIC-007: default lookback window for Binance incremental sync (5 weeks of
# buffer for missed weekly cycles). CLI/callers may override with --since or
# --lookback-days.
BINANCE_DEFAULT_LOOKBACK_DAYS = 35

_env_loaded = False


def load_env() -> None:
    global _env_loaded
    if _env_loaded:
        return
    dotenv.load_dotenv(PROJECT_ROOT / ".env")
    _env_loaded = True


def binance_credentials() -> tuple[str, str]:
    load_env()
    api_key = os.environ.get("BINANCE_API_KEY", "")
    api_secret = os.environ.get("BINANCE_API_SECRET", "")
    if not api_key or not api_secret:
        raise RuntimeError(
            "BINANCE_API_KEY and BINANCE_API_SECRET must be set in the environment"
        )
    return api_key, api_secret


def google_service_account() -> dict[str, Any]:
    """Load Google Workspace service-account credentials for EPIC-014.

    Priority: ``GOOGLE_SERVICE_ACCOUNT_FILE`` (path to a JSON key file) wins
    over ``GOOGLE_SERVICE_ACCOUNT_JSON`` (inline JSON). Returns the parsed
    credentials dict suitable for ``google.oauth2.service_account.Credentials``.
    """
    load_env()
    key_file = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
    if key_file:
        path = Path(key_file)
        if not path.is_file():
            raise RuntimeError(
                f"GOOGLE_SERVICE_ACCOUNT_FILE does not exist: {key_file}"
            )
        return json.loads(path.read_text())
    key_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if key_json:
        return json.loads(key_json)
    raise RuntimeError(
        "GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_SERVICE_ACCOUNT_JSON must be set"
    )
