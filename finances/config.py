from pathlib import Path
from zoneinfo import ZoneInfo

import dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "finances.db"
OUTPUT_DIR = PROJECT_ROOT / "output"
DATA_DIR = PROJECT_ROOT / "data"
CARACAS_TZ = ZoneInfo("America/Caracas")

_env_loaded = False


def load_env() -> None:
    global _env_loaded
    if _env_loaded:
        return
    dotenv.load_dotenv(PROJECT_ROOT / ".env")
    _env_loaded = True
