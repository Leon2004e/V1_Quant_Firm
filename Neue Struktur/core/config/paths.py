from __future__ import annotations

from pathlib import Path


def find_project_root(start: Path | None = None) -> Path:
    current = (start or Path(__file__)).resolve()

    for path in [current.parent, *current.parents]:
        has_app = (path / "app").exists()
        has_core = (path / "core").exists()
        has_data = (path / "data").exists()
        has_engine = (path / "engine").exists()

        if has_app and has_core and has_data and has_engine:
            return path

    raise RuntimeError(
        "Project root not found. Expected a directory containing "
        "'app', 'core', 'data', and 'engine'."
    )


PROJECT_ROOT = find_project_root()

APP_DIR = PROJECT_ROOT / "app"
CORE_DIR = PROJECT_ROOT / "core"
DATA_DIR = PROJECT_ROOT / "data"
ENGINE_DIR = PROJECT_ROOT / "engine"
INTERFACE_DIR = PROJECT_ROOT / "interface"
ORCHESTRATION_DIR = PROJECT_ROOT / "orchestration"
TESTS_DIR = PROJECT_ROOT / "tests"

RAW_DIR = DATA_DIR / "raw"
STORAGE_DIR = DATA_DIR / "storage"
PIPELINES_DIR = DATA_DIR / "pipelines"

RAW_TRADES_DIR = RAW_DIR / "trades"
RAW_STRATEGY_DIR = RAW_DIR / "strategy"
RAW_MARKET_DIR = RAW_DIR / "market"
RAW_SPREADS_DIR = RAW_DIR / "spreads"
RAW_ECONOMIC_DIR = RAW_DIR / "economic"


def ensure_directories_exist() -> None:
    directories = [
        APP_DIR,
        CORE_DIR,
        DATA_DIR,
        ENGINE_DIR,
        INTERFACE_DIR,
        ORCHESTRATION_DIR,
        TESTS_DIR,
        RAW_DIR,
        STORAGE_DIR,
        PIPELINES_DIR,
        RAW_TRADES_DIR,
        RAW_STRATEGY_DIR,
        RAW_MARKET_DIR,
        RAW_SPREADS_DIR,
        RAW_ECONOMIC_DIR,
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


def resolve_from_root(*parts: str) -> Path:
    return PROJECT_ROOT.joinpath(*parts)