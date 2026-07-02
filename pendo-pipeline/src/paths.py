# src/paths.py

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = PROJECT_ROOT / "data"
CONFIG_DIR = DATA_DIR / "config"

DB_DIR = PROJECT_ROOT / "db"
DUCKDB_PATH = DB_DIR / "pendo.duckdb"

APP_REGISTRY_CSV = CONFIG_DIR / "xm_pendo_app_registry.csv"
APP_REGISTRY_PARQUET = CONFIG_DIR / "xm_pendo_app_registry.parquet"