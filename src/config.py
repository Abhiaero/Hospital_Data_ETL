# src/config.py
from pathlib import Path

# Base directory -- change if you put the repo somewhere else
BASE_DIR = Path(r"D:\ML_Projects\DE_hospital_price_transparency")

RAW_DATA_DIR = BASE_DIR / "data"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# DuckDB file
DUCKDB_PATH = PROCESSED_DIR / "mrf.duckdb"

# For demo runs, limit nrows to 100; set to None or a larger value for full run
DEMO_NROWS = 100
