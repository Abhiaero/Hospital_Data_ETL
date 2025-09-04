# src/tests/test_basic.py
"""
Basic tests you can run locally to check pipeline outputs.
Run:
    python -m pytest -q
(install pytest if you want to run tests)
"""
import duckdb
from src.config import DUCKDB_PATH
def test_duckdb_exists():
    con = duckdb.connect(database=str(DUCKDB_PATH), read_only=True)
    res = con.execute("SELECT count(*) FROM unified").fetchone()
    assert res is not None
    con.close()
