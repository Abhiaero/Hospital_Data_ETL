@echo off
python -m src.ingest
python -m src.load_duckdb
streamlit run src/app.py