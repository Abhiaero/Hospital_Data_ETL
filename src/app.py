import streamlit as st
import duckdb
from pathlib import Path
import pandas as pd

# Path to DuckDB database
DUCKDB_PATH = Path("data/processed/hospitals.duckdb")

# Connection function
def get_connection():
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)

# Streamlit app
st.set_page_config(page_title="Hospital Price Transparency Explorer", layout="wide")

st.title("🏥 Hospital Price Transparency Explorer")
st.write("Run your own SQL queries on the unified hospital pricing database.")

# SQL query input
default_query = "SELECT * FROM hospital_charges LIMIT 150;"
query = st.text_area("✍️ Enter your SQL query:", value=default_query, height=150)

if st.button("Run Query"):
    try:
        with get_connection() as con:
            df = con.execute(query).fetchdf()

        if df.empty:
            st.warning("⚠️ Query returned no results.")
        else:
            st.success(f"✅ Query executed successfully. Showing {len(df)} rows.")
            st.dataframe(df, use_container_width=True)

            # Option to download results
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download results as CSV",
                data=csv,
                file_name="query_results.csv",
                mime="text/csv",
            )

    except Exception as e:
        st.error(f"❌ Error: {e}")

# Show available tables
st.subheader("📋 Available Tables in Database")
try:
    with get_connection() as con:
        tables = con.execute("SHOW TABLES;").fetchdf()
    st.table(tables)
except Exception as e:
    st.error(f"❌ Could not fetch table list: {e}")
