import duckdb
from pathlib import Path
import glob

DATA_DIR = Path("data")
PROCESSED_DIR = DATA_DIR / "processed"
DUCKDB_PATH = PROCESSED_DIR / "hospitals.duckdb"

# def create_unified_table():
#     # Connect to DuckDB
#     con = duckdb.connect(str(DUCKDB_PATH))
    
#     # Get all parquet files
#     parquet_files = glob.glob(str(PROCESSED_DIR / "*.parquet"))
#     if not parquet_files:
#         raise FileNotFoundError("No parquet files found in processed/")

#     # Create a unified table by UNION-ing all parquet files
#     con.execute("DROP TABLE IF EXISTS hospital_charges;")

#     files_str = ", ".join([f"'{f}'" for f in parquet_files])
#     con.execute(f"""
#         CREATE TABLE hospital_charges AS
#         SELECT * FROM parquet_scan([{files_str}]);
#     """)

#     # con.execute(f"""
#     #     CREATE TABLE hospital_charges AS 
#     #     SELECT * FROM parquet_scan({parquet_files});
#     # """)
    
#     # Verify count
#     count = con.execute("SELECT COUNT(*) FROM hospital_charges").fetchone()[0]
#     print(f"Unified table created with {count} rows.")
    
#     con.close()

def create_unified_table():
    con = duckdb.connect(str(DUCKDB_PATH))

    parquet_glob = str(PROCESSED_DIR / "*.parquet")

    con.execute("DROP TABLE IF EXISTS hospital_charges;")
    con.execute(f"""
        CREATE TABLE hospital_charges AS 
        SELECT * FROM parquet_scan('{parquet_glob}', union_by_name = true);
    """)

    count = con.execute("SELECT COUNT(*) FROM hospital_charges").fetchone()[0]
    print(f"Unified table created with {count} rows.")

    con.close()



if __name__ == "__main__":
    create_unified_table()
