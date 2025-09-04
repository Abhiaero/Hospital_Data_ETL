import pandas as pd
import json
import os
from pathlib import Path
from tqdm import tqdm

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

def read_csv_with_metadata(path: Path, nrows: int = 100) -> pd.DataFrame:
    """
    Reads a hospital CSV file while handling metadata rows on top.
    """
    with open(path, "r", encoding="utf-8") as f:
        first_lines = [next(f) for _ in range(5)]  # peek first few rows
    
    # Find the row that looks like the header (starts with 'description' usually)
    header_line = None
    for i, line in enumerate(first_lines):
        if "description" in line.lower():  # heuristic
            header_line = i
            break

    if header_line is None:
        raise ValueError(f"Could not find header in {path}")
    
    # Now re-read CSV with correct header
    df = pd.read_csv(path, skiprows=header_line, nrows=nrows)
    
    # Extract hospital name from metadata (line 2 usually)
    hospital_name = first_lines[1].split(",")[0].replace('"', "").strip()
    df["hospital_name"] = hospital_name
    
    return df


def read_json(path: Path, nrows: int = 100) -> pd.DataFrame:
    """
    Reads a JSON file and flattens it into a dataframe.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Flatten JSON if it's nested
    if isinstance(data, dict):
        df = pd.json_normalize(data)
    elif isinstance(data, list):
        df = pd.json_normalize(data)
    else:
        raise ValueError(f"Unsupported JSON format in {path}")
    
    df = df.head(nrows)
    df["hospital_name"] = path.stem.split("_")[1]  # crude hospital name
    return df


def read_excel(path: Path, nrows: int = 100) -> pd.DataFrame:
    """
    Reads Excel hospital file.
    """
    df = pd.read_excel(path, nrows=nrows)
    df["hospital_name"] = path.stem.split("_")[1]
    return df


def process_files():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    parquet_files = []

    for file in tqdm(list(DATA_DIR.glob("*"))):
        try:
            if file.suffix == ".csv":
                df = read_csv_with_metadata(file)
            elif file.suffix == ".json":
                df = read_json(file)
            elif file.suffix in [".xls", ".xlsx"]:
                df = read_excel(file)
            else:
                print(f"Skipping unsupported file: {file}")
                continue
            
            # Standardize some column names
            df = df.rename(columns=lambda x: x.strip().lower().replace("|", "_").replace(" ", "_"))
            
            out_path = PROCESSED_DIR / f"{file.stem}.parquet"
            df.to_parquet(out_path, index=False)
            parquet_files.append(str(out_path))
        
        except Exception as e:
            print(f"Failed processing {file}: {e}")
    
    return parquet_files


if __name__ == "__main__":
    parquet_files = process_files()
    print(f"Done. Parquet files: {parquet_files}")
