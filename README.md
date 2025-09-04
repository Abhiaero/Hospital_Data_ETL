# DE_hospital_price_transparency

Automated ETL pipeline + DuckDB + Streamlit demo for Hospital Price Transparency files.

## Setup (VS Code + venv)

1. Open folder in VS Code:
   `D:\ML_Projects\DE_hospital_price_transparency`

2. Create venv (Windows PowerShell example):
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1   # or .venv\Scripts\activate.bat for cmd



# 🏥 Hospital Data ETL Pipeline

## 📌 Project Overview
This project implements an **ETL (Extract, Transform, Load) pipeline** for **Hospital Price Transparency Data**.  
The pipeline automates the process of:
- Extracting hospital standard charges data from raw `.csv` files.
- Transforming them into a **standardized schema**.
- Loading them into a **DuckDB database** for fast querying and analytics.
- Exposing the data through a **Streamlit app** for easy exploration.

The project is designed to handle large healthcare datasets efficiently, enabling insights into hospital pricing and cost transparency.

---

## ⚙️ Tech Stack
- **Python 3.11**
- **DuckDB** – for lightweight OLAP storage
- **Pandas** – for data manipulation
- **Streamlit** – for interactive UI
- **Git** – for version control
- **Virtual Environment** (`venv`) – for dependency management

---

## 📂 Project Structure
DE_hospital_price_transparency/
│
├── data/
│ ├── raw/ # Raw hospital CSV files
│ ├── processed/ # Cleaned parquet + DuckDB files
│
├── src/
│ ├── ingest.py # Extract raw CSV → processed parquet
│ ├── transform.py # Data cleaning & standardization
│ ├── load_duckdb.py # Load data into DuckDB
│ ├── app.py # Streamlit UI
│ ├── utils.py # Helper functions
│ ├── config.py # Configurations & constants
│
├── tests/
│ └── test_basic.py # Basic test cases
│
├── run_pipeline.bat # Windows batch script to run pipeline
├── requirements.txt # Project dependencies
├── README.md # Project documentation
├── .gitignore # Ignore unwanted files




-------------------------------

## 🚀 Setup Instructions

### 1️⃣ Clone the repository
git clone https://github.com/abhiaero/Hospital_Data_ETL.git

cd Hospital_Data_ETL


Create and activate virtual environment
--------------------------------------
python -m venv DE1venv
DE1venv\Scripts\activate   # On Windows

Install dependencies
--------------------
pip install -r requirements.txt

Run the ETL pipeline
--------------------
python src/ingest.py
python src/transform.py
python src/load_duckdb.py

Or simply run:

run_pipeline.bat


Launch the Streamlit app
-----------------------
streamlit run src/app.py


Features
---------
Standardized hospital pricing data across multiple sources

Query data interactively using DuckDB

Visualization of procedure costs and payer negotiated charges

Streamlit-based interface for hospital cost comparison


.gitignore
----------
The following files are ignored from version control:

Copy code
Notes.docx
explore_data.ipynb
requirements1.txt


Future Improvements
-------------------
Add support for more hospitals and states

Build APIs for external integration

Add dashboards with cost trends over time

CI/CD pipeline for automated testing & deployment