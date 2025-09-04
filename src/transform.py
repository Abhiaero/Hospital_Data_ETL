# src/transform.py
from pathlib import Path
import pandas as pd
import numpy as np
import re
from typing import Dict

CANONICAL_COLUMNS = [
    # metadata
    "hospital_name", "last_updated_on", "version", "hospital_location", "hospital_address", "license_number",
    # main data
    "description", "code_1", "code_1_type", "code_2", "code_2_type", "code_3", "code_3_type",
    "billing_class", "setting", "modifiers", "drug_unit_of_measurement", "drug_type_of_measurement",
    "standard_charge_gross", "standard_charge_discounted_cash", "payer_name", "plan_name",
    "standard_charge_negotiated_dollar", "standard_charge_negotiated_percentage",
    "standard_charge_negotiated_algorithm", "estimated_amount", "standard_charge_methodology",
    "standard_charge_min", "standard_charge_max", "additional_generic_notes",
    # bookkeeping
    "source_file", "state"
]

# mapping of common alt names to canonical columns (keys normalized)
COMMON_REMAP = {
    "code": "code_1",
    "code_1": "code_1",
    "code_1_type": "code_1_type",
    "standard_charge_gross": "standard_charge_gross",
    "standard_charge|gross": "standard_charge_gross",
    "standard_charge|discounted_cash": "standard_charge_discounted_cash",
    "standard_charge|negotiated_dollar": "standard_charge_negotiated_dollar",
    "standard_charge|negotiated_percentage": "standard_charge_negotiated_percentage",
    "standard_charge|methodology": "standard_charge_methodology",
    "standard_charge|min": "standard_charge_min",
    "standard_charge|max": "standard_charge_max",
    "standard_charge|negotiated_algorithm": "standard_charge_negotiated_algorithm",
    "standard_charge|estimated_amount": "estimated_amount",
}

def map_columns_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns using COMMON_REMAP and then ensure canonical columns exist."""
    df = df.copy()
    rename_map = {}
    for c in df.columns:
        if c in COMMON_REMAP:
            rename_map[c] = COMMON_REMAP[c]
        # also handle raw names containing tokens
        lc = c.lower()
        if "standard_charge" in lc and ("gross" in lc or "standard_charge_gross" in lc):
            rename_map[c] = "standard_charge_gross"
        if "discounted" in lc and "cash" in lc:
            rename_map[c] = "standard_charge_discounted_cash"
        if lc.startswith("code") and "_" in lc:
            # code_1, code_2 etc usually fine
            pass
    df = df.rename(columns=rename_map)
    # ensure canonical columns exist
    for c in CANONICAL_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    # keep only canonical columns and reorder
    df = df[[c for c in CANONICAL_COLUMNS]]
    return df

def clean_money_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert money-like string columns to numeric floats. Operates in-place (returns df)."""
    df = df.copy()
    money_tokens = [c for c in df.columns if any(tok in c for tok in ["charge", "amount", "estimated", "min", "max"])]
    for col in money_tokens:
        # try to coerce: strip '$', commas, and non numeric; treat 'other' as NaN
        def _to_float(x):
            if pd.isna(x):
                return pd.NA
            s = str(x).strip()
            if s.lower() in {"", "none", "nan", "other", "other nonnumeric"}:
                return pd.NA
            # remove currency and footnotes
            s = re.sub(r"[^\d\.\-eE]", "", s)
            try:
                return float(s) if s != "" else pd.NA
            except Exception:
                return pd.NA
        df[col] = df[col].apply(_to_float)
    return df

def extract_state_from_address(addr: str) -> str:
    """Heuristic extraction of 2-letter US state code from address string, if present."""
    if not addr or pd.isna(addr):
        return None
    # look for ', <City>, <ST> ' or ', <ST> ' patterns
    m = re.search(r",\s*([A-Za-z .]+),\s*([A-Z]{2})\b", str(addr))
    if m:
        return m.group(2)
    # fallback: look for last two-letter uppercase token
    toks = re.findall(r"\b([A-Z]{2})\b", str(addr))
    if toks:
        return toks[-1]
    return None

def unify_record(df: pd.DataFrame, source_file: str, metadata: Dict) -> pd.DataFrame:
    """
    Take df from utils.read_generic and metadata extracted, map to canonical schema,
    fill hospital metadata columns, clean numeric columns, add source_file and state.
    """
    df = df.copy()
    # fill hospital-level metadata columns if present in df or metadata
    # some metadata fields (hospital_name etc.) may be in 'metadata' dict
    # ensure lowercase normalized metadata keys
    meta_norm = {k.lower(): v for k, v in (metadata or {}).items()}
    # if hospital_name exists as a column and its header is repeated across all rows we can use it;
    # otherwise use metadata
    if "hospital_name" in df.columns and df["hospital_name"].notna().any():
        # keep column as-is
        pass
    else:
        if meta_norm.get("hospital_name"):
            df["hospital_name"] = meta_norm.get("hospital_name")

    # similarly for other fields
    for field in ["last_updated_on", "version", "hospital_location", "hospital_address", "license_number"]:
        if field not in df.columns or df[field].isna().all():
            if meta_norm.get(field):
                df[field] = meta_norm.get(field)

    # normalize column names & remap
    df = map_columns_to_canonical(df)

    # add source_file column
    df["source_file"] = source_file

    # extract state heuristically from address
    df["state"] = df["hospital_address"].apply(extract_state_from_address)

    # Clean money-like columns
    df = clean_money_columns(df)

    return df
