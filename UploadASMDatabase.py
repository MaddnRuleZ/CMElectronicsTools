
#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
import pymysql

# =========================
# SETTINGS
# =========================
FILE = "/home/maddin/Arbeit/2025/CM_Elektronik/Data.xlsm"
SHEET = "Rohdaten_SIPLACE"
START_ROW = 2
DRY_RUN = False

TARGET_TABLE = "asm_logs"

# Robust column mapping:
# Some exports have a blank column after "Losname" (double tab). We try the
# primary index first, then fall back to an alternative index if present.
EXCEL_POSITIONS: Dict[str, List[int]] = {
    "barcode_leiterplatte":    [0],
    "barcode_einzelschaltung": [1],
    "linienname":              [2],
    "losname":                 [3],
    # Handle possible blank col at index 4
    "leiterplatte":            [5, 4],
    "ruestungsname":           [6, 5],
    "fehlertext":              [7, 6],
    "startdatum":              [8, 7],
    "enddatum":                [9, 8],
}

# Datetime fields to parse
DATETIME_FIELDS = {"startdatum", "enddatum"}

def read_excel_as_dataframe(path: str, sheet: Optional[str], start_row: int) -> pd.DataFrame:
    df = pd.read_excel(
        path,
        sheet_name=sheet if sheet else 0,
        header=None,
        engine="openpyxl",
        dtype=object
    )
    df = df.iloc[start_row - 1:].reset_index(drop=True)

    def _stripper(x):
        if pd.isna(x):
            return x
        if isinstance(x, str):
            return x.strip()
        return x

    df = df.apply(lambda s: s.map(_stripper))
    return df

def _chunked(iterable, size: int):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk


def fetch_trace_info_for_barcodes(conn: pyodbc.Connection, barcodes: List[str], chunk_size: int = 1000) -> Dict[str, Tuple[Optional[str], Optional[str]]]:
    """
    Returns dict: barcode -> (Losname, Leiterplatte)
    """
    out: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    if not barcodes:
        return out

    # de-dup, keep order stable
    seen = set()
    uniq = []
    for b in barcodes:
        if b not in seen:
            uniq.append(b)
            seen.add(b)

    cur = conn.cursor()
    for chunk in _chunked(uniq, chunk_size):
        placeholders = ",".join(["?"] * len(chunk))
        sql = TRACE_SQL.format(placeholders=placeholders)
        cur.execute(sql, chunk)
        for barcode, losname, leiterplatte in cur.fetchall():
            # normalize to stripped strings or None
            los = losname.strip() if isinstance(losname, str) else losname
            lei = leiterplatte.strip() if isinstance(leiterplatte, str) else leiterplatte
            out[str(barcode).strip()] = (los, lei)
    return out

def _coerce_excel_serial(val) -> Optional[datetime]:
    try:
        # Excel serials (note the 1900 leap-year bug offset handled by origin)
        if isinstance(val, (int, float)) and not isinstance(val, bool) and val > 59:
            return pd.to_datetime(val, unit="D", origin="1899-12-30").to_pydatetime()
    except Exception:
        pass
    return None

def coerce_datetime(val: Any) -> Optional[datetime]:
    if val is None or (isinstance(val, str) and val.strip() == ""):
        return None
    if isinstance(val, (pd.Timestamp, datetime)):
        try:
            return pd.to_datetime(val).to_pydatetime()
        except Exception:
            return None

    ser = _coerce_excel_serial(val)
    if ser:
        return ser

    s = str(val).strip()

    # US-style
    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    # German day-first
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    # ISO/other
    for dayfirst in (False, True):
        try:
            return pd.to_datetime(s, dayfirst=dayfirst, errors="raise").to_pydatetime()
        except Exception:
            pass
    return None

def clean_value(col: str, val: Any) -> Any:
    if pd.isna(val):
        return None
    if isinstance(val, str):
        s = val.strip()
        if s == "":
            return None
        if col in DATETIME_FIELDS:
            return coerce_datetime(s)
        return s
    if col in DATETIME_FIELDS:
        return coerce_datetime(val)
    return val

def pick_from_row(row: pd.Series, indices: List[int]) -> Any:
    for idx in indices:
        if idx < len(row):
            v = row.iloc[idx]
            if not (isinstance(v, float) and pd.isna(v)) and v is not None and str(v).strip() != "":
                return v
    # If all candidates empty, still return last checked (could be None)
    last_idx = indices[0]
    if last_idx < len(row):
        return row.iloc[last_idx]
    return None

def row_to_payload(row: pd.Series) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for col, idx_list in EXCEL_POSITIONS.items():
        val = pick_from_row(row, idx_list)
        payload[col] = clean_value(col, val)
    return payload

def get_connection():
    load_dotenv()
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "3306"))
    database = os.getenv("DB_NAME", "manufacturing")
    user = os.getenv("DB_USER", "user")
    password = os.getenv("DB_PASSWORD", os.getenv("DB_PASS", ""))

    ssl_ca = os.getenv("DB_SSL_CA")
    ssl_params = {"ca": os.path.expanduser(ssl_ca)} if ssl_ca else None

    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            autocommit=False,
            ssl=ssl_params,
        )
        return conn
    except Exception as e:
        print(f"MySQL connection error: {e}", file=sys.stderr)
        sys.exit(2)

def build_insert_sql(columns: List[str]) -> str:
    cols_sql = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO `{TARGET_TABLE}` ({cols_sql}) VALUES ({placeholders})"
    return sql

def main():
    df = read_excel_as_dataframe(FILE, SHEET, START_ROW)

    payloads: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        payload = row_to_payload(row)
        # Skip completely empty lines (no meaningful values)
        if any(v is not None and str(v).strip() != "" for v in payload.values()):
            payloads.append(payload)

    if not payloads:
        print("No rows found after parsing.")
        return

    # Ensure consistent column order
    all_columns = [
        "barcode_leiterplatte",
        "barcode_einzelschaltung",
        "linienname",
        "losname",
        "leiterplatte",
        "ruestungsname",
        "fehlertext",
        "startdatum",
        "enddatum",
    ]

    values = [[p.get(c) for c in all_columns] for p in payloads]

    print(f"Prepared {len(values)} rows with {len(all_columns)} columns.")
    if DRY_RUN:
        preview = dict(zip(all_columns, values[0]))
        print("Dry-run preview (first row):")
        for k, v in preview.items():
            print(f"  {k}: {v}")
        return

    conn = get_connection()
    try:
        sql = build_insert_sql(all_columns)
        with conn.cursor() as cur:
            cur.executemany(sql, values)
        conn.commit()
        print(f"Inserted {len(values)} rows into `{TARGET_TABLE}`.")
    except Exception as e:
        conn.rollback()
        print(f"Error during insert: {e}", file=sys.stderr)
        sys.exit(3)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
