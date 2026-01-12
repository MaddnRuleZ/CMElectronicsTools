#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from datetime import datetime
from itertools import islice
from typing import Any, Dict, List, Optional, Tuple
from dotenv import load_dotenv
import pandas as pd
import pyodbc
import pymysql


TRACE_SQL = """
WITH ranked AS (
    SELECT
        tp.Barcode AS Barcode,
        o.Name     AS Losname,
        b.Name     AS Leiterplatte,
        ROW_NUMBER() OVER (
            PARTITION BY tp.Barcode
            ORDER BY td.EndDate DESC, td.BeginDate DESC, td.Id DESC
        ) AS rn
    FROM [vTracePanel5] tp
    INNER JOIN [vTraceData5] td ON td.Id = tp.TraceDataId
    INNER JOIN [vTraceJob5]  tj ON tj.TraceDataId = td.Id
    INNER JOIN [vJob5]        j ON j.Id = tj.JobId
    LEFT  JOIN [vOrder5]      o ON o.Id = j.OrderId
    LEFT  JOIN [vBoard5]      b ON b.Id = j.BoardId
    WHERE tp.Barcode IN ({placeholders})
)
SELECT
    Barcode,
    Losname,
    Leiterplatte
FROM ranked
WHERE rn = 1;
""".strip()

# =========================
# HARD-CODED EXAMPLES
# =========================
FILE = "/root/Tab01/files/TAB01__Lpl-Seriennummern.xlsm"
SHEET = "Leiterplatten"
START_ROW = 7

# These are the EXCEL column indices in your raw dataframe (0-based)
BOARD_TOP_IDX = 1
BOARD_BOTTOM_IDX = 2

# =========================
# OPTIONAL HARD-CODED DATE FILTER (UPLOAD ONLY "YOUNGER" ROWS)
# =========================
# Set to None to disable filtering (uploads everything)
#UPLOAD_ONLY_NEWER_THAN_STR: Optional[str] = None
# Example values:
UPLOAD_ONLY_NEWER_THAN_STR = "06.12.2025"

DATE_FILTER_FIELD = "board_erfasst_am"          # which payload field to filter on
INCLUDE_ROWS_WITHOUT_DATE_WHEN_FILTERING = False  # if True, keeps rows where the date is missing

# ---------- Excel -> DB column mapping by POSITION ----------
EXCEL_TO_DB_BY_INDEX: List[Tuple[int, str]] = [
    (0,  "board_erfasst_am"),
    (1,  "board_top"),
    (2,  "board_bottom"),
    (3,  "board_ok"),
    (4,  "board_fa_nummer"),
    (5,  "board_artikel_nummer"),
    (6,  "board_erfasst_durch"),
    (7,  "smd_be_versatz"),
    (8,  "smd_hoehenversatz"),
    (9,  "smd_steht_hoch_grabstein"),
    (10, "smd_ocr_ocv_schlechtes_bauteil"),
    (11, "smd_polaritaet"),
    (12, "smd_upside_down_auf_dem_kopf"),
    (13, "smd_solder_fillet_loetstelle"),
    (14, "smd_kurzschluss"),
    (15, "smd_pad_overhang_pin_versatz"),
    (16, "smd_pin_coplanarity_zu_hoch"),
    (17, "smd_absence_bestueckt_statt_frei"),
    (18, "smd_bauteil_fehlt"),
    (19, "smd_fehlermaterial_bauteil"),
    (20, "smd_bauteil_defekt_gebrochen"),
    (22, "smdsel_tht_nicht_geloetet"),
    (23, "smdsel_tht_nicht_anliegend_durch_sl"),
    (24, "smdsel_loetfahnen"),
    (25, "smdsel_be_versatz"),
    (26, "smdsel_hoehenversatz"),
    (27, "smdsel_steht_hoch_grabsein"),
    (28, "smdsel_ocr_ocv_schlechtes_bauteil"),
    (29, "smdsel_polaritaet"),
    (30, "smdsel_upside_down_auf_dem_kopf"),
    (31, "smdsel_solder_fillet_loetstelle"),
    (32, "smdsel_kurzschluss"),
    (33, "smdsel_pad_overhang_pin_versatz"),
    (34, "smdsel_pin_coplanarity_zu_hoch"),
    (35, "smdsel_absence_bestueckt_statt_frei"),
    (36, "smdsel_bauteil_fehlt"),
    (37, "smdsel_fehlermaterial_bauteil"),
    (38, "smdsel_bauteil_defekt_gebrochen"),
    (40, "end_erfasst_am"),
    (41, "end_bestueckungsfehler_bedrahtet"),
    (42, "end_bestueckungsfehler_smd"),
    (43, "end_loetfehler_smd"),
    (44, "end_loetfehler_selektivloeten"),
    (45, "end_loetfehler_hand_bedrahtet"),
    (46, "end_platinenfehler"),
    (47, "end_bauteile"),
    (48, "end_mangelhafte_lagerung_verpackung"),
    (49, "end_fehler_bei_montage"),
    (50, "end_sonstige"),
    (51, "end_fehlerbeschreibung"),
    (53, "notes_smd"),
    (54, "notes_aoi"),
    (55, "notes_tht"),
    (56, "notes_montage"),
    (57, "notes_reparaturen"),
]

def clear_target_table(conn, table: str = "circuit_boards") -> None:
    """
    Wipes the target table before a fresh upload.
    Tries TRUNCATE first (fast + resets AUTO_INCREMENT).
    Falls back to DELETE if TRUNCATE is blocked (e.g., FK constraints).
    """
    with conn.cursor() as cur:
        try:
            cur.execute(f"TRUNCATE TABLE `{table}`;")
        except Exception:
            conn.rollback()
            cur.execute(f"DELETE FROM `{table}`;")
            try:
                cur.execute(f"ALTER TABLE `{table}` AUTO_INCREMENT = 1;")
            except Exception:
                pass
    conn.commit()

# Datetime fields we parse
DATETIME_FIELDS = {"board_erfasst_am", "end_erfasst_am"}

def read_excel_as_dataframe(path: str, sheet: Optional[str], start_row: int) -> pd.DataFrame:
    df = pd.read_excel(
        path,
        sheet_name=sheet if sheet else 0,
        header=None,
        engine="openpyxl",
        dtype=object  # keep native types for robust parsing
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

def _coerce_excel_serial(val) -> Optional[datetime]:
    try:
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

    # MM/DD/YYYY first (e.g., 9/5/2025)
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
    try:
        return pd.to_datetime(s, dayfirst=False, errors="raise").to_pydatetime()
    except Exception:
        pass
    try:
        return pd.to_datetime(s, dayfirst=True, errors="raise").to_pydatetime()
    except Exception:
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

def row_to_payload(row: pd.Series) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for idx, db_col in EXCEL_TO_DB_BY_INDEX:
        if idx >= len(row):
            continue
        payload[db_col] = clean_value(db_col, row.iloc[idx])

    if payload.get("board_erfasst_durch") is None:
        payload["board_erfasst_durch"] = "IMPORT"

    if payload.get("board_top") is None:
        payload["board_top"] = ""
    if payload.get("board_bottom") is None:
        payload["board_bottom"] = ""

    if payload.get("board_erfasst_am") is not None:
        payload["board_recorded_on"] = payload["board_erfasst_am"]

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

def build_upsert_sql(columns: List[str]) -> str:
    table = "circuit_boards"
    cols_sql = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    updates = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in columns if c not in ("created_at",))
    sql = f"""
        INSERT INTO `{table}` ({cols_sql})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {updates}
    """.strip()
    return sql

def apply_date_filter(payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not UPLOAD_ONLY_NEWER_THAN_STR:
        return payloads

    cutoff = coerce_datetime(UPLOAD_ONLY_NEWER_THAN_STR)
    if cutoff is None:
        print(
            f"Invalid UPLOAD_ONLY_NEWER_THAN_STR={UPLOAD_ONLY_NEWER_THAN_STR!r}. "
            f"Use formats like '2025-01-01' or '01.01.2025'.",
            file=sys.stderr,
        )
        sys.exit(4)

    before = len(payloads)
    kept: List[Dict[str, Any]] = []

    for p in payloads:
        dt = p.get(DATE_FILTER_FIELD)

        if dt is None:
            if INCLUDE_ROWS_WITHOUT_DATE_WHEN_FILTERING:
                kept.append(p)
            continue

        if not isinstance(dt, datetime):
            dt = coerce_datetime(dt)

        if dt is None:
            if INCLUDE_ROWS_WITHOUT_DATE_WHEN_FILTERING:
                kept.append(p)
            continue

        # "younger than cutoff" => recorded on/after cutoff
        if dt >= cutoff:
            kept.append(p)

    print(
        f"Date filter enabled: keeping {len(kept)}/{before} rows where "
        f"{DATE_FILTER_FIELD} >= {cutoff.isoformat(sep=' ', timespec='seconds')}"
    )
    return kept

def main():
    print("Loading Env")
    load_dotenv()
    print("Loaded env, generating Payloads")
    df = read_excel_as_dataframe(FILE, SHEET, START_ROW)

    payloads: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        payloads.append(row_to_payload(row))

    if not payloads:
        print("No rows found after parsing.")
        return

    # Apply optional hard-coded date filter
    payloads = apply_date_filter(payloads)

    if not payloads:
        print("No rows left after applying the date filter.")
        return

    all_columns = sorted(set().union(*[set(p.keys()) for p in payloads]))
    values = [[p.get(c) for c in all_columns] for p in payloads]

    print(f"Prepared {len(values)} rows with {len(all_columns)} columns.")

    conn = get_connection()
    try:
        # Only wipe the table on a full upload (no date filter)
        if UPLOAD_ONLY_NEWER_THAN_STR is None:
            clear_target_table(conn, "circuit_boards")
            print("Cleared `circuit_boards` before upload (full refresh).")
        else:
            print("Skipped clearing `circuit_boards` (date filter enabled).")

        sql = build_upsert_sql(all_columns)
        with conn.cursor() as cur:
            cur.executemany(sql, values)
        conn.commit()
        print(f"Upserted {len(values)} rows into `circuit_boards`.")
    except Exception as e:
        conn.rollback()
        print(f"Error during insert: {e}", file=sys.stderr)
        sys.exit(3)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
