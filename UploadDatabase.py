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
        except Exception as e:
            # TRUNCATE can fail if there are foreign key references
            conn.rollback()
            cur.execute(f"DELETE FROM `{table}`;")
            # optional: reset autoincrement after DELETE
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
def get_trace_connection() -> pyodbc.Connection:
    """
    Expects .env like:
      TRACE_HOST=...
      TRACE_DB=...
      TRACE_USER=...
      TRACE_PASSWORD=...
      TRACE_DRIVER=ODBC Driver 18 for SQL Server
      TRACE_ENCRYPT=yes
      TRACE_TRUST_CERT=yes
    """
    host = os.getenv("TRACE_HOST")
    user = os.getenv("TRACE_USER")
    password = os.getenv("TRACE_PASSWORD")

    driver = os.getenv("TRACE_DRIVER", "ODBC Driver 18 for SQL Server")
    encrypt = os.getenv("TRACE_ENCRYPT", "yes")
    trust_cert = os.getenv("TRACE_TRUST_CERT", "yes")

    # If you use integrated auth, adjust accordingly.
    if not user or not password:
        raise RuntimeError("TRACE_USER / TRACE_PASSWORD missing in env (.env).")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={host};"
        f"UID={user};"
        f"PWD={password};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_cert};"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def normalize_board_number(val: Any) -> Optional[str]:
    """
    Normalize a board number into a comparable string.
    Empty/NaN -> None (excluded from uniqueness checks).
    """
    if val is None or pd.isna(val):
        return None

    # strings
    if isinstance(val, str):
        s = val.strip()
        return None if s == "" else s

    # ints/bools
    if isinstance(val, bool):
        # unlikely for serials; treat as string anyway
        return str(val)

    if isinstance(val, int):
        return str(val)

    # floats from excel often show "12345.0"
    if isinstance(val, float):
        if val.is_integer():
            return str(int(val))
        return str(val).strip()

    # fallback
    s = str(val).strip()
    return None if s == "" else s

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

def build_offending_numbers_df(
    df: pd.DataFrame,
    top_idx: int,
    bottom_idx: int,
    start_row_excel_1based: int
) -> pd.DataFrame:
    """
    Returns a value-level dataframe of offending numbers:
    - duplicates in board_top
    - duplicates in board_bottom
    - any number appearing in both columns
    - any number appearing more than once across both columns total
    """
    # Ensure indices exist
    max_idx = max(top_idx, bottom_idx)
    if df.shape[1] <= max_idx:
        raise ValueError(
            f"Expected at least {max_idx + 1} columns, but dataframe has only {df.shape[1]} columns."
        )

    top_raw = df.iloc[:, top_idx]
    bottom_raw = df.iloc[:, bottom_idx]

    top_norm = top_raw.map(normalize_board_number)
    bottom_norm = bottom_raw.map(normalize_board_number)

    # counts
    top_counts = top_norm.dropna().value_counts()
    bottom_counts = bottom_norm.dropna().value_counts()
    combined = pd.concat([top_norm.dropna(), bottom_norm.dropna()], ignore_index=True)
    combined_counts = combined.value_counts()

    # "bad" means appears more than once across the union (covers all violations)
    bad_numbers = combined_counts[combined_counts > 1].index.tolist()
    if not bad_numbers:
        return pd.DataFrame()

    bad_set = set(bad_numbers)
    intersection_set = set(top_counts.index).intersection(set(bottom_counts.index))

    # rows (use Excel row numbers for easier locating)
    # df index 0 corresponds to Excel row START_ROW
    def excel_row(idx0: int) -> int:
        return start_row_excel_1based + idx0

    top_rows_map = (
        pd.DataFrame({"n": top_norm, "row0": df.index})
        .dropna()
        .query("n in @bad_set")
        .groupby("n")["row0"]
        .apply(lambda s: ",".join(str(excel_row(int(x))) for x in sorted(s.tolist())))
    )
    bottom_rows_map = (
        pd.DataFrame({"n": bottom_norm, "row0": df.index})
        .dropna()
        .query("n in @bad_set")
        .groupby("n")["row0"]
        .apply(lambda s: ",".join(str(excel_row(int(x))) for x in sorted(s.tolist())))
    )

    records: List[Dict[str, Any]] = []
    for n in bad_numbers:
        tc = int(top_counts.get(n, 0))
        bc = int(bottom_counts.get(n, 0))
        total = int(combined_counts.get(n, 0))
        appears_in_both = n in intersection_set

        reasons: List[str] = []
        if tc > 1:
            reasons.append("duplicate_in_board_top")
        if bc > 1:
            reasons.append("duplicate_in_board_bottom")
        if appears_in_both:
            reasons.append("appears_in_both_columns")
        # (total > 1 is always true here; the above are the specific causes)

        records.append(
            {
                "number": n,
                "top_count": tc,
                "bottom_count": bc,
                "total_count": total,
                "appears_in_both": appears_in_both,
                "reason": ";".join(reasons) if reasons else "duplicate_across_union",
                "top_excel_rows": top_rows_map.get(n, ""),
                "bottom_excel_rows": bottom_rows_map.get(n, ""),
            }
        )

    out = pd.DataFrame.from_records(records)
    out = out.sort_values(by=["total_count", "number"], ascending=[False, True]).reset_index(drop=True)
    return out

def write_offending_numbers_csv(offending: pd.DataFrame, excel_path: str) -> str:
    base_dir = os.path.dirname(os.path.abspath(excel_path)) or "."
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(base_dir, f"board_number_violations_{ts}.csv")
    offending.to_csv(out_path, index=False, encoding="utf-8")
    return out_path

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

def row_to_payload(row: pd.Series, trace_lookup: Dict[str, Tuple[Optional[str], Optional[str]]]) -> Dict[str, Any]:
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

    # ✅ verify + fix FA / board number using traceability source
    payload = check_row_fa_and_board_number(payload, trace_lookup)

    return payload

def _norm_s(val: Any) -> str:
    if val is None:
        return ""
    s = str(val).strip()
    return s


def check_row_fa_and_board_number(
    payload: Dict[str, Any],
    trace_lookup: Dict[str, Tuple[Optional[str], Optional[str]]],
) -> Dict[str, Any]:
    """
    Uses board_top/board_bottom as barcode candidates.
    If trace lookup contains info, it overwrites:
      - board_fa_nummer  <- Losname
      - board_artikel_nummer <- Leiterplatte

    If both top & bottom exist and both resolve but disagree, top wins.
    """
    top = _norm_s(payload.get("board_top"))
    bottom = _norm_s(payload.get("board_bottom"))

    top_info = trace_lookup.get(top) if top else None
    bottom_info = trace_lookup.get(bottom) if bottom else None

    chosen = None
    if top_info and bottom_info:
        # If they disagree, prefer top (and you may want to log this case)
        if top_info != bottom_info:
            chosen = top_info
        else:
            chosen = top_info
    elif top_info:
        chosen = top_info
    elif bottom_info:
        chosen = bottom_info

    if not chosen:
        return payload

    losname, leiterplatte = chosen

    if losname is not None and _norm_s(losname) != _norm_s(payload.get("board_fa_nummer")):
        payload["board_fa_nummer"] = _norm_s(losname)

    if leiterplatte is not None and _norm_s(leiterplatte) != _norm_s(payload.get("board_artikel_nummer")):
        payload["board_artikel_nummer"] = _norm_s(leiterplatte)

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

def main():
    df = read_excel_as_dataframe(FILE, SHEET, START_ROW)

    # =========================
    # PRE-CHECK FOR DUPLICATES / COLLISIONS
    # =========================
    offending = build_offending_numbers_df(
        df=df,
        top_idx=BOARD_TOP_IDX,
        bottom_idx=BOARD_BOTTOM_IDX,
        start_row_excel_1based=START_ROW,
    )

    if not offending.empty:
        out_path = write_offending_numbers_csv(offending, FILE)

        print("❌ Duplicate / collision check failed for board_top / board_bottom.")
        print(f"Found {len(offending)} offending numbers.")
        print(f"Wrote details to: {out_path}")
        # show a small preview in stdout
        preview_cols = ["number", "top_count", "bottom_count", "total_count", "appears_in_both", "reason"]
        print(offending[preview_cols].head(20).to_string(index=False))

    # collect all barcodes from the raw df (excel columns 1 and 2)
    top_barcodes = df.iloc[:, BOARD_TOP_IDX].map(normalize_board_number).dropna().tolist()
    bottom_barcodes = df.iloc[:, BOARD_BOTTOM_IDX].map(normalize_board_number).dropna().tolist()
    all_barcodes = top_barcodes + bottom_barcodes

    trace_lookup: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
    if all_barcodes:
        trace_conn = get_trace_connection()
        try:
            trace_lookup = fetch_trace_info_for_barcodes(trace_conn, all_barcodes, chunk_size=1000)
            print(f"Trace lookup loaded: {len(trace_lookup)} barcodes resolved.")
        finally:
            trace_conn.close()

    payloads: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        payloads.append(row_to_payload(row, trace_lookup))
    if not payloads:
        print("No rows found after parsing.")
        return

    all_columns = sorted(set().union(*[set(p.keys()) for p in payloads]))
    values = [[p.get(c) for c in all_columns] for p in payloads]

    print(f"Prepared {len(values)} rows with {len(all_columns)} columns.")

    conn = get_connection()
    try:
        clear_target_table(conn, "circuit_boards")
        print("Cleared `circuit_boards` before upload.")


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
