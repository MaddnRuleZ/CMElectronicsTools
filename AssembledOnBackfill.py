#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import time
from datetime import datetime
from typing import Dict, Optional, Tuple, List, Iterable, Any

from dotenv import load_dotenv
import pyodbc
import pymysql
import re
import pandas as pd

# =========================
# TRACE SQL: resolve latest assembly finished timestamp per barcode
# =========================
TRACE_SQL = "WITH ranked AS (SELECT tp.Barcode AS Barcode,td.EndDate AS AssemblyFinishedAt,ROW_NUMBER() OVER (PARTITION BY tp.Barcode ORDER BY td.EndDate DESC,td.BeginDate DESC,td.Id DESC) AS rn FROM [dbo].[vTracePanel5] tp INNER JOIN [dbo].[vTraceData5] td ON td.Id=tp.TraceDataId WHERE tp.Barcode IN ({placeholders})) SELECT Barcode,AssemblyFinishedAt FROM ranked WHERE rn=1;"

# =========================
# OPTIONAL HARD-CODED DATE FILTER (PROCESS ONLY "YOUNGER" ROWS)
# =========================
# Set to None to disable filtering (process all rows that need backfill)
ONLY_PROCESS_NEWER_THAN_STR: Optional[str] = None

# MySQL date field to use for "younger than" filtering
DATE_FIELD_IN_MYSQL = "board_erfasst_am"
INCLUDE_ROWS_WITHOUT_DATE_WHEN_FILTERING = False


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
    if isinstance(val, datetime):
        return val
    ser = _coerce_excel_serial(val)
    if ser:
        return ser
    s = str(val).strip()

    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    for fmt in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        return pd.to_datetime(s, dayfirst=False, errors="raise").to_pydatetime()
    except Exception:
        pass
    try:
        return pd.to_datetime(s, dayfirst=True, errors="raise").to_pydatetime()
    except Exception:
        return None


def _clean_barcode(v) -> str:
    s = "" if v is None else str(v)
    s = re.sub(r"\s+", "", s).strip().upper()
    return s


def _barcode_candidates(v) -> List[str]:
    b = _clean_barcode(v)
    if not b:
        return []
    cands = [b]
    if b.isdigit() and not b.startswith("CM"):
        cands.append("CM" + b)
    return cands


def _norm_s(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def get_trace_connection() -> pyodbc.Connection:
    host = os.getenv("TRACE_HOST")
    db = os.getenv("TRACE_DB")
    user = os.getenv("TRACE_USER")
    password = os.getenv("TRACE_PASSWORD")

    driver = os.getenv("TRACE_DRIVER", "ODBC Driver 18 for SQL Server")
    encrypt = os.getenv("TRACE_ENCRYPT", "yes")
    trust_cert = os.getenv("TRACE_TRUST_CERT", "yes")

    if not host:
        raise RuntimeError("TRACE_HOST missing in .env")
    if not db:
        raise RuntimeError("TRACE_DB missing in .env")
    if not user or not password:
        raise RuntimeError("TRACE_USER / TRACE_PASSWORD missing in .env")

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={host};"
        f"DATABASE={db};"
        f"UID={user};"
        f"PWD={password};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_cert};"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def fetch_trace_assembly_finished_for_barcodes_paced(
    conn: pyodbc.Connection,
    barcodes: List[str],
    chunk_size: int = 200,
    pacing_seconds: float = 0.25,
) -> Dict[str, Optional[datetime]]:
    """
    Batched + paced lookup:
      barcode -> AssemblyFinishedAt (td.EndDate of latest record for that barcode)
    """
    out: Dict[str, Optional[datetime]] = {}
    if not barcodes:
        return out

    seen = set()
    uniq: List[str] = []
    for b in barcodes:
        bb = _norm_s(b)
        if not bb:
            continue
        if bb in seen:
            continue
        seen.add(bb)
        uniq.append(bb)

    cur = conn.cursor()
    total_chunks = (len(uniq) + max(1, int(chunk_size)) - 1) // max(1, int(chunk_size))

    for i, chunk in enumerate(_chunked(uniq, max(1, int(chunk_size)))):
        placeholders = ",".join(["?"] * len(chunk))
        sql = TRACE_SQL.format(placeholders=placeholders)

        cur.execute(sql, chunk)

        for barcode, assembled_on in cur.fetchall():
            bc = _norm_s(barcode)
            # assembled_on should come back as datetime or None via pyodbc
            out[bc] = assembled_on if isinstance(assembled_on, datetime) else coerce_datetime(assembled_on)

        if pacing_seconds > 0 and i < (total_chunks - 1):
            time.sleep(pacing_seconds)

    return out


def get_mysql_connection():
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "3306"))
    database = os.getenv("DB_NAME", "manufacturing")
    user = os.getenv("DB_USER", "user")
    password = os.getenv("DB_PASSWORD", os.getenv("DB_PASS", ""))

    ssl_ca = os.getenv("DB_SSL_CA")
    ssl_params = {"ca": os.path.expanduser(ssl_ca)} if ssl_ca else None

    try:
        return pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            autocommit=False,
            ssl=ssl_params,
        )
    except Exception as e:
        print(f"MySQL connection error: {e}", file=sys.stderr)
        sys.exit(2)


def _apply_row_date_filter(rows: List[Tuple[Any, ...]], date_idx: int) -> List[Tuple[Any, ...]]:
    if not ONLY_PROCESS_NEWER_THAN_STR:
        return rows

    cutoff = coerce_datetime(ONLY_PROCESS_NEWER_THAN_STR)
    if cutoff is None:
        print(
            f"Invalid ONLY_PROCESS_NEWER_THAN_STR={ONLY_PROCESS_NEWER_THAN_STR!r}. Use formats like '2025-01-01' or '01.01.2025'.",
            file=sys.stderr,
        )
        sys.exit(4)

    before = len(rows)
    kept: List[Tuple[Any, ...]] = []

    for r in rows:
        dt_raw = r[date_idx]
        dt = coerce_datetime(dt_raw)

        if dt is None:
            if INCLUDE_ROWS_WITHOUT_DATE_WHEN_FILTERING:
                kept.append(r)
            continue

        if dt >= cutoff:
            kept.append(r)

    print(f"Date filter enabled: keeping {len(kept)}/{before} rows where {DATE_FIELD_IN_MYSQL} >= {cutoff.isoformat(sep=' ', timespec='seconds')}")
    return kept


def main() -> None:
    load_dotenv()

    trace_chunk_size = int(os.getenv("TRACE_CHUNK_SIZE", "200"))
    trace_pacing_seconds = float(os.getenv("TRACE_PACING_SECONDS", "0.25"))

    trace_conn = get_trace_connection()
    mysql_conn = get_mysql_connection()

    select_sql = f"SELECT id,board_top,board_bottom,board_assembled_on,{DATE_FIELD_IN_MYSQL} FROM circuit_boards WHERE (board_assembled_on IS NULL OR TRIM(board_assembled_on)='');"
    update_sql = "UPDATE circuit_boards SET board_assembled_on=%s WHERE id=%s;"

    try:
        # 1) Load rows needing backfill
        with mysql_conn.cursor() as cur:
            cur.execute(select_sql)
            rows = cur.fetchall()

        # rows tuple layout: (id, top, bottom, board_assembled_on, board_erfasst_am)
        rows = _apply_row_date_filter(rows, date_idx=4)

        print(f"Rows needing board_assembled_on backfill (after date filter): {len(rows)}")
        if not rows:
            return

        # 2) Collect barcode candidates per row
        barcodes: List[str] = []
        row_candidates: Dict[int, List[str]] = {}

        skipped_no_barcode = 0
        for (row_id, board_top, board_bottom, _assembled_old, _dt) in rows:
            row_id_i = int(row_id)

            cands = _barcode_candidates(board_top)
            if not cands:
                cands = _barcode_candidates(board_bottom)

            if not cands:
                skipped_no_barcode += 1
                continue

            row_candidates[row_id_i] = cands
            barcodes.extend(cands)

        if not barcodes:
            print(f"Skipped (no barcode): {skipped_no_barcode}")
            return

        # 3) Batch + pace trace lookups
        lookup = fetch_trace_assembly_finished_for_barcodes_paced(
            trace_conn,
            barcodes,
            chunk_size=trace_chunk_size,
            pacing_seconds=trace_pacing_seconds,
        )
        print(f"Trace lookup results: {len(lookup)} (unique barcodes resolved)")

        # 4) Apply updates (only board_assembled_on)
        updated = 0
        set_default_1900 = 0
        skipped_no_timestamp = 0

        DEFAULT_ASSEMBLY_DATE = datetime(1900, 1, 1)

        with mysql_conn.cursor() as cur:
            for (row_id, _top, _bottom, assembled_old, _dt) in rows:
                row_id_i = int(row_id)

                # if something non-empty is already in DB, skip (defensive)
                if _norm_s(assembled_old):
                    continue

                cands = row_candidates.get(row_id_i) or []
                assembled_on: Optional[datetime] = None
                for bc in cands:
                    assembled_on = lookup.get(bc)
                    if assembled_on is not None:
                        break

                if assembled_on is None:
                    # barcode not found in trace -> set default 1900-01-01
                    cur.execute(update_sql, (DEFAULT_ASSEMBLY_DATE, row_id_i))
                    updated += 1
                    set_default_1900 += 1
                    continue

                if not isinstance(assembled_on, datetime):
                    assembled_on = coerce_datetime(assembled_on)

                if assembled_on is None:
                    skipped_no_timestamp += 1
                    continue

                cur.execute(update_sql, (assembled_on, row_id_i))
                updated += 1


        mysql_conn.commit()

        print(f"Updated rows: {updated}")
        print(f"Skipped (no barcode): {skipped_no_barcode}")
        print(f"Skipped (no assembly timestamp): {skipped_no_timestamp}")
        print(f"Pacing: chunk_size={trace_chunk_size}, sleep={trace_pacing_seconds}s")

    except Exception as e:
        mysql_conn.rollback()
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(3)
    finally:
        try:
            mysql_conn.close()
        except Exception:
            pass
        try:
            trace_conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
