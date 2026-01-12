#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from typing import Dict, Optional, Tuple, List

from dotenv import load_dotenv
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
    FROM [dbo].[vTracePanel5] tp
    INNER JOIN [dbo].[vTraceData5] td ON td.Id = tp.TraceDataId
    INNER JOIN [dbo].[vTraceJob5]  tj ON tj.TraceDataId = td.Id
    INNER JOIN [dbo].[vJob5]        j ON j.Id = tj.JobId
    LEFT  JOIN [dbo].[vOrder5]      o ON o.Id = j.OrderId
    LEFT  JOIN [dbo].[vBoard5]      b ON b.Id = j.BoardId
    WHERE tp.Barcode IN ({placeholders})
)
SELECT
    Barcode,
    Losname,
    Leiterplatte
FROM ranked
WHERE rn = 1;
""".strip()


def _norm_s(v) -> str:
    if v is None:
        return ""
    return str(v).strip()


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


def fetch_losname_und_leiterplatte(
    conn: pyodbc.Connection,
    barcode: str,
) -> Optional[Tuple[str, str]]:
    """
    Returns (Losname, LeiterplatteSuffix) for the given barcode, or None if not found.
    LeiterplatteSuffix: everything after the first backslash (e.g. "Livetec\\LI008.001_V01" -> "LI008.001_V01")
    """
    bc = (barcode or "").strip()
    if not bc:
        return None

    sql = TRACE_SQL.format(placeholders="?")
    cur = conn.cursor()
    cur.execute(sql, (bc,))
    row = cur.fetchone()
    if not row:
        return None

    _barcode, losname, leiterplatte = row

    los = losname.strip() if isinstance(losname, str) else ("" if losname is None else str(losname))
    lei = (
        leiterplatte.strip()
        if isinstance(leiterplatte, str)
        else ("" if leiterplatte is None else str(leiterplatte))
    )

    # cut off everything before the first "\"
    if "\\" in lei:
        lei = lei.split("\\", 1)[1]

    return (los, lei)


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


def main() -> None:
    load_dotenv()

    trace_conn = get_trace_connection()
    mysql_conn = get_mysql_connection()

    cache: Dict[str, Optional[Tuple[str, str]]] = {}

    select_sql = """
        SELECT
            id,
            board_top,
            board_bottom,
            board_fa_nummer,
            board_artikel_nummer
        FROM circuit_boards
        WHERE
            (board_fa_nummer IS NULL OR TRIM(board_fa_nummer) = '')
            OR (board_artikel_nummer IS NULL OR TRIM(board_artikel_nummer) = '');
    """.strip()

    update_sql = """
        UPDATE circuit_boards
        SET board_fa_nummer = %s,
            board_artikel_nummer = %s
        WHERE id = %s;
    """.strip()

    try:
        with mysql_conn.cursor() as cur:
            cur.execute(select_sql)
            rows = cur.fetchall()

        print(f"Rows needing FA/Artikel backfill: {len(rows)}")

        updated = 0
        skipped_no_barcode = 0
        skipped_not_found = 0

        with mysql_conn.cursor() as cur:
            for (row_id, board_top, board_bottom, fa_old, art_old) in rows:
                top = _norm_s(board_top)
                bottom = _norm_s(board_bottom)

                # prefer top; fallback to bottom
                barcode = top if top else bottom
                if not barcode:
                    skipped_no_barcode += 1
                    continue

                if barcode in cache:
                    info = cache[barcode]
                else:
                    info = fetch_losname_und_leiterplatte(trace_conn, barcode)
                    cache[barcode] = info

                if not info:
                    skipped_not_found += 1
                    continue

                losname, leiterplatte = info

                # Only fill missing fields, keep existing non-empty values
                fa_new = _norm_s(fa_old) or _norm_s(losname)
                art_new = _norm_s(art_old) or _norm_s(leiterplatte)

                # If still empty, nothing to update
                if not fa_new and not art_new:
                    skipped_not_found += 1
                    continue

                cur.execute(update_sql, (fa_new, art_new, row_id))
                updated += 1

            mysql_conn.commit()

        print(f"Updated rows: {updated}")
        print(f"Skipped (no barcode): {skipped_no_barcode}")
        print(f"Skipped (not found in trace): {skipped_not_found}")
        print(f"Cache size: {len(cache)}")

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
