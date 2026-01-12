#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from typing import Optional, Tuple, List, Dict

from dotenv import load_dotenv
import pyodbc


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


def get_trace_connection() -> pyodbc.Connection:
    """
    Expects .env like:
      TRACE_HOST=...
      TRACE_USER=...
      TRACE_PASSWORD=...
      TRACE_DRIVER=ODBC Driver 18 for SQL Server
      TRACE_ENCRYPT=yes
      TRACE_TRUST_CERT=yes
    """
    load_dotenv()

    host = os.getenv("TRACE_HOST")
    user = os.getenv("TRACE_USER")
    password = os.getenv("TRACE_PASSWORD")

    driver = os.getenv("TRACE_DRIVER", "ODBC Driver 18 for SQL Server")
    encrypt = os.getenv("TRACE_ENCRYPT", "yes")
    trust_cert = os.getenv("TRACE_TRUST_CERT", "yes")

    if not host:
        raise RuntimeError("TRACE_HOST missing in env (.env).")
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

    # autocommit is fine for read-only queries
    return pyodbc.connect(conn_str, autocommit=True)


def fetch_losname_und_leiterplatte(conn: pyodbc.Connection, barcode: str) -> Optional[Tuple[str, str]]:
    """
    Returns (Losname, Leiterplatte) for the given barcode, or None if not found.
    """
    bc = (barcode or "").strip()
    if not bc:
        return None

    placeholders = "?"
    sql = TRACE_SQL.format(placeholders=placeholders)

    cur = conn.cursor()
    cur.execute(sql, (bc,))
    row = cur.fetchone()
    if not row:
        return None

    _barcode, losname, leiterplatte = row
    los = losname.strip() if isinstance(losname, str) else losname
    lei = leiterplatte.strip() if isinstance(leiterplatte, str) else leiterplatte

    # ensure return type is str (or empty string if NULL)
    return ("" if los is None else str(los), "" if lei is None else str(lei))


def main() -> None:
    barcode = "CM707413"

    try:
        conn = get_trace_connection()
    except Exception as e:
        print(f"Trace connection error: {e}", file=sys.stderr)
        sys.exit(2)

    try:
        result = fetch_losname_und_leiterplatte(conn, barcode)
        if not result:
            print("NOT_FOUND")
            sys.exit(0)

        losname, leiterplatte = result
        print(f"BARCODE={barcode.strip()}")
        print(f"LOSNAME={losname}")
        print(f"LEITERPLATTE={leiterplatte}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
