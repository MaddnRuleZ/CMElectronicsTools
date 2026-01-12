import logging

from dotenv import load_dotenv
import os
import pyodbc
from typing import Optional, Tuple
logger = logging.getLogger(__name__)

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


def get_trace_connection() -> pyodbc.Connection:
    load_dotenv()

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


def fetch_losname_und_leiterplatte(conn: pyodbc.Connection, barcode: str) -> Optional[Tuple[str, str]]:
    bc = (barcode or "").strip()
    logger.debug("fetch_losname_und_leiterplatte: start | barcode_raw=%r | barcode_clean=%r", barcode, bc)

    if not bc:
        logger.debug("fetch_losname_und_leiterplatte: empty barcode -> return None")
        return None

    sql = TRACE_SQL.format(placeholders="?")
    logger.debug("fetch_losname_und_leiterplatte: executing trace lookup | barcode=%r", bc)

    try:
        cur = conn.cursor()
        cur.execute(sql, (bc,))
        row = cur.fetchone()
    except pyodbc.Error:
        logger.exception("fetch_losname_und_leiterplatte: pyodbc error during lookup | barcode=%r", bc)
        return None
    except Exception:
        logger.exception("fetch_losname_und_leiterplatte: unexpected error during lookup | barcode=%r", bc)
        return None

    if not row:
        logger.debug("fetch_losname_und_leiterplatte: no row found | barcode=%r", bc)
        return None

    try:
        _barcode, losname, leiterplatte = row
    except Exception:
        logger.exception("fetch_losname_und_leiterplatte: unexpected row shape | barcode=%r | row=%r", bc, row)
        return None

    los = losname.strip() if isinstance(losname, str) else ("" if losname is None else str(losname))
    lei = leiterplatte.strip() if isinstance(leiterplatte, str) else ("" if leiterplatte is None else str(leiterplatte))

    logger.debug(
        "fetch_losname_und_leiterplatte: success | barcode=%r | losname=%r | leiterplatte=%r",
        bc, los[:120], lei[:120]
    )
    return (los, lei)

def main() -> None:
    barcode = "CM734652"

    try:
        conn = get_trace_connection()
    except Exception as e:
        print("X")

    try:
        result = fetch_losname_und_leiterplatte(conn, barcode)
        if not result:
            print("NOT_FOUND")

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
