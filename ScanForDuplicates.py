#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from datetime import datetime

from dotenv import load_dotenv
import pandas as pd
import pymysql


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
        return pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            autocommit=True,
            ssl=ssl_params,
        )
    except Exception as e:
        print(f"MySQL connection error: {e}", file=sys.stderr)
        sys.exit(2)


def main() -> None:
    conn = get_connection()
    try:
        # MySQL 8+ (CTEs). If you’re on MySQL 5.7, tell me and I’ll rewrite without CTEs.
        sql = """
WITH vals AS (
    SELECT
        id,
        'board_top' AS src,
        TRIM(board_top) AS num
    FROM circuit_boards
    WHERE board_top IS NOT NULL AND TRIM(board_top) <> ''
    UNION ALL
    SELECT
        id,
        'board_bottom' AS src,
        TRIM(board_bottom) AS num
    FROM circuit_boards
    WHERE board_bottom IS NOT NULL AND TRIM(board_bottom) <> ''
),
dups AS (
    SELECT
        num,
        COUNT(*) AS total_count,
        SUM(src = 'board_top') AS top_count,
        SUM(src = 'board_bottom') AS bottom_count
    FROM vals
    GROUP BY num
    HAVING COUNT(*) > 1
)
SELECT
    cb.*,
    d.num AS duplicate_number,
    d.total_count,
    d.top_count,
    d.bottom_count,
    (TRIM(cb.board_top) = d.num) AS match_top,
    (TRIM(cb.board_bottom) = d.num) AS match_bottom,
    CASE
        WHEN d.top_count > 1 AND d.bottom_count > 1 THEN 'duplicate_in_board_top;duplicate_in_board_bottom;appears_in_both_columns'
        WHEN d.top_count > 1 AND d.bottom_count > 0 THEN 'duplicate_in_board_top;appears_in_both_columns'
        WHEN d.bottom_count > 1 AND d.top_count > 0 THEN 'duplicate_in_board_bottom;appears_in_both_columns'
        WHEN d.top_count > 1 THEN 'duplicate_in_board_top'
        WHEN d.bottom_count > 1 THEN 'duplicate_in_board_bottom'
        WHEN d.top_count > 0 AND d.bottom_count > 0 THEN 'appears_in_both_columns'
        ELSE 'duplicate_across_union'
    END AS reason
FROM circuit_boards cb
JOIN dups d
  ON TRIM(cb.board_top) = d.num
  OR TRIM(cb.board_bottom) = d.num
ORDER BY d.total_count DESC, d.num ASC, cb.id ASC;
        """.strip()

        offending = pd.read_sql(sql, conn)

        if offending.empty:
            print("OK: No duplicates across board_top/board_bottom union.")
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.abspath(f"db_board_number_invariant_violations_{ts}.csv")
        offending.to_csv(out_path, index=False, encoding="utf-8")

        dup_count = offending["duplicate_number"].nunique() if "duplicate_number" in offending.columns else "?"
        print(f"VIOLATION: Found duplicates (unique numbers): {dup_count}")
        print(f"Wrote CSV: {out_path}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
