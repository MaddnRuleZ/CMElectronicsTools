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
        # MySQL 8+ (CTEs). If you're on MySQL 5.7, I can rewrite without CTEs.
        sql = """
WITH vals AS (
    SELECT
        cb.id,
        cb.board_erfasst_am,
        cb.board_recorded_on,
        'board_top' AS src,
        TRIM(cb.board_top) AS num,
        cb.board_top,
        cb.board_bottom,
        cb.board_ok,
        cb.board_fa_nummer,
        cb.board_artikel_nummer,
        cb.board_erfasst_durch
    FROM circuit_boards cb
    WHERE cb.board_top IS NOT NULL AND TRIM(cb.board_top) <> ''

    UNION ALL

    SELECT
        cb.id,
        cb.board_erfasst_am,
        cb.board_recorded_on,
        'board_bottom' AS src,
        TRIM(cb.board_bottom) AS num,
        cb.board_top,
        cb.board_bottom,
        cb.board_ok,
        cb.board_fa_nummer,
        cb.board_artikel_nummer,
        cb.board_erfasst_durch
    FROM circuit_boards cb
    WHERE cb.board_bottom IS NOT NULL AND TRIM(cb.board_bottom) <> ''
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
    v.*,
    d.total_count,
    d.top_count,
    d.bottom_count,
    CASE
        WHEN d.top_count > 0 AND d.bottom_count > 0 THEN 'appears_in_both_columns'
        WHEN d.top_count > 1 THEN 'duplicate_in_board_top'
        WHEN d.bottom_count > 1 THEN 'duplicate_in_board_bottom'
        ELSE 'duplicate_across_union'
    END AS reason
FROM vals v
JOIN dups d ON d.num = v.num
ORDER BY v.board_erfasst_am ASC, v.num ASC, v.id ASC, v.src ASC;
        """.strip()

        offending = pd.read_sql(sql, conn)

        if offending.empty:
            print("OK: No duplicates across board_top/board_bottom union.")
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.abspath(f"db_board_number_duplicates_{ts}.csv")
        offending.to_csv(out_path, index=False, encoding="utf-8")

        print(f"VIOLATION: {offending['num'].nunique()} duplicate numbers, {len(offending)} total occurrences.")
        print(f"Wrote CSV: {out_path}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
