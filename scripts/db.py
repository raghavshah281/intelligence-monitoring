import sqlite3
from pathlib import Path
from typing import Iterable, Dict, Any


DB_PATH = Path("ab_tracker.db")


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    db_path = db_path or DB_PATH
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_name TEXT NOT NULL,
            url TEXT NOT NULL,
            captured_at TEXT NOT NULL,
            screenshot_drive_id TEXT NOT NULL,
            dom_drive_id TEXT NOT NULL,
            phash TEXT,
            ahash TEXT,
            dhash TEXT,
            dom_hash TEXT
        );
        """
    )

    conn.commit()


def insert_snapshot(conn: sqlite3.Connection, data: Dict[str, Any]):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO snapshots (
            site_name, url, captured_at,
            screenshot_drive_id, dom_drive_id,
            phash, ahash, dhash, dom_hash
        ) VALUES (
            :site_name, :url, :captured_at,
            :screenshot_drive_id, :dom_drive_id,
            :phash, :ahash, :dhash, :dom_hash
        );
        """,
        data,
    )
    conn.commit()


def get_weekly_snapshots(conn: sqlite3.Connection, since_iso: str) -> Iterable[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM snapshots
        WHERE captured_at >= ?
        ORDER BY site_name, url, captured_at
        """,
        (since_iso,),
    )
    return cur.fetchall()
