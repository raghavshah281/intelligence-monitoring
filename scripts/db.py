import sqlite3
from pathlib import Path
from typing import Iterable, Dict, Any, Optional


DB_PATH = Path("ab_tracker.db")


def get_connection(db_path: Path | None = None) -> sqlite3.Connection:
    db_path = db_path or DB_PATH
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_schema(conn: sqlite3.Connection):
    """
    Create tables if they do not exist.
    Safe to run on every startup.
    """
    cur = conn.cursor()

    # Core snapshots table: one row per capture
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

    # NEW: DOM features per snapshot (hero + CTA + sections + variant_key)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshot_dom_features (
            snapshot_id INTEGER PRIMARY KEY,
            hero_heading TEXT,
            hero_subheading TEXT,
            hero_cta_text TEXT,
            hero_cta_href TEXT,
            main_sections_json TEXT,
            variant_key TEXT,
            FOREIGN KEY(snapshot_id) REFERENCES snapshots(id)
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
    """
    Get all snapshots captured since the given ISO timestamp.
    """
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


# ---- DOM feature helpers ----


def get_dom_features(conn: sqlite3.Connection, snapshot_id: int) -> Optional[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM snapshot_dom_features
        WHERE snapshot_id = ?
        """,
        (snapshot_id,),
    )
    return cur.fetchone()


def upsert_dom_features(
    conn: sqlite3.Connection,
    snapshot_id: int,
    hero_heading: str,
    hero_subheading: str,
    hero_cta_text: str,
    hero_cta_href: str,
    main_sections_json: str,
    variant_key: str | None,
):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO snapshot_dom_features (
            snapshot_id,
            hero_heading,
            hero_subheading,
            hero_cta_text,
            hero_cta_href,
            main_sections_json,
            variant_key
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(snapshot_id) DO UPDATE SET
            hero_heading = excluded.hero_heading,
            hero_subheading = excluded.hero_subheading,
            hero_cta_text = excluded.hero_cta_text,
            hero_cta_href = excluded.hero_cta_href,
            main_sections_json = excluded.main_sections_json,
            variant_key = excluded.variant_key;
        """,
        (
            snapshot_id,
            hero_heading,
            hero_subheading,
            hero_cta_text,
            hero_cta_href,
            main_sections_json,
            variant_key,
        ),
    )
    conn.commit()
