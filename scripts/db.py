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
    """
    Create tables if they do not exist.
    This is safe to run on every startup.
    """
    cur = conn.cursor()

    # Existing table: one row per screenshot/DOM capture
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

    # NEW: comparisons between consecutive snapshots for a given page
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshot_pairs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_name TEXT NOT NULL,
            url TEXT NOT NULL,
            snapshot_id_1 INTEGER NOT NULL,
            snapshot_id_2 INTEGER NOT NULL,
            compared_at TEXT NOT NULL,
            global_ssim REAL,
            changed INTEGER NOT NULL DEFAULT 0,
            UNIQUE (snapshot_id_1, snapshot_id_2),
            FOREIGN KEY(snapshot_id_1) REFERENCES snapshots(id),
            FOREIGN KEY(snapshot_id_2) REFERENCES snapshots(id)
        );
        """
    )

    # NEW: bounding boxes of changed regions between a pair of snapshots
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshot_diffs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_pair_id INTEGER NOT NULL,
            tile_index INTEGER NOT NULL DEFAULT 0,
            x INTEGER NOT NULL,
            y INTEGER NOT NULL,
            w INTEGER NOT NULL,
            h INTEGER NOT NULL,
            area INTEGER NOT NULL,
            norm_x REAL NOT NULL,
            norm_y REAL NOT NULL,
            norm_w REAL NOT NULL,
            norm_h REAL NOT NULL,
            FOREIGN KEY(snapshot_pair_id) REFERENCES snapshot_pairs(id)
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


# ---- Helpers for OpenCV diff analysis ----


def get_all_sites(conn: sqlite3.Connection) -> Iterable[tuple[str, str]]:
    """
    Return distinct (site_name, url) pairs that have at least one snapshot.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT site_name, url
        FROM snapshots
        ORDER BY site_name, url
        """
    )
    return [(row[0], row[1]) for row in cur.fetchall()]


def get_snapshots_for_site(conn: sqlite3.Connection, site_name: str, url: str) -> list[sqlite3.Row]:
    """
    Return all snapshots for a given site+url ordered by time.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM snapshots
        WHERE site_name = ? AND url = ?
        ORDER BY captured_at
        """,
        (site_name, url),
    )
    return cur.fetchall()


def snapshot_pair_exists(conn: sqlite3.Connection, sid1: int, sid2: int) -> bool:
    """
    Check if we already analyzed this pair of snapshots.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM snapshot_pairs
        WHERE snapshot_id_1 = ? AND snapshot_id_2 = ?
        """,
        (sid1, sid2),
    )
    return cur.fetchone() is not None


def insert_snapshot_pair(
    conn: sqlite3.Connection,
    site_name: str,
    url: str,
    snapshot_id_1: int,
    snapshot_id_2: int,
    compared_at: str,
    global_ssim: float,
    changed: bool,
) -> int:
    """
    Insert a row into snapshot_pairs and return its id.
    """
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO snapshot_pairs (
            site_name, url, snapshot_id_1, snapshot_id_2,
            compared_at, global_ssim, changed
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            site_name,
            url,
            snapshot_id_1,
            snapshot_id_2,
            compared_at,
            float(global_ssim),
            1 if changed else 0,
        ),
    )
    conn.commit()

    # Fetch id
    cur.execute(
        """
        SELECT id FROM snapshot_pairs
        WHERE snapshot_id_1 = ? AND snapshot_id_2 = ?
        """,
        (snapshot_id_1, snapshot_id_2),
    )
    row = cur.fetchone()
    return int(row["id"]) if row else -1


def insert_snapshot_diff(
    conn: sqlite3.Connection,
    snapshot_pair_id: int,
    tile_index: int,
    x: int,
    y: int,
    w: int,
    h: int,
    img_width: int,
    img_height: int,
):
    """
    Insert one bounding box of change for a snapshot pair.
    """
    area = w * h
    norm_x = x / float(img_width)
    norm_y = y / float(img_height)
    norm_w = w / float(img_width)
    norm_h = h / float(img_height)

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO snapshot_diffs (
            snapshot_pair_id,
            tile_index,
            x, y, w, h,
            area,
            norm_x, norm_y, norm_w, norm_h
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_pair_id,
            tile_index,
            x,
            y,
            w,
            h,
            area,
            norm_x,
            norm_y,
            norm_w,
            norm_h,
        ),
    )
    conn.commit()
