import os
from datetime import datetime, timezone
from pathlib import Path

import cv2
import numpy as np
from skimage.metrics import structural_similarity as ssim

from gdrive_client import download_file, upload_file
from db import (
    get_connection,
    init_schema,
    get_all_sites,
    get_snapshots_for_site,
    snapshot_pair_exists,
    insert_snapshot_pair,
    insert_snapshot_diff,
)

DB_LOCAL_PATH = Path("ab_tracker.db")


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_image_from_drive(drive_id: str, tmp_dir: Path) -> tuple[np.ndarray, int, int, Path]:
    """
    Download image from Drive and load via OpenCV.
    Returns (image_bgr, width, height, local_path).
    """
    local_path = tmp_dir / f"{drive_id}.png"
    download_file(drive_id, str(local_path))

    img = cv2.imread(str(local_path), cv2.IMREAD_COLOR)
    if img is None:
        raise RuntimeError(f"Failed to load image from {local_path}")

    h, w = img.shape[:2]
    return img, w, h, local_path


def compute_global_ssim(img1: np.ndarray, img2: np.ndarray) -> float:
    """
    Compute structural similarity index (SSIM) between two images.
    They must be same size; we will resize img2 to match img1 if needed.
    """
    h1, w1 = img1.shape[:2]
    h2, w2 = img2.shape[:2]

    if (h1, w1) != (h2, w2):
        img2 = cv2.resize(img2, (w1, h1), interpolation=cv2.INTER_AREA)

    gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)

    score, _ = ssim(gray1, gray2, full=True)
    return float(score)


def detect_diff_boxes(
    img1: np.ndarray,
    img2: np.ndarray,
    global_ssim: float,
    ssim_threshold: float = 0.98,
    min_area_ratio: float = 0.001,
) -> list[tuple[int, int, int, int]]:
    """
    Use OpenCV to detect bounding boxes of differences between two images.

    Returns list of (x, y, w, h) in full-image coordinates.
    """
    h1, w1 = img1.shape[:2]
    h2, w2 = img2.shape[:2]

    if (h1, w1) != (h2, w2):
        img2 = cv2.resize(img2, (w1, h1), interpolation=cv2.INTER_AREA)

    gray1 = cv2.cvtColor(img1, cv2.COLOR_BGR2GRAY)
    gray2 = cv2.cvtColor(img2, cv2.COLOR_BGR2GRAY)

    if global_ssim >= ssim_threshold:
        # Too similar; nothing significant to detect
        return []

    # Absolute difference
    diff = cv2.absdiff(gray1, gray2)
    # Optional blur to reduce noise
    diff_blur = cv2.GaussianBlur(diff, (5, 5), 0)

    # Threshold: highlight pixels that changed
    # You can tweak 25 if too sensitive / too strict
    _, thresh = cv2.threshold(diff_blur, 25, 255, cv2.THRESH_BINARY)

    # Morphological operations to merge nearby regions
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
    thresh = cv2.dilate(thresh, kernel, iterations=2)
    thresh = cv2.erode(thresh, kernel, iterations=1)

    contours, _ = cv2.findContours(
        thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )

    boxes: list[tuple[int, int, int, int]] = []
    min_area = int(min_area_ratio * w1 * h1)

    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        area = w * h
        if area < min_area:
            continue
        boxes.append((x, y, w, h))

    return boxes


def main():
    gdrive_db_file_id = os.environ["GDRIVE_DB_FILE_ID"]

    print("Starting analyze_diffs run...")

    # 1) Download DB
    print("Downloading DB from Drive...")
    download_file(gdrive_db_file_id, str(DB_LOCAL_PATH))

    # 2) Open DB and ensure schema
    conn = get_connection(DB_LOCAL_PATH)
    init_schema(conn)
    print("DB opened; schema initialized.")

    tmp_dir = Path("diff_tmp")
    tmp_dir.mkdir(exist_ok=True)

    sites = list(get_all_sites(conn))
    print(f"Found {len(sites)} site(s) with snapshots.")

    now_iso = iso_now()

    # Global SSIM threshold: above this = "no meaningful change"
    SSIM_THRESHOLD = 0.985

    for (site_name, url) in sites:
        print(f"\n[Diff] Processing site: {site_name} – {url}")
        snapshots = get_snapshots_for_site(conn, site_name, url)
        if len(snapshots) < 2:
            print("  Not enough snapshots for diff (need at least 2). Skipping.")
            continue

        for i in range(len(snapshots) - 1):
            s1 = snapshots[i]
            s2 = snapshots[i + 1]
            sid1 = int(s1["id"])
            sid2 = int(s2["id"])

            if snapshot_pair_exists(conn, sid1, sid2):
                # Already analyzed this pair in a previous run
                continue

            print(f"  Comparing snapshot {sid1} vs {sid2}...")

            # 3) Download images from Drive
            img1, w1, h1, _ = load_image_from_drive(s1["screenshot_drive_id"], tmp_dir)
            img2, w2, h2, _ = load_image_from_drive(s2["screenshot_drive_id"], tmp_dir)

            # 4) Global SSIM
            score = compute_global_ssim(img1, img2)
            print(f"    Global SSIM = {score:.5f}")

            changed = score < SSIM_THRESHOLD

            # 5) Insert snapshot_pair
            pair_id = insert_snapshot_pair(
                conn,
                site_name=site_name,
                url=url,
                snapshot_id_1=sid1,
                snapshot_id_2=sid2,
                compared_at=now_iso,
                global_ssim=score,
                changed=changed,
            )

            if not changed:
                print("    Marked as no significant change based on SSIM.")
                continue

            # 6) Detect diff boxes with OpenCV
            boxes = detect_diff_boxes(img1, img2, global_ssim=score)
            print(f"    Found {len(boxes)} change region(s).")

            for (x, y, w, h) in boxes:
                insert_snapshot_diff(
                    conn,
                    snapshot_pair_id=pair_id,
                    tile_index=0,  # we are not tiling yet; everything is tile_index=0
                    x=x,
                    y=y,
                    w=w,
                    h=h,
                    img_width=w1,
                    img_height=h1,
                )

    conn.close()

    # 7) Upload updated DB
    print("Syncing updated DB with diff data back to Drive...")
    upload_file(
        str(DB_LOCAL_PATH),
        file_id=gdrive_db_file_id,
        mime_type="application/x-sqlite3",
    )
    print("analyze_diffs run completed.")


if __name__ == "__main__":
    main()
