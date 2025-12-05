import os
import sys
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


def load_image_from_drive(drive_id: str, tmp_dir: Path):
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
    Compute SSIM between two images.
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
):
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
        # No significant change → normal path
        return []

    diff = cv2.absdiff(gray1, gray2)
    diff_blur = cv2.GaussianBlur(diff, (5, 5), 0)

    _, thresh = cv2.threshold(diff_blur, 25, 255, cv2.THRESH_BINARY)

    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3, 3))
    thresh = cv2.dilate(thresh, kernel, iterations=2)
    thresh = cv2.erode(thresh, kernel, iterations=1)

    contours, _ = cv2.findContours(
        thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )

    boxes = []
    min_area = int(min_area_ratio * w1 * h1)

    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        area = w * h
        if area < min_area:
            continue
        boxes.append((x, y, w, h))

    return boxes


def analyze():
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
    total_sites = len(sites)
    print(f"Found {total_sites} site(s) with snapshots.")

    now_iso = iso_now()
    SSIM_THRESHOLD = 0.985
    MAX_PAIRS_PER_SITE = 5  # keep each run fast

    # Track results for each site for final summary
    site_results = []  # List of (site_name, pairs_processed, status)

    for site_idx, (site_name, url) in enumerate(sites, 1):
        print(f"\n[Diff] Processing site {site_idx}/{total_sites}: {site_name} – {url}")
        snapshots = get_snapshots_for_site(conn, site_name, url)
        if len(snapshots) < 2:
            print("  Not enough snapshots for diff (need at least 2). Skipping.")
            site_results.append((site_name, 0, "skipped (insufficient snapshots)"))
            continue

        pairs_processed = 0
        site_error = None  # Track if site had an error

        # Work from latest pairs backwards
        indices = list(range(len(snapshots) - 1))
        indices.reverse()

        try:
            for idx in indices:
                if pairs_processed >= MAX_PAIRS_PER_SITE:
                    print(f"  Reached MAX_PAIRS_PER_SITE={MAX_PAIRS_PER_SITE}, stopping for this site.")
                    break

                s1 = snapshots[idx]
                s2 = snapshots[idx + 1]
                sid1 = int(s1["id"])
                sid2 = int(s2["id"])

                if snapshot_pair_exists(conn, sid1, sid2):
                    # Already done in previous run
                    continue

                print(f"  Comparing snapshot {sid1} vs {sid2}...")
                pairs_processed += 1

                try:
                    img1, w1, h1, _ = load_image_from_drive(
                        s1["screenshot_drive_id"], tmp_dir
                    )
                    img2, w2, h2, _ = load_image_from_drive(
                        s2["screenshot_drive_id"], tmp_dir
                    )

                    score = compute_global_ssim(img1, img2)
                    print(f"    Global SSIM = {score:.5f}")

                    changed = score < SSIM_THRESHOLD

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
                        print("    No significant change (SSIM above threshold). Recorded as unchanged pair.")
                        # NO EXCEPTION, just continue
                        continue

                    boxes = detect_diff_boxes(img1, img2, global_ssim=score)
                    print(f"    Found {len(boxes)} change region(s).")

                    if not boxes:
                        print("    No localized diff boxes found; change is subtle or mostly noise.")

                    for (x, y, w, h) in boxes:
                        insert_snapshot_diff(
                            conn,
                            snapshot_pair_id=pair_id,
                            tile_index=0,
                            x=x,
                            y=y,
                            w=w,
                            h=h,
                            img_width=w1,
                            img_height=h1,
                        )

                except Exception as e:
                    # One bad pair should never kill the site
                    print(f"    [Pair Error] Snapshot pair {sid1}-{sid2} failed: {e}. Skipping this pair.")
                    continue

            # Log when no new pairs were found to process
            if pairs_processed == 0:
                print(f"  No new pairs to analyze for {site_name} (all previously processed).")

        except Exception as e:
            # One bad site should not kill the whole run
            site_error = str(e)
            print(f"[Site Error] Unexpected error for site {site_name}: {e}")
        finally:
            # Record result for summary
            if site_error:
                status = f"error: {site_error}"
            elif pairs_processed == 0:
                status = "completed (no new pairs)"
            else:
                status = "completed"
            site_results.append((site_name, pairs_processed, status))

            # Always sync DB for this site, even if something went wrong
            print(f"[Diff] Finished site: {site_name}. Syncing DB to Drive...")
            try:
                upload_file(
                    str(DB_LOCAL_PATH),
                    file_id=gdrive_db_file_id,
                    mime_type="application/x-sqlite3",
                )
                print("[Diff] DB synced for this site.")
            except Exception as e:
                print(f"[Diff] Failed to sync DB for site {site_name}: {e}")

    conn.close()

    # Print final summary
    print("\n=== Analyze Diffs Summary ===")
    for name, count, status in site_results:
        print(f"  {name}: {count} pair(s) processed, status: {status}")
    print(f"Total: {len(site_results)} site(s) processed.")
    print("analyze_diffs run completed (final DB already synced per site).")


def main():
    """
    Top-level wrapper: NEVER let an exception bubble out and kill the process.
    """
    try:
        analyze()
    except Exception as e:
        # Log and exit with code 0 so GitHub never treats it as a failure/cancel.
        print(f"[TOP-LEVEL ERROR] analyze_diffs crashed unexpectedly: {e}")
        sys.exit(0)


if __name__ == "__main__":
    main()
