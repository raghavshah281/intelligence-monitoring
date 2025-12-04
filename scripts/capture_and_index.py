import os
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

from PIL import Image
import imagehash
from playwright.sync_api import sync_playwright

from gdrive_client import download_file, upload_file
from db import get_connection, init_schema, insert_snapshot


DB_LOCAL_PATH = Path("ab_tracker.db")


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def compute_hashes(image_path: Path):
    img = Image.open(image_path)
    ph = imagehash.phash(img)
    ah = imagehash.average_hash(img)
    dh = imagehash.dhash(img)
    return ph.__str__(), ah.__str__(), dh.__str__()


def compute_dom_hash(html: str) -> str:
    # Simple SHA-256 over raw HTML; you can normalize more if you want
    return hashlib.sha256(html.encode("utf-8")).hexdigest()


def load_sites() -> list[dict]:
    with open("config/sites.json", "r", encoding="utf-8") as f:
        return json.load(f)


def capture_site(page, url: str, out_dir: Path, base_name: str) -> tuple[Path, Path, str]:
    page.goto(url, wait_until="networkidle")
    # Increase device scale factor via viewport for nice quality
    page.set_viewport_size({"width": 1440, "height": 900})
    # full-page screenshot
    screenshot_path = out_dir / f"{base_name}.png"
    page.screenshot(path=str(screenshot_path), full_page=True)

    html = page.content()
    dom_path = out_dir / f"{base_name}.html"
    dom_path.write_text(html, encoding="utf-8")

    return screenshot_path, dom_path, html


def main():
    gdrive_db_file_id = os.environ["GDRIVE_DB_FILE_ID"]
    gdrive_screenshot_folder_id = os.environ["GDRIVE_SCREENSHOT_FOLDER_ID"]
    gdrive_dom_folder_id = os.environ["GDRIVE_DOM_FOLDER_ID"]

    # 1) Sync DB down from Drive
    download_file(gdrive_db_file_id, str(DB_LOCAL_PATH))

    # 2) Init DB schema if first time
    conn = get_connection(DB_LOCAL_PATH)
    init_schema(conn)

    sites = load_sites()
    run_ts = iso_now()

    out_dir = Path("artifacts")
    out_dir.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            device_scale_factor=2,  # higher DPR for sharper text
        )

        for site in sites:
            page = context.new_page()
            site_name = site["name"]
            url = site["url"]

            base_name = f"{site_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%dT%H%M%S')}"
            screenshot_path, dom_path, html = capture_site(page, url, out_dir, base_name)

            # Upload screenshot & DOM to Drive
            screenshot_drive_id = upload_file(
                str(screenshot_path),
                folder_id=gdrive_screenshot_folder_id,
                mime_type="image/png",
            )
            dom_drive_id = upload_file(
                str(dom_path),
                folder_id=gdrive_dom_folder_id,
                mime_type="text/html",
            )

            phash, ahash, dhash = compute_hashes(screenshot_path)
            dom_hash = compute_dom_hash(html)

            insert_snapshot(
                conn,
                {
                    "site_name": site_name,
                    "url": url,
                    "captured_at": run_ts,
                    "screenshot_drive_id": screenshot_drive_id,
                    "dom_drive_id": dom_drive_id,
                    "phash": phash,
                    "ahash": ahash,
                    "dhash": dhash,
                    "dom_hash": dom_hash,
                },
            )

            print(f"Captured and stored snapshot for {site_name} ({url})")

        browser.close()

    conn.close()

    # 3) Sync DB back up to Drive
    upload_file(str(DB_LOCAL_PATH), file_id=gdrive_db_file_id, mime_type="application/x-sqlite3")
    print("DB synced back to Drive.")


if __name__ == "__main__":
    main()
