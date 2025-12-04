import os
import json
import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from PIL import Image
import imagehash
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

from gdrive_client import download_file, upload_file
from db import get_connection, init_schema, insert_snapshot


DB_LOCAL_PATH = Path("ab_tracker.db")


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def compute_hashes(image_path: Path):
    """Compute perceptual hashes for an image."""
    img = Image.open(image_path)
    ph = imagehash.phash(img)
    ah = imagehash.average_hash(img)
    dh = imagehash.dhash(img)
    return str(ph), str(ah), str(dh)


def compute_dom_hash(html: str) -> str:
    """Simple SHA-256 hash for DOM HTML."""
    return hashlib.sha256(html.encode("utf-8")).hexdigest()


def load_sites() -> list[dict]:
    with open("config/sites.json", "r", encoding="utf-8") as f:
        return json.load(f)


def click_consent_if_present(page) -> None:
    """
    Try a few common cookie/consent selectors & texts.
    Best-effort: ignore failures.
    """
    # Common cookie banner buttons by CSS selectors
    css_selectors = [
        "#onetrust-accept-btn-handler",
        "button#onetrust-accept-btn-handler",
        'button[aria-label="Accept cookies"]',
        'button[aria-label="Accept Cookies"]',
    ]
    # Common text labels
    text_labels = [
        "Accept all",
        "Accept All",
        "I agree",
        "Got it",
        "I accept",
    ]

    try:
        for sel in css_selectors:
            try:
                el = page.query_selector(sel)
                if el:
                    el.click(timeout=2000)
                    return
            except Exception:
                continue

        for txt in text_labels:
            try:
                locator = page.get_by_text(txt, exact=False)
                if locator.count() > 0:
                    locator.first.click(timeout=2000)
                    return
            except Exception:
                continue
    except Exception:
        # Completely best-effort; no need to break capture
        pass


def smooth_scroll(page) -> None:
    """
    Smoothly scroll the page to trigger lazy-loaded content.
    """
    try:
        page.evaluate(
            """
            async () => {
              await new Promise(resolve => {
                const totalHeight = document.body.scrollHeight || document.documentElement.scrollHeight;
                let scrolled = 0;
                const step = Math.max(200, Math.floor(window.innerHeight * 0.6));
                const timer = setInterval(() => {
                  window.scrollBy(0, step);
                  scrolled += step;
                  if (scrolled >= totalHeight - window.innerHeight) {
                    clearInterval(timer);
                    resolve();
                  }
                }, 200);
              });
            }
            """
        )
    except Exception:
        pass


def safe_goto(page, url: str, max_timeout_ms: int = 180_000) -> None:
    """
    More robust navigation:
    - Try networkidle, then load, then domcontentloaded.
    - Increase timeouts.
    Raise last error if all modes fail.
    """
    modes = [
        {"wait_until": "networkidle", "timeout": max_timeout_ms},
        {"wait_until": "load", "timeout": max_timeout_ms},
        {"wait_until": "domcontentloaded", "timeout": max_timeout_ms},
    ]

    last_error: Exception | None = None

    for m in modes:
        try:
            print(
                f"    [nav] goto({url}, wait_until={m['wait_until']}, timeout={m['timeout']}ms)"
            )
            page.goto(url, wait_until=m["wait_until"], timeout=m["timeout"])
            return
        except PlaywrightTimeoutError as e:
            print(f"    [nav] Timeout with wait_until={m['wait_until']}: {e}")
            last_error = e
        except Exception as e:
            print(f"    [nav] Error with wait_until={m['wait_until']}: {e}")
            last_error = e

    if last_error:
        raise last_error
    else:
        raise RuntimeError(f"Failed to navigate to {url} with all wait modes.")


def capture_site(page, url: str, out_dir: Path, base_name: str) -> tuple[Path, Path, str]:
    """
    Visit URL with robust navigation, handle cookie banners,
    scroll, then capture full-page screenshot and DOM HTML.
    Returns (screenshot_path, dom_path, html_string).
    """
    safe_goto(page, url)

    # Try to clear cookie banners
    click_consent_if_present(page)

    # Let things settle a bit
    page.wait_for_timeout(2500)

    # Trigger lazy-loaded sections
    smooth_scroll(page)
    page.wait_for_timeout(1000)

    screenshot_path = out_dir / f"{base_name}.png"
    page.screenshot(path=str(screenshot_path), full_page=True)

    html = page.content()
    dom_path = out_dir / f"{base_name}.html"
    dom_path.write_text(html, encoding="utf-8")

    return screenshot_path, dom_path, html


def main():
    gdrive_db_file_id = os.environ["GDRIVE_DB_FILE_ID"]
    screenshot_folder_id = os.environ["GDRIVE_SCREENSHOT_FOLDER_ID"]
    dom_folder_id = os.environ["GDRIVE_DOM_FOLDER_ID"]

    print("Starting capture_and_index run...")
    print(f"[DEBUG] GDRIVE_DB_FILE_ID: {gdrive_db_file_id!r}")
    print(f"[DEBUG] GDRIVE_SCREENSHOT_FOLDER_ID: {screenshot_folder_id!r}")
    print(f"[DEBUG] GDRIVE_DOM_FOLDER_ID: {dom_folder_id!r}")

    # 1) Download DB from Drive
    print("Downloading DB from Drive...")
    download_file(gdrive_db_file_id, str(DB_LOCAL_PATH))

    # 2) Open or repair DB schema
    try:
        conn = get_connection(DB_LOCAL_PATH)
        init_schema(conn)
        print("DB opened and schema initialized.")
    except sqlite3.DatabaseError as e:
        print(f"Downloaded file is not a valid SQLite DB ({e}). Recreating fresh DB...")
        if DB_LOCAL_PATH.exists():
            DB_LOCAL_PATH.unlink()
        conn = get_connection(DB_LOCAL_PATH)
        init_schema(conn)
        print("Fresh DB created and schema initialized.")

    sites = load_sites()
    run_ts = iso_now()

    out_dir = Path("artifacts")
    out_dir.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage", "--no-sandbox"],
        )
        context = browser.new_context(
            viewport={"width": 1366, "height": 768},
            device_scale_factor=2,
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/118 Safari/537.36"
            ),
            timezone_id="Asia/Kolkata",
            locale="en-US",
        )

        for site in sites:
            site_name = site["name"]
            url = site["url"]

            page = context.new_page()
            base_name = f"{site_name.replace(' ', '_')}_{datetime.now().strftime('%Y%m%dT%H%M%S')}"
            print(f"\nCapturing {site_name} – {url}")

            try:
                screenshot_path, dom_path, html = capture_site(
                    page, url, out_dir, base_name
                )
            except Exception as e:
                # IMPORTANT: don't kill entire workflow for one failing URL
                print(f"[ERROR] Failed to capture {site_name} ({url}): {e}")
                page.close()
                continue

            # 3) Upload screenshot & DOM to Google Drive
            screenshot_drive_id = upload_file(
                str(screenshot_path),
                folder_id=screenshot_folder_id,
                mime_type="image/png",
            )
            dom_drive_id = upload_file(
                str(dom_path),
                folder_id=dom_folder_id,
                mime_type="text/html",
            )

            print(f"Uploaded screenshot to Drive (id={screenshot_drive_id}).")
            print(f"Uploaded DOM snapshot to Drive (id={dom_drive_id}).")

            # 4) Compute hashes
            phash, ahash, dhash = compute_hashes(screenshot_path)
            dom_hash = compute_dom_hash(html)

            # 5) Insert into SQLite
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

            page.close()

        browser.close()

    conn.close()

    # 6) Upload updated DB back to Drive
    print("Syncing updated DB back to Drive...")
    upload_file(
        str(DB_LOCAL_PATH),
        file_id=gdrive_db_file_id,
        mime_type="application/x-sqlite3",
    )
    print("DB synced back to Drive.")
    print("capture_and_index run completed.")


if __name__ == "__main__":
    main()
