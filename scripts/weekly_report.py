import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict

from gdrive_client import download_file, upload_file
from db import get_connection, init_schema, get_weekly_snapshots
from clickup_client import post_task_comment


DB_LOCAL_PATH = "ab_tracker.db"


def hamming_distance_hex(h1: str, h2: str) -> int:
    # imagehash uses hex strings; convert to int and xor
    try:
        return bin(int(h1, 16) ^ int(h2, 16)).count("1")
    except ValueError:
        return 64  # max distance for 64-bit hash


def infer_site_summary(rows):
    """
    Very simple heuristic:
    - if only one cluster of hashes: "no observable layout changes"
    - if multiple: "observed multiple visual variants"
    """

    if not rows:
        return {"status": "no_data", "details": "", "examples": []}

    # cluster by phash with a loose threshold
    clusters = []  # list of (cluster_hash, [rows])

    for r in rows:
        assigned = False
        for chash, bucket in clusters:
            if r["phash"] and hamming_distance_hex(r["phash"], chash) <= 5:
                bucket.append(r)
                assigned = True
                break
        if not assigned:
            clusters.append((r["phash"], [r]))

    if len(clusters) == 1:
        # treat as stable layout
        return {
            "status": "no_observable_change",
            "details": "No significant visual layout variants detected this week.",
            "examples": [clusters[0][1][-1]],  # latest snapshot
        }

    # multiple clusters: treat as probable variant changes
    # pick one example row from each cluster
    examples = [bucket[-1] for _, bucket in clusters]

    return {
        "status": "has_changes",
        "details": f"Detected {len(clusters)} distinct visual variants based on screenshot hashes.",
        "examples": examples,
    }


def build_clickup_message(per_site_summaries, week_start, week_end):
    if not per_site_summaries:
        return f"Weekly AB / UX Watch – Week of {week_start.date()} to {week_end.date()}\n\nNo observable data captured."

    any_changes = any(s["summary"]["status"] == "has_changes" for s in per_site_summaries)

    header = f"Weekly AB / UX Watch – Week of {week_start.date()} to {week_end.date()}\n\n"

    if not any_changes:
        # satisfy your requirement: plain words if nothing interesting
        return header + "No observable layout or AB-type changes across monitored pages this week."

    lines = [header]

    for entry in per_site_summaries:
        site_name = entry["site_name"]
        url = entry["url"]
        summary = entry["summary"]

        lines.append(f"**{site_name}** – {url}")
        lines.append(f"- {summary['details']}")

        # show some Drive links (raw IDs for now; you can wrap in full URLs)
        for ex in summary["examples"]:
            screenshot_id = ex["screenshot_drive_id"]
            # standard Google Drive file link:
            screenshot_link = f"https://drive.google.com/file/d/{screenshot_id}/view"
            lines.append(f"  - Example variant screenshot: {screenshot_link}")

        lines.append("")  # blank line between sites

    lines.append("_Note: This is an automated summary based on screenshot hash differences. Deeper qualitative analysis can be added via an AI step here._")

    return "\n".join(lines)


def main():
    gdrive_db_file_id = os.environ["GDRIVE_DB_FILE_ID"]
    clickup_task_id = os.environ["CLICKUP_TASK_ID"]

    # 1) Download DB
    download_file(gdrive_db_file_id, DB_LOCAL_PATH)

    # 2) Open DB
    conn = get_connection()
    init_schema(conn)

    # 3) Determine week range (last 7 days)
    now = datetime.now(timezone.utc)
    week_end = now
    week_start = now - timedelta(days=7)

    rows = get_weekly_snapshots(conn, week_start.isoformat())

    # group by (site_name, url)
    per_site = defaultdict(list)
    for r in rows:
        key = (r["site_name"], r["url"])
        per_site[key].append(r)

    per_site_summaries = []
    for (site_name, url), srows in per_site.items():
        summary = infer_site_summary(srows)
        per_site_summaries.append(
            {
                "site_name": site_name,
                "url": url,
                "summary": summary,
            }
        )

    message = build_clickup_message(per_site_summaries, week_start, week_end)

    # 4) Post single comment to ClickUp task
    print("Posting weekly comment to ClickUp...")
    post_task_comment(clickup_task_id, message)
    print("Posted.")

    conn.close()
    # 5) Upload DB back (unchanged in this script, but safe to keep sync)
    upload_file(DB_LOCAL_PATH, file_id=gdrive_db_file_id, mime_type="application/x-sqlite3")


if __name__ == "__main__":
    main()
