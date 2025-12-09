import os
import json
import hashlib
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from pathlib import Path

from bs4 import BeautifulSoup

from gdrive_client import download_file, upload_file
from db import (
    get_connection,
    init_schema,
    get_weekly_snapshots,
    get_dom_features,
    upsert_dom_features,
)
from clickup_client import post_task_comment
from ai_client import summarise_dom_variants_with_flash


DB_LOCAL_PATH = Path("ab_tracker.db")


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------- DOM feature extraction ----------


CTA_KEYWORDS = [
    "get started",
    "start free",
    "start trial",
    "try for free",
    "try it free",
    "sign up",
    "sign in",
    "book a demo",
    "request demo",
    "talk to sales",
    "contact sales",
    "join now",
    "start now",
    "learn more",
]


def normalize_space(text: str) -> str:
    return " ".join(text.split())


def extract_dom_features_from_html(html: str) -> dict:
    """
    Heuristic hero + CTA + sections extraction that does NOT rely solely on <h1>.
    Returns:
      {
        "hero_heading": str,
        "hero_subheading": str,
        "hero_cta_text": str,
        "hero_cta_href": str,
        "main_sections": [str, ...]
      }
    """
    soup = BeautifulSoup(html, "lxml")

    body = soup.body or soup
    # Remove scripts/styles/noscript
    for tag in body.find_all(["script", "style", "noscript"]):
        tag.decompose()

    # Collect candidate hero blocks
    candidates = []
    for idx, el in enumerate(
        body.find_all(["section", "header", "main", "div", "article"], recursive=True)
    ):
        if idx > 80:  # limit search near top of page
            break
        text = normalize_space(el.get_text(separator=" ", strip=True))
        if len(text) < 30:
            continue  # skip tiny blocks

        # Compute some signals
        classes = " ".join(el.get("class", [])).lower()
        el_id = (el.get("id") or "").lower()

        has_hero_class = any(
            kw in classes or kw in el_id for kw in ["hero", "banner", "jumbotron"]
        )
        has_media = el.find(["img", "picture", "video"]) is not None

        # Heading candidates inside this block
        heading_el = el.find(["h1", "h2", "h3"])
        heading_text = ""
        if heading_el:
            heading_text = normalize_space(heading_el.get_text(" ", strip=True))

        # Fallback: treat first decent <p> as heading if no <h1-3>
        if not heading_text:
            p = el.find("p")
            if p:
                pt = normalize_space(p.get_text(" ", strip=True))
                if 15 <= len(pt) <= 160:
                    heading_text = pt

        # CTA candidate inside this block
        cta_el = None
        cta_text = ""
        cta_href = ""
        for btn in el.find_all(["a", "button"]):
            label = normalize_space(btn.get_text(" ", strip=True)).lower()
            if any(kw in label for kw in CTA_KEYWORDS):
                cta_el = btn
                cta_text = normalize_space(btn.get_text(" ", strip=True))
                cta_href = btn.get("href") or ""
                break

        # Position score: earlier elements score higher
        pos_score = max(0, 5 - idx // 3)
        heading_score = 2 if heading_text else 0
        cta_score = 3 if cta_el else 0
        hero_class_score = 1 if has_hero_class else 0
        visual_score = 1 if has_media else 0

        hero_score = pos_score + heading_score + cta_score + hero_class_score + visual_score

        candidates.append(
            {
                "idx": idx,
                "element": el,
                "text": text,
                "hero_score": hero_score,
                "heading_text": heading_text,
                "cta_text": cta_text,
                "cta_href": cta_href,
            }
        )

    hero_heading = ""
    hero_subheading = ""
    hero_cta_text = ""
    hero_cta_href = ""

    if candidates:
        best = max(candidates, key=lambda c: c["hero_score"])
        hero_heading = best["heading_text"] or best["text"][:200]
        hero_cta_text = best["cta_text"]
        hero_cta_href = best["cta_href"]

        # Subheading: try to find first <p> after the heading element
        # If not found, skip
        el = best["element"]
        if el:
            # naive subheading: first <p> inside hero block
            p = el.find("p")
            if p:
                sub = normalize_space(p.get_text(" ", strip=True))
                if sub != hero_heading:
                    hero_subheading = sub

    # Collect main section headings (h2/h3) across body
    main_sections = []
    for h in body.find_all(["h2", "h3"]):
        txt = normalize_space(h.get_text(" ", strip=True))
        if txt and txt not in main_sections:
            main_sections.append(txt)
        if len(main_sections) >= 8:
            break

    return {
        "hero_heading": hero_heading,
        "hero_subheading": hero_subheading,
        "hero_cta_text": hero_cta_text,
        "hero_cta_href": hero_cta_href,
        "main_sections": main_sections,
    }


def compute_variant_key(
    hero_heading: str,
    hero_cta_text: str,
    fallback: str | None = None,
) -> str | None:
    """
    Build a stable variant key from hero + CTA; if both are empty,
    fall back to dom_hash or None.
    """
    base = ""
    hh = (hero_heading or "").strip().lower()
    hc = (hero_cta_text or "").strip().lower()

    if hh or hc:
        base = f"{hh}|{hc}"
    elif fallback:
        base = fallback.strip().lower()
    else:
        return None

    h = hashlib.sha256(base.encode("utf-8")).hexdigest()
    return h


def ensure_dom_features_for_snapshot(conn, snapshot_row, tmp_dir: Path) -> dict:
    """
    Ensure snapshot_dom_features exists for this snapshot_id.
    Returns a dict of features (hero + CTA + sections + variant_key).
    """
    snapshot_id = int(snapshot_row["id"])
    existing = get_dom_features(conn, snapshot_id)
    if existing:
        return dict(existing)

    dom_drive_id = snapshot_row["dom_drive_id"]
    dom_local_path = tmp_dir / f"{snapshot_id}.html"
    print(f"[DOM] Downloading HTML for snapshot {snapshot_id} from Drive id={dom_drive_id}...")
    download_file(dom_drive_id, str(dom_local_path))

    html = dom_local_path.read_text(encoding="utf-8", errors="ignore")
    feats = extract_dom_features_from_html(html)

    hero_heading = feats.get("hero_heading") or ""
    hero_subheading = feats.get("hero_subheading") or ""
    hero_cta_text = feats.get("hero_cta_text") or ""
    hero_cta_href = feats.get("hero_cta_href") or ""
    main_sections = feats.get("main_sections") or []

    main_sections_json = json.dumps(main_sections, ensure_ascii=False)

    variant_key = compute_variant_key(
        hero_heading,
        hero_cta_text,
        fallback=snapshot_row["dom_hash"],
    )

    upsert_dom_features(
        conn,
        snapshot_id=snapshot_id,
        hero_heading=hero_heading,
        hero_subheading=hero_subheading,
        hero_cta_text=hero_cta_text,
        hero_cta_href=hero_cta_href,
        main_sections_json=main_sections_json,
        variant_key=variant_key,
    )

    return {
        "snapshot_id": snapshot_id,
        "hero_heading": hero_heading,
        "hero_subheading": hero_subheading,
        "hero_cta_text": hero_cta_text,
        "hero_cta_href": hero_cta_href,
        "main_sections_json": main_sections_json,
        "variant_key": variant_key,
    }


# ---------- Variant clustering & message building ----------


def build_site_variants(snapshots_with_features):
    """
    Group snapshots into variants based on variant_key.
    Returns dict[variant_key] = {
        "hero_heading", "hero_cta_text",
        "count", "first_ts", "last_ts",
        "example_screenshot_id",
        "main_sections"
    }
    """
    variants = {}

    for s in snapshots_with_features:
        sid = s["id"]
        captured_at = s["captured_at"]
        screenshot_id = s["screenshot_drive_id"]
        feats = s["dom_features"]

        vk = feats["variant_key"]
        if not vk:
            # treat all 'no key' as one bucket
            vk = "__no_variant_key__"

        hero_heading = feats.get("hero_heading") or ""
        hero_cta_text = feats.get("hero_cta_text") or ""
        main_sections = json.loads(feats.get("main_sections_json") or "[]")

        if vk not in variants:
            variants[vk] = {
                "hero_heading": hero_heading,
                "hero_cta_text": hero_cta_text,
                "count": 0,
                "first_ts": captured_at,
                "last_ts": captured_at,
                "example_screenshot_id": screenshot_id,
                "main_sections": main_sections,
            }

        v = variants[vk]
        v["count"] += 1
        if captured_at < v["first_ts"]:
            v["first_ts"] = captured_at
        if captured_at > v["last_ts"]:
            v["last_ts"] = captured_at
            v["example_screenshot_id"] = screenshot_id
        # For headings/CTA, if later snapshots have non-empty, we can overwrite
        if hero_heading and not v["hero_heading"]:
            v["hero_heading"] = hero_heading
        if hero_cta_text and not v["hero_cta_text"]:
            v["hero_cta_text"] = hero_cta_text

    return variants


def build_raw_variant_text_for_ai(variants: dict) -> str:
    """
    Create a machine-readable but human-friendly text block
    describing all variants for a site.
    """
    lines = []
    for idx, (vk, v) in enumerate(variants.items(), start=1):
        if vk == "__no_variant_key__":
            label = f"Variant {idx} (no stable DOM key)"
        else:
            label = f"Variant {idx}"

        hero_h = v["hero_heading"] or "(no clear hero heading)"
        cta = v["hero_cta_text"] or "(no clear primary CTA)"
        first_ts = v["first_ts"]
        last_ts = v["last_ts"]
        count = v["count"]
        secs = v.get("main_sections") or []

        lines.append(
            f"{label}: seen {count} time(s) between {first_ts} and {last_ts}.\n"
            f"- Hero heading: \"{hero_h}\"\n"
            f"- Primary CTA: \"{cta}\"\n"
        )
        if secs:
            section_str = "; ".join(secs[:5])
            lines.append(f"- Key sections (h2/h3): {section_str}\n")

    return "\n".join(lines)


def build_clickup_message(per_site_summaries, week_start, week_end):
    header = f"Weekly AB / UX Watch – Week of {week_start.date()} to {week_end.date()}\n\n"

    if not per_site_summaries:
        return header + "No observable data captured."

    lines = [header]

    for entry in per_site_summaries:
        site_name = entry["site_name"]
        url = entry["url"]
        summary_text = entry["summary_text"]
        variants = entry["variants"]

        lines.append(f"**{site_name}** – {url}")
        lines.append(summary_text)

        # List variants with example screenshot links
        for idx, (vk, v) in enumerate(variants.items(), start=1):
            hero_h = v["hero_heading"] or "(no clear hero heading)"
            cta = v["hero_cta_text"] or "(no clear primary CTA)"
            ss_id = v["example_screenshot_id"]
            ss_link = f"https://drive.google.com/file/d/{ss_id}/view"

            lines.append(
                f"  - Variant {idx}: hero=\"{hero_h}\" | CTA=\"{cta}\" "
                f"(example screenshot: {ss_link})"
            )

        lines.append("")  # blank line between sites

    return "\n".join(lines)


# ---------- Main ----------


def main():
    gdrive_db_file_id = os.environ["GDRIVE_DB_FILE_ID"]
    clickup_task_id = os.environ["CLICKUP_TASK_ID"]

    # 1) Download DB
    download_file(gdrive_db_file_id, str(DB_LOCAL_PATH))

    # 2) Open DB
    conn = get_connection(DB_LOCAL_PATH)
    init_schema(conn)

    # 3) Determine week range (last 7 days)
    now = datetime.now(timezone.utc)
    week_end = now
    week_start = now - timedelta(days=7)

    since_iso = week_start.isoformat()
    rows = get_weekly_snapshots(conn, since_iso)

    # 4) Ensure DOM features for each snapshot, group by site
    tmp_dir = Path("weekly_dom_tmp")
    tmp_dir.mkdir(exist_ok=True)

    per_site_snapshots = defaultdict(list)

    for r in rows:
        feats = ensure_dom_features_for_snapshot(conn, r, tmp_dir)
        snap = dict(r)
        snap["dom_features"] = feats
        key = (snap["site_name"], snap["url"])
        per_site_snapshots[key].append(snap)

    per_site_summaries = []

    for (site_name, url), snaps in per_site_snapshots.items():
        # Build variants
        variants = build_site_variants(snaps)

        if not variants:
            summary_text = "No DOM features or variants could be extracted for this page."
        elif len(variants) == 1:
            # Stable hero/CTA
            v = next(iter(variants.values()))
            hero_h = v["hero_heading"] or "(no clear hero heading)"
            cta = v["hero_cta_text"] or "(no clear primary CTA)"
            summary_text = (
                "No significant hero or primary CTA changes detected this week. "
                f"Hero remained \"{hero_h}\" with CTA \"{cta}\"."
            )
        else:
            # Multiple variants – use AI to summarize
            raw_text = build_raw_variant_text_for_ai(variants)
            try:
                ai_summary = summarise_dom_variants_with_flash(site_name, url, raw_text)
                summary_text = ai_summary or raw_text
            except Exception as e:
                print(f"[AI] Error summarizing {site_name}: {e}. Falling back to raw text.")
                summary_text = raw_text

        per_site_summaries.append(
            {
                "site_name": site_name,
                "url": url,
                "summary_text": summary_text,
                "variants": variants,
            }
        )

    # 5) Build final message
    message = build_clickup_message(per_site_summaries, week_start, week_end)

    # 6) Post single comment to ClickUp task
    print("Posting weekly comment to ClickUp...")
    post_task_comment(clickup_task_id, message)
    print("Posted.")

    conn.close()

    # 7) Upload DB back (now enriched with snapshot_dom_features)
    upload_file(
        str(DB_LOCAL_PATH),
        file_id=gdrive_db_file_id,
        mime_type="application/x-sqlite3",
    )


if __name__ == "__main__":
    main()
