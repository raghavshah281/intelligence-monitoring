import os
import base64
from typing import List

from google import genai


def _get_client() -> "genai.Client":
    api_key = os.environ.get("LLM_API_KEY")
    if not api_key:
        raise RuntimeError("LLM_API_KEY is not set in environment.")
    # New Gemini API client
    return genai.Client(api_key=api_key)


def _encode_image_to_inline_part(path: str) -> dict:
    with open(path, "rb") as f:
        data = f.read()
    return {
        "inline_data": {
            "mime_type": "image/png",  # adjust if you ever use jpg
            "data": base64.b64encode(data).decode("ascii"),
        }
    }


def describe_visual_changes_with_gemini(
    image_paths: List[str],
) -> str:
    """
    Use Gemini 3 Pro to compare one or two screenshots
    and describe the main UX/UI differences.
    """
    if not image_paths:
        return ""

    client = _get_client()

    contents: list = []
    # If we have 2 images: treat first as "earlier", second as "later".
    if len(image_paths) >= 2:
        contents.append(_encode_image_to_inline_part(image_paths[0]))
        contents.append("This is the EARLIER version of the page (Version A).")
        contents.append(_encode_image_to_inline_part(image_paths[1]))
        contents.append(
            "This is the LATER version of the page (Version B). "
            "Compare Version A and Version B and describe the main UX/UI differences "
            "in a SHORT bullet list. Focus ONLY on concrete visible changes: "
            "hero headline, hero image, primary CTA text, CTA placement, "
            "navigation bar, layout density, new/removed sections. "
            "Avoid marketing fluff. Output 5–8 bullets max."
        )
    else:
        # Only one image – just ask for a UX description
        contents.append(_encode_image_to_inline_part(image_paths[0]))
        contents.append(
            "Describe the key UX/UI elements of this landing page (hero, CTA, layout) "
            "in a short bullet list."
        )

    response = client.models.generate_content(
        model="gemini-3-pro-preview",  # multimodal, good for image understanding
        contents=contents,
    )

    # response.text will contain plain text (bullets)
    return (response.text or "").strip()


def polish_summary_with_flash(raw_text: str, site_name: str, url: str) -> str:
    """
    Use Gemini 2.5 Flash to turn the raw bullet list into
    a concise ClickUp-ready summary.
    """
    client = _get_client()

    prompt = (
        f"You are summarizing weekly UI/UX changes for an internal experiment log.\n"
        f"Site: {site_name} ({url})\n\n"
        f"Here is a bullet list of visual differences between two versions of the page:\n"
        f"{raw_text}\n\n"
        "Write a concise summary (2–4 short bullet points) that:\n"
        "- Names the main changes (hero, CTA, layout, sections) in plain language.\n"
        "- Avoids speculation about business metrics (no mentions of CTR, conversion, etc.).\n"
        "- Is suitable to paste into a weekly product/marketing update.\n"
        "Do NOT repeat the site name or URL; just describe the changes.\n"
    )

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
    )

    return (response.text or "").strip()
