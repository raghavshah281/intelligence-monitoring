import os
from google import genai


def _get_client() -> "genai.Client":
    api_key = os.environ.get("LLM_API_KEY")
    if not api_key:
        raise RuntimeError("LLM_API_KEY is not set in environment.")
    return genai.Client(api_key=api_key)


def summarise_dom_variants_with_flash(
    site_name: str,
    url: str,
    raw_variant_text: str,
) -> str:
    """
    Use Gemini Flash to turn raw DOM variant descriptions into
    a concise weekly UX summary for this site.
    """
    client = _get_client()

    prompt = (
        f"You are summarizing weekly UI/UX changes for an internal experiment log.\n"
        f"Site: {site_name} ({url})\n\n"
        f"Below are DOM-based variant descriptions observed this week. Each variant "
        f"includes the hero heading, primary CTA text, and sometimes key sections.\n\n"
        f"{raw_variant_text}\n\n"
        "Write a concise summary (2–5 short bullet points) that:\n"
        "- Highlights how the hero and primary CTA changed (or stayed stable).\n"
        "- Mentions any major section-level changes if visible (e.g., pricing, features, signup).\n"
        "- Avoids speculation about business metrics (no mentions of CTR, conversion, etc.).\n"
        "- Is suitable to paste into a weekly product/marketing update.\n"
        "Do NOT repeat the site name or URL; just describe the observed changes.\n"
    )

    response = client.models.generate_content(
        model="gemini-1.5-flash",
        contents=prompt,
    )

    return (response.text or "").strip()
