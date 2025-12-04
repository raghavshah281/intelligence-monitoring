import os
import requests

# Allow overriding the base URL via env (staging vs prod).
# For staging you will set: CLICKUP_API_BASE_URL = "https://api.clickup-stg.com/api/v2"
CLICKUP_API_BASE = os.getenv("CLICKUP_API_BASE_URL", "https://api.clickup.com/api/v2")


def post_task_comment(task_id: str, text: str):
    """
    Post a single comment to a ClickUp task.

    Requires:
      - CLICKUP_API_TOKEN in environment
      - (optional) CLICKUP_API_BASE_URL in environment if you want staging
    """
    token = os.environ["CLICKUP_API_TOKEN"]
    url = f"{CLICKUP_API_BASE}/task/{task_id}/comment"

    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
    }

    payload = {
        "comment_text": text,
        "notify_all": False,
    }

    resp = requests.post(url, json=payload, headers=headers, timeout=30)

    if resp.status_code >= 400:
        # Helpful debug info in logs
        print(f"[ClickUp] POST {url} returned {resp.status_code}")
        try:
            print("[ClickUp] Response body:", resp.text)
        except Exception:
            pass
        resp.raise_for_status()

    return resp.json()
