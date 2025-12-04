import os
import requests


CLICKUP_API_BASE = "https://api.clickup.com/api/v2"


def post_task_comment(task_id: str, text: str):
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
    resp.raise_for_status()
    return resp.json()
