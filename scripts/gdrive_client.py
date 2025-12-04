import os
import io
import json

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from googleapiclient.errors import HttpError

SCOPES = ["https://www.googleapis.com/auth/drive"]


def get_drive_service():
    """
    Build and return a Google Drive service client using the service account JSON
    from the GDRIVE_SERVICE_ACCOUNT_JSON environment variable.
    """
    info = json.loads(os.environ["GDRIVE_SERVICE_ACCOUNT_JSON"])
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


def download_file(file_id: str, dest_path: str):
    """
    Download a file from Google Drive by its file ID to a local path.
    """
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(dest_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        # Optional: print progress
        # print(f"Download {int(status.progress() * 100)}%")
    fh.close()


def upload_file(
    file_path: str,
    file_id: str | None = None,
    mime_type: str = "application/octet-stream",
) -> str:
    """
    Upload a local file to Google Drive.

    - If file_id is provided: update that existing file (used for the SQLite DB).
    - If file_id is None: create a new file in the ROOT of the Drive
      (used for screenshots and DOM snapshots).
    """
    service = get_drive_service()

    media = MediaIoBaseUpload(
        open(file_path, "rb"), mimetype=mime_type, resumable=True
    )

    # --- Update existing file (DB) ---
    if file_id:
        try:
            updated = (
                service.files()
                .update(fileId=file_id, media_body=media, fields="id")
                .execute()
            )
            return updated["id"]
        except HttpError as e:
            print(f"[Drive] Error updating file with ID={file_id}.")
            print(f"[Drive] HTTP error: {e}")
            raise

    # --- Create new file in root (screenshots / DOM) ---
    file_metadata = {"name": os.path.basename(file_path)}

    try:
        created = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        return created["id"]
    except HttpError as e:
        print("[Drive] Error creating file in Drive root.")
        print(f"[Drive] HTTP error: {e}")
        raise
