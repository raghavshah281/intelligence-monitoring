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
        # print(f"Download {int(status.progress() * 100)}%")  # optional
    fh.close()


def upload_file(
    file_path: str,
    file_id: str | None = None,
    folder_id: str | None = None,
    mime_type: str = "application/octet-stream",
) -> str:
    """
    Upload a local file to Google Drive.

    - If file_id is provided: update that existing file (used for SQLite DB).
      If this fails, we raise an error: DB sync must not silently fail.
    - If file_id is None: create a new file (used for screenshots/DOM).
      If folder_id is invalid or inaccessible (404), we fall back to creating
      the file in the root of the Drive instead of failing the whole run.
    """
    service = get_drive_service()

    media = MediaIoBaseUpload(
        open(file_path, "rb"), mimetype=mime_type, resumable=True
    )

    # --- Case 1: update existing file (DB) ---
    if file_id:
        try:
            updated = (
                service.files()
                .update(fileId=file_id, media_body=media, fields="id")
                .execute()
            )
            return updated["id"]
        except HttpError as e:
            # DB file is critical; if we can't update it, surface the error.
            print(f"[Drive] Error updating file with ID={file_id}.")
            print(f"[Drive] HTTP error: {e}")
            raise

    # --- Case 2: create new file (screenshots / DOM snapshots) ---
    file_metadata: dict = {"name": os.path.basename(file_path)}

    if folder_id:
        file_metadata["parents"] = [folder_id]

    try:
        created = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        return created["id"]
    except HttpError as e:
        # If folder doesn't exist or isn't accessible, Drive returns 404.
        if folder_id and e.resp is not None and e.resp.status == 404:
            print(
                f"[Drive] Folder ID {folder_id!r} not found or inaccessible. "
                f"Falling back to creating file in root. Error: {e}"
            )
            # Remove parents, retry in root
            file_metadata.pop("parents", None)
            created = (
                service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            return created["id"]

        print("[Drive] Error creating file in Drive (no fallback possible).")
        print(f"[Drive] HTTP error: {e}")
        raise
