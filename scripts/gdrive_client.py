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
    print(f"[DEBUG] Using service account: {info.get('client_email')!r}")
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


def download_file(file_id: str, dest_path: str):
    """
    Download a file from Google Drive by its file ID to a local path.
    Works for both My Drive and Shared Drives.
    """
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(dest_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        # print(f"Download {int(status.progress() * 100)}%")
    fh.close()


def upload_file(
    file_path: str,
    file_id: str | None = None,
    folder_id: str | None = None,
    mime_type: str = "application/octet-stream",
) -> str:
    """
    Upload a local file to Google Drive.

    - If file_id is provided: update that existing file (used for the SQLite DB).
    - If file_id is None: create a new file in the folder given by folder_id
      (this folder must exist in a My Drive or Shared Drive that the service account can access).
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
                .update(
                    fileId=file_id,
                    media_body=media,
                    fields="id",
                    supportsAllDrives=True,
                )
                .execute()
            )
            return updated["id"]
        except HttpError as e:
            print(f"[Drive] Error updating file with ID={file_id}.")
            print(f"[Drive] HTTP error: {e}")
            raise

    # --- Create new file in a specific folder (screenshots / DOM) ---
    if not folder_id:
        raise ValueError(
            "folder_id is required when creating new files; "
            "service account cannot use its own root (no storage quota)."
        )

    file_metadata = {
        "name": os.path.basename(file_path),
        "parents": [folder_id],
    }

    try:
        created = (
            service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            )
            .execute()
        )
        return created["id"]
    except HttpError as e:
        print(
            f"[Drive] Error creating file in folder {folder_id!r}. "
            f"Check that this folder ID is correct and that the service account is a member of the Shared Drive."
        )
        print(f"[Drive] HTTP error: {e}")
        raise
