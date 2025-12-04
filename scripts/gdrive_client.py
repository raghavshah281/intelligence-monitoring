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
        # You can log progress here if desired:
        # print(f"Download {int(status.progress() * 100)}%.")
    fh.close()


def upload_file(
    file_path: str,
    file_id: str | None = None,
    folder_id: str | None = None,
    mime_type: str = "application/octet-stream",
) -> str:
    """
    Upload a local file to Google Drive.

    - If file_id is provided, update that existing file.
    - Else, create a new file. If folder_id is provided, attempt to put it in that folder.
      If the folder_id is invalid or not found (404), falls back to creating in root.
    """
    service = get_drive_service()

    media = MediaIoBaseUpload(
        open(file_path, "rb"), mimetype=mime_type, resumable=True
    )

    # Case 1: update existing file by ID (used for the SQLite DB)
    if file_id:
        try:
            updated = (
                service.files()
                .update(fileId=file_id, media_body=media, fields="id")
                .execute()
            )
            return updated["id"]
        except HttpError as e:
            # For the DB file, if this fails, we WANT to know.
            print(f"[Drive] Error updating file {file_id}: {e}")
            raise

    # Case 2: create new file (used for screenshots/DOM)
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
        # If the folder_id is invalid (404), fall back to root
        if folder_id and e.resp is not None and e.resp.status == 404:
            print(
                f"[Drive] Folder ID {folder_id} not found or inaccessible. "
                f"Creating file in root instead. Error: {e}"
            )
            file_metadata.pop("parents", None)
            created = (
                service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            return created["id"]

        print(f"[Drive] Error creating file in Drive: {e}")
        raise
