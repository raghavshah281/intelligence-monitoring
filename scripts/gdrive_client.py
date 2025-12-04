import os
import io
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

SCOPES = ["https://www.googleapis.com/auth/drive"]


def get_drive_service():
    info = json.loads(os.environ["GDRIVE_SERVICE_ACCOUNT_JSON"])
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)


def download_file(file_id: str, dest_path: str):
    service = get_drive_service()
    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(dest_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()
        # you could log status.progress() here if needed


def upload_file(
    file_path: str,
    file_id: str | None = None,
    folder_id: str | None = None,
    mime_type: str = "application/octet-stream",
) -> str:
    service = get_drive_service()

    media = MediaIoBaseUpload(
        open(file_path, "rb"), mimetype=mime_type, resumable=True
    )

    if file_id:
        updated = service.files().update(fileId=file_id, media_body=media).execute()
        return updated["id"]
    else:
        file_metadata: dict = {"name": os.path.basename(file_path)}
        if folder_id:
            file_metadata["parents"] = [folder_id]
        created = (
            service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )
        return created["id"]

