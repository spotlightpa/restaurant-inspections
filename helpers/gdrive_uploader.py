import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import google.auth

GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")

MIME_TYPES = {
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".csv": "text/csv",
    ".pdf": "application/pdf",
}

def get_drive_service():
    creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/drive"])
    return build("drive", "v3", credentials=creds)


def upload_to_gdrive(file_path, folder_id=GDRIVE_FOLDER_ID):
    """Upload a file to Google Drive, replacing any existing file with the same name."""
    service = get_drive_service()
    filename = os.path.basename(file_path)
    ext = os.path.splitext(filename)[1]
    mime_type = MIME_TYPES.get(ext, "application/octet-stream")

    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    existing = results.get("files", [])

    media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)

    if existing:
        file_id = existing[0]["id"]
        try:
            service.files().update(fileId=file_id, media_body=media).execute()
            print(f"Updated existing GDrive file: {filename} (id={file_id})")
        except Exception as update_err:
            print(f"Update failed ({update_err}), creating new file instead.")
            metadata = {"name": filename, "parents": [folder_id]}
            result = service.files().create(
                body=metadata, media_body=media, fields="id"
            ).execute()
            print(f"Uploaded new GDrive file: {filename} (id={result['id']})")
    else:
        metadata = {"name": filename, "parents": [folder_id]}
        result = service.files().create(
            body=metadata, media_body=media, fields="id"
        ).execute()
        print(f"Uploaded new GDrive file: {filename} (id={result['id']})")