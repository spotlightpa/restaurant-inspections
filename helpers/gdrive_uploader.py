import os
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import google.auth

MIME_TYPES = {
    ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".csv": "text/csv",
    ".pdf": "application/pdf",
}

def get_drive_service():
    creds, project = google.auth.default(scopes=["https://www.googleapis.com/auth/drive"])
    print(f"[GDrive] Auth project: {project}")
    print(f"[GDrive] Creds type: {type(creds).__name__}")
    return build("drive", "v3", credentials=creds)


def get_or_create_subfolder(county_slug, parent_folder_id=None):
    """Get or create a subfolder named after the county inside the parent folder."""
    if parent_folder_id is None:
        parent_folder_id = os.getenv("GDRIVE_FOLDER_ID")
    if not parent_folder_id:
        raise ValueError("GDRIVE_FOLDER_ID is not set.")

    service = get_drive_service()
    folder_name = county_slug.title()

    query = (
        f"name='{folder_name}' and '{parent_folder_id}' in parents "
        f"and mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    results = service.files().list(
        q=query,
        fields="files(id, name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
    ).execute()
    existing = results.get("files", [])

    if existing:
        folder_id = existing[0]["id"]
        print(f"[GDrive] Found existing subfolder '{folder_name}' (id={folder_id})")
        return folder_id

    metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_folder_id]
    }
    folder = service.files().create(
        body=metadata,
        fields="id",
        supportsAllDrives=True
    ).execute()
    folder_id = folder["id"]
    print(f"[GDrive] Created subfolder '{folder_name}' (id={folder_id})")
    return folder_id


def upload_to_gdrive(file_path, folder_id=None, filename_override=None):
    if folder_id is None:
        folder_id = os.getenv("GDRIVE_FOLDER_ID")
    if not folder_id:
        raise ValueError("GDRIVE_FOLDER_ID is not set.")

    service = get_drive_service()
    filename = filename_override or os.path.basename(file_path)
    ext = os.path.splitext(filename)[1]
    mime_type = MIME_TYPES.get(ext, "application/octet-stream")

    print(f"[GDrive] Uploading: {filename} ({mime_type}) to folder: {folder_id}")
    print(f"[GDrive] File size: {os.path.getsize(file_path)} bytes")

    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(
        q=query,
        fields="files(id, name)",
        includeItemsFromAllDrives=True,
        supportsAllDrives=True
    ).execute()
    existing = results.get("files", [])
    print(f"[GDrive] Existing files found: {len(existing)}")

    media = MediaFileUpload(file_path, mimetype=mime_type, resumable=False)

    if existing:
        file_id = existing[0]["id"]
        print(f"[GDrive] Found existing file id={file_id}, updating...")
        try:
            service.files().update(
                fileId=file_id,
                media_body=media,
                supportsAllDrives=True
            ).execute()
            print(f"[GDrive] Updated: {filename} (id={file_id})")
        except Exception as e:
            print(f"[GDrive] Update failed: {e}")
            raise
    else:
        print(f"[GDrive] No existing file found, creating new...")
        metadata = {"name": filename, "parents": [folder_id]}
        try:
            result = service.files().create(
                body=metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True
            ).execute()
            file_id = result["id"]
            print(f"[GDrive] Created: {filename} (id={file_id})")
            print(f"[GDrive] URL: https://drive.google.com/file/d/{file_id}/view")
        except Exception as e:
            print(f"[GDrive] Create failed: {e}")
            print(f"[GDrive] metadata was: {metadata}")
            raise