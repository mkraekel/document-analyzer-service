"""
Google Drive Integration
Downloads files from shared Google Drive folders for document analysis.
Uses Google Service Account credentials.
"""

import io
import json
import logging
import os
import re
from typing import Optional

logger = logging.getLogger(__name__)

# Supported file extensions for document analysis
SUPPORTED_EXTENSIONS = {"pdf", "jpg", "jpeg", "png", "webp", "gif", "tiff", "tif"}

MIME_MAP = {
    "pdf": "application/pdf",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "tiff": "image/tiff",
    "tif": "image/tiff",
    "webp": "image/webp",
    "gif": "image/gif",
}

# Google Drive API scopes
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def _get_service():
    """Build Google Drive API service using Service Account credentials."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
    if not creds_json:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON not configured")

    creds_info = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(
        creds_info, scopes=SCOPES
    )
    return build("drive", "v3", credentials=credentials)


def extract_drive_ids(links: list[str]) -> list[dict]:
    """
    Extract folder/file IDs from Google Drive URLs.

    Supports:
    - https://drive.google.com/drive/folders/FOLDER_ID
    - https://drive.google.com/drive/folders/FOLDER_ID?usp=sharing
    - https://drive.google.com/file/d/FILE_ID/view
    - https://drive.google.com/open?id=ID
    """
    results = []
    seen = set()

    for link in links:
        if not link or not isinstance(link, str):
            continue

        drive_id = None
        id_type = "unknown"

        # Folder URL
        m = re.search(r"/folders/([a-zA-Z0-9_-]+)", link)
        if m:
            drive_id = m.group(1)
            id_type = "folder"

        # File URL
        if not drive_id:
            m = re.search(r"/file/d/([a-zA-Z0-9_-]+)", link)
            if m:
                drive_id = m.group(1)
                id_type = "file"

        # Generic ?id= parameter
        if not drive_id:
            m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", link)
            if m:
                drive_id = m.group(1)

        if drive_id and drive_id not in seen:
            seen.add(drive_id)
            results.append({"id": drive_id, "type": id_type, "url": link})

    return results


def list_files_in_folder(folder_id: str, max_depth: int = 4) -> list[dict]:
    """
    List all files in a Google Drive folder recursively.

    Returns list of dicts: {id, name, mimeType, size}
    Handles sub-folders up to max_depth levels deep.
    """
    service = _get_service()
    return _list_recursive(service, folder_id, depth=0, max_depth=max_depth)


def _list_recursive(service, folder_id: str, depth: int, max_depth: int) -> list[dict]:
    """Recursively list files in a folder."""
    all_files = []

    try:
        page_token = None
        while True:
            response = service.files().list(
                q=f"'{folder_id}' in parents and trashed = false",
                fields="nextPageToken, files(id, name, mimeType, size)",
                pageSize=100,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                pageToken=page_token,
            ).execute()

            items = response.get("files", [])

            for item in items:
                if item["mimeType"] == "application/vnd.google-apps.folder":
                    if depth < max_depth:
                        sub_files = _list_recursive(
                            service, item["id"], depth + 1, max_depth
                        )
                        all_files.extend(sub_files)
                    else:
                        logger.warning(
                            f"Skipping sub-folder '{item['name']}' at max depth {max_depth}"
                        )
                else:
                    all_files.append(item)

            page_token = response.get("nextPageToken")
            if not page_token:
                break

    except Exception as e:
        logger.error(f"Error listing folder {folder_id}: {e}")

    return all_files


def download_file(file_id: str) -> bytes:
    """Download a file from Google Drive by ID. Returns raw bytes."""
    from googleapiclient.http import MediaIoBaseDownload

    service = _get_service()
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    buffer.seek(0)
    return buffer.read()


def get_file_metadata(file_id: str) -> Optional[dict]:
    """Get metadata for a single file."""
    service = _get_service()
    try:
        return service.files().get(
            fileId=file_id,
            fields="id, name, mimeType, size",
            supportsAllDrives=True,
        ).execute()
    except Exception as e:
        logger.error(f"Error getting file metadata {file_id}: {e}")
        return None


def is_supported_file(filename: str) -> bool:
    """Check if a file has a supported extension for document analysis."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return ext in SUPPORTED_EXTENSIONS


def get_mime_type(filename: str) -> str:
    """Get MIME type from filename extension."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    return MIME_MAP.get(ext, "application/octet-stream")


def process_google_drive_links(
    case_id: str,
    links: list[str],
) -> dict:
    """
    Main function: Download and analyze all files from Google Drive links.

    Returns:
        {
            "success": True/False,
            "files_found": int,
            "files_processed": int,
            "files_skipped": int,
            "results": [{filename, doc_type, success, error?}],
            "errors": [str]
        }
    """
    import case_logic as cases

    results = []
    errors = []
    files_found = 0
    files_processed = 0
    files_skipped = 0

    # 1. Extract IDs from links
    drive_ids = extract_drive_ids(links)
    if not drive_ids:
        return {
            "success": False,
            "files_found": 0,
            "files_processed": 0,
            "files_skipped": 0,
            "results": [],
            "errors": ["Keine gültigen Google Drive Links gefunden"],
        }

    logger.info(f"[{case_id}] Google Drive: {len(drive_ids)} IDs extracted from {len(links)} links")

    # 1b. Get case's OneDrive folder ID for uploading
    case = cases.load_case(case_id)
    onedrive_folder_id = case.get("onedrive_folder_id", "") if case else ""

    # Wenn noch kein OneDrive-Ordner, kurz warten (n8n erstellt ihn parallel)
    # time.sleep ist OK weil diese Funktion immer via asyncio.to_thread() aufgerufen wird
    if not onedrive_folder_id:
        import time
        logger.info(f"[{case_id}] Kein OneDrive-Ordner – warte 15s auf n8n Setup...")
        time.sleep(15)
        case = cases.load_case(case_id)
        onedrive_folder_id = case.get("onedrive_folder_id", "") if case else ""
        if onedrive_folder_id:
            logger.info(f"[{case_id}] OneDrive-Ordner jetzt verfuegbar: {onedrive_folder_id}")
        else:
            logger.warning(f"[{case_id}] Immer noch kein OneDrive-Ordner – Upload wird uebersprungen")

    # 2. Collect all files
    all_files = []
    for drive_item in drive_ids:
        try:
            if drive_item["type"] == "folder":
                folder_files = list_files_in_folder(drive_item["id"])
                all_files.extend(folder_files)
                logger.info(f"[{case_id}] Folder {drive_item['id']}: {len(folder_files)} files found")
            elif drive_item["type"] == "file":
                meta = get_file_metadata(drive_item["id"])
                if meta:
                    all_files.append(meta)
            else:
                # Try as folder first, fall back to file
                try:
                    folder_files = list_files_in_folder(drive_item["id"])
                    all_files.extend(folder_files)
                except Exception:
                    meta = get_file_metadata(drive_item["id"])
                    if meta:
                        all_files.append(meta)
        except Exception as e:
            err = f"Error accessing {drive_item['url']}: {e}"
            logger.error(f"[{case_id}] {err}")
            errors.append(err)

    files_found = len(all_files)
    logger.info(f"[{case_id}] Google Drive: {files_found} total files found")

    # 3. Filter supported files
    supported_files = [f for f in all_files if is_supported_file(f.get("name", ""))]
    files_skipped = files_found - len(supported_files)

    if not supported_files:
        return {
            "success": True,
            "files_found": files_found,
            "files_processed": 0,
            "files_skipped": files_skipped,
            "results": [],
            "errors": errors or [f"{files_found} Dateien gefunden, aber keine unterstützten Formate"],
        }

    # 4. Download files and process through central pipeline
    from document_processor import FileInput
    from main import processor

    files = []
    for file_info in supported_files:
        fname = file_info.get("name", "unknown")
        fid = file_info["id"]
        try:
            logger.info(f"[{case_id}] Downloading: {fname}")
            file_bytes = download_file(fid)
            mime = get_mime_type(fname)
            files.append(FileInput(
                filename=fname, file_bytes=file_bytes, mime_type=mime,
                gdrive_file_id=fid, source="gdrive",
            ))
        except Exception as e:
            err_msg = f"Download failed for {fname}: {e}"
            logger.error(f"[{case_id}] {err_msg}")
            errors.append(err_msg)
            results.append({"filename": fname, "success": False, "error": str(e), "gdrive_file_id": fid})

    if files:
        batch_result = processor.process_batch(
            case_id, files, upload_to_onedrive_folder=onedrive_folder_id,
        )
        for r in batch_result.get("results", []):
            results.append(r)
            if r.get("success"):
                files_processed += 1
            else:
                errors.append(f"{r.get('filename')}: {r.get('error', 'unknown')}")

    return {
        "success": files_processed > 0,
        "files_found": files_found,
        "files_processed": files_processed,
        "files_skipped": files_skipped,
        "results": results,
        "errors": errors,
    }
