from __future__ import annotations

import os
import threading

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

_FOLDER_MIME = "application/vnd.google-apps.folder"
_SCOPES = ["https://www.googleapis.com/auth/drive"]
# Required for API calls that may touch Shared Drive content.
_SD = {"supportsAllDrives": True, "includeItemsFromAllDrives": True}
_SD_W = {"supportsAllDrives": True}

_DEFAULT_TOKEN_PATH = ".secrets/oauth_token.json"

# googleapiclient's `service` object (and the underlying httplib2 Http) is NOT
# thread-safe. We share one Credentials object across threads (it has its own
# refresh lock), but build a fresh service per thread via threading.local.
_creds_lock = threading.Lock()
_cached_creds: Credentials | None = None
_thread_local = threading.local()


def _get_creds() -> Credentials:
    global _cached_creds
    with _creds_lock:
        if _cached_creds is not None and _cached_creds.valid:
            return _cached_creds

        client_json = os.environ.get("GOOGLE_OAUTH_CREDENTIALS")
        if not client_json:
            raise RuntimeError("GOOGLE_OAUTH_CREDENTIALS is not set")
        token_path = os.environ.get("GOOGLE_OAUTH_TOKEN_PATH", _DEFAULT_TOKEN_PATH)

        creds: Credentials | None = None
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, _SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(client_json, _SCOPES)
                creds = flow.run_local_server(port=0)
            os.makedirs(os.path.dirname(token_path) or ".", exist_ok=True)
            with open(token_path, "w") as fh:
                fh.write(creds.to_json())

        _cached_creds = creds
        return creds  # type: ignore[return-value]


def _service():
    """Per-thread Drive service (httplib2 isn't thread-safe)."""
    svc = getattr(_thread_local, "service", None)
    if svc is None:
        svc = build("drive", "v3", credentials=_get_creds(), cache_discovery=False)
        _thread_local.service = svc
    return svc


def get_item_name(file_id: str) -> str:
    """Return the name of any Drive file or folder, or raise if not found / not accessible."""
    resp = _service().files().get(fileId=file_id, fields="name", **_SD_W).execute()
    return resp["name"]


def list_folders(parent_id: str) -> dict[str, str]:
    """Return {name: folder_id} for all non-trashed subfolders of parent_id."""
    svc = _service()
    result: dict[str, str] = {}
    page_token: str | None = None
    q = f"'{parent_id}' in parents and mimeType='{_FOLDER_MIME}' and trashed=false"
    while True:
        resp = (
            svc.files()
            .list(
                q=q,
                fields="nextPageToken, files(id, name)",
                pageToken=page_token,
                pageSize=1000,
                **_SD,
            )
            .execute()
        )
        for f in resp.get("files", []):
            result[f["name"]] = f["id"]
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return result


def resolve_category_folder(parent_folder_id: str, category: str) -> str:
    """Return the ID of the child folder of `parent_folder_id` whose name matches
    `category` (case-insensitive). Raises a clear error if no match is found.

    Used to support a parent/category/supplier/sku layout where the env var
    GOOGLE_DRIVE_ROOT_FOLDER_ID points at the parent and CLI commands take
    `--category products|materials|...`.
    """
    children = list_folders(parent_folder_id)
    target = category.strip().lower()
    for name, fid in children.items():
        if name.strip().lower() == target:
            return fid
    available = ", ".join(sorted(children.keys())) or "(none)"
    raise RuntimeError(
        f"Category folder {category!r} not found under parent {parent_folder_id}. "
        f"Available children: {available}"
    )


def count_files(folder_id: str) -> int:
    """Count non-folder, non-trashed files directly inside folder_id."""
    svc = _service()
    count = 0
    page_token: str | None = None
    q = f"'{folder_id}' in parents and mimeType!='{_FOLDER_MIME}' and trashed=false"
    while True:
        resp = (
            svc.files()
            .list(
                q=q,
                fields="nextPageToken, files(id)",
                pageToken=page_token,
                pageSize=1000,
                **_SD,
            )
            .execute()
        )
        count += len(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return count


def list_children_meta(folder_id: str) -> list[dict]:
    """Return [{id, name, kind, size, md5, width, height}, ...] for all non-trashed items.

    size is a string (Drive returns it that way) or None for folders/Google Docs.
    md5 is None for folders, Google Docs, and files without a checksum.
    width/height come from imageMediaMetadata (None for non-images).
    """
    svc = _service()
    result: list[dict] = []
    page_token: str | None = None
    q = f"'{folder_id}' in parents and trashed=false"
    fields = (
        "nextPageToken, files(id, name, mimeType, size, md5Checksum, "
        "imageMediaMetadata(width, height))"
    )
    while True:
        resp = (
            svc.files()
            .list(
                q=q,
                fields=fields,
                pageToken=page_token,
                pageSize=1000,
                **_SD,
            )
            .execute()
        )
        for f in resp.get("files", []):
            meta = f.get("imageMediaMetadata") or {}
            result.append({
                "id": f["id"],
                "name": f["name"],
                "kind": "folder" if f["mimeType"] == _FOLDER_MIME else "file",
                "size": f.get("size"),
                "md5": f.get("md5Checksum"),
                "width": meta.get("width"),
                "height": meta.get("height"),
            })
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return result


def list_children(folder_id: str) -> list[dict[str, str]]:
    """Return [{id, name, kind}, ...] (kind = 'folder' | 'file') for all non-trashed items."""
    svc = _service()
    result: list[dict[str, str]] = []
    page_token: str | None = None
    q = f"'{folder_id}' in parents and trashed=false"
    while True:
        resp = (
            svc.files()
            .list(
                q=q,
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
                pageSize=1000,
                **_SD,
            )
            .execute()
        )
        for f in resp.get("files", []):
            result.append({
                "id": f["id"],
                "name": f["name"],
                "kind": "folder" if f["mimeType"] == _FOLDER_MIME else "file",
            })
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return result


def list_files(folder_id: str) -> list[dict[str, str]]:
    """Return [{id, name}, ...] for all non-folder, non-trashed files in folder_id."""
    svc = _service()
    result: list[dict[str, str]] = []
    page_token: str | None = None
    q = f"'{folder_id}' in parents and mimeType!='{_FOLDER_MIME}' and trashed=false"
    while True:
        resp = (
            svc.files()
            .list(
                q=q,
                fields="nextPageToken, files(id, name)",
                pageToken=page_token,
                pageSize=1000,
                **_SD,
            )
            .execute()
        )
        result.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return result


def get_first_file_url(folder_id: str) -> str | None:
    """Return the web view URL of the first (alphabetically) file in folder_id, or None."""
    q = f"'{folder_id}' in parents and mimeType!='{_FOLDER_MIME}' and trashed=false"
    resp = (
        _service()
        .files()
        .list(
            q=q,
            fields="files(id, webViewLink)",
            pageSize=1,
            orderBy="name",
            **_SD,
        )
        .execute()
    )
    files = resp.get("files", [])
    return files[0].get("webViewLink") if files else None


def find_or_create_folder(name: str, parent_id: str) -> str:
    """Return the ID of a named subfolder of parent_id, creating it if it doesn't exist."""
    existing = list_folders(parent_id)
    if name in existing:
        return existing[name]
    resp = (
        _service()
        .files()
        .create(
            body={"name": name, "mimeType": _FOLDER_MIME, "parents": [parent_id]},
            fields="id",
            **_SD_W,
        )
        .execute()
    )
    return resp["id"]


def copy_file(file_id: str, dest_folder_id: str, name: str) -> str:
    """Copy a file into dest_folder_id with the given name. Returns the new file ID."""
    resp = (
        _service()
        .files()
        .copy(
            fileId=file_id,
            body={"name": name, "parents": [dest_folder_id]},
            fields="id",
            **_SD_W,
        )
        .execute()
    )
    return resp["id"]


def download_file(file_id: str, local_path: str) -> None:
    """Download a Drive file's binary content to local_path."""
    request = _service().files().get_media(fileId=file_id, supportsAllDrives=True)
    with open(local_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def upload_file(local_path: str, dest_folder_id: str, name: str, mime_type: str) -> str:
    """Upload a local file into dest_folder_id with the given name. Returns the new file ID."""
    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)
    resp = (
        _service()
        .files()
        .create(
            body={"name": name, "parents": [dest_folder_id]},
            media_body=media,
            fields="id",
            **_SD_W,
        )
        .execute()
    )
    return resp["id"]


def rename_item(file_id: str, new_name: str) -> None:
    """Rename a Drive file or folder in place."""
    _service().files().update(fileId=file_id, body={"name": new_name}, **_SD_W).execute()


def trash_item(file_id: str) -> None:
    """Move a Drive file or folder to trash (recoverable, not permanent delete)."""
    _service().files().update(fileId=file_id, body={"trashed": True}, **_SD_W).execute()


def list_files_with_anyone(folder_id: str) -> list[dict]:
    """Return [{id, name, anyone_role}, ...] for files in folder_id.

    anyone_role is 'reader'/'writer'/'commenter' if the file has an 'anyone' permission,
    or None if it doesn't.
    """
    svc = _service()
    result: list[dict] = []
    page_token: str | None = None
    q = f"'{folder_id}' in parents and mimeType!='{_FOLDER_MIME}' and trashed=false"
    fields = (
        "nextPageToken, files(id, name, permissions(id, type, role))"
    )
    while True:
        resp = (
            svc.files()
            .list(q=q, fields=fields, pageToken=page_token, pageSize=1000, **_SD)
            .execute()
        )
        for f in resp.get("files", []):
            anyone_role = None
            for p in f.get("permissions", []) or []:
                if p.get("type") == "anyone":
                    anyone_role = p.get("role")
                    break
            result.append({
                "id": f["id"], "name": f["name"], "anyone_role": anyone_role,
            })
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return result


def add_anyone_permission(file_id: str, role: str = "reader") -> None:
    """Grant 'anyone with the link' a role (default reader = view-only)."""
    _service().permissions().create(
        fileId=file_id,
        body={"type": "anyone", "role": role},
        fields="id",
        **_SD_W,
    ).execute()


def remove_anyone_permission(file_id: str) -> bool:
    """Remove any 'anyone' permission on file_id. Returns True if one was removed."""
    svc = _service()
    resp = svc.permissions().list(
        fileId=file_id,
        fields="permissions(id, type, role)",
        **_SD_W,
    ).execute()
    removed = False
    for p in resp.get("permissions", []):
        if p.get("type") == "anyone":
            svc.permissions().delete(
                fileId=file_id, permissionId=p["id"], **_SD_W,
            ).execute()
            removed = True
    return removed


def move_item(file_id: str, new_parent_id: str) -> None:
    """Re-parent a Drive file or folder under new_parent_id (removes prior parents)."""
    svc = _service()
    info = svc.files().get(fileId=file_id, fields="parents", **_SD_W).execute()
    prev = ",".join(info.get("parents", []) or [])
    svc.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=prev,
        fields="id, parents",
        **_SD_W,
    ).execute()
