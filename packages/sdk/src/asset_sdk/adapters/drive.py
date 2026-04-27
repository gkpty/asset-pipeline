from __future__ import annotations

import os
from functools import lru_cache

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

_FOLDER_MIME = "application/vnd.google-apps.folder"
_SCOPES = ["https://www.googleapis.com/auth/drive"]
# Required for API calls that may touch Shared Drive content.
_SD = {"supportsAllDrives": True, "includeItemsFromAllDrives": True}
_SD_W = {"supportsAllDrives": True}

_DEFAULT_TOKEN_PATH = ".secrets/oauth_token.json"


def _get_creds() -> Credentials:
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

    return creds  # type: ignore[return-value]


@lru_cache(maxsize=1)
def _service():
    return build("drive", "v3", credentials=_get_creds(), cache_discovery=False)


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


def rename_item(file_id: str, new_name: str) -> None:
    """Rename a Drive file or folder in place."""
    _service().files().update(fileId=file_id, body={"name": new_name}, **_SD_W).execute()
