"""Set Drive file permissions on every file under a given subfolder of every SKU.

Use case: make all `photos/` files publicly readable while keeping `models/` private.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Generator, NamedTuple

from asset_sdk.adapters import drive


@dataclass
class FileTarget:
    sku: str
    supplier: str
    file_id: str
    file_name: str
    current_anyone_role: str | None  # 'reader' / 'writer' / None


def _resolve_subfolder(parent_id: str, rel_path: str) -> str | None:
    current = parent_id
    for part in rel_path.split("/"):
        children = drive.list_folders(current)
        if part not in children:
            return None
        current = children[part]
    return current


def _walk_files(folder_id: str, sku: str, supplier: str) -> list[FileTarget]:
    """Recursively collect all files (anywhere) under folder_id with their anyone-permission status."""
    out: list[FileTarget] = []
    stack = [folder_id]
    while stack:
        fid = stack.pop()
        # Collect files (with anyone-permission status) at this level
        for item in drive.list_files_with_anyone(fid):
            out.append(FileTarget(
                sku=sku, supplier=supplier,
                file_id=item["id"], file_name=item["name"],
                current_anyone_role=item.get("anyone_role"),
            ))
        # Descend into subfolders
        for sub in drive.list_folders(fid).values():
            stack.append(sub)
    return out


def find_targets(
    root_folder_id: str,
    structure: str,
    src_subdir: str,
    sku_filter: str | None = None,
    supplier_filter: str | None = None,
) -> list[FileTarget]:
    """Walk products drive, find <sku>/<src_subdir> folders, return every file under them."""
    targets: list[FileTarget] = []

    def _process(sku: str, sku_id: str, supplier: str) -> None:
        if sku_filter and sku != sku_filter:
            return
        sub_id = _resolve_subfolder(sku_id, src_subdir)
        if not sub_id:
            return
        targets.extend(_walk_files(sub_id, sku, supplier))

    if structure == "flat":
        for sku, sid in drive.list_folders(root_folder_id).items():
            _process(sku, sid, "")
    else:
        for sup_name, sup_id in drive.list_folders(root_folder_id).items():
            if supplier_filter and sup_name.lower() != supplier_filter.lower():
                continue
            for sku, sid in drive.list_folders(sup_id).items():
                _process(sku, sid, sup_name)

    return targets


def summarise(targets: list[FileTarget], access: str) -> dict[str, int]:
    """Bucket targets by what would happen on execute."""
    counts = {"to_change": 0, "no_change": 0, "total": len(targets)}
    for t in targets:
        if access == "public":
            if t.current_anyone_role == "reader":
                counts["no_change"] += 1
            else:
                counts["to_change"] += 1
        else:  # private
            if t.current_anyone_role is None:
                counts["no_change"] += 1
            else:
                counts["to_change"] += 1
    return counts


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

class PermProgress(NamedTuple):
    target: FileTarget
    index: int
    total: int
    action: str          # "made_public" | "made_private" | "no_change" | "error"
    error: str | None = None


def execute(
    targets: list[FileTarget], access: str,
) -> Generator[PermProgress, None, None]:
    if access not in ("public", "private"):
        raise ValueError("access must be 'public' or 'private'")
    total = len(targets)
    for i, t in enumerate(targets, 1):
        try:
            if access == "public":
                if t.current_anyone_role == "reader":
                    yield PermProgress(t, i, total, "no_change")
                    continue
                drive.add_anyone_permission(t.file_id, role="reader")
                yield PermProgress(t, i, total, "made_public")
            else:
                if t.current_anyone_role is None:
                    yield PermProgress(t, i, total, "no_change")
                    continue
                drive.remove_anyone_permission(t.file_id)
                yield PermProgress(t, i, total, "made_private")
        except Exception as exc:
            yield PermProgress(t, i, total, "error", str(exc))
