"""Regroup flat per-SKU files into per-SKU folders.

Input  : <category>/<sku>.<ext>            (or <category>/<supplier>/<sku>.<ext>)
Output : <category>/<sku>/1.<ext>          (or <category>/<supplier>/<sku>/1.<ext>)

Optionally nest one level deeper via `subdir`: <sku>/<subdir>/1.<ext>.

Used to migrate categories like materials/upholstery from a flat layout into the
SKU-folder convention used by products. Idempotent: files already inside an SKU
folder are skipped, and SKU folders that already contain at least one file are
left alone.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Generator

from asset_sdk.adapters import drive


@dataclass
class RegroupPlan:
    supplier: str            # "" when structure == "flat"
    sku: str                 # filename stem
    parent_folder_id: str    # category folder id (or supplier folder id)
    file_id: str
    file_name: str           # original name, e.g. "ABC-123.jpg"
    target_name: str         # always "1.<ext>"
    skip_reason: str = ""    # non-empty → action is a no-op (folder exists & non-empty, etc.)


def _collect_files(category_folder_id: str) -> list[tuple[str, str, dict]]:
    """Return [(supplier, parent_id, file_meta), ...] for every flat file that
    needs regrouping. Auto-detects layout:

      - If the category root contains flat files → those are the targets,
        supplier="" (flat layout).
      - Else, walk each subfolder of the category root. A subfolder counts as a
        supplier only if it contains files AND no further subfolders (i.e. it
        hasn't been migrated yet). Subfolders with subfolders inside are
        treated as already-regrouped SKU layouts and skipped.

    file_meta has at least {"id", "name"}.
    """
    out: list[tuple[str, str, dict]] = []

    root_files = drive.list_files(category_folder_id)
    if root_files:
        # Flat layout: <category>/<sku>.<ext>
        return [("", category_folder_id, f) for f in root_files]

    # Supplier layout candidate: <category>/<supplier>/<sku>.<ext>
    for sup_name, sup_id in drive.list_folders(category_folder_id).items():
        sub_files = drive.list_files(sup_id)
        sub_folders = drive.list_folders(sup_id)
        if sub_files and not sub_folders:
            for f in sub_files:
                out.append((sup_name, sup_id, f))
    return out


def build_plan(
    category_folder_id: str,
    subdir: str = "",
) -> list[RegroupPlan]:
    """Walk the category and return a per-file plan.

    Each plan entry covers ONE source file. Layout (flat vs supplier) is
    auto-detected — see `_collect_files`. Idempotency rules:
      - Hidden files (.DS_Store etc.) are filtered out.
      - If a folder already exists at <parent>/<sku>/[<subdir>/] AND is non-empty,
        the file is marked skip_reason='target folder already populated'.
    """
    plans: list[RegroupPlan] = []
    files = _collect_files(category_folder_id)

    for supplier, parent_id, f in files:
        name = f["name"]
        if name.startswith("."):
            continue
        stem = PurePosixPath(name).stem
        ext = PurePosixPath(name).suffix.lower()
        if not stem or not ext:
            continue  # ignore extension-less files

        target_name = f"1{ext}"

        # Detect existing <sku>/ folder under parent_id.
        sku_folders = drive.list_folders(parent_id)
        existing_sku_id = sku_folders.get(stem)
        skip = ""
        if existing_sku_id:
            # If the SKU folder exists, see what's inside.
            target_dir = existing_sku_id
            if subdir:
                children = drive.list_folders(existing_sku_id)
                target_dir = children.get(subdir, "")
            if target_dir and drive.list_files(target_dir):
                skip = "target folder already populated"

        plans.append(RegroupPlan(
            supplier=supplier,
            sku=stem,
            parent_folder_id=parent_id,
            file_id=f["id"],
            file_name=name,
            target_name=target_name,
            skip_reason=skip,
        ))
    return plans


@dataclass
class RegroupProgress:
    plan: RegroupPlan
    done: bool
    error: str = ""


def execute(
    plans: list[RegroupPlan],
    subdir: str = "",
) -> Generator[RegroupProgress, None, None]:
    """Apply each plan: ensure <sku>/[<subdir>/] exists, move file in, rename to 1.<ext>.

    Yields RegroupProgress per plan so callers can drive a progress bar.
    """
    for p in plans:
        if p.skip_reason:
            yield RegroupProgress(plan=p, done=False)
            continue
        try:
            sku_folder_id = drive.find_or_create_folder(p.sku, p.parent_folder_id)
            dest_id = sku_folder_id
            if subdir:
                dest_id = drive.find_or_create_folder(subdir, sku_folder_id)
            # Re-parent first, then rename. Doing it in this order avoids a transient
            # "1.jpg" collision if the source happened to share the target name.
            drive.move_item(p.file_id, dest_id)
            drive.rename_item(p.file_id, p.target_name)
            yield RegroupProgress(plan=p, done=True)
        except Exception as exc:
            yield RegroupProgress(plan=p, done=False, error=str(exc))


def summarise(plans: list[RegroupPlan]) -> dict[str, int]:
    """Counts for the dry-run summary."""
    return {
        "total": len(plans),
        "actionable": sum(1 for p in plans if not p.skip_reason),
        "skipped": sum(1 for p in plans if p.skip_reason),
    }
