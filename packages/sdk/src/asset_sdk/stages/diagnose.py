from __future__ import annotations

from dataclasses import dataclass, field

from asset_sdk.adapters import drive
from asset_sdk.config import InputPaths


@dataclass
class SkuResult:
    sku: str
    supplier: str
    sku_dir_found: bool
    missing_subdirs: list[str] = field(default_factory=list)
    # config key → file count (0 when the subfolder is missing)
    dir_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class DiagnoseReport:
    results: list[SkuResult]
    # supplier/sku paths present in Drive that have no matching SKU in the sheet
    orphan_dirs: list[str]


def _resolve_and_count(
    sku_folder_id: str,
    rel_path: str,
    folder_cache: dict[str, dict[str, str]],
) -> int | None:
    """
    Navigate rel_path (e.g. 'thumbnails/website_thumbnail') down from sku_folder_id,
    caching intermediate folder listings to avoid redundant API calls.
    Returns the file count at the leaf, or None if any path component is missing.
    """
    current_id = sku_folder_id
    for part in rel_path.split("/"):
        if current_id not in folder_cache:
            folder_cache[current_id] = drive.list_folders(current_id)
        if part not in folder_cache[current_id]:
            return None
        current_id = folder_cache[current_id][part]
    return drive.count_files(current_id)


def run(
    root_folder_id: str,
    sheet_rows: list[dict[str, str]],
    sku_col: str,
    supplier_col: str,
    paths: InputPaths,
    structure: str = "supplier",
) -> DiagnoseReport:
    """
    structure="supplier"  →  root / <supplier> / <sku>
    structure="flat"      →  root / <sku>
    """
    known_skus: set[str] = {
        row.get(sku_col, "").strip()
        for row in sheet_rows
        if row.get(sku_col, "").strip()
    }

    # --- Build disk_skus: {sku_name: (supplier_label, sku_folder_id)} ---
    folder_cache: dict[str, dict[str, str]] = {}
    disk_skus: dict[str, tuple[str, str]] = {}

    if structure == "flat":
        sku_folders = drive.list_folders(root_folder_id)
        folder_cache[root_folder_id] = sku_folders
        for sku_name, sku_folder_id in sku_folders.items():
            disk_skus[sku_name] = ("", sku_folder_id)
    else:
        supplier_folders = drive.list_folders(root_folder_id)
        folder_cache[root_folder_id] = supplier_folders
        for supplier_name, supplier_folder_id in supplier_folders.items():
            sku_folders = drive.list_folders(supplier_folder_id)
            folder_cache[supplier_folder_id] = sku_folders
            for sku_name, sku_folder_id in sku_folders.items():
                disk_skus[sku_name] = (supplier_name, sku_folder_id)

    # Orphans: folders on disk with no matching SKU in the sheet.
    orphan_skus = sorted(set(disk_skus.keys()) - known_skus)
    orphan_dirs = (
        orphan_skus
        if structure == "flat"
        else [f"{disk_skus[s][0]}/{s}" for s in orphan_skus]
    )

    # --- Match each sheet row to its Drive folder and count files ---
    results: list[SkuResult] = []
    for row in sheet_rows:
        sku = row.get(sku_col, "").strip()
        if not sku:
            continue
        supplier = row.get(supplier_col, "").strip()

        if sku not in disk_skus:
            results.append(SkuResult(sku=sku, supplier=supplier, sku_dir_found=False))
            continue

        _, sku_folder_id = disk_skus[sku]
        dir_counts: dict[str, int] = {}
        missing_subdirs: list[str] = []

        for key, _display, rel_path in paths.entries():
            count = _resolve_and_count(sku_folder_id, rel_path, folder_cache)
            if count is None:
                missing_subdirs.append(rel_path)
                dir_counts[key] = 0
            else:
                dir_counts[key] = count

        results.append(SkuResult(
            sku=sku,
            supplier=supplier,
            sku_dir_found=True,
            missing_subdirs=missing_subdirs,
            dir_counts=dir_counts,
        ))

    return DiagnoseReport(results=results, orphan_dirs=orphan_dirs)


def to_sheet_rows(
    report: DiagnoseReport,
    paths: InputPaths,
) -> tuple[list[str], list[list]]:
    entries = paths.entries()
    dir_headers = [display for _key, display, _path in entries]
    headers = ["SKU", "Supplier", "Status", "Issues"] + dir_headers

    rows: list[list] = []
    for r in report.results:
        if not r.sku_dir_found:
            status, issues = "MISSING DIR", ""
        elif r.missing_subdirs:
            status = "INCOMPLETE"
            issues = "Missing: " + ", ".join(r.missing_subdirs)
        else:
            status, issues = "OK", ""

        counts = [r.dir_counts.get(key, 0) for key, _d, _p in entries]
        rows.append([r.sku, r.supplier, status, issues, *counts])

    for orphan in report.orphan_dirs:
        rows.append([orphan, "", "ORPHAN DIR", "Not in sheet", *([0] * len(entries))])

    return headers, rows
