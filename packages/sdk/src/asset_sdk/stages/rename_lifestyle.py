from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Generator, NamedTuple

from asset_sdk.adapters import drive

_IMAGE_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".webp",
    ".tiff", ".tif", ".bmp", ".heic", ".heif",
    ".raw", ".cr2", ".nef", ".arw",
}


@dataclass
class LifestyleEntry:
    folder_name: str            # current name in Drive (parent product)
    folder_id: str
    in_sheet: bool              # False when no matching parent product found in sheet
    all_skus: list[str] = field(default_factory=list)  # all SKUs for this parent product
    first_file_url: str | None = None

    @property
    def selected_sku(self) -> str | None:
        return self.all_skus[0] if self.all_skus else None

    @property
    def alt_skus(self) -> list[str]:
        return self.all_skus[1:]

    @property
    def multiple_skus(self) -> bool:
        return len(self.all_skus) > 1


def build_report(
    lifestyle_folder_id: str,
    sheet_rows: list[dict[str, str]],
    sku_col: str,
    parent_product_col: str,
) -> list[LifestyleEntry]:
    # Build {parent_product: [sku, ...]} preserving sheet row order.
    parent_to_skus: dict[str, list[str]] = {}
    for row in sheet_rows:
        sku = row.get(sku_col, "").strip()
        parent = row.get(parent_product_col, "").strip()
        if sku and parent:
            parent_to_skus.setdefault(parent, []).append(sku)

    lifestyle_folders = drive.list_folders(lifestyle_folder_id)  # {name: id}

    entries: list[LifestyleEntry] = []
    for folder_name, folder_id in sorted(lifestyle_folders.items()):
        skus = parent_to_skus.get(folder_name, [])
        entries.append(LifestyleEntry(
            folder_name=folder_name,
            folder_id=folder_id,
            in_sheet=bool(skus),
            all_skus=skus,
            first_file_url=drive.get_first_file_url(folder_id),
        ))

    return entries


def to_sheet_rows(entries: list[LifestyleEntry]) -> tuple[list[str], list[list]]:
    # How many alt SKU columns do we need?
    max_alts = max((len(e.alt_skus) for e in entries), default=0)

    alt_headers = [f"Alt SKU {i + 1}" for i in range(max_alts)]
    headers = ["Parent Product", "Multiple SKUs", "URL", "Selected SKU"] + alt_headers

    rows = []
    for e in entries:
        alt_cells = e.alt_skus + [""] * (max_alts - len(e.alt_skus))
        rows.append([
            e.folder_name,
            "TRUE" if e.multiple_skus else "FALSE",
            e.first_file_url or "",
            e.selected_sku if e.in_sheet else "NOT IN SHEET",
            *alt_cells,
        ])

    return headers, rows


class CopyProgress(NamedTuple):
    entry_name: str   # source lifestyle folder name (parent product)
    dest_sku: str     # target SKU folder name
    file_index: int   # 1-based index of the file just copied
    file_total: int   # total image files in this entry


def execute_copy(
    entries: list[LifestyleEntry],
    root_folder_id: str,
    structure: str,
    lifestyle_subdir: str,
) -> Generator[CopyProgress, None, None]:
    """Copy image files from each lifestyle folder into root/[supplier]/sku/lifestyle_subdir.

    Yields a CopyProgress after each file is copied so callers can show live progress.
    """
    if structure == "flat":
        sku_folders: dict[str, str] = drive.list_folders(root_folder_id)
    else:
        supplier_folders = drive.list_folders(root_folder_id)
        sku_folders = {}
        for supplier_folder_id in supplier_folders.values():
            sku_folders.update(drive.list_folders(supplier_folder_id))

    for e in entries:
        if not e.selected_sku or e.selected_sku not in sku_folders:
            continue
        sku_folder_id = sku_folders[e.selected_sku]

        dest_folder_id = sku_folder_id
        for part in lifestyle_subdir.split("/"):
            dest_folder_id = drive.find_or_create_folder(part, dest_folder_id)

        image_files = [
            f for f in drive.list_files(e.folder_id)
            if PurePosixPath(f["name"]).suffix.lower() in _IMAGE_EXTENSIONS
        ]
        already_there = {f["name"] for f in drive.list_files(dest_folder_id)}
        to_copy = [f for f in image_files if f["name"] not in already_there]
        if not to_copy:
            # Nothing new to copy — signal so callers can still track overall progress.
            yield CopyProgress(e.folder_name, e.selected_sku, 0, 0)
            continue
        for i, file in enumerate(to_copy, 1):
            drive.copy_file(file["id"], dest_folder_id, file["name"])
            yield CopyProgress(e.folder_name, e.selected_sku, i, len(to_copy))
