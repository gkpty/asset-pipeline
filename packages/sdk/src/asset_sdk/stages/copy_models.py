from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generator, NamedTuple

from asset_sdk.adapters import drive
from asset_sdk.config import InputPaths

# Expected subdirectory names inside each SKU folder (case-insensitive).
_EXPECTED_DIRS = {"obj", "skp", "dwg", "pdf", "gltf"}


@dataclass
class ModelEntry:
    sku_name: str
    sku_folder_id: str
    in_sheet: bool
    in_products: bool
    present_dirs: list[str]  # lowercase: which of the expected dirs are present
    extra_items: list[str]   # unexpected file/folder names directly inside the SKU folder


def build_report(
    models_folder_id: str,
    sheet_skus: set[str],
    root_folder_id: str,
    structure: str,
) -> tuple[list[ModelEntry], list[str]]:
    """Scan the models folder and cross-reference with the sheet and products drive.

    Returns:
        entries        — one ModelEntry per SKU folder found in models_folder_id
        missing_skus   — SKU names that are in the sheet but absent from models
    """
    if structure == "flat":
        products_skus: set[str] = set(drive.list_folders(root_folder_id).keys())
    else:
        products_skus = set()
        for supplier_id in drive.list_folders(root_folder_id).values():
            products_skus.update(drive.list_folders(supplier_id).keys())

    model_folders = drive.list_folders(models_folder_id)  # {sku_name: folder_id}

    entries: list[ModelEntry] = []
    for sku_name, sku_folder_id in sorted(model_folders.items()):
        children = drive.list_children(sku_folder_id)
        present_dirs: list[str] = []
        extra_items: list[str] = []
        for item in children:
            name_lower = item["name"].lower()
            if item["kind"] == "folder" and name_lower in _EXPECTED_DIRS:
                present_dirs.append(name_lower)
            else:
                extra_items.append(item["name"])

        entries.append(ModelEntry(
            sku_name=sku_name,
            sku_folder_id=sku_folder_id,
            in_sheet=sku_name in sheet_skus,
            in_products=sku_name in products_skus,
            present_dirs=sorted(present_dirs),
            extra_items=sorted(extra_items),
        ))

    missing_skus = sorted(sheet_skus - set(model_folders.keys()))
    return entries, missing_skus


def to_sheet_rows(
    entries: list[ModelEntry],
    missing_skus: list[str],
) -> tuple[list[str], list[list]]:
    headers = [
        "SKU", "In Sheet", "In Products Drive",
        "OBJ", "SKP", "DWG", "PDF", "GLTF",
        "Extra Items", "Status",
    ]

    rows: list[list] = []
    for e in entries:
        issues: list[str] = []
        if not e.in_sheet:
            issues.append("not in sheet")
        if not e.in_products:
            issues.append("not in products drive")
        if e.extra_items:
            issues.append("has extra items")

        rows.append([
            e.sku_name,
            "TRUE" if e.in_sheet else "FALSE",
            "TRUE" if e.in_products else "FALSE",
            "TRUE" if "obj" in e.present_dirs else "FALSE",
            "TRUE" if "skp" in e.present_dirs else "FALSE",
            "TRUE" if "dwg" in e.present_dirs else "FALSE",
            "TRUE" if "pdf" in e.present_dirs else "FALSE",
            "TRUE" if "gltf" in e.present_dirs else "FALSE",
            ", ".join(e.extra_items),
            "OK" if not issues else ", ".join(issues),
        ])

    for sku in missing_skus:
        rows.append([
            sku,
            "TRUE", "",
            "", "", "", "", "",
            "",
            "missing from models",
        ])

    return headers, rows


# ---------------------------------------------------------------------------
# Copy
# ---------------------------------------------------------------------------

class ModelCopyProgress(NamedTuple):
    sku_name: str
    file_index: int   # 1-based; 0 means nothing to copy (already done)
    file_total: int   # 0 means nothing to copy
    source_dir: str   # e.g. "OBJ" — which subdirectory is being copied


def execute_copy(
    entries: list[ModelEntry],
    root_folder_id: str,
    structure: str,
    paths: InputPaths,
) -> Generator[ModelCopyProgress, None, None]:
    """Copy model files from the models folder into each SKU's product folder.

    Destination mapping:
      OBJ  → paths.models_obj   (e.g. models/obj)
      SKP  → paths.models_skp
      DWG  → paths.models_dwg
      GLTF → paths.models_gltf
      PDF  → paths.diagram      (not models/pdf — diagrams go in /diagram)

    Yields a ModelCopyProgress after each file is copied.
    Yields file_total=0 when an entry has nothing new to copy (already done).
    """
    if structure == "flat":
        sku_folders: dict[str, str] = drive.list_folders(root_folder_id)
    else:
        sku_folders = {}
        for supplier_id in drive.list_folders(root_folder_id).values():
            sku_folders.update(drive.list_folders(supplier_id))

    dest_map: dict[str, str] = {
        "obj":  paths.models_obj,
        "skp":  paths.models_skp,
        "dwg":  paths.models_dwg,
        "gltf": paths.models_gltf,
        "pdf":  paths.diagram,
    }

    for e in entries:
        if not e.in_products or e.sku_name not in sku_folders:
            continue
        sku_dest_id = sku_folders[e.sku_name]

        # Resolve source subdirectories.
        src_children = drive.list_children(e.sku_folder_id)
        src_dirs: dict[str, str] = {
            item["name"].lower(): item["id"]
            for item in src_children
            if item["kind"] == "folder" and item["name"].lower() in _EXPECTED_DIRS
        }

        # Collect all files to copy, grouped by destination subdir.
        # (file_id, file_name, source_dir_upper, dest_subdir)
        candidates: list[tuple[str, str, str, str]] = []
        for dir_lower, src_id in src_dirs.items():
            dest_subdir = dest_map[dir_lower]
            for f in drive.list_files(src_id):
                candidates.append((f["id"], f["name"], dir_lower.upper(), dest_subdir))

        if not candidates:
            yield ModelCopyProgress(e.sku_name, 0, 0, "")
            continue

        # Resolve (and cache) destination folders + existing file names.
        dest_cache: dict[str, tuple[str, set[str]]] = {}
        for _, _, _, dest_subdir in candidates:
            if dest_subdir not in dest_cache:
                dest_id = sku_dest_id
                for part in dest_subdir.split("/"):
                    dest_id = drive.find_or_create_folder(part, dest_id)
                existing = {f["name"] for f in drive.list_files(dest_id)}
                dest_cache[dest_subdir] = (dest_id, existing)

        to_copy = [c for c in candidates if c[1] not in dest_cache[c[3]][1]]

        if not to_copy:
            yield ModelCopyProgress(e.sku_name, 0, 0, "")
            continue

        for i, (file_id, file_name, src_dir, dest_subdir) in enumerate(to_copy, 1):
            dest_id = dest_cache[dest_subdir][0]
            drive.copy_file(file_id, dest_id, file_name)
            yield ModelCopyProgress(e.sku_name, i, len(to_copy), src_dir)
