from __future__ import annotations

from dataclasses import dataclass
from typing import Generator, NamedTuple

from asset_sdk.adapters import drive
from asset_sdk.config import InputPaths

# Expected subdirectory names inside each SKU folder (case-insensitive, after normalisation).
_EXPECTED_DIRS = {"obj", "skp", "dwg", "cad", "pdf", "gltf"}

# Known typos / transpositions mapped to their canonical name.
_DIR_ALIASES: dict[str, str] = {
    "gitf": "gltf",
    "gitlf": "gltf",
    "gtlf": "gltf",
    "glft": "gltf",
}


def _normalise(name: str) -> str:
    lower = name.lower()
    return _DIR_ALIASES.get(lower, lower)


@dataclass
class ModelEntry:
    sku_name: str
    sku_folder_id: str
    supplier_name: str | None     # source supplier (None when structure='flat')
    is_orphan: bool               # True when no matching SKU folder exists in products drive
    dir_sources: dict[str, str]   # {canonical dir name: source folder id}
    extra_items: list[str]        # unrecognised items at the SKU root level
    has_nested_models: bool = False

    @property
    def present_dirs(self) -> list[str]:
        return sorted(self.dir_sources.keys())


def _scan_sku_folder(sku_folder_id: str) -> tuple[dict[str, str], list[str], bool]:
    """Return (dir_sources, extra_items, has_nested_models) for one SKU folder."""
    children = drive.list_children(sku_folder_id)

    dir_sources: dict[str, str] = {}
    extra_items: list[str] = []
    nested_models_id: str | None = None

    for item in children:
        canonical = _normalise(item["name"])
        if item["kind"] == "folder":
            if canonical in _EXPECTED_DIRS:
                dir_sources[canonical] = item["id"]
            elif canonical == "models":
                nested_models_id = item["id"]
            else:
                extra_items.append(item["name"])
        else:
            extra_items.append(item["name"])

    has_nested = False
    if nested_models_id:
        for item in drive.list_children(nested_models_id):
            canonical = _normalise(item["name"])
            if item["kind"] == "folder" and canonical in _EXPECTED_DIRS:
                if canonical not in dir_sources:
                    dir_sources[canonical] = item["id"]
                    has_nested = True

    return dir_sources, sorted(extra_items), has_nested


def _collect_sku_folders(
    folder_id: str, structure: str
) -> dict[str, tuple[str, str | None]]:
    """Return {sku_name: (sku_folder_id, supplier_name)} from a root folder."""
    if structure == "flat":
        return {name: (fid, None) for name, fid in drive.list_folders(folder_id).items()}

    result: dict[str, tuple[str, str | None]] = {}
    for supplier_name, supplier_id in drive.list_folders(folder_id).items():
        for sku_name, sku_id in drive.list_folders(supplier_id).items():
            result[sku_name] = (sku_id, supplier_name)
    return result


def build_report(
    models_folder_id: str,
    root_folder_id: str,
    structure: str,
) -> tuple[list[ModelEntry], list[str]]:
    """Scan the models folder and cross-reference with the products drive.

    Returns:
        entries  — one ModelEntry per SKU folder in models (orphans included)
        missing  — SKU names present in products drive but absent from models
    """
    products_skus = _collect_sku_folders(root_folder_id, structure)
    model_skus = _collect_sku_folders(models_folder_id, structure)

    entries: list[ModelEntry] = []
    for sku_name, (sku_folder_id, supplier_name) in sorted(model_skus.items()):
        dir_sources, extra_items, has_nested = _scan_sku_folder(sku_folder_id)
        entries.append(ModelEntry(
            sku_name=sku_name,
            sku_folder_id=sku_folder_id,
            supplier_name=supplier_name,
            is_orphan=sku_name not in products_skus,
            dir_sources=dir_sources,
            extra_items=extra_items,
            has_nested_models=has_nested,
        ))

    missing = sorted(set(products_skus.keys()) - set(model_skus.keys()))
    return entries, missing


def to_sheet_rows(
    entries: list[ModelEntry],
    missing: list[str],
) -> tuple[list[str], list[list]]:
    headers = [
        "SKU", "Supplier", "Status",
        "OBJ", "SKP", "DWG/CAD", "PDF", "GLTF",
        "Nested Models Folder", "Extra Items",
    ]

    rows: list[list] = []
    for e in entries:
        if e.is_orphan:
            status = "ORPHAN (will be created)"
        else:
            status = "OK"
        if e.extra_items:
            status += " — has extras"

        has_dwg_cad = "dwg" in e.dir_sources or "cad" in e.dir_sources
        rows.append([
            e.sku_name,
            e.supplier_name or "",
            status,
            "TRUE" if "obj"  in e.dir_sources else "FALSE",
            "TRUE" if "skp"  in e.dir_sources else "FALSE",
            "TRUE" if has_dwg_cad             else "FALSE",
            "TRUE" if "pdf"  in e.dir_sources else "FALSE",
            "TRUE" if "gltf" in e.dir_sources else "FALSE",
            "TRUE" if e.has_nested_models     else "FALSE",
            ", ".join(e.extra_items),
        ])

    for sku in missing:
        rows.append([sku, "", "MISSING (no models)", "", "", "", "", "", "", ""])

    return headers, rows


# ---------------------------------------------------------------------------
# Copy
# ---------------------------------------------------------------------------

class ModelCopyProgress(NamedTuple):
    sku_name: str
    file_index: int   # 1-based; 0 = nothing new to copy
    file_total: int   # 0 = nothing new to copy
    source_dir: str   # e.g. "OBJ"


def _dest_subdir(dir_lower: str, paths: InputPaths) -> str:
    return {
        "obj":  paths.models_obj,
        "skp":  paths.models_skp,
        "dwg":  paths.models_dwg,
        "cad":  paths.models_dwg,    # CAD → same as DWG
        "gltf": paths.models_gltf,
        "pdf":  paths.diagram,       # PDFs → /diagram
    }[dir_lower]


def execute_copy(
    entries: list[ModelEntry],
    root_folder_id: str,
    structure: str,
    paths: InputPaths,
) -> Generator[ModelCopyProgress, None, None]:
    """Copy model files into each SKU's product folder, creating orphan SKU folders as needed."""
    products_skus = _collect_sku_folders(root_folder_id, structure)
    # Cache supplier folder IDs so we don't re-create them for each orphan SKU.
    supplier_cache: dict[str, str] = (
        dict(drive.list_folders(root_folder_id)) if structure == "supplier" else {}
    )

    for e in entries:
        # Resolve (or create) the destination SKU folder.
        if e.sku_name in products_skus:
            sku_dest_id = products_skus[e.sku_name][0]
        elif structure == "flat":
            sku_dest_id = drive.find_or_create_folder(e.sku_name, root_folder_id)
        else:
            supplier = e.supplier_name or "_unsorted"
            if supplier not in supplier_cache:
                supplier_cache[supplier] = drive.find_or_create_folder(supplier, root_folder_id)
            sku_dest_id = drive.find_or_create_folder(e.sku_name, supplier_cache[supplier])

        # Collect candidate files: (file_id, file_name, source_label, dest_subdir)
        candidates: list[tuple[str, str, str, str]] = []
        for dir_canonical, src_folder_id in e.dir_sources.items():
            dest_subdir = _dest_subdir(dir_canonical, paths)
            for f in drive.list_files(src_folder_id):
                candidates.append((f["id"], f["name"], dir_canonical.upper(), dest_subdir))

        if not candidates:
            yield ModelCopyProgress(e.sku_name, 0, 0, "")
            continue

        # Resolve and cache each destination subfolder once, with its existing file names.
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

        for i, (file_id, file_name, src_label, dest_subdir) in enumerate(to_copy, 1):
            dest_id = dest_cache[dest_subdir][0]
            drive.copy_file(file_id, dest_id, file_name)
            yield ModelCopyProgress(e.sku_name, i, len(to_copy), src_label)
