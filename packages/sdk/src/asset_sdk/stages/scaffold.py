"""Scaffold the products drive: create missing SKU folders, ensure each SKU has the
canonical subdirectory structure from `paths.input`, and (with --fix / --clean)
correct loose files, typo'd folder names, and remove or quarantine non-canonical
directories.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from difflib import get_close_matches
from pathlib import PurePosixPath
from typing import Generator, NamedTuple

from asset_sdk.adapters import drive
from asset_sdk.config import InputPaths

# Files we always treat as junk under --clean.
_JUNK_NAMES = {".ds_store", "thumbs.db", "desktop.ini"}
# Image extensions routed to product_photos under --fix.
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff", ".tif", ".gif"}


# ---------------------------------------------------------------------------
# Action model
# ---------------------------------------------------------------------------

@dataclass
class Action:
    kind: str
    sku: str
    supplier: str
    description: str
    src_id: str | None = None
    src_name: str | None = None
    target_name: str | None = None
    target_subdir: str | None = None
    target_rel_path: str | None = None
    parent_id: str | None = None


_ACTION_PRIORITY = {
    "CREATE_SKU":     1,
    "RENAME_DIR":     2,
    "CREATE_SUBDIR":  3,
    "MOVE_FILE":      4,
    "DELETE_FILE":    5,
    "MOVE_DIR":       6,
    "DELETE_DIR":     7,
    "DUPLICATE_DIR":  99,   # informational only
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _canonical_top_level_dirs(paths: InputPaths) -> set[str]:
    """First path component of every entry in InputPaths."""
    return {entry[2].split("/")[0] for entry in paths.entries()}


def _classify_pdf(filename: str) -> str:
    """Return the canonical-key subdir to route a PDF into based on filename keywords.

    Returns one of: 'assembly_instructions', 'carton_layout', 'diagram'.
    """
    name = filename.lower()
    if any(k in name for k in ("assembly", "instruct", "instruccion", "manual")):
        return "assembly_instructions"
    if any(k in name for k in ("carton", "package", "packaging", "embalaje")):
        return "carton_layout"
    return "diagram"


# ---------------------------------------------------------------------------
# Build plan
# ---------------------------------------------------------------------------

def build_plan(
    root_folder_id: str,
    sheet_rows: list[dict],
    sku_col: str,
    supplier_col: str,
    paths: InputPaths,
    structure: str,
    fix: bool = False,
    clean: bool = False,
    move_unknown: bool = False,
    typo_cutoff: float = 0.65,
    internal_dirs: list[str] | None = None,
) -> list[Action]:
    """Walk the products drive and emit the actions needed to scaffold/fix/clean it.

    `internal_dirs`: if provided, only subdirs whose rel_path is in this list
    get CREATE_SUBDIR actions. When None, every entry in `paths.entries()` is
    scaffolded (current default). Used for categories like upholstery that
    only need a photos/ folder, not the full product subdir tree.
    Note: this only filters what gets *created*. Cleanup (`--clean`) and
    file routing (`--fix`) still use the full canonical paths set.
    """
    actions: list[Action] = []
    canonical_top = _canonical_top_level_dirs(paths)
    canonical_top_lower = {c.lower(): c for c in canonical_top}

    # Resolve which paths to scaffold. Filter by rel_path, not by key — the
    # CLI passes the visible folder names (photos, lifestyle, models/dwg).
    all_entries = paths.entries()
    if internal_dirs is None:
        scaffold_entries = all_entries
    else:
        wanted = {d.strip().lower() for d in internal_dirs if d.strip()}
        scaffold_entries = [e for e in all_entries if e[2].lower() in wanted]

    # 1. Snapshot existing drive structure
    if structure == "flat":
        existing_skus: dict[str, tuple[str, str]] = {
            name: ("", fid) for name, fid in drive.list_folders(root_folder_id).items()
        }
    else:
        existing_skus = {}
        for sup_name, sup_id in drive.list_folders(root_folder_id).items():
            for name, fid in drive.list_folders(sup_id).items():
                existing_skus[name] = (sup_name, fid)

    # 2. CREATE_SKU for any sheet SKU missing in drive
    sheet_sku_to_supplier: dict[str, str] = {}
    for row in sheet_rows:
        sku = str(row.get(sku_col, "")).strip()
        if not sku or "-" not in sku:
            continue  # skip category-separator rows
        sheet_sku_to_supplier[sku] = str(row.get(supplier_col, "")).strip()

    skus_to_create: list[tuple[str, str]] = []
    for sku, supplier in sheet_sku_to_supplier.items():
        if sku in existing_skus:
            continue
        if structure == "supplier" and not supplier:
            actions.append(Action(
                kind="DUPLICATE_DIR", sku=sku, supplier="",
                description="Sheet SKU has no supplier — cannot decide where to create the folder",
            ))
            continue
        skus_to_create.append((sku, supplier))
        actions.append(Action(
            kind="CREATE_SKU", sku=sku, supplier=supplier,
            description="Create SKU folder",
            target_name=sku,
        ))

    # 3. Walk every SKU (existing + to-be-created) and ensure structure
    all_skus: list[tuple[str, str, str | None]] = [
        (sku, sup, fid) for sku, (sup, fid) in existing_skus.items()
    ] + [(sku, sup, None) for sku, sup in skus_to_create]

    for sku, supplier, sku_id in all_skus:
        if sku_id is None:
            # New SKU: emit CREATE_SUBDIR for every canonical path (or only
            # those in internal_dirs if a filter was passed).
            for _key, _disp, rel_path in scaffold_entries:
                actions.append(Action(
                    kind="CREATE_SUBDIR", sku=sku, supplier=supplier,
                    description=f"Create '{rel_path}/' inside new SKU",
                    target_rel_path=rel_path,
                ))
            continue

        children = drive.list_children(sku_id)
        existing_subdirs: dict[str, str] = {
            it["name"]: it["id"] for it in children if it["kind"] == "folder"
        }
        loose_files = [it for it in children if it["kind"] == "file"]

        # 3a. Detect duplicate subfolders (case-insensitive collision)
        if fix:
            by_lower: dict[str, list[tuple[str, str]]] = {}
            for name, fid in existing_subdirs.items():
                by_lower.setdefault(name.lower(), []).append((name, fid))
            for low, group in by_lower.items():
                if len(group) > 1:
                    names = ", ".join(sorted(n for n, _ in group))
                    actions.append(Action(
                        kind="DUPLICATE_DIR", sku=sku, supplier=supplier,
                        description=f"Duplicate folders matching '{low}': {names} (manual review needed)",
                    ))

        # 3b. Typo / wrong-case rename
        renamed_to: dict[str, str] = {}  # old_name → canonical_name (so subsequent steps see the rename)
        if fix:
            canonical_list = [c.lower() for c in canonical_top]
            # Track targets that are already taken (either pre-existing OR claimed by an earlier rename).
            taken_targets: set[str] = set(existing_subdirs.keys())
            for name, fid in list(existing_subdirs.items()):
                if name in canonical_top:
                    continue
                # Wrong-case match (e.g. "Photos" → "photos")
                if name.lower() in canonical_top_lower and name != canonical_top_lower[name.lower()]:
                    canonical = canonical_top_lower[name.lower()]
                    if canonical in taken_targets and canonical != name:
                        # The properly-named folder already exists alongside the wrong-case one.
                        continue
                    actions.append(Action(
                        kind="RENAME_DIR", sku=sku, supplier=supplier,
                        description=f"Rename '{name}/' → '{canonical}/' (case fix)",
                        src_id=fid, src_name=name, target_name=canonical,
                    ))
                    renamed_to[name] = canonical
                    taken_targets.discard(name)
                    taken_targets.add(canonical)
                    continue
                # Fuzzy match against canonical names
                matches = get_close_matches(name.lower(), canonical_list, n=1, cutoff=typo_cutoff)
                if not matches:
                    continue
                canonical = canonical_top_lower[matches[0]]
                if canonical in taken_targets:
                    # Either the canonical already exists, or another rename has claimed it this run.
                    continue
                actions.append(Action(
                    kind="RENAME_DIR", sku=sku, supplier=supplier,
                    description=f"Rename '{name}/' → '{canonical}/' (typo fix)",
                    src_id=fid, src_name=name, target_name=canonical,
                ))
                renamed_to[name] = canonical
                taken_targets.discard(name)
                taken_targets.add(canonical)

        # Apply renames to the local view so CREATE_SUBDIR / DELETE_DIR see post-rename state.
        for old, new in renamed_to.items():
            existing_subdirs[new] = existing_subdirs.pop(old)

        # 3c. CREATE_SUBDIR for missing canonical paths (with caching).
        # Honors --internal-dirs by iterating only over the filtered entries.
        folder_cache: dict[str, dict[str, str]] = {sku_id: dict(existing_subdirs)}
        for _key, _disp, rel_path in scaffold_entries:
            current_id = sku_id
            for part in rel_path.split("/"):
                if current_id not in folder_cache:
                    folder_cache[current_id] = {
                        f["name"]: f["id"]
                        for f in drive.list_children(current_id) if f["kind"] == "folder"
                    }
                if part not in folder_cache[current_id]:
                    actions.append(Action(
                        kind="CREATE_SUBDIR", sku=sku, supplier=supplier,
                        description=f"Create '{rel_path}/' subdir",
                        target_rel_path=rel_path,
                    ))
                    break
                current_id = folder_cache[current_id][part]

        # 3d. --fix: route loose files into the right subdir
        if fix:
            for f in loose_files:
                name = f["name"]
                # Junk is owned by --clean, not --fix
                if name.lower() in _JUNK_NAMES:
                    continue
                ext = PurePosixPath(name).suffix.lower()
                if ext in _IMAGE_EXTS:
                    target_subdir = paths.product_photos
                    actions.append(Action(
                        kind="MOVE_FILE", sku=sku, supplier=supplier,
                        description=f"Move image '{name}' → '{target_subdir}/'",
                        src_id=f["id"], src_name=name, target_subdir=target_subdir,
                    ))
                elif ext == ".pdf":
                    cls = _classify_pdf(name)
                    target_subdir = getattr(paths, cls)
                    actions.append(Action(
                        kind="MOVE_FILE", sku=sku, supplier=supplier,
                        description=f"Move PDF '{name}' → '{target_subdir}/' (classified as {cls})",
                        src_id=f["id"], src_name=name, target_subdir=target_subdir,
                    ))

        # 3e. --clean: junk files + non-canonical dirs
        if clean:
            for f in loose_files:
                if f["name"].lower() in _JUNK_NAMES:
                    actions.append(Action(
                        kind="DELETE_FILE", sku=sku, supplier=supplier,
                        description=f"Delete junk file '{f['name']}'",
                        src_id=f["id"], src_name=f["name"],
                    ))
            for name, fid in existing_subdirs.items():
                if name in canonical_top:
                    continue
                if move_unknown:
                    actions.append(Action(
                        kind="MOVE_DIR", sku=sku, supplier=supplier,
                        description=f"Move '{name}/' → MOVED_FOLDER/{name}/{sku}/",
                        src_id=fid, src_name=name,
                    ))
                else:
                    actions.append(Action(
                        kind="DELETE_DIR", sku=sku, supplier=supplier,
                        description=f"Delete '{name}/'",
                        src_id=fid, src_name=name,
                    ))

    return actions


def to_sheet_rows(actions: list[Action]) -> tuple[list[str], list[list]]:
    headers = ["SKU", "Supplier", "Action", "Detail"]
    sorted_actions = sorted(
        actions,
        key=lambda a: (a.sku, _ACTION_PRIORITY.get(a.kind, 99)),
    )
    rows = [
        [a.sku, a.supplier, a.kind, a.description]
        for a in sorted_actions
    ]
    return headers, rows


def summarise(actions: list[Action]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for a in actions:
        counts[a.kind] = counts.get(a.kind, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

class ScaffoldProgress(NamedTuple):
    action: Action
    index: int
    total: int
    error: str | None = None


def execute(
    actions: list[Action],
    root_folder_id: str,
    structure: str,
    moved_folder_name: str = "MOVED_FOLDER",
    moved_folder_id: str = "",
) -> Generator[ScaffoldProgress, None, None]:
    """Execute actions in the safe priority order."""
    sorted_actions = sorted(
        actions,
        key=lambda a: (_ACTION_PRIORITY.get(a.kind, 99), a.sku),
    )
    actionable = [a for a in sorted_actions if a.kind != "DUPLICATE_DIR"]
    total = len(actionable)

    # Existing SKU index so non-CREATE_SKU actions can find their SKU folder ID.
    if structure == "flat":
        sku_id_cache: dict[tuple[str, str], str] = {
            (name, ""): fid for name, fid in drive.list_folders(root_folder_id).items()
        }
        supplier_cache: dict[str, str] = {}
    else:
        supplier_cache = dict(drive.list_folders(root_folder_id))
        sku_id_cache = {}
        for sup_name, sup_id in supplier_cache.items():
            for name, fid in drive.list_folders(sup_id).items():
                sku_id_cache[(name, sup_name)] = fid

    # If a specific Drive folder ID was passed, use it; otherwise lazily find/create
    # `moved_folder_name` under the products root on first MOVE_DIR.
    moved_root_id: str | None = moved_folder_id or None
    moved_category_cache: dict[str, str] = {}

    for i, a in enumerate(actionable, 1):
        try:
            if a.kind == "CREATE_SKU":
                if structure == "supplier":
                    if a.supplier not in supplier_cache:
                        supplier_cache[a.supplier] = drive.find_or_create_folder(a.supplier, root_folder_id)
                    parent = supplier_cache[a.supplier]
                else:
                    parent = root_folder_id
                sku_id = drive.find_or_create_folder(a.sku, parent)
                sku_id_cache[(a.sku, a.supplier)] = sku_id

            else:
                sku_id = sku_id_cache.get((a.sku, a.supplier))
                if sku_id is None and a.kind in ("CREATE_SUBDIR", "MOVE_FILE"):
                    # Skip — SKU folder doesn't exist (CREATE_SKU may have been skipped).
                    yield ScaffoldProgress(a, i, total, error="SKU folder missing")
                    continue

                if a.kind == "RENAME_DIR":
                    drive.rename_item(a.src_id, a.target_name)
                elif a.kind == "CREATE_SUBDIR":
                    current = sku_id
                    for part in a.target_rel_path.split("/"):
                        current = drive.find_or_create_folder(part, current)
                elif a.kind == "MOVE_FILE":
                    current = sku_id
                    for part in a.target_subdir.split("/"):
                        current = drive.find_or_create_folder(part, current)
                    drive.move_item(a.src_id, current)
                elif a.kind == "DELETE_FILE":
                    drive.trash_item(a.src_id)
                elif a.kind == "DELETE_DIR":
                    drive.trash_item(a.src_id)
                elif a.kind == "MOVE_DIR":
                    if moved_root_id is None:
                        moved_root_id = drive.find_or_create_folder(moved_folder_name, root_folder_id)
                    if a.src_name not in moved_category_cache:
                        moved_category_cache[a.src_name] = drive.find_or_create_folder(a.src_name, moved_root_id)
                    category_id = moved_category_cache[a.src_name]
                    # Rename to <sku> and move into the category.
                    drive.rename_item(a.src_id, a.sku)
                    drive.move_item(a.src_id, category_id)

            yield ScaffoldProgress(a, i, total)
        except Exception as exc:
            yield ScaffoldProgress(a, i, total, error=str(exc))
