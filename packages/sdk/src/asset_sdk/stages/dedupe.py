from __future__ import annotations

from dataclasses import dataclass
from typing import Generator, NamedTuple

from asset_sdk.adapters import drive


@dataclass
class DedupePlan:
    sku: str
    supplier: str            # supplier of the duplicate folder being acted on
    primary_supplier: str    # supplier of the primary folder (for MERGE destination)
    action: str              # DELETE | MERGE
    dup_folder_id: str
    primary_folder_id: str


def _collect_locations(
    root_folder_id: str, structure: str,
) -> dict[tuple[str, str], list[str]]:
    """Return {(supplier, sku): [folder_id, ...]} preserving duplicates."""
    locations: dict[tuple[str, str], list[str]] = {}
    if structure == "flat":
        for item in drive.list_children(root_folder_id):
            if item["kind"] == "folder":
                locations.setdefault(("", item["name"]), []).append(item["id"])
    else:
        for sup in drive.list_children(root_folder_id):
            if sup["kind"] != "folder":
                continue
            for sku in drive.list_children(sup["id"]):
                if sku["kind"] == "folder":
                    locations.setdefault((sup["name"], sku["name"]), []).append(sku["id"])
    return locations


def _pick_primary_idx(occurrences: list[tuple[str, str]]) -> int:
    """Same heuristic as diagnose: most-subfolders wins (ties → first)."""
    if len(occurrences) == 1:
        return 0
    best_idx, best_score = 0, -1
    for i, (_, fid) in enumerate(occurrences):
        n = len(drive.list_folders(fid))
        if n > best_score:
            best_idx, best_score = i, n
    return best_idx


def build_plan(
    report_rows: list[dict],
    root_folder_id: str,
    structure: str,
) -> tuple[list[DedupePlan], list[str]]:
    """Read the diagnose report and produce a list of DELETE/MERGE actions.

    For each duplicate row (isDuplicate=TRUE) with a Suggested Action of DELETE or MERGE,
    we resolve which Drive folder is the duplicate (vs the primary) and queue the action.
    """
    locations = _collect_locations(root_folder_id, structure)

    # Group every Drive occurrence by SKU and pick the primary using the same heuristic
    # diagnose uses, so the report's primary/duplicate split lines up with reality.
    by_sku: dict[str, list[tuple[str, str]]] = {}  # {sku: [(supplier, folder_id), ...]}
    for (sup, sku), fids in locations.items():
        for fid in fids:
            by_sku.setdefault(sku, []).append((sup, fid))

    primary_by_sku: dict[str, tuple[str, str]] = {}
    dup_pool: dict[str, list[tuple[str, str]]] = {}
    for sku, occurrences in by_sku.items():
        pidx = _pick_primary_idx(occurrences)
        primary_by_sku[sku] = occurrences[pidx]
        dup_pool[sku] = [o for i, o in enumerate(occurrences) if i != pidx]

    plans: list[DedupePlan] = []
    warnings: list[str] = []

    for row in report_rows:
        if str(row.get("isDuplicate", "")).strip().upper() != "TRUE":
            continue
        action = str(row.get("Suggested Action", "")).strip().upper()
        if action not in ("DELETE", "MERGE"):
            continue

        sku = str(row.get("SKU", "")).strip()
        supplier = str(row.get("Supplier", "")).strip()
        if not sku:
            continue

        pool = dup_pool.get(sku, [])
        # Match by supplier first; if multiple match (rare: two dups in same supplier), take in order.
        match_idx = next(
            (i for i, (sup, _) in enumerate(pool) if sup == supplier),
            None,
        )
        if match_idx is None:
            warnings.append(
                f"No duplicate folder found in Drive for {supplier}/{sku} (already cleaned up?)"
            )
            continue

        sup, dup_id = pool.pop(match_idx)
        primary_sup, primary_id = primary_by_sku[sku]
        plans.append(DedupePlan(
            sku=sku, supplier=sup, primary_supplier=primary_sup,
            action=action, dup_folder_id=dup_id, primary_folder_id=primary_id,
        ))

    return plans, warnings


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

class DedupeProgress(NamedTuple):
    plan: DedupePlan
    index: int
    total: int
    files_copied: int   # nonzero only on MERGE


def _merge_into(src_folder_id: str, dest_folder_id: str) -> int:
    """Recursively copy files from src into dest, preserving subfolder structure.

    Skips files whose name already exists at the destination subfolder (idempotent).
    Returns the count of files actually copied.
    """
    copied = 0

    def _walk(src_id: str, dst_id: str) -> int:
        local = 0
        existing_files = {f["name"] for f in drive.list_files(dst_id)}
        existing_subdirs = drive.list_folders(dst_id)
        for item in drive.list_children(src_id):
            if item["kind"] == "folder":
                if item["name"] in existing_subdirs:
                    sub_dst = existing_subdirs[item["name"]]
                else:
                    sub_dst = drive.find_or_create_folder(item["name"], dst_id)
                local += _walk(item["id"], sub_dst)
            else:
                if item["name"] not in existing_files:
                    drive.copy_file(item["id"], dst_id, item["name"])
                    existing_files.add(item["name"])
                    local += 1
        return local

    copied = _walk(src_folder_id, dest_folder_id)
    return copied


def execute(plans: list[DedupePlan]) -> Generator[DedupeProgress, None, None]:
    for i, p in enumerate(plans, 1):
        copied = 0
        if p.action == "MERGE":
            copied = _merge_into(p.dup_folder_id, p.primary_folder_id)
        # Both DELETE and MERGE end with trashing the duplicate folder.
        drive.trash_item(p.dup_folder_id)
        yield DedupeProgress(p, i, len(plans), copied)
