from __future__ import annotations

from dataclasses import dataclass
from typing import Generator, NamedTuple

from asset_sdk.adapters import drive


@dataclass
class RenamePlan:
    sku: str           # current folder name
    supplier: str      # supplier folder ('' in flat mode)
    new_sku: str       # target folder name (from Suggested Rename column)
    folder_id: str     # Drive folder ID to rename


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


def build_plan(
    report_rows: list[dict],
    root_folder_id: str,
    structure: str,
) -> tuple[list[RenamePlan], list[str]]:
    """Build a rename plan from the diagnose report.

    Returns (plans, warnings). Rows with no Suggested Rename are silently skipped;
    warnings describe rows that couldn't be planned (missing folder, duplicate, collision).
    """
    locations = _collect_locations(root_folder_id, structure)
    existing_skus = {sku for _, sku in locations.keys()}

    plans: list[RenamePlan] = []
    warnings: list[str] = []

    # Pass 1: build candidate plans
    candidates: list[RenamePlan] = []
    for row in report_rows:
        new_sku = str(row.get("Suggested Rename", "")).strip()
        if not new_sku:
            continue

        is_duplicate = str(row.get("isDuplicate", "")).strip().upper() == "TRUE"
        sku = str(row.get("SKU", "")).strip()
        supplier = str(row.get("Supplier", "")).strip()

        if not sku:
            continue

        if is_duplicate:
            warnings.append(
                f"Skipping duplicate {supplier}/{sku} → {new_sku} (resolve duplicate first)"
            )
            continue

        key = (supplier, sku)
        folder_ids = locations.get(key, [])
        if not folder_ids:
            warnings.append(f"Folder not found: {supplier}/{sku} (cannot rename to {new_sku})")
            continue
        if len(folder_ids) > 1:
            warnings.append(
                f"Multiple folders match {supplier}/{sku} — skipping (resolve duplicates first)"
            )
            continue

        candidates.append(RenamePlan(
            sku=sku, supplier=supplier, new_sku=new_sku, folder_id=folder_ids[0],
        ))

    # Pass 2: detect collisions (multiple sources → same target, or target already exists)
    target_counts: dict[str, int] = {}
    for p in candidates:
        target_counts[p.new_sku] = target_counts.get(p.new_sku, 0) + 1

    for p in candidates:
        if target_counts[p.new_sku] > 1:
            warnings.append(
                f"Skipping {p.supplier}/{p.sku} → {p.new_sku}: "
                f"multiple sources targeting the same name"
            )
            continue
        if p.new_sku in existing_skus and p.new_sku != p.sku:
            warnings.append(
                f"Skipping {p.supplier}/{p.sku} → {p.new_sku}: "
                f"target name already exists in products drive"
            )
            continue
        plans.append(p)

    return plans, warnings


class RenameProgress(NamedTuple):
    plan: RenamePlan
    index: int
    total: int


def execute_renames(plans: list[RenamePlan]) -> Generator[RenameProgress, None, None]:
    for i, p in enumerate(plans, 1):
        drive.rename_item(p.folder_id, p.new_sku)
        yield RenameProgress(p, i, len(plans))
