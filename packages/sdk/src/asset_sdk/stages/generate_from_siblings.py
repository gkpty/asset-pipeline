"""Generate missing assets by copying from a sibling SKU under the same parent product.

For asset kinds like assembly_instructions, diagram, or models/dwg, products that
share the same parent_product typically share the same documentation. So if SKU-A
is missing an assembly_instructions/ folder but SKU-B (same parent) has one, we
can copy SKU-B's files into SKU-A.

Tie-break when multiple siblings have the asset: first one in sheet row order
(predictable; the dry-run report lets the user override the source).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generator, NamedTuple

from asset_sdk.adapters import drive


@dataclass
class CopyPlan:
    sku: str
    supplier: str
    parent_product: str
    kind: str                 # type label, e.g. "diagram", "assembly_instructions", "models_dwg"
    source_sku: str           # "" when no source was found
    source_supplier: str
    file_count: int           # files in the source's kind folder; 0 when no source
    action: str               # COPY | SKIP
    notes: str = ""
    # Cached IDs we resolved during build_plan (used by execute when not edited).
    target_sku_folder_id: str = ""
    source_sku_folder_id: str = ""
    source_kind_folder_id: str = ""


def _build_sku_index(category_folder_id: str, structure: str) -> dict[str, tuple[str, str]]:
    """Return {sku: (supplier, sku_folder_id)} for every SKU folder under the category."""
    index: dict[str, tuple[str, str]] = {}
    if structure == "flat":
        for sku, sid in drive.list_folders(category_folder_id).items():
            index[sku] = ("", sid)
    else:
        for sup_name, sup_id in drive.list_folders(category_folder_id).items():
            for sku, sid in drive.list_folders(sup_id).items():
                index[sku] = (sup_name, sid)
    return index


def _resolve_optional(parent_id: str, rel_path: str) -> str | None:
    """Walk rel_path (e.g. 'models/dwg') from parent_id; None if any segment missing."""
    current = parent_id
    for part in rel_path.split("/"):
        children = drive.list_folders(current)
        if part not in children:
            return None
        current = children[part]
    return current


def _resolve_or_create(parent_id: str, rel_path: str) -> str:
    """Walk rel_path from parent_id, creating any missing intermediate folders."""
    current = parent_id
    for part in rel_path.split("/"):
        current = drive.find_or_create_folder(part, current)
    return current


def build_plan(
    category_folder_id: str,
    structure: str,
    sheet_rows: list[dict[str, str]],
    sku_col: str,
    supplier_col: str,
    parent_product_col: str,
    kind_label: str,
    kind_path: str,
    sku_index: dict[str, tuple[str, str]] | None = None,
    part_col: str = "",
) -> list[CopyPlan]:
    """Walk the category and produce one CopyPlan per SKU in the sheet for one kind.

    `sku_index` may be passed in to avoid re-walking the category folder when
    building plans for multiple kinds back-to-back; if omitted, it's built here.

    `part_col` (optional): when provided, siblings must match on BOTH parent_product
    AND this column's value. Used for modular products (e.g. modular sofas) where
    parts within the same parent_product are physically distinct and can't share
    assets. Empty values match empty values, so non-modular products are unaffected.

    Behavior per SKU:
      - If the SKU has no folder on Drive → SKIP, note 'sku folder not found'.
      - If the target kind subfolder already has files → SKIP, note 'already populated'.
      - If no sibling under the same (parent_product, part) has files in that kind
        → SKIP, note 'no sibling with files'.
      - Otherwise → COPY from the first sibling in sheet row order that has files.
    """
    if sku_index is None:
        sku_index = _build_sku_index(category_folder_id, structure)

    # Build {(parent_product, part): [(sku, supplier), ...]} preserving sheet row order.
    # `part` is empty for non-modular products and matches empty-to-empty cleanly,
    # so non-modular catalogs behave exactly like a parent-only group.
    group_to_skus: dict[tuple[str, str], list[tuple[str, str]]] = {}
    sku_to_group: dict[str, tuple[str, str]] = {}
    for row in sheet_rows:
        sku = (row.get(sku_col) or "").strip()
        sup = (row.get(supplier_col) or "").strip()
        parent = (row.get(parent_product_col) or "").strip()
        part = (row.get(part_col) or "").strip() if part_col else ""
        if not sku or not parent:
            continue
        key = (parent, part)
        sku_to_group[sku] = key
        group_to_skus.setdefault(key, []).append((sku, sup))

    # Cache (sku → kind folder id, file count) so siblings aren't re-queried.
    kind_cache: dict[str, tuple[str | None, int]] = {}

    def _get_kind(sku: str) -> tuple[str | None, int]:
        if sku in kind_cache:
            return kind_cache[sku]
        if sku not in sku_index:
            kind_cache[sku] = (None, 0)
            return kind_cache[sku]
        _, sku_id = sku_index[sku]
        kind_id = _resolve_optional(sku_id, kind_path)
        count = drive.count_files(kind_id) if kind_id else 0
        kind_cache[sku] = (kind_id, count)
        return kind_cache[sku]

    plans: list[CopyPlan] = []
    for row in sheet_rows:
        sku = (row.get(sku_col) or "").strip()
        sup = (row.get(supplier_col) or "").strip()
        group_key = sku_to_group.get(sku)
        if not sku or not group_key:
            continue
        parent, part = group_key

        if sku not in sku_index:
            plans.append(CopyPlan(
                sku=sku, supplier=sup, parent_product=parent, kind=kind_label,
                source_sku="", source_supplier="", file_count=0,
                action="SKIP", notes="sku folder not found in Drive",
            ))
            continue

        target_sup, target_sku_id = sku_index[sku]
        my_kind_id, my_count = _get_kind(sku)
        if my_count > 0:
            plans.append(CopyPlan(
                sku=sku, supplier=target_sup, parent_product=parent, kind=kind_label,
                source_sku="", source_supplier="", file_count=my_count,
                action="SKIP", notes="target already populated",
                target_sku_folder_id=target_sku_id,
            ))
            continue

        siblings = [(s, sp) for (s, sp) in group_to_skus.get(group_key, []) if s != sku]
        chosen_sku = ""
        chosen_sup = ""
        chosen_kind_id = ""
        chosen_count = 0
        for sib_sku, sib_sup_sheet in siblings:
            sib_kind_id, sib_count = _get_kind(sib_sku)
            if sib_kind_id and sib_count > 0:
                chosen_sku = sib_sku
                chosen_sup = sku_index[sib_sku][0] or sib_sup_sheet
                chosen_kind_id = sib_kind_id
                chosen_count = sib_count
                break

        no_sibling_note = (
            "no sibling with files (under same parent_product"
            + (f" + part={part!r}" if part else "")
            + ")"
        )
        if not chosen_sku:
            plans.append(CopyPlan(
                sku=sku, supplier=target_sup, parent_product=parent, kind=kind_label,
                source_sku="", source_supplier="", file_count=0,
                action="SKIP", notes=no_sibling_note,
                target_sku_folder_id=target_sku_id,
            ))
            continue

        plans.append(CopyPlan(
            sku=sku, supplier=target_sup, parent_product=parent, kind=kind_label,
            source_sku=chosen_sku, source_supplier=chosen_sup, file_count=chosen_count,
            action="COPY", notes="",
            target_sku_folder_id=target_sku_id,
            source_sku_folder_id=sku_index[chosen_sku][1],
            source_kind_folder_id=chosen_kind_id,
        ))

    return plans


def to_sheet_rows(plans: list[CopyPlan]) -> tuple[list[str], list[list]]:
    headers = [
        "SKU", "Supplier", "Parent Product", "Type",
        "Source SKU", "Source Supplier", "File Count",
        "Action", "Notes",
    ]
    rows: list[list] = []
    for p in plans:
        rows.append([
            p.sku, p.supplier, p.parent_product, p.kind,
            p.source_sku, p.source_supplier, p.file_count,
            p.action, p.notes,
        ])
    return headers, rows


class CopyProgress(NamedTuple):
    sku: str
    kind: str
    source_sku: str
    file_index: int
    file_total: int
    skipped: bool
    error: str = ""


def execute(
    report_rows: list[dict[str, str]],
    category_folder_id: str,
    structure: str,
    kind_paths: dict[str, str],
) -> Generator[CopyProgress, None, None]:
    """Read the (possibly edited) report and copy files for every Action=COPY row.

    `kind_paths` maps each type label (e.g. "diagram", "models_dwg") to its
    relative path under the SKU folder ("diagram", "models/dwg"). The Type column
    in each row drives which path applies. Users can change Source SKU or set
    Action=SKIP to override the auto-pick.
    """
    sku_index = _build_sku_index(category_folder_id, structure)

    for row in report_rows:
        action = (row.get("Action") or "").strip().upper()
        if action != "COPY":
            continue

        sku = (row.get("SKU") or "").strip()
        source_sku = (row.get("Source SKU") or "").strip()
        kind = (row.get("Type") or "").strip().lower()
        kind_path = kind_paths.get(kind)

        if not kind_path:
            yield CopyProgress(
                sku=sku, kind=kind, source_sku=source_sku, file_index=0, file_total=0,
                skipped=True, error=f"unknown Type {kind!r} in report row",
            )
            continue
        if not sku or not source_sku:
            yield CopyProgress(
                sku=sku, kind=kind, source_sku=source_sku, file_index=0, file_total=0,
                skipped=True, error="missing SKU or Source SKU in report row",
            )
            continue
        if sku not in sku_index:
            yield CopyProgress(
                sku=sku, kind=kind, source_sku=source_sku, file_index=0, file_total=0,
                skipped=True, error=f"target SKU folder not found: {sku}",
            )
            continue
        if source_sku not in sku_index:
            yield CopyProgress(
                sku=sku, kind=kind, source_sku=source_sku, file_index=0, file_total=0,
                skipped=True, error=f"source SKU folder not found: {source_sku}",
            )
            continue

        _, target_sku_id = sku_index[sku]
        _, source_sku_id = sku_index[source_sku]

        source_kind_id = _resolve_optional(source_sku_id, kind_path)
        if not source_kind_id:
            yield CopyProgress(
                sku=sku, kind=kind, source_sku=source_sku, file_index=0, file_total=0,
                skipped=True, error=f"source kind folder missing: {source_sku}/{kind_path}",
            )
            continue

        files = drive.list_files(source_kind_id)
        files = [f for f in files if not f["name"].startswith(".")]
        if not files:
            yield CopyProgress(
                sku=sku, kind=kind, source_sku=source_sku, file_index=0, file_total=0,
                skipped=True, error="source kind folder is empty",
            )
            continue

        try:
            target_kind_id = _resolve_or_create(target_sku_id, kind_path)
        except Exception as exc:
            yield CopyProgress(
                sku=sku, kind=kind, source_sku=source_sku, file_index=0, file_total=0,
                skipped=False, error=f"could not create target kind folder: {exc}",
            )
            continue

        # Refresh existing target file names so a partially-populated target is handled gracefully.
        existing = {f["name"] for f in drive.list_files(target_kind_id)}

        for i, f in enumerate(files, 1):
            if f["name"] in existing:
                yield CopyProgress(
                    sku=sku, kind=kind, source_sku=source_sku, file_index=i, file_total=len(files),
                    skipped=True,
                )
                continue
            try:
                drive.copy_file(f["id"], target_kind_id, f["name"])
                yield CopyProgress(
                    sku=sku, kind=kind, source_sku=source_sku, file_index=i, file_total=len(files),
                    skipped=False,
                )
            except Exception as exc:
                yield CopyProgress(
                    sku=sku, kind=kind, source_sku=source_sku, file_index=i, file_total=len(files),
                    skipped=False, error=str(exc),
                )


def summarise(plans: list[CopyPlan]) -> dict[str, int]:
    return {
        "total": len(plans),
        "to_copy": sum(1 for p in plans if p.action == "COPY"),
        "skipped": sum(1 for p in plans if p.action == "SKIP"),
    }
