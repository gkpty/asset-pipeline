from __future__ import annotations

from dataclasses import dataclass, field
from difflib import get_close_matches

from asset_sdk.adapters import drive
from asset_sdk.config import InputPaths

# Similarity threshold for "Suggested Rename" — only on orphan rows.
_RENAME_SIMILARITY_CUTOFF = 0.6


@dataclass
class SkuRow:
    """One row in the diagnose report (one per disk occurrence, plus MISSING DIR rows)."""
    sku: str
    supplier: str
    status: str                       # OK | INCOMPLETE | MISSING DIR | ORPHAN DIR
    is_duplicate: bool                # True for non-primary occurrences of a duplicated SKU
    suggested_rename: str             # only set on ORPHAN DIR rows
    suggested_action: str             # DELETE | MERGE — only set when is_duplicate=True
    issues: str
    dir_counts: dict[str, int] = field(default_factory=dict)


@dataclass
class DiagnoseReport:
    rows: list[SkuRow]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_and_count(
    sku_folder_id: str,
    rel_path: str,
    folder_cache: dict[str, dict[str, str]],
) -> int | None:
    """Navigate rel_path down from sku_folder_id, caching intermediate folder listings."""
    current_id = sku_folder_id
    for part in rel_path.split("/"):
        if current_id not in folder_cache:
            folder_cache[current_id] = drive.list_folders(current_id)
        if part not in folder_cache[current_id]:
            return None
        current_id = folder_cache[current_id][part]
    return drive.count_files(current_id)


def _label(supplier: str, sku: str) -> str:
    return f"{supplier}/{sku}" if supplier else sku


@dataclass
class _FileInfo:
    size: int
    md5: str
    width: int | None
    height: int | None


def _collect_files(folder_id: str) -> list[_FileInfo]:
    """Recursively collect file info (size, md5, dimensions) for every file in folder_id."""
    files: list[_FileInfo] = []
    stack = [folder_id]
    while stack:
        fid = stack.pop()
        for item in drive.list_children_meta(fid):
            if item["kind"] == "folder":
                stack.append(item["id"])
            else:
                files.append(_FileInfo(
                    size=int(item.get("size") or 0),
                    md5=item.get("md5") or "",
                    width=item.get("width"),
                    height=item.get("height"),
                ))
    return files


def _human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{int(n)}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def _compare_contents(
    primary: list[_FileInfo],
    dup: list[_FileInfo],
) -> tuple[str, str]:
    """Compare two folder content snapshots ignoring filenames and folder structure.

    Strategy (first match wins):
      1. Identical byte content (size + md5 multiset matches) → DELETE
      2. Different file count → MERGE
      3. Same count + identical sorted sizes → DELETE
      4. Same count + identical sorted image dimensions → DELETE  (re-encoded photos)
      5. Same count + each size within 5% → DELETE
      6. Otherwise → MERGE
    """
    n_p, n_d = len(primary), len(dup)

    # Tier 1
    if sorted((f.size, f.md5) for f in primary) == sorted((f.size, f.md5) for f in dup):
        return "DELETE", f"Identical content ({n_p} files, same md5 + sizes)"

    # Tier 2
    if n_p != n_d:
        return "MERGE", f"Different file count ({n_p} vs {n_d})"

    primary_sizes = sorted(f.size for f in primary)
    dup_sizes = sorted(f.size for f in dup)

    # Tier 3
    if primary_sizes == dup_sizes:
        return "DELETE", (
            f"Same {n_p} files with identical sizes (different names/md5 but byte-size match)"
        )

    # Tier 4 — image dimensions match (works even when some files lack dim metadata,
    # as long as the dimensioned subsets are equal AND the non-dimensioned counts match).
    p_dims = sorted((f.width, f.height) for f in primary if f.width and f.height)
    d_dims = sorted((f.width, f.height) for f in dup if f.width and f.height)
    if (
        p_dims
        and p_dims == d_dims
        and (n_p - len(p_dims)) == (n_d - len(d_dims))
    ):
        non_img = n_p - len(p_dims)
        suffix = f" + {non_img} non-image files" if non_img else ""
        return "DELETE", (
            f"Same {len(p_dims)} images at identical pixel dimensions{suffix} "
            f"(filenames + bytes differ; re-encoded)"
        )

    # Tier 5 — per-pair sizes within 10% (was 5%): catches lossless metadata re-encoding.
    pairs = list(zip(primary_sizes, dup_sizes))
    if all(abs(a - b) / max(a, b, 1) < 0.10 for a, b in pairs):
        total_diff = sum(abs(a - b) for a, b in pairs)
        return "DELETE", (
            f"Same {n_p} files; sizes within 10% per pair "
            f"(Δ {_human_bytes(total_diff)} total) — likely re-encoded duplicates"
        )

    # Tier 6 — per-pair sizes within 30%: photos re-encoded at different quality settings
    # often differ this much. Sorted-pair alignment makes random collisions unlikely.
    if all(abs(a - b) / max(a, b, 1) < 0.30 for a, b in pairs):
        total_diff = sum(abs(a - b) for a, b in pairs)
        return "DELETE", (
            f"Same {n_p} files; sizes within 30% per pair "
            f"(Δ {_human_bytes(total_diff)} total) — likely same content at different quality"
        )

    # Tier 7 — last resort: fall through to MERGE
    total_diff = sum(abs(a - b) for a, b in pairs)
    return "MERGE", (
        f"Same count ({n_p}) but sizes differ materially "
        f"(Δ {_human_bytes(total_diff)} total) and dimensions don't match"
    )


def _pick_primary_idx(locations: list[tuple[str, str]]) -> int:
    """Return the index of the location with the most subfolders (ties → first)."""
    if len(locations) == 1:
        return 0
    best_idx, best_score = 0, -1
    for i, (_, fid) in enumerate(locations):
        n = len(drive.list_folders(fid))
        if n > best_score:
            best_idx, best_score = i, n
    return best_idx


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

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
    # Real SKUs contain dashes; category-separator rows put a bare label in the SKU column.
    known_skus: set[str] = {
        str(row.get(sku_col, "")).strip()
        for row in sheet_rows
        if "-" in str(row.get(sku_col, "")).strip()
    }

    # Collect every disk occurrence (preserving duplicates) via list_children.
    sku_items: list[tuple[str, str, str]] = []  # (sku, supplier, folder_id)
    if structure == "flat":
        for item in drive.list_children(root_folder_id):
            if item["kind"] == "folder":
                sku_items.append((item["name"], "", item["id"]))
    else:
        for sup in drive.list_children(root_folder_id):
            if sup["kind"] != "folder":
                continue
            for sku in drive.list_children(sup["id"]):
                if sku["kind"] == "folder":
                    sku_items.append((sku["name"], sup["name"], sku["id"]))

    # Group by SKU name → list of (supplier, folder_id)
    groups: dict[str, list[tuple[str, str]]] = {}
    for name, sup, fid in sku_items:
        groups.setdefault(name, []).append((sup, fid))

    primary_idx_by_sku: dict[str, int] = {
        sku: _pick_primary_idx(locs) for sku, locs in groups.items()
    }

    folder_cache: dict[str, dict[str, str]] = {}

    def _diag(folder_id: str) -> tuple[dict[str, int], list[str]]:
        dir_counts: dict[str, int] = {}
        missing_subdirs: list[str] = []
        for key, _display, rel_path in paths.entries():
            count = _resolve_and_count(folder_id, rel_path, folder_cache)
            if count is None:
                missing_subdirs.append(rel_path)
                dir_counts[key] = 0
            else:
                dir_counts[key] = count
        return dir_counts, missing_subdirs

    rows: list[SkuRow] = []
    missing_sheet_skus: list[str] = []  # populated during pass 1, used for orphan suggestions in pass 2

    # ---------- Pass 1: sheet rows ----------
    for sheet_row in sheet_rows:
        sku = str(sheet_row.get(sku_col, "")).strip()
        if not sku or "-" not in sku:
            continue
        supplier_hint = str(sheet_row.get(supplier_col, "")).strip()

        if sku not in groups:
            rows.append(SkuRow(
                sku=sku, supplier=supplier_hint, status="MISSING DIR",
                is_duplicate=False, suggested_rename="", suggested_action="",
                issues="",
            ))
            missing_sheet_skus.append(sku)
            continue

        locations = groups[sku]
        pidx = primary_idx_by_sku[sku]
        primary_sup, primary_id = locations[pidx]
        counts, missing_subdirs = _diag(primary_id)

        status = "INCOMPLETE" if missing_subdirs else "OK"
        issues = ("Missing: " + ", ".join(missing_subdirs)) if missing_subdirs else ""

        rows.append(SkuRow(
            sku=sku, supplier=primary_sup, status=status,
            is_duplicate=False, suggested_rename="", suggested_action="",
            issues=issues, dir_counts=counts,
        ))

        # Duplicate occurrences for this sheet SKU
        if len(locations) > 1:
            primary_files = _collect_files(primary_id)
            primary_label = _label(primary_sup, sku)
            for i, (sup, fid) in enumerate(locations):
                if i == pidx:
                    continue
                dup_counts, _ = _diag(fid)
                action, reason = _compare_contents(primary_files, _collect_files(fid))
                iss = f"vs {primary_label}: {reason}"
                rows.append(SkuRow(
                    sku=sku, supplier=sup, status=status,
                    is_duplicate=True, suggested_rename="", suggested_action=action,
                    issues=iss, dir_counts=dup_counts,
                ))

    # ---------- Pass 2: orphan SKUs (in drive, not in sheet) ----------
    orphan_skus = sorted(name for name in groups if name not in known_skus)
    for sku in orphan_skus:
        locations = groups[sku]
        pidx = primary_idx_by_sku[sku]
        primary_sup, primary_id = locations[pidx]
        counts, _ = _diag(primary_id)

        suggestion = ""
        if missing_sheet_skus:
            matches = get_close_matches(sku, missing_sheet_skus, n=1, cutoff=_RENAME_SIMILARITY_CUTOFF)
            if matches:
                suggestion = matches[0]

        rows.append(SkuRow(
            sku=sku, supplier=primary_sup, status="ORPHAN DIR",
            is_duplicate=False, suggested_rename=suggestion, suggested_action="",
            issues="Not in sheet", dir_counts=counts,
        ))

        if len(locations) > 1:
            primary_files = _collect_files(primary_id)
            primary_label = _label(primary_sup, sku)
            for i, (sup, fid) in enumerate(locations):
                if i == pidx:
                    continue
                dup_counts, _ = _diag(fid)
                action, reason = _compare_contents(primary_files, _collect_files(fid))
                iss = f"vs {primary_label}: {reason}; not in sheet"
                rows.append(SkuRow(
                    sku=sku, supplier=sup, status="ORPHAN DIR",
                    is_duplicate=True, suggested_rename=suggestion, suggested_action=action,
                    issues=iss, dir_counts=dup_counts,
                ))

    return DiagnoseReport(rows=rows)


def to_sheet_rows(
    report: DiagnoseReport,
    paths: InputPaths,
) -> tuple[list[str], list[list]]:
    entries = paths.entries()
    dir_headers = [display for _key, display, _path in entries]
    headers = [
        "SKU", "Supplier", "Status", "isDuplicate",
        "Suggested Rename", "Suggested Action", "Issues",
    ] + dir_headers

    rows: list[list] = []
    for r in report.rows:
        counts = [r.dir_counts.get(key, 0) for key, _d, _p in entries]
        rows.append([
            r.sku,
            r.supplier,
            r.status,
            "TRUE" if r.is_duplicate else "FALSE",
            r.suggested_rename,
            r.suggested_action,
            r.issues,
            *counts,
        ])

    return headers, rows
