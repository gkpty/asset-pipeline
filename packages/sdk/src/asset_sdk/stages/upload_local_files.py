from __future__ import annotations

import mimetypes
import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Generator, Iterable, NamedTuple

from asset_sdk.adapters import drive
from asset_sdk.config import InputPaths

# Friendly --type aliases → InputPaths field names.
_TYPE_ALIASES: dict[str, str] = {
    "photo": "product_photos",
    "photos": "product_photos",
    "product_photo": "product_photos",
    "product_photos": "product_photos",
    "lifestyle": "lifestyle_photos",
    "lifestyle_photo": "lifestyle_photos",
    "lifestyle_photos": "lifestyle_photos",
    "video": "videos",
    "videos": "videos",
    "diagram": "diagram",
    "assembly": "assembly_instructions",
    "assembly_instructions": "assembly_instructions",
    "carton": "carton_layout",
    "carton_layout": "carton_layout",
    "barcode": "barcode",
    "obj": "models_obj",
    "skp": "models_skp",
    "dwg": "models_dwg",
    "gltf": "models_gltf",
    "thumbnails_website": "thumbnails_website",
    "thumbnails_system": "thumbnails_system",
}

# Tunable thresholds for the matcher.
_FUZZY_CUTOFF = 0.72        # for difflib SequenceMatcher.ratio() against single-string fields
_TOKEN_COVERAGE_CUTOFF = 0.7  # fraction of candidate-name tokens that must appear in filename
_MIN_TOKEN_LEN = 3            # ignore tokens shorter than this


def resolve_type_subdir(asset_type: str, paths: InputPaths) -> str:
    """Map a --type value (e.g. 'photo' or 'lifestyle_photos') to a relative drive subdir."""
    key = _TYPE_ALIASES.get(asset_type.lower(), asset_type.lower())
    if not hasattr(paths, key):
        raise ValueError(f"Unknown asset type: {asset_type!r}")
    return getattr(paths, key)


# ---------------------------------------------------------------------------
# Identification
# ---------------------------------------------------------------------------

@dataclass
class FileMatch:
    rel_path: str
    sku: str | None
    supplier: str | None
    confidence: str          # HIGH | MEDIUM | LOW | NONE
    reason: str
    dest_path: str


@dataclass
class _Candidate:
    sku: str
    supplier: str
    supplier_ref: str
    name: str
    norm_sku: str
    norm_ref: str
    norm_name: str
    tok_ref: set[str] = field(default_factory=set)
    tok_name: set[str] = field(default_factory=set)


def _normalise(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", s.lower())


def _tokens(s: str) -> set[str]:
    """Lowercase, split on non-alphanum, drop tokens shorter than _MIN_TOKEN_LEN."""
    return {t for t in re.split(r"[^a-z0-9]+", s.lower()) if len(t) >= _MIN_TOKEN_LEN}


def _walk_files(input_dir: Path) -> Iterable[Path]:
    for p in input_dir.rglob("*"):
        if p.is_file() and not p.name.startswith("."):
            yield p


def _read_pdf_text(path: Path) -> str:
    """Return PDF title metadata + first-page extracted text. Best-effort; returns '' on failure."""
    try:
        from pypdf import PdfReader
        reader = PdfReader(str(path))
        parts: list[str] = []
        if reader.metadata and reader.metadata.title:
            parts.append(str(reader.metadata.title))
        if len(reader.pages) > 0:
            try:
                parts.append(reader.pages[0].extract_text() or "")
            except Exception:
                pass
        return " ".join(parts)
    except Exception:
        return ""


def _build_candidates(
    sheet_rows: list[dict[str, str]],
    sku_col: str,
    name_col: str,
    supplier_col: str,
    supplier_ref_col: str,
    supplier_filter: str | None,
) -> list[_Candidate]:
    def _s(row: dict, col: str) -> str:
        v = row.get(col, "")
        return str(v).strip() if v is not None else ""

    out: list[_Candidate] = []
    for row in sheet_rows:
        sku = _s(row, sku_col)
        # Real SKUs contain dashes; category-separator rows put a bare label in the SKU column.
        if not sku or "-" not in sku:
            continue
        supplier = _s(row, supplier_col)
        if supplier_filter and supplier.lower() != supplier_filter.lower():
            continue
        ref = _s(row, supplier_ref_col)
        name = _s(row, name_col)
        out.append(_Candidate(
            sku=sku, supplier=supplier, supplier_ref=ref, name=name,
            norm_sku=_normalise(sku),
            norm_ref=_normalise(ref),
            norm_name=_normalise(name),
            tok_ref=_tokens(ref),
            tok_name=_tokens(name),
        ))
    return out


def _longest_substring_match(
    haystack_norm: str,
    candidates: list[_Candidate],
    field_attr: str,
) -> _Candidate | None:
    """Return the candidate whose normalised field is the longest substring of haystack_norm."""
    best: _Candidate | None = None
    best_len = 0
    for c in candidates:
        norm = getattr(c, field_attr)
        if not norm or len(norm) < 3:  # avoid 1–2 char matches
            continue
        if norm in haystack_norm and len(norm) > best_len:
            best, best_len = c, len(norm)
    return best


def _best_token_coverage(
    haystack_tokens: set[str],
    candidates: list[_Candidate],
    field_attr: str,
    cutoff: float,
    min_tokens: int = 1,
) -> tuple[_Candidate | None, float, set[str]]:
    """Find the candidate whose tokens (field_attr) have the highest coverage in haystack_tokens.

    Returns (best_candidate, coverage_fraction, matched_tokens). best_candidate is None if no
    candidate meets the cutoff.
    """
    best: _Candidate | None = None
    best_cov = cutoff - 0.0001
    best_matched: set[str] = set()
    for c in candidates:
        toks: set[str] = getattr(c, field_attr)
        if not toks or len(toks) < min_tokens:
            continue
        matched = toks & haystack_tokens
        cov = len(matched) / len(toks)
        if cov > best_cov:
            best, best_cov, best_matched = c, cov, matched
    return (best, best_cov, best_matched) if best else (None, 0.0, set())


def _best_fuzzy(
    needle: str,
    candidates: list[_Candidate],
    field_attr: str,
    cutoff: float,
) -> tuple[_Candidate | None, float]:
    """Find the candidate whose normalised field (field_attr) is the closest fuzzy match to needle."""
    best: _Candidate | None = None
    best_ratio = cutoff - 0.0001
    for c in candidates:
        s: str = getattr(c, field_attr)
        if not s:
            continue
        sm = SequenceMatcher(None, needle, s)
        # Cheap pre-filters cut total runtime substantially when there are many candidates.
        if sm.real_quick_ratio() < cutoff:
            continue
        if sm.quick_ratio() < cutoff:
            continue
        r = sm.ratio()
        if r > best_ratio:
            best, best_ratio = c, r
    return (best, best_ratio) if best else (None, 0.0)


def _identify(path: Path, candidates: list[_Candidate]) -> tuple[_Candidate | None, str, str]:
    """Return (best_candidate, confidence, decision) — decision explains why this match was chosen."""
    norm_filename = _normalise(path.stem)
    fname_tokens = _tokens(path.stem)

    # 1. Filename contains an exact SKU / supplier ref / product name (HIGH)
    if c := _longest_substring_match(norm_filename, candidates, "norm_sku"):
        return c, "HIGH", f"Filename contains SKU '{c.sku}'"
    if c := _longest_substring_match(norm_filename, candidates, "norm_ref"):
        return c, "HIGH", f"Filename contains supplier ref '{c.supplier_ref}'"
    if c := _longest_substring_match(norm_filename, candidates, "norm_name"):
        return c, "HIGH", f"Filename contains product name '{c.name}'"

    # 2. Token coverage on filename — partial match against name / supplier ref (MEDIUM)
    name_c, name_cov, name_hit = _best_token_coverage(
        fname_tokens, candidates, "tok_name", _TOKEN_COVERAGE_CUTOFF, min_tokens=2,
    )
    ref_c, ref_cov, ref_hit = _best_token_coverage(
        fname_tokens, candidates, "tok_ref", _TOKEN_COVERAGE_CUTOFF, min_tokens=1,
    )
    # Prefer whichever scored higher; bias toward names (multi-token, more discriminating).
    if name_c and (not ref_c or name_cov >= ref_cov):
        hit = ", ".join(sorted(name_hit))
        return name_c, "MEDIUM", (
            f"Filename matches {len(name_hit)}/{len(name_c.tok_name)} tokens of "
            f"product name '{name_c.name}' ({hit})"
        )
    if ref_c:
        hit = ", ".join(sorted(ref_hit))
        return ref_c, "MEDIUM", (
            f"Filename matches {len(ref_hit)}/{len(ref_c.tok_ref)} tokens of "
            f"supplier ref '{ref_c.supplier_ref}' ({hit})"
        )

    # 3. PDF content — title metadata + first-page text (MEDIUM/LOW)
    if path.suffix.lower() == ".pdf":
        pdf_text = _read_pdf_text(path)
        norm_pdf = _normalise(pdf_text)
        pdf_tokens = _tokens(pdf_text)
        if norm_pdf:
            if c := _longest_substring_match(norm_pdf, candidates, "norm_sku"):
                return c, "MEDIUM", f"PDF text contains SKU '{c.sku}'"
            if c := _longest_substring_match(norm_pdf, candidates, "norm_ref"):
                return c, "MEDIUM", f"PDF text contains supplier ref '{c.supplier_ref}'"
            if c := _longest_substring_match(norm_pdf, candidates, "norm_name"):
                return c, "MEDIUM", f"PDF text contains product name '{c.name}'"
        if pdf_tokens:
            name_c, name_cov, name_hit = _best_token_coverage(
                pdf_tokens, candidates, "tok_name", _TOKEN_COVERAGE_CUTOFF, min_tokens=2,
            )
            if name_c:
                hit = ", ".join(sorted(name_hit))
                return name_c, "LOW", (
                    f"PDF text matches {len(name_hit)}/{len(name_c.tok_name)} tokens of "
                    f"product name '{name_c.name}' ({hit})"
                )

    # 4. Fuzzy match against SKU / supplier ref / product name (LOW)
    sku_c, sku_r = _best_fuzzy(norm_filename, candidates, "norm_sku", _FUZZY_CUTOFF)
    ref_c, ref_r = _best_fuzzy(norm_filename, candidates, "norm_ref", _FUZZY_CUTOFF)
    name_c, name_r = _best_fuzzy(norm_filename, candidates, "norm_name", _FUZZY_CUTOFF)
    candidates_with_score = [
        (sku_c, sku_r, "SKU", lambda x: x.sku),
        (ref_c, ref_r, "supplier ref", lambda x: x.supplier_ref),
        (name_c, name_r, "product name", lambda x: x.name),
    ]
    candidates_with_score = [t for t in candidates_with_score if t[0]]
    if candidates_with_score:
        best_c, best_r, label, getter = max(candidates_with_score, key=lambda t: t[1])
        return best_c, "LOW", f"Fuzzy match {best_r:.0%} against {label} '{getter(best_c)}'"

    return None, "NONE", "No match found"


def build_report(
    input_dir: Path,
    asset_type_subdir: str,
    sheet_rows: list[dict[str, str]],
    sku_col: str,
    name_col: str,
    supplier_col: str,
    supplier_ref_col: str,
    structure: str,
    supplier_filter: str | None = None,
) -> list[FileMatch]:
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    candidates = _build_candidates(
        sheet_rows, sku_col, name_col, supplier_col, supplier_ref_col, supplier_filter,
    )

    matches: list[FileMatch] = []
    for path in sorted(_walk_files(input_dir)):
        rel_path = str(path.relative_to(input_dir))
        cand, confidence, reason = _identify(path, candidates)

        if cand:
            if structure == "flat":
                dest = f"{cand.sku}/{asset_type_subdir}/{path.name}"
            else:
                dest = f"{cand.supplier}/{cand.sku}/{asset_type_subdir}/{path.name}"
        else:
            dest = ""

        matches.append(FileMatch(
            rel_path=rel_path,
            sku=cand.sku if cand else None,
            supplier=cand.supplier if cand else None,
            confidence=confidence,
            reason=reason,
            dest_path=dest,
        ))

    return matches


def to_sheet_rows(matches: list[FileMatch]) -> tuple[list[str], list[list]]:
    headers = ["File", "Destination SKU", "Supplier", "Confidence", "Match Decision", "Destination Path"]
    rows = [
        [m.rel_path, m.sku or "", m.supplier or "", m.confidence, m.reason, m.dest_path]
        for m in matches
    ]
    return headers, rows


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

class UploadProgress(NamedTuple):
    rel_path: str
    sku: str
    file_index: int
    file_total: int
    skipped: bool   # True when the file already existed at the destination


def _guess_mime(path: Path) -> str:
    mime, _ = mimetypes.guess_type(str(path))
    return mime or "application/octet-stream"


def _collect_sku_folders(root_folder_id: str, structure: str) -> dict[str, str]:
    if structure == "flat":
        return drive.list_folders(root_folder_id)
    out: dict[str, str] = {}
    for sup_id in drive.list_folders(root_folder_id).values():
        out.update(drive.list_folders(sup_id))
    return out


def execute_copy(
    report_rows: list[dict[str, str]],
    input_dir: Path,
    asset_type_subdir: str,
    root_folder_id: str,
    structure: str,
) -> Generator[UploadProgress, None, None]:
    """Read the edited report and upload each file to its (user-confirmed) destination SKU."""
    sku_folders = _collect_sku_folders(root_folder_id, structure)

    actionable = [r for r in report_rows if r.get("Destination SKU", "").strip()]
    total = len(actionable)

    # Cache per-SKU destination subfolder ID and existing filenames.
    dest_cache: dict[str, tuple[str, set[str]]] = {}

    for i, row in enumerate(actionable, 1):
        rel_path = row.get("File", "").strip()
        sku = row.get("Destination SKU", "").strip()
        if not rel_path or not sku:
            continue

        local_path = input_dir / rel_path
        if not local_path.is_file():
            yield UploadProgress(rel_path, sku, i, total, skipped=True)
            continue
        if sku not in sku_folders:
            yield UploadProgress(rel_path, sku, i, total, skipped=True)
            continue

        if sku not in dest_cache:
            dest_id = sku_folders[sku]
            for part in asset_type_subdir.split("/"):
                dest_id = drive.find_or_create_folder(part, dest_id)
            existing = {f["name"] for f in drive.list_files(dest_id)}
            dest_cache[sku] = (dest_id, existing)

        dest_id, existing = dest_cache[sku]
        if local_path.name in existing:
            yield UploadProgress(rel_path, sku, i, total, skipped=True)
            continue

        drive.upload_file(str(local_path), dest_id, local_path.name, _guess_mime(local_path))
        existing.add(local_path.name)
        yield UploadProgress(rel_path, sku, i, total, skipped=False)
