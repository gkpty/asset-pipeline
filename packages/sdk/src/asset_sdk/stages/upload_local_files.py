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
#
# The "supplier" variants kick in when --supplier narrows the candidate pool
# to a single supplier. In that mode the risk of cross-supplier collisions
# disappears (e.g. two suppliers both selling something called "linen"),
# so we can match on weaker evidence without producing nonsense.
_FUZZY_CUTOFF = 0.72                 # difflib SequenceMatcher.ratio() against a normalised field
_FUZZY_CUTOFF_SUPPLIER = 0.55
_TOKEN_COVERAGE_CUTOFF = 0.7         # weighted-token-overlap fraction
_TOKEN_COVERAGE_CUTOFF_SUPPLIER = 0.45
_MIN_TOKEN_LEN = 3                   # ignore tokens shorter than this

# OS-generated junk that's typically dropped alongside real assets when
# folders are zipped/copied from Windows or NAS shares. macOS dotfiles
# (.DS_Store, ._FILE) are already covered by the `.`-prefix filter.
_JUNK_FILENAMES = {
    "Thumbs.db",
    "ehthumbs.db",
    "desktop.ini",
    "Desktop.ini",
}


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
    """Recursively yield real asset files from `input_dir`.

    Filters out:
      - Dotfiles (`.DS_Store`, `._FILE`, etc.)
      - OS-generated junk by name (`Thumbs.db`, `desktop.ini`, ...)
    Folder names are preserved in the relative path so the matcher can
    use them via _haystack — nested layouts like
    `S5-5/PRODUCT/VARIANT/1.jpg` work without any caller changes."""
    for p in input_dir.rglob("*"):
        if not p.is_file():
            continue
        if p.name.startswith("."):
            continue
        if p.name in _JUNK_FILENAMES:
            continue
        yield p


def _haystack(path: Path, input_dir: Path) -> tuple[str, set[str]]:
    """Build the searchable string + token set for a file.

    Includes the relative directory chain so files like
    `Bermuda White Linen/photos/IMG_001.jpg` get matched on the parent
    folder name (which is usually the product name) rather than on the
    meaningless `IMG_001` stem. Falls back to just the stem if `path` is
    outside `input_dir`.
    """
    try:
        rel = path.relative_to(input_dir)
        parts = list(rel.parts[:-1]) + [path.stem]
    except ValueError:
        parts = [path.stem]
    raw = " ".join(parts)
    return _normalise(raw), _tokens(raw)


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
    """Find the candidate with the strongest token overlap with the haystack.

    Score is `max(forward, reverse)` where:
      forward = matched_weight / cand_weight   — how completely the candidate is covered
      reverse = matched_weight / file_weight   — how informed the haystack is by the candidate

    Tokens contribute by length (longer = more discriminating), so a 7-letter
    overlap on "bermuda" outweighs a 3-letter overlap on "the".

    Taking the max lets us match in both directions: a verbose filename whose
    tokens fully cover a short candidate name, AND a terse filename that
    contains most of a long candidate name. Either signal is good evidence.
    """
    best: _Candidate | None = None
    best_score = cutoff - 0.0001
    best_matched: set[str] = set()
    haystack_weight = sum(len(t) for t in haystack_tokens) or 1
    for c in candidates:
        toks: set[str] = getattr(c, field_attr)
        if not toks or len(toks) < min_tokens:
            continue
        matched = toks & haystack_tokens
        if not matched:
            continue
        matched_weight = sum(len(t) for t in matched)
        cand_weight = sum(len(t) for t in toks) or 1
        forward = matched_weight / cand_weight
        reverse = matched_weight / haystack_weight
        score = max(forward, reverse)
        if score > best_score:
            best, best_score, best_matched = c, score, matched
    return (best, best_score, best_matched) if best else (None, 0.0, set())


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


def _identify(
    path: Path,
    input_dir: Path,
    candidates: list[_Candidate],
    supplier_narrowed: bool,
) -> tuple[_Candidate | None, str, str]:
    """Return (best_candidate, confidence, decision) — decision explains why this match was chosen.

    The haystack is the full relative path (parent folders + stem), not just
    the stem, so files in product-named directories match correctly.

    When `supplier_narrowed` is True, fuzzy and token-coverage cutoffs are
    relaxed because cross-supplier collisions are impossible.
    """
    norm_path, path_tokens = _haystack(path, input_dir)

    fuzzy_cutoff = _FUZZY_CUTOFF_SUPPLIER if supplier_narrowed else _FUZZY_CUTOFF
    coverage_cutoff = _TOKEN_COVERAGE_CUTOFF_SUPPLIER if supplier_narrowed else _TOKEN_COVERAGE_CUTOFF

    # 1. Path contains an exact SKU / supplier ref / product name (HIGH)
    if c := _longest_substring_match(norm_path, candidates, "norm_sku"):
        return c, "HIGH", f"Path contains SKU '{c.sku}'"
    if c := _longest_substring_match(norm_path, candidates, "norm_ref"):
        return c, "HIGH", f"Path contains supplier ref '{c.supplier_ref}'"
    if c := _longest_substring_match(norm_path, candidates, "norm_name"):
        return c, "HIGH", f"Path contains product name '{c.name}'"

    # 1b. 100% token coverage on supplier ref / product name (HIGH).
    # Promotes cases where the candidate's discriminating tokens all appear
    # in the path but special characters / interspersed words break the
    # contiguous-substring check above. Common when folder names like
    # "712#MOON-4(2A)-EP251" carry a supplier ref "712#-4(2A)-EP251" whose
    # alphanumeric tokens are present in order but not contiguously.
    ref_full_c, ref_full_cov, ref_full_hit = _best_token_coverage(
        path_tokens, candidates, "tok_ref", 0.9999, min_tokens=1,
    )
    if ref_full_c and ref_full_cov >= 0.9999:
        hit = ", ".join(sorted(ref_full_hit))
        return ref_full_c, "HIGH", (
            f"Path fully covers supplier ref tokens of "
            f"'{ref_full_c.supplier_ref}' ({hit})"
        )
    name_full_c, name_full_cov, name_full_hit = _best_token_coverage(
        path_tokens, candidates, "tok_name", 0.9999, min_tokens=2,
    )
    if name_full_c and name_full_cov >= 0.9999:
        hit = ", ".join(sorted(name_full_hit))
        return name_full_c, "HIGH", (
            f"Path fully covers product name tokens of "
            f"'{name_full_c.name}' ({hit})"
        )

    # 2. Token coverage on path — partial match against name / supplier ref (MEDIUM)
    name_c, name_cov, name_hit = _best_token_coverage(
        path_tokens, candidates, "tok_name", coverage_cutoff, min_tokens=2,
    )
    ref_c, ref_cov, ref_hit = _best_token_coverage(
        path_tokens, candidates, "tok_ref", coverage_cutoff, min_tokens=1,
    )
    # Prefer whichever scored higher; bias toward names (multi-token, more discriminating).
    if name_c and (not ref_c or name_cov >= ref_cov):
        hit = ", ".join(sorted(name_hit))
        return name_c, "MEDIUM", (
            f"Path matches {len(name_hit)}/{len(name_c.tok_name)} tokens of "
            f"product name '{name_c.name}' ({hit}, score {name_cov:.0%})"
        )
    if ref_c:
        hit = ", ".join(sorted(ref_hit))
        return ref_c, "MEDIUM", (
            f"Path matches {len(ref_hit)}/{len(ref_c.tok_ref)} tokens of "
            f"supplier ref '{ref_c.supplier_ref}' ({hit}, score {ref_cov:.0%})"
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
                pdf_tokens, candidates, "tok_name", coverage_cutoff, min_tokens=2,
            )
            if name_c:
                hit = ", ".join(sorted(name_hit))
                return name_c, "LOW", (
                    f"PDF text matches {len(name_hit)}/{len(name_c.tok_name)} tokens of "
                    f"product name '{name_c.name}' ({hit}, score {name_cov:.0%})"
                )

    # 4. Fuzzy match against SKU / supplier ref / product name (LOW)
    sku_c, sku_r = _best_fuzzy(norm_path, candidates, "norm_sku", fuzzy_cutoff)
    ref_c, ref_r = _best_fuzzy(norm_path, candidates, "norm_ref", fuzzy_cutoff)
    name_c, name_r = _best_fuzzy(norm_path, candidates, "norm_name", fuzzy_cutoff)
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
    supplier_narrowed = bool(supplier_filter)

    matches: list[FileMatch] = []
    for path in sorted(_walk_files(input_dir)):
        rel_path = str(path.relative_to(input_dir))
        cand, confidence, reason = _identify(path, input_dir, candidates, supplier_narrowed)

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
    skipped: bool   # True when the upload didn't happen for any reason
    # Short tag explaining why a skip happened (empty on successful uploads):
    #   "local missing"   — input_dir/rel_path doesn't exist on disk
    #   "no sku folder"   — Drive folder for this SKU doesn't exist
    #   "already exists"  — destination already has a file with this name
    skip_reason: str = ""


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
            yield UploadProgress(
                rel_path, sku, i, total,
                skipped=True, skip_reason="local missing",
            )
            continue
        if sku not in sku_folders:
            yield UploadProgress(
                rel_path, sku, i, total,
                skipped=True, skip_reason="no sku folder",
            )
            continue

        if sku not in dest_cache:
            dest_id = sku_folders[sku]
            for part in asset_type_subdir.split("/"):
                dest_id = drive.find_or_create_folder(part, dest_id)
            existing = {f["name"] for f in drive.list_files(dest_id)}
            dest_cache[sku] = (dest_id, existing)

        dest_id, existing = dest_cache[sku]
        if local_path.name in existing:
            yield UploadProgress(
                rel_path, sku, i, total,
                skipped=True, skip_reason="already exists",
            )
            continue

        drive.upload_file(str(local_path), dest_id, local_path.name, _guess_mime(local_path))
        existing.add(local_path.name)
        yield UploadProgress(rel_path, sku, i, total, skipped=False)
