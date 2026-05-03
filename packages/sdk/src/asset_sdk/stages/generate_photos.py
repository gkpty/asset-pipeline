"""Generate product photos for a SKU that's missing them.

Mechanism: pick a sibling SKU under the same parent_product that has photos.
For each material column where the target SKU's value differs from the sibling's
(e.g. sibling has top="glass", target has top="travertine"), resolve a reference
image of the target material from <parent>/<materials_or_upholstery>/<value>/photos/.
Then for every sibling photo, call OpenAI's gpt-image-1 with [sibling_photo,
material_refs...] and a prompt like:

    Photograph of a <parent product>.
    Match the composition, angle, lighting, and framing of reference image #1.
    Replace the top (glass) with travertine, shown in reference image #2.
    Replace the seat (leather) with fabric, shown in reference image #3.
    Keep all other materials and details from the first reference image unchanged.

Each output is uploaded to <sku>/<photos_subdir>/<n>.jpg, matching the sibling's
photo numbering.

Cost is computed up front so the dry-run report shows total $$ before any API
call. The CLI's --budget flag enforces a pre-flight ceiling.
"""
from __future__ import annotations

import io
import json
import os
import re
import tempfile
import time
from base64 import b64decode, b64encode
from dataclasses import dataclass, field
from pathlib import Path as _Path
from typing import Generator, NamedTuple

import numpy as np
from PIL import Image as _PILImage

from asset_sdk.adapters import drive

# Claude's vision API caps each image at 5 MB. We aim under 4.5 MB for headroom.
_CLAUDE_MAX_BYTES = 4_500_000
_CLAUDE_MAX_DIM = 2048

# Image-type detection thresholds. A shot with >= this much near-white pixel
# coverage is treated as a product-silhouette photo (clean studio background).
# Below this, it's treated as a macro/detail close-up (the whole frame is the
# subject — fabric, wood grain, etc.).
_PRODUCT_WHITE_PCT = 0.20
_NEAR_WHITE_THRESHOLD = 245

# Master sheet column → subfolder under each SKU where target photos go.
_PHOTOS_SUBDIR_DEFAULT = "photos"


# ---------------------------------------------------------------------------
# Plan dataclass + helpers
# ---------------------------------------------------------------------------

@dataclass
class PhotoPlan:
    sku: str
    supplier: str
    parent_product: str
    source_sku: str            # sibling chosen as the composition reference
    photo_count: int           # how many outputs to generate (= sibling's photo count)
    target_materials: dict[str, str] = field(default_factory=dict)
    # column → (sibling_value, target_value) only for columns that differ.
    replacements: dict[str, tuple[str, str]] = field(default_factory=dict)
    # column → True if a reference image was successfully resolved for the target value.
    resolved: dict[str, bool] = field(default_factory=dict)
    missing_materials: list[str] = field(default_factory=list)
    cost_usd: float = 0.0
    action: str = "SKIP"       # COPY | GENERATE | SKIP
    notes: str = ""
    # Filenames of the sibling photos that drive this plan (display + audit).
    # For COPY: these are duplicated verbatim. For GENERATE: these are the
    # composition references fed to gpt-image-1.
    source_photos: list[str] = field(default_factory=list)


def _build_sku_index(category_folder_id: str, structure: str) -> dict[str, tuple[str, str]]:
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
    current = parent_id
    for part in rel_path.split("/"):
        children = drive.list_folders(current)
        if part not in children:
            return None
        current = children[part]
    return current


def _resolve_or_create(parent_id: str, rel_path: str) -> str:
    current = parent_id
    for part in rel_path.split("/"):
        current = drive.find_or_create_folder(part, current)
    return current


def _list_photos(sku_id: str, photos_subdir: str) -> list[dict]:
    folder = _resolve_optional(sku_id, photos_subdir)
    if not folder:
        return []
    files = drive.list_files(folder)
    files = [f for f in files if not f["name"].startswith(".")]
    files.sort(key=lambda f: f["name"])
    return files


def _resolve_material_photo(
    parent_root_id: str,
    category_name: str,
    material_sku: str,
    photos_subdir: str,
) -> dict | None:
    """Return the first photo file under <parent_root>/<category>/<material_sku>/<photos_subdir>/, or None."""
    if not material_sku:
        return None
    cat_id = drive.resolve_category_folder(parent_root_id, category_name)
    folders = drive.list_folders(cat_id)
    sku_id = folders.get(material_sku)
    if not sku_id:
        return None
    photos = _list_photos(sku_id, photos_subdir)
    return photos[0] if photos else None


# ---------------------------------------------------------------------------
# build_plan
# ---------------------------------------------------------------------------

def build_plan(
    *,
    parent_root_id: str,
    category_folder_id: str,
    structure: str,
    sheet_rows: list[dict[str, str]],
    sku_col: str,
    supplier_col: str,
    parent_product_col: str,
    photos_subdir: str,
    material_columns: dict[str, str],
    cost_per_image_usd: float,
    part_col: str = "",
    size_col: str = "",
) -> list[PhotoPlan]:
    """One PhotoPlan per SKU in the sheet.

    `part_col` (optional): when provided, siblings must match on BOTH parent_product
    AND this column's value. Used for modular products (e.g. modular sofas) where
    parts within the same parent_product are physically distinct (left armrest vs
    corner piece) and can't share photos. Empty values match empty values, so
    non-modular products are unaffected.

    `size_col` (optional): scopes the COPY action. The duplicate-photos shortcut
    only triggers when sibling and target share all materials AND differ in
    size (queen→king, small pot→large pot). With `size_col=""` the COPY pass is
    disabled and every actionable plan goes through GENERATE.

    SKIP rules:
      - Target SKU folder not found on Drive.
      - Target SKU already has photos.
      - No sibling under the same (parent_product, part) has photos.
      - All siblings share the target's materials but also share its size
        (no useful sibling).
    """
    sku_index = _build_sku_index(category_folder_id, structure)

    # SKU → row map (so we can look up sibling materials).
    sku_to_row: dict[str, dict[str, str]] = {}
    # Group siblings by (parent_product, part). Empty part matches empty part.
    group_to_skus: dict[tuple[str, str], list[tuple[str, str]]] = {}
    sku_to_group: dict[str, tuple[str, str]] = {}
    for row in sheet_rows:
        sku = (row.get(sku_col) or "").strip()
        sup = (row.get(supplier_col) or "").strip()
        parent = (row.get(parent_product_col) or "").strip()
        part = (row.get(part_col) or "").strip() if part_col else ""
        if not sku:
            continue
        sku_to_row[sku] = row
        if parent:
            key = (parent, part)
            sku_to_group[sku] = key
            group_to_skus.setdefault(key, []).append((sku, sup))

    # Cache of sibling photo lists keyed by SKU id (avoids re-querying when many targets share a sibling).
    photo_cache: dict[str, list[dict]] = {}

    def _photos_of(sku: str) -> list[dict]:
        if sku in photo_cache:
            return photo_cache[sku]
        if sku not in sku_index:
            photo_cache[sku] = []
            return []
        _, sid = sku_index[sku]
        photo_cache[sku] = _list_photos(sid, photos_subdir)
        return photo_cache[sku]

    # Cache of material-photo resolution results: (column, value) → bool.
    material_cache: dict[tuple[str, str], bool] = {}

    def _has_material_photo(col: str, value: str) -> bool:
        key = (col, value)
        if key in material_cache:
            return material_cache[key]
        if not value:
            material_cache[key] = False
            return False
        cat = material_columns.get(col)
        if not cat:
            material_cache[key] = False
            return False
        try:
            material_cache[key] = _resolve_material_photo(parent_root_id, cat, value, photos_subdir) is not None
        except Exception:
            material_cache[key] = False
        return material_cache[key]

    plans: list[PhotoPlan] = []

    for row in sheet_rows:
        sku = (row.get(sku_col) or "").strip()
        sup = (row.get(supplier_col) or "").strip()
        parent = (row.get(parent_product_col) or "").strip()
        part = (row.get(part_col) or "").strip() if part_col else ""
        target_size = (row.get(size_col) or "").strip() if size_col else ""
        if not sku:
            continue

        target_materials = {col: (row.get(col) or "").strip() for col in material_columns}

        # Target must exist on Drive.
        if sku not in sku_index:
            plans.append(PhotoPlan(
                sku=sku, supplier=sup, parent_product=parent,
                source_sku="", photo_count=0,
                target_materials=target_materials,
                action="SKIP", notes="sku folder not found in Drive",
            ))
            continue

        target_sup, _target_sku_id = sku_index[sku]

        # Already populated?
        if _photos_of(sku):
            plans.append(PhotoPlan(
                sku=sku, supplier=target_sup, parent_product=parent,
                source_sku="", photo_count=0,
                target_materials=target_materials,
                action="SKIP", notes="target already has photos",
            ))
            continue

        # Two-pass sibling selection within (parent, part):
        #
        #   Pass 1 (COPY): a sibling whose photos can be reused verbatim. Requires
        #   ALL material columns to match AND the sibling's size to differ from
        #   the target's. The "different size" guard prevents copying between
        #   true duplicate listings — only legitimate "same product, different
        #   size" pairs should reuse photos. With size_col="" the pass is off.
        #
        #   Pass 2 (GENERATE): any sibling under (parent, part) with photos.
        #   We'll AI-edit the sibling's photos to apply the differing materials.
        #
        # The (parent, part) grouping ensures modular parts (left armrest vs corner)
        # never match across-part.
        group_key = (parent, part)
        siblings = [(s, sp) for (s, sp) in group_to_skus.get(group_key, []) if s != sku]

        copy_match: tuple[str, str, list[dict]] | None = None  # (sku, supplier, photos)
        gen_match: tuple[str, str, list[dict]] | None = None
        for sib_sku, sib_sup_sheet in siblings:
            sps = _photos_of(sib_sku)
            if not sps:
                continue
            sib_supplier = (sku_index.get(sib_sku, ("", ""))[0]) or sib_sup_sheet
            if gen_match is None:
                gen_match = (sib_sku, sib_supplier, sps)
            sib_row = sku_to_row.get(sib_sku, {})
            sib_materials = {col: (sib_row.get(col) or "").strip() for col in material_columns}
            sib_size = (sib_row.get(size_col) or "").strip() if size_col else ""
            materials_match = sib_materials == target_materials
            size_differs = bool(size_col) and sib_size != target_size
            if materials_match and size_differs:
                copy_match = (sib_sku, sib_supplier, sps)
                break  # earliest in row order wins

        # Pass 1: duplicate-eligible — copy verbatim, no AI.
        if copy_match:
            s_sku, _s_sup, sps = copy_match
            sib_size = (sku_to_row.get(s_sku, {}).get(size_col) or "").strip() if size_col else ""
            note = "duplicate from sibling (same materials, different size"
            if sib_size or target_size:
                note += f": {sib_size or '∅'} → {target_size or '∅'}"
            note += ")"
            plans.append(PhotoPlan(
                sku=sku, supplier=target_sup, parent_product=parent,
                source_sku=s_sku, photo_count=len(sps),
                target_materials=target_materials,
                replacements={}, resolved={}, missing_materials=[],
                cost_usd=0.0,
                action="COPY",
                notes=note,
                source_photos=[f["name"] for f in sps],
            ))
            continue

        # No sibling at all → SKIP.
        if gen_match is None:
            note = "no sibling with photos (under same parent_product"
            if part:
                note += f" + part={part!r}"
            note += ")"
            plans.append(PhotoPlan(
                sku=sku, supplier=target_sup, parent_product=parent,
                source_sku="", photo_count=0,
                target_materials=target_materials,
                action="SKIP", notes=note,
            ))
            continue

        # Pass 2: GENERATE with material replacements.
        chosen_sibling, _chosen_sibling_supplier, sibling_photos = gen_match
        sibling_row = sku_to_row.get(chosen_sibling, {})
        sibling_materials = {col: (sibling_row.get(col) or "").strip() for col in material_columns}

        # Compute replacements: target value present and different from sibling's.
        replacements: dict[str, tuple[str, str]] = {}
        resolved: dict[str, bool] = {}
        missing: list[str] = []
        for col in material_columns:
            t = target_materials.get(col, "")
            s = sibling_materials.get(col, "")
            if t and t != s:
                replacements[col] = (s, t)
                ok = _has_material_photo(col, t)
                resolved[col] = ok
                if not ok:
                    missing.append(col)

        # If every material column matches the sibling but COPY wasn't taken,
        # it's because the sibling's size also matches the target's. The two
        # SKUs are essentially the same product with the same size — there's
        # nothing useful to do here. SKIP.
        if not replacements:
            plans.append(PhotoPlan(
                sku=sku, supplier=target_sup, parent_product=parent,
                source_sku=chosen_sibling, photo_count=len(sibling_photos),
                target_materials=target_materials,
                action="SKIP",
                notes="materials match sibling but size also matches — no useful action",
                source_photos=[f["name"] for f in sibling_photos],
            ))
            continue

        photo_count = len(sibling_photos)
        cost = round(photo_count * float(cost_per_image_usd), 4)

        notes = ""
        if missing:
            notes = f"missing material refs: {', '.join(missing)}"

        plans.append(PhotoPlan(
            sku=sku, supplier=target_sup, parent_product=parent,
            source_sku=chosen_sibling, photo_count=photo_count,
            target_materials=target_materials,
            replacements=replacements,
            resolved=resolved,
            missing_materials=missing,
            cost_usd=cost,
            action="GENERATE",
            notes=notes,
            source_photos=[f["name"] for f in sibling_photos],
        ))

    return plans


_ACTION_ORDER = {"COPY": 0, "GENERATE": 1, "SKIP": 2}


def to_sheet_rows(
    plans: list[PhotoPlan],
    material_columns: dict[str, str],
) -> tuple[list[str], list[list]]:
    # Sort: COPY rows first (cheap, fast), then GENERATE (expensive AI calls), then SKIP.
    # Within each group, alphabetical by SKU for stable diffs.
    sorted_plans = sorted(
        plans,
        key=lambda p: (_ACTION_ORDER.get(p.action, 99), p.sku),
    )

    # Order columns alphabetically for stable diffs / readable layout.
    mat_cols = sorted(material_columns.keys())
    headers = (
        ["SKU", "Supplier", "Parent Product", "Source SKU", "Source Photos", "Photo Count"]
        + [c.replace("_", " ").title() for c in mat_cols]
        + ["Replacements", "Materials Resolved", "Cost USD", "Action", "Notes"]
    )
    rows: list[list] = []
    for p in sorted_plans:
        target_cells = [p.target_materials.get(c, "") for c in mat_cols]
        repl_str = "; ".join(
            f"{c}: {sv or '∅'}→{tv}" for c, (sv, tv) in p.replacements.items()
        )
        if p.replacements:
            ok_count = sum(1 for c in p.replacements if p.resolved.get(c))
            resolved_str = f"{ok_count}/{len(p.replacements)}"
        else:
            resolved_str = ""
        source_photos_str = ", ".join(p.source_photos)
        rows.append([
            p.sku, p.supplier, p.parent_product, p.source_sku, source_photos_str, p.photo_count,
            *target_cells,
            repl_str, resolved_str, f"{p.cost_usd:.4f}", p.action, p.notes,
        ])
    return headers, rows


def summarise(plans: list[PhotoPlan]) -> dict[str, float]:
    return {
        "total":          len(plans),
        "to_copy":        sum(1 for p in plans if p.action == "COPY"),
        "copy_images":    sum(p.photo_count for p in plans if p.action == "COPY"),
        "to_generate":    sum(1 for p in plans if p.action == "GENERATE"),
        "skipped":        sum(1 for p in plans if p.action == "SKIP"),
        "total_cost_usd": round(sum(p.cost_usd for p in plans if p.action == "GENERATE"), 4),
        "total_images":   sum(p.photo_count for p in plans if p.action == "GENERATE"),
    }


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

def build_prompt(
    parent_product: str,
    replacements: dict[str, tuple[str, str]],
    image_index_map: dict[str, int],
    image_type: str = "product",
) -> str:
    """Minimal, declarative prompt — branched by source image type.

    `image_type` (from `_detect_and_crop`):
      - "product": full product on a clean studio background. The model often
        wants to add scenery / other furniture; we explicitly forbid that.
      - "macro":   close-up detail (fabric weave, wood grain). The model often
        wants to zoom out to a full product shot; we explicitly forbid that.

    Same prompt every attempt; if a model fails, we just try the next model.
    Claude's per-attempt retry feedback was contradictory across attempts,
    so we no longer feed it back into the prompt.
    """
    pp = parent_product.strip() or "product"
    if image_type == "macro":
        lines: list[str] = [
            f"This is a MACRO CLOSE-UP DETAIL photograph (a tight crop of a {pp}).",
            "Reproduce image #1 EXACTLY: same tight crop, same framing, same depth-of-field, "
            "same camera angle, same composition, same scale.",
            "DO NOT zoom out. DO NOT show the full product. DO NOT add any furniture, "
            "rooms, scenery, props, or additional objects. The output must be a close-up "
            "detail shot, not a full product photo.",
        ]
    else:
        lines = [
            f"Photograph of a single {pp} on a clean white seamless studio background.",
            "Reproduce image #1 EXACTLY: same product, same shape, same silhouette, same "
            "proportions, same camera angle, same framing, same composition, same lighting.",
            "DO NOT add other furniture, decor, rooms, walls, floors, or scenery. DO NOT "
            "alter the product's shape, profile, or angle. The output must be the same "
            "single product on a plain white background.",
        ]

    if replacements:
        lines.append("")
        lines.append("Change ONLY the materials of these specific surfaces:")
        for col, (_sibling_val, target_val) in replacements.items():
            idx = image_index_map.get(col)
            label = col.replace("_", " ")
            if idx:
                lines.append(f"- {label}: use the material shown in image #{idx}")
            else:
                lines.append(f"- {label}: use {target_val}")
        lines.append("")
        lines.append("Every other pixel should match image #1 as closely as possible.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

class GenProgress(NamedTuple):
    sku: str
    source_sku: str
    file_index: int            # 1-based
    file_total: int
    output_name: str
    skipped: bool
    error: str = ""


def _download(file_id: str, dest_path: str) -> None:
    drive.download_file(file_id, dest_path)


def _openai_client():
    """Lazy import + construct so the module loads without OPENAI_API_KEY set."""
    from openai import OpenAI
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is not set. Add it to .env or your environment."
        )
    return OpenAI(api_key=api_key)


def _gemini_client():
    """Lazy import + construct so the module loads without GEMINI_API_KEY set."""
    from google import genai
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "GEMINI_API_KEY is not set. Add it to .env or your environment "
            "(get a key at https://aistudio.google.com/apikey)."
        )
    return genai.Client(api_key=api_key)


# ---------------------------------------------------------------------------
# Provider dispatch
# ---------------------------------------------------------------------------

# Used to detect which provider to call from a model name. Add prefixes here as
# new model families ship.
_OPENAI_MODEL_PREFIXES = ("gpt-image", "gpt-", "dall-e", "dall-")
_GEMINI_MODEL_PREFIXES = ("gemini-",)


def _provider_for(model: str) -> str:
    """Return 'openai' or 'gemini' based on the model name prefix. Raises on unknown."""
    m = model.strip().lower()
    if any(m.startswith(p) for p in _OPENAI_MODEL_PREFIXES):
        return "openai"
    if any(m.startswith(p) for p in _GEMINI_MODEL_PREFIXES):
        return "gemini"
    raise RuntimeError(
        f"Cannot determine provider for model {model!r}. "
        f"Expected prefix from: {_OPENAI_MODEL_PREFIXES + _GEMINI_MODEL_PREFIXES}."
    )


def _pick_openai_size(sibling_path: str, requested: str) -> str:
    """Resolve `size` for OpenAI's images.edit. With 'auto', detect the source's
    aspect ratio and pick the closest preset (1024x1024, 1536x1024, 1024x1536).
    Anything else is passed through.
    """
    if requested.lower() != "auto":
        return requested
    try:
        with _PILImage.open(sibling_path) as img:
            w, h = img.size
        if w == 0 or h == 0:
            return "1024x1024"
        ratio = w / h
        if ratio > 1.3:
            return "1536x1024"
        if ratio < 0.77:
            return "1024x1536"
        return "1024x1024"
    except Exception:
        return "1024x1024"


# ---------------------------------------------------------------------------
# Claude-vision verifier
# ---------------------------------------------------------------------------

@dataclass
class Verdict:
    ok: bool
    shape_match: bool = True
    angle_match: bool = True
    materials_correct: bool = True
    bleed_through: list[str] = field(default_factory=list)
    quality_issues: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    retry_instructions: str = ""
    raw_response: str = ""    # for debugging when JSON parsing fails


def _anthropic_client():
    """Lazy import + construct so the module loads without ANTHROPIC_API_KEY set."""
    from anthropic import Anthropic
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError(
            "ANTHROPIC_API_KEY is not set. Add it to .env or pass --no-verify."
        )
    return Anthropic(api_key=api_key)


def _detect_and_crop(local_path: str) -> tuple[str, str]:
    """Return (path_to_use, image_type).

    Strategy:
      - "product"  → image has substantial white background (typical studio
        product photo). Crop tightly to the non-white bounding box plus a small
        margin so the product fills the frame; this gives the model a stronger
        shape signal and cuts down on hallucinated background.
      - "macro"    → image is mostly non-white (close-up of fabric, wood, etc.).
        Don't crop. The whole image IS the subject; cropping would just trim
        random texture.

    Always returns a path that exists. On any exception (corrupt file, etc.)
    falls back to the original path with a 'product' label.
    """
    try:
        with _PILImage.open(local_path) as img:
            img = img.convert("RGB")
            arr = np.array(img)
    except Exception:
        return local_path, "product"

    near_white_mask = np.all(arr >= _NEAR_WHITE_THRESHOLD, axis=-1)
    near_white_pct = float(near_white_mask.mean())

    if near_white_pct < _PRODUCT_WHITE_PCT:
        return local_path, "macro"

    product_mask = ~near_white_mask
    if not product_mask.any():
        return local_path, "product"

    rows = np.any(product_mask, axis=1)
    cols = np.any(product_mask, axis=0)
    rmin, rmax = np.where(rows)[0][[0, -1]]
    cmin, cmax = np.where(cols)[0][[0, -1]]
    h, w = arr.shape[:2]

    bw, bh = int(cmax - cmin), int(rmax - rmin)
    pad_x = max(int(bw * 0.05), 8)
    pad_y = max(int(bh * 0.05), 8)
    rmin = max(0, int(rmin) - pad_y)
    rmax = min(h, int(rmax) + pad_y + 1)
    cmin = max(0, int(cmin) - pad_x)
    cmax = min(w, int(cmax) + pad_x + 1)

    cropped = img.crop((cmin, rmin, cmax, rmax))
    new_path = local_path.rsplit(".", 1)[0] + "_cropped.jpg"
    cropped.save(new_path, "JPEG", quality=95)
    return new_path, "product"


def _prepare_for_claude(local_path: str) -> bytes:
    """Re-encode an image as JPEG and downscale until it fits Claude's 5 MB limit.

    Always emits JPEG so the media_type we declare matches the bytes (gpt-image-1
    sometimes returns PNG; raw product photos can be PNG/WEBP/etc.). Caps the
    longest edge at _CLAUDE_MAX_DIM so a 7 MB original gets shrunk before
    encoding instead of after.
    """
    with _PILImage.open(local_path) as img:
        img = img.convert("RGB")
        w, h = img.size
        longest = max(w, h)
        if longest > _CLAUDE_MAX_DIM:
            scale = _CLAUDE_MAX_DIM / longest
            img = img.resize((int(w * scale), int(h * scale)), _PILImage.LANCZOS)

        # Encode at quality 85 first; drop quality if still too large.
        for quality in (85, 75, 65, 55, 45):
            buf = io.BytesIO()
            img.save(buf, format="JPEG", quality=quality, optimize=True)
            data = buf.getvalue()
            if len(data) <= _CLAUDE_MAX_BYTES:
                return data
        # Last-ditch: shrink dimensions and try again at quality 60.
        smaller = img.resize((img.size[0] // 2, img.size[1] // 2), _PILImage.LANCZOS)
        buf = io.BytesIO()
        smaller.save(buf, format="JPEG", quality=60, optimize=True)
        return buf.getvalue()


def _image_block(local_path: str) -> dict:
    """Build an Anthropic content block from a local image, always as JPEG ≤ 5 MB."""
    data = _prepare_for_claude(local_path)
    return {
        "type": "image",
        "source": {
            "type": "base64",
            "media_type": "image/jpeg",
            "data": b64encode(data).decode("ascii"),
        },
    }


def _build_verifier_prompt(
    parent_product: str,
    replacements: dict[str, tuple[str, str]],
    image_index_map: dict[str, int],
) -> str:
    """Tell Claude what to check and what JSON shape to return.

    image_index_map: column → 1-based reference image position. The verifier
    expects images to be supplied in order: [original sibling, material refs..., generated].
    """
    # Describe the labelled images.
    pp = parent_product.strip() or "product"
    lines: list[str] = [
        f"You are a visual QA reviewer for AI-generated {pp} photography.",
        "",
        "Reference images supplied in order:",
        "  Image 1: ORIGINAL — the source product photo. The generated image MUST match this exactly in product shape, proportions, camera angle, framing, lighting, and styling.",
    ]
    for col, (sibling_val, target_val) in replacements.items():
        idx = image_index_map.get(col)
        if idx:
            lines.append(
                f"  Image {idx}: TARGET MATERIAL — {col} = {target_val!r} "
                f"(replacing {sibling_val or '∅'} from the original)."
            )
    final_idx = (max(image_index_map.values()) if image_index_map else 1) + 1
    lines.extend([
        f"  Image {final_idx}: GENERATED — the AI-edited result you are reviewing.",
        "",
        "Check the GENERATED image against the references:",
        "  1. Shape — same product silhouette, proportions, structural design as Image 1?",
        "  2. Angle/framing — same camera angle, crop, and composition as Image 1?",
        "  3. Materials — does each visible surface match the corresponding target material reference (correct color, texture, weave, finish)?",
        "  4. Bleed-through — ZERO patterns/colors/textures from the original's old materials carried over into the new ones (e.g. ghost grid, color leakage, residual print)?",
        "  5. Quality — any warping, distortion, weird AI artifacts, broken edges, or hallucinated details?",
        "",
        "Reply with ONLY a JSON object (no prose, no markdown fences). Keys:",
        "  ok                  — bool, true if the image is acceptable for production use",
        "  shape_match         — bool",
        "  angle_match         — bool",
        "  materials_correct   — bool",
        "  bleed_through       — list of strings naming any leftover features from the original materials",
        "  quality_issues      — list of strings naming visual artifacts or distortions",
        "  reasons             — list of strings explaining WHY ok is false (empty if ok)",
        "  retry_instructions  — string with concrete, specific instructions for the next regeneration "
        "    attempt (e.g. \"the seat cushion still shows a faint windowpane grid; the new fabric must "
        "    be solid cream linen with no horizontal or vertical lines anywhere\"). "
        "    Keep this empty when ok is true.",
        "",
        "Be strict. Marginal failures should be marked ok=false so they get regenerated.",
    ])
    return "\n".join(lines)


_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def _parse_verdict(text: str) -> Verdict:
    """Best-effort JSON extraction. If parsing fails, treat as ok=false with a
    raw response captured for debugging — avoids crashing on a malformed model
    reply."""
    raw = text.strip()
    # Strip markdown fences if Claude added them despite instructions.
    if raw.startswith("```"):
        raw = raw.strip("`")
        # remove leading "json" language hint if present
        if raw.lower().startswith("json"):
            raw = raw[4:].lstrip()
    m = _JSON_RE.search(raw)
    if not m:
        return Verdict(
            ok=False,
            reasons=["verifier returned no JSON; treating as failure"],
            retry_instructions="",
            raw_response=text,
        )
    try:
        data = json.loads(m.group(0))
    except json.JSONDecodeError as exc:
        return Verdict(
            ok=False,
            reasons=[f"verifier returned malformed JSON: {exc}"],
            retry_instructions="",
            raw_response=text,
        )

    def _bool(key: str, default: bool = True) -> bool:
        v = data.get(key, default)
        return bool(v) if isinstance(v, bool) else default

    def _list(key: str) -> list[str]:
        v = data.get(key, [])
        return [str(x) for x in v] if isinstance(v, list) else []

    return Verdict(
        ok=_bool("ok", False),
        shape_match=_bool("shape_match"),
        angle_match=_bool("angle_match"),
        materials_correct=_bool("materials_correct"),
        bleed_through=_list("bleed_through"),
        quality_issues=_list("quality_issues"),
        reasons=_list("reasons"),
        retry_instructions=str(data.get("retry_instructions") or ""),
        raw_response=text,
    )


def verify_with_claude(
    *,
    client,
    model: str,
    parent_product: str,
    sibling_photo_path: str,
    generated_photo_path: str,
    material_paths_by_col: dict[str, str],
    replacements: dict[str, tuple[str, str]],
) -> Verdict:
    """Send all references + the generated photo to Claude and parse the verdict.

    Image order matters: it must match the indices the prompt references. We
    feed [sibling, ...materials in alphabetical column order..., generated].
    """
    ordered_cols = sorted(material_paths_by_col.keys())
    image_index_map = {col: 2 + i for i, col in enumerate(ordered_cols)}
    images = [_image_block(sibling_photo_path)]
    for col in ordered_cols:
        images.append(_image_block(material_paths_by_col[col]))
    images.append(_image_block(generated_photo_path))

    prompt = _build_verifier_prompt(parent_product, replacements, image_index_map)

    try:
        response = client.messages.create(
            model=model,
            max_tokens=1024,
            messages=[{
                "role": "user",
                "content": [{"type": "text", "text": prompt}, *images],
            }],
        )
    except Exception as exc:
        return Verdict(
            ok=False,
            reasons=[f"claude call failed: {exc}"],
            retry_instructions="",
        )

    # Claude returns response.content as a list of content blocks; first text block is our answer.
    text = ""
    for block in getattr(response, "content", []) or []:
        if getattr(block, "type", None) == "text":
            text = block.text
            break
    return _parse_verdict(text)


def _generate_openai(
    client,
    *,
    model: str,
    quality: str,
    size: str,
    sibling_photo_path: str,
    material_paths: list[str],
    prompt: str,
) -> bytes:
    """Call OpenAI's images.edit (gpt-image-1, gpt-image-1, ...) with one sibling
    reference + N material refs. Returns raw image bytes (PNG or JPEG)."""
    resolved_size = _pick_openai_size(sibling_photo_path, size)
    images = [open(sibling_photo_path, "rb")] + [open(p, "rb") for p in material_paths]
    try:
        result = client.images.edit(
            model=model,
            image=images,
            prompt=prompt,
            size=resolved_size,
            quality=quality,
        )
    finally:
        for fh in images:
            try:
                fh.close()
            except Exception:
                pass

    data = result.data[0]
    if getattr(data, "b64_json", None):
        return b64decode(data.b64_json)
    if getattr(data, "url", None):
        import httpx
        with httpx.Client(timeout=60.0) as c:
            r = c.get(data.url)
            r.raise_for_status()
            return r.content
    raise RuntimeError("OpenAI response did not include image bytes or URL.")


def _generate_gemini(
    client,
    *,
    model: str,
    sibling_photo_path: str,
    material_paths: list[str],
    prompt: str,
) -> bytes:
    """Call Gemini's generate_content with image inputs. Returns raw image bytes.

    Image order matches the prompt: sibling first, then materials. Aspect ratio
    is implicitly preserved by Gemini matching the source image's dimensions.
    """
    from google.genai import types

    parts: list = [types.Part.from_text(text=prompt)]
    with open(sibling_photo_path, "rb") as fh:
        parts.append(types.Part.from_bytes(data=fh.read(), mime_type="image/jpeg"))
    for p in material_paths:
        with open(p, "rb") as fh:
            parts.append(types.Part.from_bytes(data=fh.read(), mime_type="image/jpeg"))

    response = client.models.generate_content(
        model=model,
        contents=[types.Content(role="user", parts=parts)],
        config=types.GenerateContentConfig(
            response_modalities=["IMAGE"],
        ),
    )

    candidates = getattr(response, "candidates", None) or []
    for cand in candidates:
        content = getattr(cand, "content", None)
        if not content:
            continue
        for part in getattr(content, "parts", []) or []:
            inline = getattr(part, "inline_data", None)
            if inline and getattr(inline, "mime_type", "").startswith("image/"):
                return inline.data
    raise RuntimeError(
        "Gemini response did not include an image part. "
        "Check that the model supports image output and your API key has access."
    )


def _generate_dispatch(
    *,
    clients: dict,
    model: str,
    quality: str,
    size: str,
    sibling_photo_path: str,
    material_paths: list[str],
    prompt: str,
) -> bytes:
    """Route a generate call to the right provider based on model name prefix.
    Lazily creates and caches the provider client in `clients` (dict keyed by
    provider name)."""
    provider = _provider_for(model)
    if provider == "openai":
        if "openai" not in clients:
            clients["openai"] = _openai_client()
        return _generate_openai(
            clients["openai"],
            model=model, quality=quality, size=size,
            sibling_photo_path=sibling_photo_path,
            material_paths=material_paths,
            prompt=prompt,
        )
    if provider == "gemini":
        if "gemini" not in clients:
            clients["gemini"] = _gemini_client()
        return _generate_gemini(
            clients["gemini"],
            model=model,
            sibling_photo_path=sibling_photo_path,
            material_paths=material_paths,
            prompt=prompt,
        )
    raise RuntimeError(f"Unsupported provider {provider!r} for model {model!r}")


def execute(
    *,
    plans: list[PhotoPlan],
    parent_root_id: str,
    category_folder_id: str,
    structure: str,
    photos_subdir: str,
    material_columns: dict[str, str],
    models: list[str],
    quality: str,
    size: str,
    verify_enabled: bool = True,
    verify_model: str = "claude-sonnet-4-6",
    max_retries: int = 2,
    debug: bool = False,
    logger=print,
) -> Generator[GenProgress, None, None]:
    """Apply every Action=COPY or Action=GENERATE plan.

      - COPY: duplicates each sibling photo into the target's photos folder
        verbatim (no AI call, $0 cost). Used when the sibling has identical
        materials — common case is a different size of the same product.
      - GENERATE: AI-edits each sibling photo with material reference images.
        Models cycle through `models` on retry: attempt 1 → models[0]; retry 1
        → models[1]; retry 2 → models[0] (wraps). Provider is detected from
        the model name prefix (gpt-* → OpenAI, gemini-* → Google).
        When `verify_enabled`, each output is reviewed by Claude (`verify_model`).
        On failure, the prompt is augmented with the verifier's retry_instructions
        and re-called with the next model in the cycle. Once shot 1 passes,
        its output is added as a "materials anchor" reference for shots 2..N.

    SKIP plans are ignored. Provider clients (OpenAI, Gemini, Anthropic) are
    initialized lazily, so a run with only COPY plans needs no API keys.
    """
    if not models:
        raise RuntimeError("execute(): models must be a non-empty list")

    sku_index = _build_sku_index(category_folder_id, structure)
    clients: dict = {}              # provider name → client (lazy)
    verify_client = None            # lazy: created on first verification

    for plan in plans:
        if plan.action not in ("COPY", "GENERATE"):
            continue
        if plan.sku not in sku_index:
            yield GenProgress(
                sku=plan.sku, source_sku=plan.source_sku, file_index=0,
                file_total=plan.photo_count, output_name="",
                skipped=True, error=f"target SKU folder not found: {plan.sku}",
            )
            continue
        if plan.source_sku not in sku_index:
            yield GenProgress(
                sku=plan.sku, source_sku=plan.source_sku, file_index=0,
                file_total=plan.photo_count, output_name="",
                skipped=True, error=f"source SKU folder not found: {plan.source_sku}",
            )
            continue

        _, target_sku_id = sku_index[plan.sku]
        _, source_sku_id = sku_index[plan.source_sku]

        sibling_photos = _list_photos(source_sku_id, photos_subdir)
        if not sibling_photos:
            yield GenProgress(
                sku=plan.sku, source_sku=plan.source_sku, file_index=0,
                file_total=plan.photo_count, output_name="",
                skipped=True, error="sibling has no photos at execute time",
            )
            continue

        # Ensure target photos folder exists.
        try:
            target_photos_id = _resolve_or_create(target_sku_id, photos_subdir)
        except Exception as exc:
            yield GenProgress(
                sku=plan.sku, source_sku=plan.source_sku, file_index=0,
                file_total=plan.photo_count, output_name="",
                skipped=False, error=f"could not create target photos folder: {exc}",
            )
            continue

        # If target was populated between dry-run and execute, refuse to overwrite.
        existing_names = {f["name"] for f in drive.list_files(target_photos_id)}
        existing_names = {n for n in existing_names if not n.startswith(".")}

        # ----- COPY branch: duplicate sibling photos verbatim, no AI. -----
        if plan.action == "COPY":
            for i, f in enumerate(sibling_photos, 1):
                if f["name"] in existing_names:
                    yield GenProgress(
                        sku=plan.sku, source_sku=plan.source_sku, file_index=i,
                        file_total=len(sibling_photos), output_name=f["name"],
                        skipped=True, error="target already has a file with this name",
                    )
                    continue
                try:
                    drive.copy_file(f["id"], target_photos_id, f["name"])
                    existing_names.add(f["name"])
                    yield GenProgress(
                        sku=plan.sku, source_sku=plan.source_sku, file_index=i,
                        file_total=len(sibling_photos), output_name=f["name"],
                        skipped=False,
                    )
                except Exception as exc:
                    yield GenProgress(
                        sku=plan.sku, source_sku=plan.source_sku, file_index=i,
                        file_total=len(sibling_photos), output_name=f["name"],
                        skipped=False, error=f"copy failed: {exc}",
                    )
            continue

        # ----- GENERATE branch: AI-edit sibling photos with material refs. -----
        # Provider clients are created lazily by _generate_dispatch as each
        # provider is hit (cycle through `models` on retry).

        # Resolve + download material reference photos once per SKU. Order is
        # deterministic (alphabetical column name) so prompt indices are stable.
        with tempfile.TemporaryDirectory(prefix=f"genphotos_{plan.sku}_") as tmp:
            tmp_path = _Path(tmp)
            material_paths_by_col: dict[str, str] = {}
            material_resolution_error: str | None = None
            for col in sorted(plan.replacements.keys()):
                if not plan.resolved.get(col):
                    continue
                _, target_val = plan.replacements[col]
                cat = material_columns.get(col)
                if not cat:
                    continue
                try:
                    f = _resolve_material_photo(parent_root_id, cat, target_val, photos_subdir)
                except Exception as exc:
                    material_resolution_error = f"could not resolve material {col}={target_val}: {exc}"
                    break
                if not f:
                    continue
                local = str(tmp_path / f"{col}_{target_val}_{f['name']}")
                try:
                    _download(f["id"], local)
                except Exception as exc:
                    material_resolution_error = f"could not download material {col}={target_val}: {exc}"
                    break
                material_paths_by_col[col] = local

            if material_resolution_error:
                yield GenProgress(
                    sku=plan.sku, source_sku=plan.source_sku, file_index=0,
                    file_total=plan.photo_count, output_name="",
                    skipped=False, error=material_resolution_error,
                )
                continue

            # image_index_map: column → 1-based reference image index. Image #1 is the
            # sibling, the next N are the materials in alphabetical column order.
            # When the materials anchor (a previously-approved generated photo)
            # is added, it gets the slot right after the materials.
            ordered_cols = list(material_paths_by_col.keys())
            base_image_index_map = {col: 2 + i for i, col in enumerate(ordered_cols)}
            material_paths = [material_paths_by_col[c] for c in ordered_cols]

            anchor_path: str | None = None  # set after shot 1 passes verification

            for i, sib in enumerate(sibling_photos, 1):
                # Output name mirrors sibling's index; .jpg extension.
                out_name = f"{i}.jpg"
                if out_name in existing_names:
                    yield GenProgress(
                        sku=plan.sku, source_sku=plan.source_sku, file_index=i,
                        file_total=len(sibling_photos), output_name=out_name,
                        skipped=True, error="target already has a file with this name",
                    )
                    continue

                sib_raw_local = str(tmp_path / f"sibling_{sib['name']}")
                try:
                    _download(sib["id"], sib_raw_local)
                except Exception as exc:
                    yield GenProgress(
                        sku=plan.sku, source_sku=plan.source_sku, file_index=i,
                        file_total=len(sibling_photos), output_name=out_name,
                        skipped=False, error=f"could not download sibling photo: {exc}",
                    )
                    continue

                # Auto-crop white background (product silhouettes only) and
                # detect image type (product vs macro). Cropping gives the
                # model a much stronger product-shape signal; the type drives
                # the prompt branch so macro shots aren't hallucinated into
                # full-product scenes.
                sib_local, image_type = _detect_and_crop(sib_raw_local)
                logger(f"  source type: {image_type}" + (
                    " (auto-cropped)" if sib_local != sib_raw_local else ""
                ))

                # Compose the model input: sibling + materials, plus the
                # anchor (clean shot 1 output) if available.
                gen_image_paths = [sib_local]
                gen_index_map = dict(base_image_index_map)
                if anchor_path:
                    # Anchor (a previously-approved output) goes in slot #2.
                    gen_image_paths.insert(1, anchor_path)
                    gen_index_map = {col: idx + 1 for col, idx in base_image_index_map.items()}
                gen_image_paths.extend(material_paths)

                prompt = build_prompt(
                    plan.parent_product, plan.replacements, gen_index_map,
                    image_type=image_type,
                )
                verdict: Verdict | None = None
                attempt = 0
                upload_done = False
                last_provider_error: str = ""
                failed_dest_id: str | None = None  # lazy: only created when --debug uploads happen

                # Header for this shot, with the prompt printed once.
                logger(f"\n[shot {i}/{len(sibling_photos)}] {plan.sku} → {out_name}")
                logger("  Prompt:")
                for line in prompt.splitlines():
                    logger(f"    {line}")

                while True:
                    attempt += 1
                    # Cycle through models on each attempt (wraps when retries > len(models)).
                    current_model = models[(attempt - 1) % len(models)]
                    try:
                        provider_label = _provider_for(current_model)
                    except Exception:
                        provider_label = "?"

                    logger(f"  → attempt {attempt} ({provider_label}/{current_model})")
                    logger(f"    ⏳ generating…")
                    gen_start = time.time()
                    try:
                        image_bytes = _generate_dispatch(
                            clients=clients,
                            model=current_model,
                            quality=quality,
                            size=size,
                            sibling_photo_path=gen_image_paths[0],
                            material_paths=gen_image_paths[1:],
                            prompt=prompt,
                        )
                    except Exception as exc:
                        gen_elapsed = time.time() - gen_start
                        last_provider_error = f"{provider_label} ({current_model}) error: {exc}"
                        logger(f"    ✗ generation failed in {gen_elapsed:.1f}s: {exc}")
                        if attempt > max_retries:
                            yield GenProgress(
                                sku=plan.sku, source_sku=plan.source_sku, file_index=i,
                                file_total=len(sibling_photos), output_name=out_name,
                                skipped=False,
                                error=f"all {attempt} attempts failed; last error: {last_provider_error}",
                            )
                            break
                        continue

                    gen_elapsed = time.time() - gen_start
                    logger(f"    ✓ generated {len(image_bytes):,} bytes in {gen_elapsed:.1f}s")

                    # Normalize to real JPEG. gpt-image-* sometimes returns PNG; without
                    # this the verifier rejects on a MIME mismatch.
                    candidate_path = str(
                        tmp_path / f"shot_{i}_attempt_{attempt}_{provider_label}.jpg"
                    )
                    try:
                        with _PILImage.open(io.BytesIO(image_bytes)) as raw_out:
                            raw_out.convert("RGB").save(
                                candidate_path, format="JPEG", quality=92, optimize=True,
                            )
                    except Exception:
                        with open(candidate_path, "wb") as fh:
                            fh.write(image_bytes)

                    if not verify_enabled:
                        verdict = Verdict(ok=True)
                        logger("    (verifier off — accepting)")
                    else:
                        if verify_client is None:
                            try:
                                verify_client = _anthropic_client()
                            except Exception as exc:
                                logger(f"    ✗ verifier unavailable: {exc}")
                                yield GenProgress(
                                    sku=plan.sku, source_sku=plan.source_sku, file_index=i,
                                    file_total=len(sibling_photos), output_name=out_name,
                                    skipped=False,
                                    error=f"verifier unavailable: {exc} (use --no-verify to skip)",
                                )
                                break
                        logger(f"    ⏳ verifying with {verify_model}…")
                        verify_start = time.time()
                        verdict = verify_with_claude(
                            client=verify_client,
                            model=verify_model,
                            parent_product=plan.parent_product,
                            sibling_photo_path=sib_local,
                            generated_photo_path=candidate_path,
                            material_paths_by_col=material_paths_by_col,
                            replacements=plan.replacements,
                        )
                        verify_elapsed = time.time() - verify_start
                        if verdict.ok:
                            logger(f"    ✓ verifier passed in {verify_elapsed:.1f}s")
                        else:
                            reason = "; ".join(verdict.reasons) or verdict.retry_instructions or "rejected"
                            logger(f"    ✗ verifier rejected in {verify_elapsed:.1f}s: {reason}")

                    if verdict.ok or attempt > max_retries:
                        # Accept or give up: upload either way.
                        if not verdict.ok:
                            logger(f"    (uploading anyway — {max_retries} retries exhausted)")
                        try:
                            drive.upload_file(candidate_path, target_photos_id, out_name, "image/jpeg")
                            upload_done = True
                            logger(f"    ✓ uploaded → {out_name}")
                        except Exception as exc:
                            logger(f"    ✗ upload failed: {exc}")
                            yield GenProgress(
                                sku=plan.sku, source_sku=plan.source_sku, file_index=i,
                                file_total=len(sibling_photos), output_name=out_name,
                                skipped=False, error=f"upload failed: {exc}",
                            )
                            break
                        break

                    # Verifier rejected and we have retries left. Optionally archive
                    # the failed candidate to <photos>/failed_generations/ for review.
                    if debug:
                        try:
                            if failed_dest_id is None:
                                failed_dest_id = drive.find_or_create_folder(
                                    "failed_generations", target_photos_id,
                                )
                            failed_name = (
                                f"shot{i}_attempt{attempt}_{provider_label}_{current_model}.jpg"
                            )
                            drive.upload_file(
                                candidate_path, failed_dest_id, failed_name, "image/jpeg",
                            )
                            logger(f"    [debug] saved failed attempt → failed_generations/{failed_name}")
                        except Exception as exc:
                            logger(f"    [debug] could not archive failed attempt: {exc}")
                    # Loop back to retry with next model in the cycle (same prompt).

                if not upload_done:
                    continue  # already yielded an error

                existing_names.add(out_name)

                # Save shot 1 as the materials anchor for subsequent shots.
                if i == 1 and (verdict is None or verdict.ok):
                    anchor_path = candidate_path

                err_note = ""
                if verdict and not verdict.ok and attempt > max_retries:
                    err_note = f"uploaded after {max_retries} failed verifications: " + (
                        "; ".join(verdict.reasons) or verdict.retry_instructions or "see logs"
                    )

                yield GenProgress(
                    sku=plan.sku, source_sku=plan.source_sku, file_index=i,
                    file_total=len(sibling_photos), output_name=out_name,
                    skipped=False, error=err_note,
                )
