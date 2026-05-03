from __future__ import annotations

import hashlib
import io
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Generator, NamedTuple

import numpy as np
from PIL import Image

from asset_sdk.adapters import drive
from asset_sdk.config import OptimizeConfig

_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff", ".tif"}


# ---------------------------------------------------------------------------
# Image pipeline
# ---------------------------------------------------------------------------

def _to_rgb_white_bg(img: Image.Image) -> Image.Image:
    """Flatten any alpha channel onto a white background."""
    if img.mode in ("RGBA", "LA"):
        bg = Image.new("RGB", img.size, (255, 255, 255))
        bg.paste(img, mask=img.split()[-1])
        return bg
    if img.mode != "RGB":
        return img.convert("RGB")
    return img


def _clean_near_white(arr: np.ndarray, threshold: int) -> np.ndarray:
    """Clamp any pixel whose every channel ≥ threshold to pure white (255,255,255)."""
    mask = np.all(arr >= threshold, axis=-1)
    arr[mask] = (255, 255, 255)
    return arr


def _product_bbox(arr: np.ndarray, threshold: int) -> tuple[int, int, int, int] | None:
    """Bounding box of non-white pixels, or None if the image is all-white."""
    mask = ~np.all(arr >= threshold, axis=-1)
    if not mask.any():
        return None
    rows = np.any(mask, axis=1)
    cols = np.any(mask, axis=0)
    rmin, rmax = np.where(rows)[0][[0, -1]]
    cmin, cmax = np.where(cols)[0][[0, -1]]
    return int(cmin), int(rmin), int(cmax) + 1, int(rmax) + 1


def optimize_image(
    img: Image.Image,
    cfg: OptimizeConfig,
    *,
    clean_bg: bool = True,
    pad_to_square: bool = True,
    resize: bool = True,
) -> Image.Image:
    """Standardisation pipeline. Each step is conditional so the per-row
    Action flags from the report (Remove BG, Pad to Square, Resize, …)
    can opt out of any individual transform.

    Steps (each gated by its own flag):

      1. Convert to RGB, flattening alpha onto white. (always)
      2. Clean near-white background to pure white. (clean_bg)
      3. Detect product bounding box and recenter on a square white canvas
         with target_padding_pct margin. Wide products get top/bottom padding;
         tall products get side padding. (pad_to_square)
      4. Resize the canvas to target_size × target_size. (resize)

    `compress` (controlling the JPEG quality cap) is applied later in
    `save_jpeg`, which always honors max_file_mb. Pass `compress=False` to
    `save_jpeg` directly when a row sets Compress=FALSE.
    """
    img = _to_rgb_white_bg(img)

    if clean_bg:
        arr = np.array(img)
        arr = _clean_near_white(arr, cfg.white_threshold)
        bbox = _product_bbox(arr, cfg.white_threshold)
        cleaned = Image.fromarray(arr)
    else:
        # Skip background cleaning (close-up shots). bbox detection still uses
        # near-white as the criterion, but we don't rewrite any pixels.
        arr = np.array(img)
        bbox = _product_bbox(arr, cfg.white_threshold)
        cleaned = img

    if pad_to_square:
        if bbox is None:
            # All-near-white input — just emit a white square at target size (or original size).
            target_dim = cfg.target_size if resize else max(cleaned.size)
            return Image.new("RGB", (target_dim, target_dim), (255, 255, 255))

        product = cleaned.crop(bbox)
        pw, ph = product.size

        # Canvas size such that longest side = (1 - 2*padding%) of canvas.
        pad_frac = cfg.target_padding_pct / 100.0
        inner_frac = max(1.0 - 2 * pad_frac, 0.05)
        canvas_size = max(int(max(pw, ph) / inner_frac), pw, ph)

        canvas = Image.new("RGB", (canvas_size, canvas_size), (255, 255, 255))
        canvas.paste(product, ((canvas_size - pw) // 2, (canvas_size - ph) // 2))
        result = canvas
    else:
        # Don't pad to square — keep original framing.
        result = cleaned

    if resize:
        # If pad_to_square produced a canvas, this resizes it to the target square.
        # If not (close-up keeping aspect), this scales the longest edge to target_size.
        rw, rh = result.size
        if rw == rh:
            return result.resize((cfg.target_size, cfg.target_size), Image.LANCZOS)
        scale = cfg.target_size / max(rw, rh)
        return result.resize((int(rw * scale), int(rh * scale)), Image.LANCZOS)

    return result


def save_jpeg(
    img: Image.Image,
    path: str,
    cfg: OptimizeConfig,
    *,
    compress: bool = True,
) -> int:
    """Save as JPEG. With `compress=True` (default), decreasing quality until
    the file fits under cfg.max_file_mb. With `compress=False`, save once at
    cfg.jpg_quality without the size cap (used when the row's Compress flag
    is FALSE)."""
    if not compress:
        img.save(path, "JPEG", quality=cfg.jpg_quality, optimize=True)
        return os.path.getsize(path)
    cap_bytes = int(cfg.max_file_mb * 1024 * 1024)
    for q in (cfg.jpg_quality, 80, 75, 70, 65, 60, 55, 50):
        img.save(path, "JPEG", quality=q, optimize=True)
        size = os.path.getsize(path)
        if size <= cap_bytes:
            return size
    return os.path.getsize(path)


# ---------------------------------------------------------------------------
# Drive walking
# ---------------------------------------------------------------------------

@dataclass
class PhotoTarget:
    sku: str
    supplier: str
    sku_folder_id: str
    src_subdir: str
    dest_subdir: str
    src_folder_id: str
    files: list[dict]   # [{id, name}, ...] — image files only


def _is_image(name: str) -> bool:
    return Path(name).suffix.lower() in _IMAGE_EXTENSIONS


def _resolve_subfolder(parent_id: str, rel_path: str) -> str | None:
    """Walk rel_path ('photos' or 'thumbnails/website_thumbnail') from parent_id."""
    current = parent_id
    for part in rel_path.split("/"):
        children = drive.list_folders(current)
        if part not in children:
            return None
        current = children[part]
    return current


def find_targets(
    root_folder_id: str,
    structure: str,
    src_subdir: str,
    dest_subdir: str,
    sku_filter: str | None = None,
    supplier_filter: str | None = None,
) -> list[PhotoTarget]:
    """Scan products drive for SKUs that have a populated src_subdir folder."""
    targets: list[PhotoTarget] = []

    def _process_sku(sku_name: str, sku_folder_id: str, supplier_name: str) -> None:
        if sku_filter and sku_name != sku_filter:
            return
        src_id = _resolve_subfolder(sku_folder_id, src_subdir)
        if not src_id:
            return
        files = [f for f in drive.list_files(src_id) if _is_image(f["name"])]
        if not files:
            return
        targets.append(PhotoTarget(
            sku=sku_name, supplier=supplier_name,
            sku_folder_id=sku_folder_id,
            src_subdir=src_subdir, dest_subdir=dest_subdir,
            src_folder_id=src_id, files=files,
        ))

    if structure == "flat":
        for sku, sku_id in drive.list_folders(root_folder_id).items():
            _process_sku(sku, sku_id, "")
    else:
        for sup_name, sup_id in drive.list_folders(root_folder_id).items():
            if supplier_filter and sup_name.lower() != supplier_filter.lower():
                continue
            for sku, sku_id in drive.list_folders(sup_id).items():
                _process_sku(sku, sku_id, sup_name)

    return targets


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Analysis (dry-run report)
# ---------------------------------------------------------------------------

@dataclass
class PhotoAnalysis:
    sku: str
    supplier: str
    file_id: str
    file_name: str
    current_format: str           # e.g., "JPEG", "PNG", "WEBP"
    current_width: int
    current_height: int
    current_size_bytes: int
    target_size_bytes: int        # actual JPEG-encoded size after optimize_image (0 for duplicates)
    content_hash: str             # md5 of the source bytes
    is_duplicate_of: str          # filename of the first occurrence within the same SKU; "" if unique
    pure_white_pct: float
    near_white_pct: float
    has_background: bool          # True when near_white < 80%
    current_padding_pct: float | None  # None when image is all-white
    aspect_label: str             # "1:1", "tall", "wide"


def _aspect_label(w: int, h: int) -> str:
    if w == 0 or h == 0:
        return "—"
    ratio = w / h
    if 0.95 <= ratio <= 1.05:
        return "1:1"
    if ratio < 0.95:
        return f"tall ({w}:{h})"
    return f"wide ({w}:{h})"


def _analyze_image(
    img: Image.Image,
    src_path: str,
    cfg: OptimizeConfig,
) -> tuple[float, float, bool, float | None, str]:
    """Return (pure_white_pct, near_white_pct, has_background, padding_pct, aspect_label)."""
    rgb = _to_rgb_white_bg(img)
    arr = np.array(rgb)

    pure_white = float(np.all(arr == 255, axis=-1).mean()) * 100
    near_white = float(np.all(arr >= cfg.white_threshold, axis=-1).mean()) * 100
    has_bg = near_white < 80

    bbox = _product_bbox(arr, cfg.white_threshold)
    h, w = arr.shape[:2]
    if bbox:
        cmin, rmin, cmax, rmax = bbox
        margins = [
            rmin / h * 100,
            (h - rmax) / h * 100,
            cmin / w * 100,
            (w - cmax) / w * 100,
        ]
        padding = sum(margins) / 4
    else:
        padding = None

    return pure_white, near_white, has_bg, padding, _aspect_label(w, h)


def _md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _estimate_target_size(img: Image.Image, cfg: OptimizeConfig) -> int:
    """Run the optimize pipeline to an in-memory JPEG buffer and return its size.

    Honors the same defaults the report's auto-set Action flags will use:
    clean background, pad to square, resize, compress (cap to max_file_mb).
    """
    optimized = optimize_image(img, cfg, clean_bg=True, pad_to_square=True, resize=True)
    cap_bytes = int(cfg.max_file_mb * 1024 * 1024)
    for q in (cfg.jpg_quality, 80, 75, 70, 65, 60, 55, 50):
        buf = io.BytesIO()
        optimized.save(buf, format="JPEG", quality=q, optimize=True)
        size = buf.tell()
        if size <= cap_bytes:
            return size
    return size


def analyze(
    targets: list[PhotoTarget],
    cfg: OptimizeConfig,
) -> Generator[tuple[PhotoTarget, dict, "PhotoAnalysis | None", str | None], None, None]:
    """Download each file in each target and analyze it.

    Yields (target, file_meta, analysis_or_None, error_or_None) per file so the caller
    can drive a progress bar. analysis is None when an error occurs.

    Adds within-SKU duplicate detection (md5 of source bytes) and an accurate
    target_size estimate (actual JPEG encode of the optimized image to a buffer).
    Duplicates skip the size estimate since they won't be uploaded.
    """
    with tempfile.TemporaryDirectory() as tmp:
        for t in targets:
            # SKU-scoped: hash → first filename that had it. Subsequent matches
            # are flagged as duplicates pointing back to that filename.
            seen_in_sku: dict[str, str] = {}
            for f in t.files:
                src_path = os.path.join(tmp, f["name"])
                try:
                    drive.download_file(f["id"], src_path)
                    digest = _md5_file(src_path)
                    duplicate_of = seen_in_sku.get(digest, "")
                    if not duplicate_of:
                        seen_in_sku[digest] = f["name"]

                    with Image.open(src_path) as img:
                        fmt = (img.format or "UNKNOWN").upper()
                        w, h = img.size
                        size_bytes = os.path.getsize(src_path)
                        pure, near, has_bg, padding, aspect = _analyze_image(img, src_path, cfg)
                        # Skip the (expensive) JPEG-encode estimate for duplicates.
                        target_bytes = 0 if duplicate_of else _estimate_target_size(img, cfg)

                    analysis = PhotoAnalysis(
                        sku=t.sku, supplier=t.supplier,
                        file_id=f["id"], file_name=f["name"],
                        current_format=fmt,
                        current_width=w, current_height=h,
                        current_size_bytes=size_bytes,
                        target_size_bytes=target_bytes,
                        content_hash=digest,
                        is_duplicate_of=duplicate_of,
                        pure_white_pct=pure, near_white_pct=near,
                        has_background=has_bg,
                        current_padding_pct=padding,
                        aspect_label=aspect,
                    )
                    yield t, f, analysis, None
                except Exception as exc:
                    yield t, f, None, str(exc)
                finally:
                    try:
                        os.unlink(src_path)
                    except OSError:
                        pass


def _human_mb(b: int) -> str:
    return f"{b / 1024 / 1024:.1f}MB"


def to_sheet_rows(
    analyses: list[PhotoAnalysis],
    cfg: OptimizeConfig,
) -> tuple[list[str], list[list]]:
    """Build the editable Optimize Report.

    Headers:
        SKU | Supplier | File | Preview | Format | Width | Height | Aspect
        | Current MB | Target MB | Has BG | Duplicate Of
        | Convert JPG | Remove BG | Pad to Square | Resize | Compress
        | Notes

    The five rightmost flags are auto-set based on analysis but EDITABLE — set
    Remove BG=FALSE on a close-up that legitimately has no white background;
    set Pad to Square=FALSE if you want to preserve a non-square crop; etc.

    Duplicates within the same SKU are flagged via Duplicate Of and skipped on
    execute (the canonical version gets optimized to the same dest path).
    """
    headers = [
        "SKU", "Supplier", "File", "Preview",
        "Format", "Width", "Height", "Aspect",
        "Current MB", "Target MB",
        "Has BG", "Duplicate Of",
        "Convert JPG", "Remove BG", "Pad to Square", "Resize", "Compress",
        "Notes",
    ]

    rows: list[list] = []
    for a in analyses:
        is_dup = bool(a.is_duplicate_of)

        # Default action flags. Duplicates default everything to FALSE — the
        # canonical row will produce the optimized output, so this row is
        # skipped on execute regardless of flag values.
        if is_dup:
            convert_jpg = remove_bg = pad_square = resize = compress = "FALSE"
        else:
            convert_jpg = "TRUE" if a.current_format != "JPEG" else "FALSE"
            remove_bg = "TRUE" if a.has_background else "FALSE"
            # Pad to square unless already 1:1 AND already at target size.
            already_square = a.aspect_label == "1:1"
            already_target_dim = a.current_width == cfg.target_size and a.current_height == cfg.target_size
            pad_square = "FALSE" if already_square and already_target_dim else "TRUE"
            resize = "TRUE" if not already_target_dim else "FALSE"
            # Always cap to max_file_mb if currently above it; default TRUE so
            # any newly-optimized image stays under the cap (cheap when already small).
            compress = "TRUE"

        notes_parts: list[str] = []
        if is_dup:
            notes_parts.append(f"duplicate of {a.is_duplicate_of}")
        if a.has_background:
            notes_parts.append(f"{a.near_white_pct:.0f}% near-white")
        if a.aspect_label != "1:1":
            notes_parts.append(a.aspect_label)
        if a.current_size_bytes > cfg.max_file_mb * 1024 * 1024:
            notes_parts.append(f"oversized ({_human_mb(a.current_size_bytes)})")
        notes = "; ".join(notes_parts)

        preview_url = f"https://drive.google.com/thumbnail?id={a.file_id}&sz=w400"
        preview = f'=IMAGE("{preview_url}")'

        target_mb_str = (
            "—" if is_dup or a.target_size_bytes == 0
            else f"{a.target_size_bytes / 1024 / 1024:.2f}"
        )

        rows.append([
            a.sku,
            a.supplier,
            a.file_name,
            preview,
            a.current_format,
            a.current_width,
            a.current_height,
            a.aspect_label,
            f"{a.current_size_bytes / 1024 / 1024:.2f}",
            target_mb_str,
            "TRUE" if a.has_background else "FALSE",
            a.is_duplicate_of,
            convert_jpg, remove_bg, pad_square, resize, compress,
            notes,
        ])

    return headers, rows


# ---------------------------------------------------------------------------
# Execute (unchanged)
# ---------------------------------------------------------------------------

class OptimizeProgress(NamedTuple):
    sku: str
    supplier: str
    file_name: str
    file_index: int      # within current SKU
    file_total: int      # within current SKU
    sku_index: int       # 1-based
    sku_total: int
    skipped: bool        # True when output already existed


def _bool(s: str | None) -> bool:
    """Parse a TRUE/FALSE/yes/no/1/0 cell from the report."""
    if not s:
        return False
    return str(s).strip().upper() in ("TRUE", "YES", "Y", "1", "T")


def execute(
    report_rows: list[dict[str, str]],
    targets: list[PhotoTarget],
    cfg: OptimizeConfig,
) -> Generator[OptimizeProgress, None, None]:
    """Apply each report row's per-action flags to its photo.

    Read the (possibly edited) report and for each row:
      - SKIP if the row has Duplicate Of set (the canonical version handles upload).
      - SKIP if every action flag is FALSE (no work to do).
      - Otherwise download → run optimize_image with only the TRUE flags →
        save_jpeg honoring Compress → upload to <sku>/<photos-optimized>/.

    `targets` is still needed to resolve <sku>/<photos>/ source folder + the
    destination folder ID. We index it by (sku, file_name).
    """
    # Build (sku, file_name) → (target, file_meta) lookup so each report row
    # can find its source on Drive without re-walking the category.
    file_index: dict[tuple[str, str], tuple[PhotoTarget, dict]] = {}
    for t in targets:
        for f in t.files:
            file_index[(t.sku, f["name"])] = (t, f)

    # Pre-resolve dest folder per SKU; cache existing-output names.
    dest_cache: dict[str, tuple[str, set[str]]] = {}
    def _dest_for(t: PhotoTarget) -> tuple[str, set[str]]:
        if t.sku not in dest_cache:
            dest_id = drive.find_or_create_folder(t.dest_subdir, t.sku_folder_id)
            existing = {f["name"] for f in drive.list_files(dest_id)}
            dest_cache[t.sku] = (dest_id, existing)
        return dest_cache[t.sku]

    # We need a per-(sku) running totals for the progress NamedTuple. Group rows by SKU.
    rows_by_sku: dict[str, list[dict[str, str]]] = {}
    sku_order: list[str] = []
    for r in report_rows:
        sku = (r.get("SKU") or "").strip()
        if not sku:
            continue
        if sku not in rows_by_sku:
            rows_by_sku[sku] = []
            sku_order.append(sku)
        rows_by_sku[sku].append(r)

    sku_total = len(sku_order)

    with tempfile.TemporaryDirectory() as tmp:
        for sku_idx, sku in enumerate(sku_order, 1):
            sku_rows = rows_by_sku[sku]
            file_total = len(sku_rows)

            for file_idx, r in enumerate(sku_rows, 1):
                file_name = (r.get("File") or "").strip()
                supplier = (r.get("Supplier") or "").strip()
                duplicate_of = (r.get("Duplicate Of") or "").strip()

                if duplicate_of:
                    yield OptimizeProgress(
                        sku=sku, supplier=supplier, file_name=file_name,
                        file_index=file_idx, file_total=file_total,
                        sku_index=sku_idx, sku_total=sku_total,
                        skipped=True,
                    )
                    continue

                convert_jpg = _bool(r.get("Convert JPG"))
                remove_bg = _bool(r.get("Remove BG"))
                pad_square = _bool(r.get("Pad to Square"))
                resize_flag = _bool(r.get("Resize"))
                compress = _bool(r.get("Compress"))

                # All-FALSE row → nothing to do (treat as opt-out).
                if not (convert_jpg or remove_bg or pad_square or resize_flag or compress):
                    yield OptimizeProgress(
                        sku=sku, supplier=supplier, file_name=file_name,
                        file_index=file_idx, file_total=file_total,
                        sku_index=sku_idx, sku_total=sku_total,
                        skipped=True,
                    )
                    continue

                key = (sku, file_name)
                if key not in file_index:
                    # Source disappeared between dry-run and execute — skip with a marker.
                    yield OptimizeProgress(
                        sku=sku, supplier=supplier, file_name=file_name,
                        file_index=file_idx, file_total=file_total,
                        sku_index=sku_idx, sku_total=sku_total,
                        skipped=True,
                    )
                    continue
                t, f = file_index[key]

                dest_id, existing_names = _dest_for(t)
                out_name = Path(file_name).stem + ".jpg"
                if out_name in existing_names:
                    yield OptimizeProgress(
                        sku=sku, supplier=supplier, file_name=file_name,
                        file_index=file_idx, file_total=file_total,
                        sku_index=sku_idx, sku_total=sku_total,
                        skipped=True,
                    )
                    continue

                src_path = os.path.join(tmp, file_name)
                drive.download_file(f["id"], src_path)

                with Image.open(src_path) as src_img:
                    optimized = optimize_image(
                        src_img, cfg,
                        clean_bg=remove_bg,
                        pad_to_square=pad_square,
                        resize=resize_flag,
                    )

                out_path = os.path.join(tmp, out_name)
                save_jpeg(optimized, out_path, cfg, compress=compress)
                drive.upload_file(out_path, dest_id, out_name, "image/jpeg")
                existing_names.add(out_name)

                try:
                    os.unlink(src_path)
                except OSError:
                    pass
                try:
                    os.unlink(out_path)
                except OSError:
                    pass

                yield OptimizeProgress(
                    sku=sku, supplier=supplier, file_name=file_name,
                    file_index=file_idx, file_total=file_total,
                    sku_index=sku_idx, sku_total=sku_total,
                    skipped=False,
                )
