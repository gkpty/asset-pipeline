from __future__ import annotations

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


def optimize_image(img: Image.Image, cfg: OptimizeConfig) -> Image.Image:
    """Standardisation pipeline:

    1. Convert to RGB, flattening alpha onto white.
    2. Clean near-white background to pure white.
    3. Detect product bounding box; crop to it.
    4. Center on a square white canvas with target_padding_pct on the longest side.
    5. Resize the canvas to target_size × target_size (LANCZOS).
    """
    img = _to_rgb_white_bg(img)
    arr = np.array(img)
    arr = _clean_near_white(arr, cfg.white_threshold)

    bbox = _product_bbox(arr, cfg.white_threshold)
    cleaned = Image.fromarray(arr)

    if bbox is None:
        # All-white input — just emit a white square at target size.
        return Image.new("RGB", (cfg.target_size, cfg.target_size), (255, 255, 255))

    product = cleaned.crop(bbox)
    pw, ph = product.size

    # Canvas size such that longest side = (1 - 2*padding%) of canvas.
    pad_frac = cfg.target_padding_pct / 100.0
    inner_frac = max(1.0 - 2 * pad_frac, 0.05)
    canvas_size = max(int(max(pw, ph) / inner_frac), pw, ph)

    canvas = Image.new("RGB", (canvas_size, canvas_size), (255, 255, 255))
    canvas.paste(product, ((canvas_size - pw) // 2, (canvas_size - ph) // 2))

    return canvas.resize((cfg.target_size, cfg.target_size), Image.LANCZOS)


def save_jpeg(img: Image.Image, path: str, cfg: OptimizeConfig) -> int:
    """Save as JPEG, decreasing quality until the file fits under max_file_mb."""
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


def analyze(
    targets: list[PhotoTarget],
    cfg: OptimizeConfig,
) -> Generator[tuple[PhotoTarget, dict, "PhotoAnalysis | None", str | None], None, None]:
    """Download each file in each target and analyze it.

    Yields (target, file_meta, analysis_or_None, error_or_None) per file so the caller
    can drive a progress bar. analysis is None when an error occurs.
    """
    with tempfile.TemporaryDirectory() as tmp:
        for t in targets:
            for f in t.files:
                src_path = os.path.join(tmp, f["name"])
                try:
                    drive.download_file(f["id"], src_path)
                    with Image.open(src_path) as img:
                        fmt = (img.format or "UNKNOWN").upper()
                        w, h = img.size
                        size_bytes = os.path.getsize(src_path)
                        pure, near, has_bg, padding, aspect = _analyze_image(img, src_path, cfg)
                    analysis = PhotoAnalysis(
                        sku=t.sku, supplier=t.supplier,
                        file_id=f["id"], file_name=f["name"],
                        current_format=fmt,
                        current_width=w, current_height=h,
                        current_size_bytes=size_bytes,
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
    headers = [
        "SKU", "Supplier", "File", "Preview",
        "Format", "Dimensions", "Aspect", "File Size",
        "Padding", "Background", "Actions",
    ]

    rows: list[list] = []
    for a in analyses:
        actions: list[str] = []
        if a.current_format != "JPEG":
            actions.append(f"format {a.current_format} → JPG")
        if a.current_width != cfg.target_size or a.current_height != cfg.target_size:
            actions.append(
                f"resize {a.current_width}×{a.current_height} → "
                f"{cfg.target_size}×{cfg.target_size}"
            )
        if (
            a.current_padding_pct is not None
            and abs(a.current_padding_pct - cfg.target_padding_pct) > 1.5
        ):
            actions.append(
                f"padding {a.current_padding_pct:.0f}% → {cfg.target_padding_pct:.0f}%"
            )
        if a.aspect_label != "1:1":
            actions.append("square canvas")
        if a.has_background or a.pure_white_pct < a.near_white_pct - 5:
            actions.append("clean background to pure white")
        if a.current_size_bytes > cfg.max_file_mb * 1024 * 1024:
            actions.append(f"compress (currently {_human_mb(a.current_size_bytes)})")

        # =IMAGE(url, mode, height_px, width_px) — mode 4 = explicit pixel size.
        preview_url = f"https://drive.google.com/thumbnail?id={a.file_id}&sz=w200"
        preview = f'=IMAGE("{preview_url}", 4, 100, 100)'

        if a.current_padding_pct is None:
            pad_str = "—"
        else:
            pad_str = f"{a.current_padding_pct:.0f}% → {cfg.target_padding_pct:.0f}%"

        if a.has_background:
            bg_str = f"Has background ({a.near_white_pct:.0f}% near-white)"
        elif a.pure_white_pct < a.near_white_pct - 5:
            bg_str = f"Subtle off-white ({a.pure_white_pct:.0f}% pure / {a.near_white_pct:.0f}% near)"
        else:
            bg_str = f"Clean ({a.pure_white_pct:.0f}% pure white)"

        rows.append([
            a.sku,
            a.supplier,
            a.file_name,
            preview,
            f"{a.current_format} → JPG",
            f"{a.current_width}×{a.current_height} → {cfg.target_size}×{cfg.target_size}",
            a.aspect_label,
            f"{_human_mb(a.current_size_bytes)} → ≤{cfg.max_file_mb}MB",
            pad_str,
            bg_str,
            "; ".join(actions) if actions else "—",
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


def execute(
    targets: list[PhotoTarget],
    cfg: OptimizeConfig,
) -> Generator[OptimizeProgress, None, None]:
    """Download → optimize → upload each image. Idempotent: skips files that already
    exist in the destination folder."""
    with tempfile.TemporaryDirectory() as tmp:
        for sku_idx, t in enumerate(targets, 1):
            dest_id = drive.find_or_create_folder(t.dest_subdir, t.sku_folder_id)
            existing = {f["name"] for f in drive.list_files(dest_id)}

            for file_idx, f in enumerate(t.files, 1):
                out_name = Path(f["name"]).stem + ".jpg"
                if out_name in existing:
                    yield OptimizeProgress(
                        sku=t.sku, supplier=t.supplier, file_name=f["name"],
                        file_index=file_idx, file_total=len(t.files),
                        sku_index=sku_idx, sku_total=len(targets),
                        skipped=True,
                    )
                    continue

                src_path = os.path.join(tmp, f["name"])
                drive.download_file(f["id"], src_path)

                with Image.open(src_path) as src_img:
                    optimized = optimize_image(src_img, cfg)

                out_path = os.path.join(tmp, out_name)
                save_jpeg(optimized, out_path, cfg)
                drive.upload_file(out_path, dest_id, out_name, "image/jpeg")
                existing.add(out_name)

                # Free disk for the next file.
                try:
                    os.unlink(src_path)
                except OSError:
                    pass
                try:
                    os.unlink(out_path)
                except OSError:
                    pass

                yield OptimizeProgress(
                    sku=t.sku, supplier=t.supplier, file_name=f["name"],
                    file_index=file_idx, file_total=len(t.files),
                    sku_index=sku_idx, sku_total=len(targets),
                    skipped=False,
                )
