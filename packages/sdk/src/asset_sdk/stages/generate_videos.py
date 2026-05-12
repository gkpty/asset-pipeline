"""Generate one short promotional video per product SKU from its existing
white-background photos.

Mechanism: for each SKU, pick the first product (white-bg, not macro) photo
under <sku>/photos/, then call Replicate's image-to-video model (default
bytedance/seedance-1-pro) with that photo as the start frame and a simple
"slow dolly-in, photorealistic" prompt. The MP4 output is uploaded to
<sku>/videos/<sku>.mp4.

Cost is computed up front so the dry-run report shows total $$ before any API
call. The CLI's --budget flag enforces a pre-flight ceiling.
"""
from __future__ import annotations

import io
import os
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path as _Path
from typing import Generator, NamedTuple

import re
import subprocess

import numpy as np
from PIL import Image as _PILImage, ImageDraw as _PILDraw, ImageFont as _PILFont

from asset_sdk.adapters import drive

# Same near-white heuristic as generate_photos so we only seed image-to-video
# from product silhouettes (not macro / detail shots).
_PRODUCT_WHITE_PCT = 0.20
_NEAR_WHITE_THRESHOLD = 245

# Font candidates probed when title_font_path is unset. First match wins.
_SYSTEM_FONT_CANDIDATES = [
    "/System/Library/Fonts/Helvetica.ttc",
    "/System/Library/Fonts/Supplemental/Arial.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "C:\\Windows\\Fonts\\arial.ttf",
]


@dataclass
class VideoPlan:
    sku: str
    supplier: str
    parent_product: str
    # Filenames of the photos to use as references / keyframes. Pre-filled at
    # plan time with every photo in the SKU's folder; the user can trim /
    # reorder this in the report before --execute.
    source_photos: list[str] = field(default_factory=list)
    # Per-SKU scene description. Non-empty → place the product in this
    # setting (reference-images mode, Seedance 2.0+). Empty → keep the
    # white studio background. Pre-filled from the CLI default; editable
    # per-row in the report.
    background: str = ""
    # Per-SKU audio style. Non-empty → generate background music in this
    # style (Seedance 2.0+). Empty → silent video. Pre-filled from the CLI
    # default; editable per-row in the report.
    audio: str = ""
    # Title text shown on the opening title card. Pre-filled from the
    # sheet's `name` column at plan time; editable per-row. Empty (or
    # title cards disabled) → no opening card for this SKU.
    title: str = ""
    # Per-SKU prompt override. Non-empty → replaces the default motion /
    # style block entirely for this row. Useful for products where the
    # generic prompt produces artifacts ("extra leg", "merged surfaces")
    # and you need more specific guidance.
    prompt_override: str = ""
    cost_usd: float = 0.0
    action: str = "SKIP"            # GENERATE | SKIP
    notes: str = ""


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


def _list_videos(sku_id: str, videos_subdir: str) -> list[dict]:
    folder = _resolve_optional(sku_id, videos_subdir)
    if not folder:
        return []
    files = drive.list_files(folder)
    return [f for f in files if not f["name"].startswith(".")]


# ---------------------------------------------------------------------------
# build_plan
# ---------------------------------------------------------------------------

def build_plan(
    *,
    category_folder_id: str,
    structure: str,
    sheet_rows: list[dict[str, str]],
    sku_col: str,
    supplier_col: str,
    parent_product_col: str,
    photos_subdir: str,
    videos_subdir: str,
    cost_per_video_usd: float,
    name_col: str = "",
    default_background: str = "",
    default_audio: str = "",
    default_prompt: str = "",
    sku_filter: str | None = None,
) -> list[VideoPlan]:
    """One VideoPlan per SKU in the sheet.

    SKIP rules:
      - Target SKU folder not found on Drive.
      - Target SKU already has a file under videos/.
      - Target SKU has no photos under photos/.
    """
    sku_index = _build_sku_index(category_folder_id, structure)
    plans: list[VideoPlan] = []

    for row in sheet_rows:
        sku = (row.get(sku_col) or "").strip()
        sup = (row.get(supplier_col) or "").strip()
        parent = (row.get(parent_product_col) or "").strip()
        if not sku:
            continue
        if sku_filter is not None and sku != sku_filter:
            continue

        # Pre-compute everything we can fill on a row, including SKIP rows
        # — so the report always shows defaults that the user can edit. A
        # SKIP row the user wants to run becomes a GENERATE with one flip
        # of the Action cell.
        product_name = (row.get(name_col) or "").strip() if name_col else ""

        def _new_plan(
            *, supplier: str, action: str, notes: str,
            photos_list: list[dict] | None = None, cost: float = 0.0,
        ) -> VideoPlan:
            return VideoPlan(
                sku=sku, supplier=supplier, parent_product=parent,
                source_photos=[p["name"] for p in (photos_list or [])],
                background=default_background,
                audio=default_audio,
                title=product_name,
                prompt_override=default_prompt,
                cost_usd=cost,
                action=action,
                notes=notes,
            )

        if sku not in sku_index:
            plans.append(_new_plan(
                supplier=sup, action="SKIP",
                notes="sku folder not found in Drive",
            ))
            continue

        target_sup, target_sku_id = sku_index[sku]

        # List photos eagerly so SKIP rows that DO have photos still show
        # them in the report (useful when flipping to GENERATE).
        photos = _list_photos(target_sku_id, photos_subdir)

        if _list_videos(target_sku_id, videos_subdir):
            plans.append(_new_plan(
                supplier=target_sup, action="SKIP",
                notes="target already has a video", photos_list=photos,
            ))
            continue

        if not photos:
            plans.append(_new_plan(
                supplier=target_sup, action="SKIP",
                notes="no photos to seed from",
            ))
            continue

        # GENERATE: list ALL photo filenames so the user can edit the cell
        # before --execute to keep only the reference shots they want. The
        # order in the cell is preserved at execute time — for keyframe
        # mode, the first entry is the first frame, the last is the last.
        plans.append(_new_plan(
            supplier=target_sup, action="GENERATE",
            notes="",
            photos_list=photos,
            cost=round(float(cost_per_video_usd), 4),
        ))

    return plans


_ACTION_ORDER = {"GENERATE": 0, "SKIP": 1}


def to_sheet_rows(plans: list[VideoPlan]) -> tuple[list[str], list[list]]:
    sorted_plans = sorted(plans, key=lambda p: (_ACTION_ORDER.get(p.action, 99), p.sku))
    headers = [
        "SKU", "Supplier", "Parent Product", "Source Photos",
        "Title", "Background", "Audio", "Prompt",
        "Cost USD", "Action", "Notes",
    ]
    rows: list[list] = []
    for p in sorted_plans:
        rows.append([
            p.sku, p.supplier, p.parent_product, ", ".join(p.source_photos),
            p.title, p.background, p.audio, p.prompt_override,
            f"{p.cost_usd:.4f}", p.action, p.notes,
        ])
    return headers, rows


def summarise(plans: list[VideoPlan]) -> dict[str, float]:
    return {
        "total":          len(plans),
        "to_generate":    sum(1 for p in plans if p.action == "GENERATE"),
        "skipped":        sum(1 for p in plans if p.action == "SKIP"),
        "total_cost_usd": round(sum(p.cost_usd for p in plans if p.action == "GENERATE"), 4),
    }


# ---------------------------------------------------------------------------
# Prompt + frame selection
# ---------------------------------------------------------------------------

def build_prompt(
    parent_product: str,
    default_prompt: str,
    extra_prompt: str,
    background: str = "",
    audio: str = "",
    reference_count: int = 0,
) -> str:
    """Compose the final prompt.

    Default flow: subject line + the configured motion/style block + any
    user extras.

    When `background` is supplied: builds a "place the product in <scene>"
    prompt that overrides the white-studio framing and asks the model to
    integrate the product photorealistically. Used by --add-background +
    reference-images mode (Seedance 2.0+).

    When `audio` is supplied: appends an instruction for Seedance 2.0+'s
    audio synthesis to generate background music of the requested style and
    explicitly suppress dialogue / voiceover."""
    pp = parent_product.strip() or "product"
    parts: list[str] = []

    if background.strip():
        bg = background.strip()
        parts.append(f"A short cinematic shot of a {pp} placed in {bg}.")
        if reference_count > 0:
            refs = ", ".join(f"[Image{i+1}]" for i in range(reference_count))
            parts.append(
                f"The {pp} must exactly match the shape, materials, proportions, and "
                f"design shown in the reference images ({refs}). Treat those references "
                "as the ground truth for what the product looks like."
            )
        parts.append(
            f"Replace the white studio background with {bg}. Photorealistic integration: "
            "appropriate lighting, soft natural shadows, contact shadows on the floor, "
            "and reflections / occlusion that match the environment."
        )
        parts.append(
            f"The camera makes a slow, smooth push-in toward the {pp} with a very "
            "gentle frame shift — subtle, elegant motion only. The product itself is "
            f"completely static: the {pp} does not rotate, tilt, lift, deform, or "
            "change pose; only the camera moves. Every part of the product — its "
            "geometry, topology, part count, silhouette, and details — is identical "
            "in every frame; nothing is added, duplicated, split, or hallucinated. "
            "The scene around the product stays completely constant: same composition, "
            "same lighting, same shadows, same surfaces."
        )
        parts.append("Photorealistic. No text, watermarks, logos, captions, or overlays.")
    else:
        parts.append(f"A short promotional video of a {pp}.")
        if default_prompt.strip():
            parts.append(default_prompt.strip())

    if extra_prompt and extra_prompt.strip():
        parts.append(extra_prompt.strip())

    if audio.strip():
        parts.append(
            f"Audio: {audio.strip()} background music throughout, instrumental only. "
            "No dialogue, no voiceover, no spoken words, no sound effects."
        )
    return " ".join(parts)


def _is_seedance_v2(model: str) -> bool:
    """Seedance 2.0+ supports reference_images / generate_audio; v1.x has
    camera_fixed instead and no audio / reference inputs."""
    return model.strip().lower().startswith("bytedance/seedance-2")


def _classify_image_type(local_path: str) -> str:
    """Return 'product' (mostly white background) or 'macro' (close-up texture)."""
    try:
        with _PILImage.open(local_path) as img:
            arr = np.array(img.convert("RGB"))
    except Exception:
        return "product"
    near_white = np.all(arr >= _NEAR_WHITE_THRESHOLD, axis=-1).mean()
    return "macro" if float(near_white) < _PRODUCT_WHITE_PCT else "product"


def _download_and_classify(
    photos: list[dict],
    download_to_dir: _Path,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Download every photo and partition by classification. Returns
    (all_downloaded, whitebg_only). Order preserved."""
    downloaded: list[tuple[str, str]] = []
    for f in photos:
        local = str(download_to_dir / f["name"])
        try:
            drive.download_file(f["id"], local)
            downloaded.append((f["name"], local))
        except Exception:
            continue
    whitebg = [(n, p) for (n, p) in downloaded if _classify_image_type(p) == "product"]
    return downloaded, whitebg


def _pick_keyframes(
    pool: list[tuple[str, str]],
    *,
    use_two_frames: bool = True,
) -> list[tuple[str, str]]:
    """Pick 1-2 keyframes for image+last_frame_image mode from an ordered pool.
    Caller is responsible for choosing the pool (e.g. user-curated list vs.
    auto white-bg)."""
    if not pool:
        return []
    if not use_two_frames or len(pool) == 1:
        return [pool[0]]
    # First and last by pool order — for a user-curated list this is exactly
    # their stated start + end; for the auto-pick path it's the alphabetical
    # extremes of the white-bg shots.
    return [pool[0], pool[-1]]


def _pick_reference_images(
    pool: list[tuple[str, str]],
    *,
    max_refs: int,
) -> list[tuple[str, str]]:
    """Pick up to `max_refs` references for reference_images mode (Seedance
    2.0+) from an ordered pool."""
    return pool[: max(1, int(max_refs))]


def _select_pool(
    downloaded: list[tuple[str, str]],
    whitebg: list[tuple[str, str]],
    *,
    user_curated: bool,
) -> list[tuple[str, str]]:
    """The pool of (name, path) tuples to draw keyframes / references from.

    When the user curated the Source Photos cell, treat their selection as
    authoritative — use the full downloaded list (already filtered + ordered
    to match their cell). Otherwise prefer white-bg auto-classified shots,
    falling back to all downloaded if nothing classifies."""
    if user_curated:
        return downloaded
    return whitebg if whitebg else downloaded


_ASPECT_RATIO_FLOAT = {
    "16:9": 16/9, "4:3": 4/3, "1:1": 1.0, "3:4": 3/4,
    "9:16": 9/16, "21:9": 21/9, "9:21": 9/21,
}


def _letterbox_to_aspect(local_path: str, target_aspect: str, out_path: str) -> str:
    """Pad an image with white so its width:height matches `target_aspect`
    (e.g. '9:16'). Returns the path to use — `out_path` on success, the
    original `local_path` on any failure or when the image already matches.

    Seedance ignores the `aspect_ratio` input field whenever an `image` is
    supplied, so we have to letterbox the keyframe ourselves to actually get
    a vertical / square / landscape output."""
    target = _ASPECT_RATIO_FLOAT.get(target_aspect)
    if target is None:
        return local_path
    try:
        with _PILImage.open(local_path) as img:
            img = img.convert("RGB")
            w, h = img.size
            if not w or not h:
                return local_path
            cur = w / h
            if abs(cur - target) < 0.01:
                return local_path
            if cur > target:
                # Source too wide → pad top + bottom to make it taller.
                new_h = int(round(w / target))
                canvas = _PILImage.new("RGB", (w, new_h), (255, 255, 255))
                canvas.paste(img, (0, (new_h - h) // 2))
            else:
                # Source too tall → pad left + right to make it wider.
                new_w = int(round(h * target))
                canvas = _PILImage.new("RGB", (new_w, h), (255, 255, 255))
                canvas.paste(img, ((new_w - w) // 2, 0))
            canvas.save(out_path, "JPEG", quality=95)
        return out_path
    except Exception:
        return local_path


# ---------------------------------------------------------------------------
# Title + logo overlays (via bundled ffmpeg from imageio-ffmpeg)
# ---------------------------------------------------------------------------

def _ffmpeg_exe() -> str:
    """Resolve the ffmpeg binary path. Falls back to the imageio-ffmpeg-
    bundled binary when ffmpeg isn't on PATH — keeps card rendering working
    without `brew install ffmpeg`."""
    import imageio_ffmpeg
    return imageio_ffmpeg.get_ffmpeg_exe()


def _probe_video_dimensions(path: str) -> tuple[int, int]:
    """Parse `width x height` out of ffmpeg's stderr for a video file. ffmpeg
    always prints the stream metadata at startup, so we run it with no output
    and scrape stderr. Cheaper than depending on ffprobe (not bundled)."""
    out = subprocess.run(
        [_ffmpeg_exe(), "-i", path],
        capture_output=True, text=True,
    )
    m = re.search(r"Stream #\d+:\d+.*?Video.*?,\s*(\d{2,5})x(\d{2,5})", out.stderr, re.S)
    if not m:
        raise RuntimeError(f"could not probe dimensions of {path}: ffmpeg gave no Stream line")
    return int(m.group(1)), int(m.group(2))


def _probe_video_duration(path: str) -> float:
    """Parse the `Duration: HH:MM:SS.ff` line out of ffmpeg's stderr."""
    out = subprocess.run(
        [_ffmpeg_exe(), "-i", path],
        capture_output=True, text=True,
    )
    m = re.search(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)", out.stderr)
    if not m:
        raise RuntimeError(f"could not probe duration of {path}")
    h, mn, s = int(m.group(1)), int(m.group(2)), float(m.group(3))
    return h * 3600 + mn * 60 + s


def _has_audio_stream(path: str) -> bool:
    """ffmpeg's stderr prints one `Stream #x:y(...): Audio: ...` line per audio track."""
    out = subprocess.run(
        [_ffmpeg_exe(), "-i", path],
        capture_output=True, text=True,
    )
    return bool(re.search(r"Stream #\d+:\d+.*?:\s*Audio:", out.stderr))


def _hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def _find_font(configured: str) -> str | None:
    if configured and os.path.isfile(configured):
        return configured
    for p in _SYSTEM_FONT_CANDIDATES:
        if os.path.isfile(p):
            return p
    return None


def _fit_font(
    text: str, font_path: str | None, max_w: int, max_h: int,
) -> _PILFont.FreeTypeFont:
    """Binary-search the largest font size whose bbox fits inside the budget."""
    if not font_path:
        return _PILFont.load_default()
    lo, hi = 16, min(max_w, max_h)
    best: _PILFont.FreeTypeFont = _PILFont.truetype(font_path, lo)
    tmp = _PILImage.new("RGB", (1, 1))
    draw = _PILDraw.Draw(tmp)
    while lo <= hi:
        mid = (lo + hi) // 2
        try:
            f = _PILFont.truetype(font_path, mid)
        except OSError:
            break
        bbox = draw.textbbox((0, 0), text, font=f)
        if (bbox[2] - bbox[0]) <= max_w and (bbox[3] - bbox[1]) <= max_h:
            best = f
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def _render_title_overlay_png(
    text: str, width: int, height: int, *,
    text_color: str, font_path: str | None,
    out_path: str,
) -> None:
    """Render the title TEXT only (transparent background) at video
    dimensions, anchored bottom-center. A subtle white outline keeps the
    text legible on varied first-frame backgrounds."""
    canvas = _PILImage.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = _PILDraw.Draw(canvas)
    # Text budget: 84% of width, 14% of height. Roomy for long names.
    max_w = int(width * 0.84)
    max_h = int(height * 0.14)
    font = _fit_font(text, font_path, max_w, max_h)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    x = (width - tw) // 2 - bbox[0]
    # Baseline 88% down from the top — keeps clear of the bottom safe-area
    # without crowding the product.
    y = int(height * 0.88) - th - bbox[1]
    font_size = getattr(font, "size", 32)
    stroke_w = max(2, int(font_size * 0.04))
    draw.text(
        (x, y), text,
        fill=_hex_to_rgb(text_color),
        font=font,
        stroke_width=stroke_w,
        stroke_fill=(255, 255, 255, 200),
    )
    canvas.save(out_path, "PNG")


def _render_logo_overlay_png(
    logo_path: str, width: int, height: int, *, out_path: str,
) -> None:
    """Render the LOGO only (transparent background) at video dimensions,
    centered, fit to ~50% of the shorter edge — matches the previous
    end-card framing the user said worked well."""
    canvas = _PILImage.new("RGBA", (width, height), (0, 0, 0, 0))
    with _PILImage.open(logo_path) as logo_raw:
        logo = logo_raw.convert("RGBA")
        budget = int(min(width, height) * 0.5)
        logo.thumbnail((budget, budget), _PILImage.LANCZOS)
        px = (width - logo.width) // 2
        py = (height - logo.height) // 2
        canvas.paste(logo, (px, py), logo)
    canvas.save(out_path, "PNG")


def _extract_frame(video_path: str, *, when: str, out_path: str) -> None:
    """Pull one frame out of a video as a high-quality PNG.

    `when` is "first" (frame at t=0) or "last" (a frame just before EOF).
    For "last" we probe the duration and use OUTPUT-seek (`-ss` after `-i`)
    instead of `-sseof`: tested with the bundled ffmpeg on real Seedance
    MP4s, `-sseof` silently produces no frame on some clips while ffmpeg
    still exits 0. Output-seek reads from the start (negligible for a 6s
    clip) and is frame-accurate."""
    cmd = [_ffmpeg_exe(), "-y", "-loglevel", "error", "-i", video_path]
    if when == "last":
        duration = _probe_video_duration(video_path)
        # Seek 100ms before EOF so we land on a real frame even if the
        # very last sample is short.
        seek_to = max(0.0, duration - 0.1)
        cmd += ["-ss", f"{seek_to:.3f}"]
    cmd += ["-frames:v", "1", "-q:v", "1", out_path]
    result = subprocess.run(cmd, capture_output=True)
    if (
        result.returncode != 0
        or not os.path.isfile(out_path)
        or os.path.getsize(out_path) == 0
    ):
        stderr = (result.stderr or b"").decode("utf-8", errors="replace")[:400]
        raise RuntimeError(
            f"frame extraction failed ({when}) for {video_path}: "
            f"returncode={result.returncode}, stderr={stderr!r}"
        )


def _build_bookend_clip(
    *,
    still_frame_path: str,
    overlay_png: str,
    fade_kind: str,           # "in" or "out"
    duration: float,
    with_audio: bool,
    out_path: str,
) -> None:
    """Build a `duration`-second mp4 of a still frame with a transparent
    overlay composited on top, animated with an alpha fade.

    `fade_kind="out"` → overlay starts fully visible, fades to invisible
    (used for the title intro at the start of the final video).
    `fade_kind="in"`  → overlay starts invisible, fades to fully visible
    (used for the logo outro at the end of the final video).

    A silent stereo AAC track is added when `with_audio=True` so this clip
    can be concat'd cleanly with audio-bearing seedance output."""
    cmd: list[str] = [
        _ffmpeg_exe(), "-y", "-loglevel", "error",
        "-loop", "1", "-t", f"{duration:.3f}", "-i", still_frame_path,
        "-loop", "1", "-t", f"{duration:.3f}", "-i", overlay_png,
    ]
    if with_audio:
        cmd += ["-f", "lavfi", "-t", f"{duration:.3f}", "-i",
                "anullsrc=channel_layout=stereo:sample_rate=44100"]
    cmd += [
        "-filter_complex",
        f"[1:v]format=rgba,fade=t={fade_kind}:st=0:d={duration:.3f}:alpha=1[fx];"
        f"[0:v][fx]overlay=x=0:y=0:format=auto[outv]",
        "-map", "[outv]",
    ]
    if with_audio:
        cmd += ["-map", "2:a", "-c:a", "aac"]
    cmd += [
        "-t", f"{duration:.3f}",
        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-r", "30",
        out_path,
    ]
    subprocess.run(cmd, check=True, capture_output=True)


def _concat_clips(
    clip_paths: list[str], output_path: str, *, with_audio: bool,
) -> None:
    """Concatenate clips with ffmpeg's concat filter (re-encodes; safe
    across container / codec differences). Emits H.264 + AAC when audio."""
    cmd: list[str] = [_ffmpeg_exe(), "-y", "-loglevel", "error"]
    for p in clip_paths:
        cmd += ["-i", p]
    n = len(clip_paths)
    if with_audio:
        streams = "".join(f"[{i}:v:0][{i}:a:0]" for i in range(n))
        filter_str = f"{streams}concat=n={n}:v=1:a=1[outv][outa]"
        cmd += ["-filter_complex", filter_str, "-map", "[outv]", "-map", "[outa]"]
        cmd += ["-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac"]
    else:
        streams = "".join(f"[{i}:v:0]" for i in range(n))
        filter_str = f"{streams}concat=n={n}:v=1:a=0[outv]"
        cmd += ["-filter_complex", filter_str, "-map", "[outv]"]
        cmd += ["-c:v", "libx264", "-pix_fmt", "yuv420p"]
    cmd += [output_path]
    subprocess.run(cmd, check=True, capture_output=True)


def _apply_bookend_overlays(
    seedance_path: str,
    *,
    title_png: str | None,
    title_seconds: float,
    logo_png: str | None,
    logo_seconds: float,
    tmp_dir: _Path,
    output_path: str,
) -> None:
    """Build the final mp4 = [title intro] + seedance + [logo outro].

    The intro is the seedance's first frame held for `title_seconds` with
    the title overlay fading OUT (so the title reads at t=0 and clears
    naturally before the product motion starts).

    The outro is the seedance's last frame held for `logo_seconds` with
    the logo overlay fading IN (logo arrives smoothly on the same pose
    the product video ends on, then sits at full opacity until the cut).

    Either bookend can be skipped by passing the corresponding *_png as
    None. Raises if both are None — caller should just upload seedance
    directly in that case."""
    if not title_png and not logo_png:
        raise RuntimeError("_apply_bookend_overlays called with no overlays")

    with_audio = _has_audio_stream(seedance_path)
    clips: list[str] = []

    if title_png:
        first_frame = str(tmp_dir / "_first_frame.png")
        _extract_frame(seedance_path, when="first", out_path=first_frame)
        title_intro = str(tmp_dir / "_title_intro.mp4")
        _build_bookend_clip(
            still_frame_path=first_frame,
            overlay_png=title_png,
            fade_kind="out",
            duration=title_seconds,
            with_audio=with_audio,
            out_path=title_intro,
        )
        clips.append(title_intro)

    clips.append(seedance_path)

    if logo_png:
        last_frame = str(tmp_dir / "_last_frame.png")
        _extract_frame(seedance_path, when="last", out_path=last_frame)
        logo_outro = str(tmp_dir / "_logo_outro.mp4")
        _build_bookend_clip(
            still_frame_path=last_frame,
            overlay_png=logo_png,
            fade_kind="in",
            duration=logo_seconds,
            with_audio=with_audio,
            out_path=logo_outro,
        )
        clips.append(logo_outro)

    _concat_clips(clips, output_path, with_audio=with_audio)


# ---------------------------------------------------------------------------
# Execute
# ---------------------------------------------------------------------------

class GenProgress(NamedTuple):
    sku: str
    output_name: str
    skipped: bool
    error: str = ""


def _replicate_client():
    """Lazy import + construct so the module loads without REPLICATE_API_TOKEN set."""
    import replicate
    token = os.environ.get("REPLICATE_API_TOKEN")
    if not token:
        raise RuntimeError(
            "REPLICATE_API_TOKEN is not set. Add it to .env or your environment "
            "(get a token at https://replicate.com/account/api-tokens)."
        )
    return replicate.Client(api_token=token)


def _run_replicate(
    client,
    *,
    model: str,
    seed_photo_path: str | None,
    last_frame_path: str | None,
    reference_paths: list[str] | None,
    prompt: str,
    duration: int,
    resolution: str,
    aspect_ratio: str,
    camera_fixed: bool,
    generate_audio: bool,
) -> bytes:
    """Call Replicate image-to-video and return the MP4 bytes.

    Two mutually-exclusive input modes:
      - **Keyframe** (`seed_photo_path` + optional `last_frame_path`):
        Seedance interpolates motion between defined poses. Use for clean
        turntable rotation on the original white studio background.
      - **Reference** (`reference_paths`, Seedance 2.0+ only): up to 9
        photos as identity / style guidance. The model invents a fresh
        composition consistent with the references — used by --add-background
        so the product can be placed into a new scene.

    Model-specific knobs:
      - Seedance 1.x → `camera_fixed`
      - Seedance 2.0+ → `generate_audio`"""
    is_v2 = _is_seedance_v2(model)
    open_files: list = []
    try:
        payload: dict = {
            "prompt": prompt,
            "duration": duration,
            "resolution": resolution,
            "aspect_ratio": aspect_ratio,
        }
        if reference_paths:
            if not is_v2:
                raise RuntimeError(
                    f"reference_images mode requires Seedance 2.0+; got {model!r}"
                )
            handles = []
            for p in reference_paths:
                fh = open(p, "rb")
                open_files.append(fh)
                handles.append(fh)
            payload["reference_images"] = handles
        elif seed_photo_path:
            seed_fh = open(seed_photo_path, "rb")
            open_files.append(seed_fh)
            payload["image"] = seed_fh
            if last_frame_path:
                tail_fh = open(last_frame_path, "rb")
                open_files.append(tail_fh)
                payload["last_frame_image"] = tail_fh
        else:
            raise RuntimeError("no input photos supplied to _run_replicate")

        if is_v2:
            payload["generate_audio"] = bool(generate_audio)
        else:
            payload["camera_fixed"] = bool(camera_fixed)

        output = client.run(model, input=payload)
    finally:
        for fh in open_files:
            try:
                fh.close()
            except Exception:
                pass

    # Replicate's `run` returns either a FileOutput, a URL string, an iterator
    # of those, or a list. Normalize to a single bytes blob.
    if hasattr(output, "read"):
        return output.read()
    if isinstance(output, (list, tuple)) and output:
        first = output[0]
        if hasattr(first, "read"):
            return first.read()
        if isinstance(first, str):
            return _download_url(first)
    if isinstance(output, str):
        return _download_url(output)
    # Iterator fallback.
    try:
        first = next(iter(output))
        if hasattr(first, "read"):
            return first.read()
        if isinstance(first, str):
            return _download_url(first)
    except (StopIteration, TypeError):
        pass
    raise RuntimeError(f"Replicate returned unexpected output type: {type(output).__name__}")


def _download_url(url: str) -> bytes:
    import httpx
    with httpx.Client(timeout=120.0) as c:
        r = c.get(url)
        r.raise_for_status()
        return r.content


def execute(
    *,
    plans: list[VideoPlan],
    category_folder_id: str,
    structure: str,
    photos_subdir: str,
    videos_subdir: str,
    models: list[str],
    duration: int,
    resolution: str,
    aspect_ratio: str,
    default_prompt: str,
    extra_prompt: str = "",
    camera_fixed: bool = True,
    use_last_frame: bool = True,
    max_reference_images: int = 9,
    add_title_card: bool = False,
    logo_local_path: str = "",
    title_card_seconds: float = 1.5,
    logo_card_seconds: float = 1.5,
    title_font_path: str = "",
    card_text_color: str = "#111111",
    max_retries: int = 1,
    debug: bool = False,
    logger=print,
) -> Generator[GenProgress, None, None]:
    """Generate one MP4 per GENERATE plan and upload to <sku>/<videos_subdir>/<sku>.mp4."""
    sku_index = _build_sku_index(category_folder_id, structure)

    if not models:
        yield GenProgress(sku="", output_name="", skipped=False,
                          error="no models configured (set generate.video.models in pipeline.config.toml)")
        return

    client = None  # lazy

    for plan in plans:
        if plan.action != "GENERATE":
            continue
        if plan.sku not in sku_index:
            yield GenProgress(sku=plan.sku, output_name="", skipped=True,
                              error=f"target SKU folder not found: {plan.sku}")
            continue

        _, target_sku_id = sku_index[plan.sku]

        all_photos = _list_photos(target_sku_id, photos_subdir)
        if not all_photos:
            yield GenProgress(sku=plan.sku, output_name="", skipped=True,
                              error="sibling has no photos at execute time")
            continue

        out_name = f"{plan.sku}.mp4"

        # If the user curated `Source Photos` in the report, treat the cell
        # as authoritative — filter + reorder Drive's listing to match. We
        # DON'T auto-classify those (no white-bg filter); the user's
        # selection is final. Falls back to Drive's full listing when the
        # cell is left untouched.
        user_curated = bool(plan.source_photos) and (
            [p["name"] for p in all_photos] != list(plan.source_photos)
        )
        if user_curated:
            name_to_meta = {p["name"]: p for p in all_photos}
            photos = [name_to_meta[n] for n in plan.source_photos if n in name_to_meta]
            missing = [n for n in plan.source_photos if n not in name_to_meta]
            if missing:
                logger(
                    f"  [warn] {plan.sku}: ignoring unknown Source Photos entries: "
                    f"{', '.join(missing)}"
                )
            if not photos:
                yield GenProgress(
                    sku=plan.sku, output_name=out_name, skipped=False,
                    error=(
                        f"none of the curated Source Photos exist in Drive: "
                        f"{plan.source_photos}"
                    ),
                )
                continue
        else:
            photos = all_photos

        try:
            videos_folder_id = _resolve_or_create(target_sku_id, videos_subdir)
        except Exception as exc:
            yield GenProgress(sku=plan.sku, output_name=out_name, skipped=False,
                              error=f"could not create videos folder: {exc}")
            continue

        existing = {f["name"] for f in drive.list_files(videos_folder_id) if not f["name"].startswith(".")}
        if existing and not debug:
            yield GenProgress(sku=plan.sku, output_name=out_name, skipped=True,
                              error="videos/ is already populated; pass --debug to overwrite")
            continue

        with tempfile.TemporaryDirectory(prefix=f"genvideo_{plan.sku}_") as tmp:
            tmp_path = _Path(tmp)
            downloaded, whitebg = _download_and_classify(photos, tmp_path)
            if not downloaded:
                yield GenProgress(sku=plan.sku, output_name=out_name, skipped=False,
                                  error="could not download any source photo")
                continue

            # Mode dispatch (per-SKU, from the plan's Background cell):
            #   - reference: non-empty Background → Seedance 2.0+ only. Up to 9
            #     photos for identity/style; image+last_frame_image are not used.
            #   - keyframe: empty Background → first + optional last shot, letterboxed.
            sku_background = (plan.background or "").strip()
            sku_audio = (plan.audio or "").strip()
            using_reference = bool(sku_background)
            ref_local_paths: list[str] = []
            seed_local: str | None = None
            tail_local: str | None = None
            seed_name = tail_name = ""

            pool = _select_pool(downloaded, whitebg, user_curated=user_curated)
            if using_reference:
                refs = _pick_reference_images(pool, max_refs=max_reference_images)
                ref_local_paths = [p for (_n, p) in refs]
                ref_names = [n for (n, _p) in refs]
            else:
                keyframes = _pick_keyframes(pool, use_two_frames=use_last_frame)
                seed_name, seed_raw = keyframes[0]
                seed_local = _letterbox_to_aspect(
                    seed_raw, aspect_ratio, str(tmp_path / f"_lb_first_{seed_name}.jpg"),
                )
                if len(keyframes) >= 2:
                    tail_name, tail_raw = keyframes[1]
                    tail_local = _letterbox_to_aspect(
                        tail_raw, aspect_ratio, str(tmp_path / f"_lb_last_{tail_name}.jpg"),
                    )

            logger(f"\n[{plan.sku}] → {out_name}")
            logger(f"  selection: {'user-curated from sheet' if user_curated else 'auto (white-bg classifier)'}")
            if using_reference:
                logger(f"  mode: reference_images ({len(ref_local_paths)} refs)")
                logger(f"  refs: {', '.join(ref_names)}")
                logger(f"  background: {sku_background}")
            else:
                logger("  mode: keyframe")
                logger(f"  first frame: {seed_name}")
                if tail_local:
                    logger(f"  last frame:  {tail_name}")
                else:
                    logger("  last frame:  (none — single-frame mode)")
            # Per-SKU prompt override (from the report's Prompt column)
            # replaces the configured default motion/style block. Useful for
            # products that need more specific guidance to avoid artifacts
            # (e.g. "extra leg" on a chair) — the user writes a tighter
            # prompt in the sheet for just that row.
            effective_default_prompt = (
                plan.prompt_override.strip() or default_prompt
            )
            prompt = build_prompt(
                plan.parent_product, effective_default_prompt, extra_prompt,
                background=sku_background, audio=sku_audio,
                reference_count=len(ref_local_paths),
            )
            logger(f"  prompt: {prompt}")
            logger(
                f"  duration={duration}s  resolution={resolution}  "
                f"aspect={aspect_ratio}  audio={'on' if sku_audio else 'off'}"
            )

            attempt = 0
            last_error = ""
            video_bytes: bytes | None = None
            used_model = ""
            while True:
                attempt += 1
                model = models[(attempt - 1) % len(models)]
                used_model = model
                shot_start = time.time()
                is_v2 = _is_seedance_v2(model)
                if using_reference and not is_v2:
                    yield GenProgress(
                        sku=plan.sku, output_name=out_name, skipped=False,
                        error=(
                            f"Background={sku_background!r} requires a Seedance 2.0+ "
                            f"model (got {model!r}). Clear the Background cell for this "
                            "SKU or switch models."
                        ),
                    )
                    break
                if sku_audio and not is_v2:
                    yield GenProgress(
                        sku=plan.sku, output_name=out_name, skipped=False,
                        error=(
                            f"Audio={sku_audio!r} requires a Seedance 2.0+ model "
                            f"(got {model!r}); 1.x has no audio output. Clear the "
                            "Audio cell for this SKU or switch models."
                        ),
                    )
                    break
                try:
                    if client is None:
                        client = _replicate_client()
                    logger(f"  attempt {attempt}: model={model}")
                    video_bytes = _run_replicate(
                        client,
                        model=model,
                        seed_photo_path=seed_local,
                        last_frame_path=tail_local,
                        reference_paths=ref_local_paths or None,
                        prompt=prompt,
                        duration=duration,
                        resolution=resolution,
                        aspect_ratio=aspect_ratio,
                        camera_fixed=camera_fixed,
                        generate_audio=bool(sku_audio),
                    )
                    elapsed = time.time() - shot_start
                    logger(f"  ✓ generated in {elapsed:.1f}s ({len(video_bytes)/1_000_000:.1f} MB)")
                    break
                except Exception as exc:
                    last_error = str(exc)
                    elapsed = time.time() - shot_start
                    logger(f"  ✗ attempt {attempt} failed in {elapsed:.1f}s: {exc}")
                    if attempt > max_retries:
                        yield GenProgress(
                            sku=plan.sku, output_name=out_name, skipped=False,
                            error=f"all {attempt} attempts failed; last error: {last_error}",
                        )
                        break

            if video_bytes is None:
                continue

            seedance_path = str(tmp_path / f"_seedance_{out_name}")
            with open(seedance_path, "wb") as fh:
                fh.write(video_bytes)

            # Title / logo bookend cards. Either, both, or neither — the
            # final upload path is whatever came out the other end of the
            # pipeline.
            sku_title = (plan.title or "").strip() if add_title_card else ""
            apply_title = bool(sku_title)
            apply_logo = bool(logo_local_path) and os.path.isfile(logo_local_path)

            local_mp4 = str(tmp_path / out_name)
            if apply_title or apply_logo:
                try:
                    # Render overlay PNGs at the actual video dimensions so
                    # the alpha composite is pixel-aligned.
                    width, height = _probe_video_dimensions(seedance_path)
                    title_png_path: str | None = None
                    if apply_title:
                        title_png_path = str(tmp_path / "_title_overlay.png")
                        _render_title_overlay_png(
                            sku_title, width, height,
                            text_color=card_text_color,
                            font_path=_find_font(title_font_path),
                            out_path=title_png_path,
                        )
                    logo_png_path: str | None = None
                    if apply_logo:
                        logo_png_path = str(tmp_path / "_logo_overlay.png")
                        _render_logo_overlay_png(
                            logo_local_path, width, height,
                            out_path=logo_png_path,
                        )
                    _apply_bookend_overlays(
                        seedance_path,
                        title_png=title_png_path,
                        title_seconds=title_card_seconds,
                        logo_png=logo_png_path,
                        logo_seconds=logo_card_seconds,
                        tmp_dir=tmp_path,
                        output_path=local_mp4,
                    )
                    parts = []
                    if apply_title:
                        parts.append(
                            f"title intro {title_card_seconds:.1f}s (first frame + text, fade-out)"
                        )
                    if apply_logo:
                        parts.append(
                            f"logo outro {logo_card_seconds:.1f}s (last frame + logo, fade-in)"
                        )
                    logger(f"  ✓ bookend overlays added ({', '.join(parts)})")
                except subprocess.CalledProcessError as exc:
                    stderr = (exc.stderr or b"").decode("utf-8", errors="replace")[:300]
                    logger(f"  ✗ ffmpeg failed: {stderr}")
                    yield GenProgress(
                        sku=plan.sku, output_name=out_name, skipped=False,
                        error=f"ffmpeg failed while building bookend overlays: {stderr}",
                    )
                    continue
                except Exception as exc:
                    logger(f"  ✗ bookend render failed: {exc}")
                    yield GenProgress(
                        sku=plan.sku, output_name=out_name, skipped=False,
                        error=f"bookend render failed: {exc}",
                    )
                    continue
            else:
                # No overlays requested — promote the raw Seedance output
                # to the final upload path.
                os.rename(seedance_path, local_mp4)

            try:
                if debug:
                    for f in drive.list_files(videos_folder_id):
                        if f["name"] == out_name:
                            drive.trash_item(f["id"])
                            logger(f"  [debug] trashed existing {out_name}")
                            break
                drive.upload_file(local_mp4, videos_folder_id, out_name, "video/mp4")
                logger(f"  ✓ uploaded → {out_name}  (via {used_model})")
            except Exception as exc:
                logger(f"  ✗ upload failed: {exc}")
                yield GenProgress(sku=plan.sku, output_name=out_name, skipped=False,
                                  error=f"upload failed: {exc}")
                continue

            # ----- Debug artifacts -----
            # When --debug is on, upload every intermediate that ffmpeg + the
            # render stage produced so the user can A/B against the final.
            # Missing files (e.g. logo overlay when --add-logo-card is off)
            # are silently skipped. Prior debug files of the same name are
            # trashed so re-runs stay clean.
            if debug:
                try:
                    debug_folder_id = drive.find_or_create_folder("_debug", videos_folder_id)
                    artifacts = [
                        # Raw Seedance output — the most useful one: lets you
                        # see what came back from the API before ffmpeg touched it.
                        (str(tmp_path / f"_seedance_{out_name}"),
                         f"{plan.sku}_seedance.mp4", "video/mp4"),
                        # Overlay PNGs (transparent canvases).
                        (str(tmp_path / "_title_overlay.png"),
                         "_title.png", "image/png"),
                        (str(tmp_path / "_logo_overlay.png"),
                         "_logo.png", "image/png"),
                        # Extracted bookend stills.
                        (str(tmp_path / "_first_frame.png"),
                         "_first_frame.png", "image/png"),
                        (str(tmp_path / "_last_frame.png"),
                         "_last_frame.png", "image/png"),
                        # Composited 1s bookend clips.
                        (str(tmp_path / "_title_intro.mp4"),
                         "_title_intro.mp4", "video/mp4"),
                        (str(tmp_path / "_logo_outro.mp4"),
                         "_logo_outro.mp4", "video/mp4"),
                    ]
                    # Clear prior debug files of names we'd be overwriting.
                    known_names = {name for (_, name, _) in artifacts}
                    for f in drive.list_files(debug_folder_id):
                        if f["name"] in known_names:
                            drive.trash_item(f["id"])
                    uploaded = 0
                    for local, drive_name, mime in artifacts:
                        if os.path.isfile(local) and os.path.getsize(local) > 0:
                            try:
                                drive.upload_file(local, debug_folder_id, drive_name, mime)
                                uploaded += 1
                            except Exception as exc:
                                logger(f"  [debug] upload failed for {drive_name}: {exc}")
                    if uploaded:
                        logger(f"  [debug] {uploaded} artifact(s) → _debug/")
                except Exception as exc:
                    logger(f"  [debug] could not upload debug folder: {exc}")

            yield GenProgress(sku=plan.sku, output_name=out_name, skipped=False)
