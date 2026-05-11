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

import numpy as np
from PIL import Image as _PILImage

from asset_sdk.adapters import drive

# Same near-white heuristic as generate_photos so we only seed image-to-video
# from product silhouettes (not macro / detail shots).
_PRODUCT_WHITE_PCT = 0.20
_NEAR_WHITE_THRESHOLD = 245


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
    default_background: str = "",
    default_audio: str = "",
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

        if sku not in sku_index:
            plans.append(VideoPlan(
                sku=sku, supplier=sup, parent_product=parent,
                action="SKIP", notes="sku folder not found in Drive",
            ))
            continue

        target_sup, target_sku_id = sku_index[sku]

        if _list_videos(target_sku_id, videos_subdir):
            plans.append(VideoPlan(
                sku=sku, supplier=target_sup, parent_product=parent,
                action="SKIP", notes="target already has a video",
            ))
            continue

        photos = _list_photos(target_sku_id, photos_subdir)
        if not photos:
            plans.append(VideoPlan(
                sku=sku, supplier=target_sup, parent_product=parent,
                action="SKIP", notes="no photos to seed from",
            ))
            continue

        # List ALL photo filenames so the user can edit the cell before
        # --execute to keep only the reference shots they want. The order in
        # the cell is preserved at execute time — for keyframe mode, the
        # first entry becomes the first frame and the last entry becomes
        # the last frame.
        plans.append(VideoPlan(
            sku=sku, supplier=target_sup, parent_product=parent,
            source_photos=[p["name"] for p in photos],
            background=default_background,
            audio=default_audio,
            cost_usd=round(float(cost_per_video_usd), 4),
            action="GENERATE",
        ))

    return plans


_ACTION_ORDER = {"GENERATE": 0, "SKIP": 1}


def to_sheet_rows(plans: list[VideoPlan]) -> tuple[list[str], list[list]]:
    sorted_plans = sorted(plans, key=lambda p: (_ACTION_ORDER.get(p.action, 99), p.sku))
    headers = [
        "SKU", "Supplier", "Parent Product", "Source Photos",
        "Background", "Audio",
        "Cost USD", "Action", "Notes",
    ]
    rows: list[list] = []
    for p in sorted_plans:
        rows.append([
            p.sku, p.supplier, p.parent_product, ", ".join(p.source_photos),
            p.background, p.audio,
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
            f"The {pp} rotates slowly in place on its vertical axis — a gentle turntable "
            "motion. The camera is completely locked and static: no pan, no tilt, no "
            "dolly, no zoom, no orbit. Only the product's rotation moves; the environment "
            "and the camera stay perfectly still."
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
            prompt = build_prompt(
                plan.parent_product, default_prompt, extra_prompt,
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

            local_mp4 = str(tmp_path / out_name)
            with open(local_mp4, "wb") as fh:
                fh.write(video_bytes)

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

            yield GenProgress(sku=plan.sku, output_name=out_name, skipped=False)
