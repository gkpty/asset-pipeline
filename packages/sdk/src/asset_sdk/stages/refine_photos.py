"""Non-AI color refinement for product photos.

When an AI generation run produces a set of photos and only one has the
correct fabric color (e.g. 4 out of 5 came out beige when the correct
photo is cream), this stage applies Reinhard LAB color transfer from
the correct reference to the rest — no API calls, no cost, ~50ms per
photo.

Reinhard 2001 (https://www.cs.tau.ac.il/~turkel/imagepapers/ColorTransfer.pdf):
in CIE L*a*b* space, compute the per-channel mean + std of the reference
and target, then re-fit each target pixel to the reference's distribution:

    out_lab = (target_lab - target_mean) * (ref_std / target_std) + ref_mean

The white-background mask is the practical win on top of vanilla Reinhard:
we compute the stats over NON-near-white pixels only (the fabric region),
and we only modify non-near-white pixels in the output. The studio
backdrop stays pure white instead of getting tinted.
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Generator, NamedTuple

import numpy as np
from PIL import Image as _PILImage
from skimage import color as _skcolor

from asset_sdk.adapters import drive


# ---------------------------------------------------------------------------
# Color transfer math
# ---------------------------------------------------------------------------

# Cap the per-channel scale factor (ref_std / tgt_std) to prevent gamut
# blowouts. Without this, a target with low chroma variance against a
# reference with high chroma variance produces scale factors of 5-10×,
# which pushes the b-channel (blue↔yellow) far past CIE L*a*b*'s usable
# range — when converted back to RGB those out-of-gamut pixels clamp to
# saturated blue / yellow stripes. The Reinhard paper itself notes the
# need for some scale damping in practice.
_SCALE_MIN, _SCALE_MAX = 0.5, 2.0

# Below this fraction of pixels in either image's fabric mask, the
# computed statistics aren't reliable enough to trust. Skip the transfer.
_MIN_MASK_FRACTION = 0.01

# Floor on per-channel std to avoid divide-by-near-zero on near-uniform
# targets. In CIE L*a*b* units (L: 0-100, a/b: -128 to +127), 1.0 is
# conservative — typical product-photo fabric regions have std 5-30.
_STD_FLOOR = 1.0

# CIE-LAB distance from pure white (L=100, a=0, b=0) below which the
# reference fabric counts as "near-white" (ivory / cream / off-white).
# Empirical: pure white = 0, ivory ≈ 7-12, cream ≈ 15-20, light beige ≈
# 25-30, navy ≈ 80. We use the cutoff to switch between two output modes:
#   - distance < cutoff (light fabric): apply transform GLOBALLY so the
#     backdrop picks up the same subtle color cast as the reference.
#     Otherwise the highlight side of the fabric stays untransformed and
#     the result reads as whiter than the reference.
#   - distance >= cutoff (dark / colored fabric): apply transform only
#     to non-near-white pixels and restore the bg byte-exact. Without
#     this, transforming a dark fabric's stats onto a white backdrop
#     pixel pushes it noticeably toward the fabric's hue (e.g. navy
#     fabric tints the backdrop blue).
_LIGHT_FABRIC_LAB_DISTANCE = 25.0


def _rgb_to_lab(rgb_arr: np.ndarray) -> np.ndarray:
    """HxWx3 uint8 RGB → HxWx3 float64 CIE L*a*b*.

    Uses scikit-image (mathematically correct via XYZ intermediate).
    PIL's built-in `convert('LAB')` was producing visible blue/yellow
    stripes in the b-channel after the inverse roundtrip on certain
    images — replaced after a real-world regression."""
    return _skcolor.rgb2lab(rgb_arr.astype(np.float64) / 255.0)


def _lab_to_rgb(lab_arr: np.ndarray) -> np.ndarray:
    """HxWx3 float64 CIE L*a*b* → HxWx3 uint8 RGB. Clips out-of-gamut."""
    rgb = _skcolor.lab2rgb(lab_arr)
    return np.clip(rgb * 255.0, 0, 255).astype(np.uint8)


def _fabric_mask(rgb_arr: np.ndarray, white_threshold: int) -> np.ndarray:
    """True where the pixel is NOT near-white — i.e. is the product / fabric
    region rather than the studio backdrop. Threshold is per-channel; all
    three channels must clear the bar for a pixel to count as background."""
    near_white = np.all(rgb_arr >= white_threshold, axis=-1)
    return ~near_white


def color_transfer(
    reference_rgb: np.ndarray,
    target_rgb: np.ndarray,
    *,
    mask_white_bg: bool = True,
    white_threshold: int = 250,
) -> np.ndarray:
    """Apply Reinhard LAB color transfer from `reference_rgb` to `target_rgb`.

    Both arrays are HxWx3 uint8 RGB. Shapes don't have to match — the
    transfer is statistical (computed over the whole image), not pixel-wise.

    Returns a new HxWx3 uint8 RGB array, same shape as `target_rgb`.

    `mask_white_bg=True` (the default) — STATS ONLY:
      Per-channel mean and std are computed from non-near-white pixels in
      each image (the fabric region), keeping the studio backdrop's noise
      out of the stats. The transform itself still applies to every pixel
      including the backdrop — so near-white backdrop pixels pick up the
      reference's subtle color cast (e.g. an ivory reference tints the
      backdrop slightly ivory). This is the right behavior for near-white
      fabrics where the fabric and backdrop merge at the highlight side
      of the histogram; the old "preserve backdrop bit-exact" mode left
      those highlight pixels untransformed and made the result look
      whiter than the reference.

    `mask_white_bg=False`: stats computed over EVERY pixel (treats the
    backdrop as part of the distribution). Use when the photos don't have
    a white studio backdrop.

    If either fabric mask is too small (<1% of pixels — typically an
    all-white photo), returns the target unchanged."""
    ref_lab = _rgb_to_lab(reference_rgb)
    tgt_lab = _rgb_to_lab(target_rgb)

    if mask_white_bg:
        ref_mask = _fabric_mask(reference_rgb, white_threshold)
        tgt_mask = _fabric_mask(target_rgb, white_threshold)
    else:
        ref_mask = np.ones(reference_rgb.shape[:2], dtype=bool)
        tgt_mask = np.ones(target_rgb.shape[:2], dtype=bool)

    # Both fabric regions need enough pixels for stable stats. If either
    # is essentially empty (mostly-white photo) the transfer would be
    # arithmetic noise — bail and return the target unchanged.
    min_pixels = int(target_rgb.shape[0] * target_rgb.shape[1] * _MIN_MASK_FRACTION)
    if ref_mask.sum() < min_pixels or tgt_mask.sum() < min_pixels:
        return target_rgb.copy()

    ref_pixels = ref_lab[ref_mask]
    tgt_pixels = tgt_lab[tgt_mask]
    ref_mean = ref_pixels.mean(axis=0)
    ref_std = ref_pixels.std(axis=0)
    tgt_mean = tgt_pixels.mean(axis=0)
    tgt_std = tgt_pixels.std(axis=0)

    # Floor the target std so a near-uniform channel doesn't divide to
    # infinity, and CAP the per-channel scale so we never amplify a
    # pixel's deviation from its mean by more than 2×. Without this cap,
    # a low-chroma target against a high-chroma reference yields scale
    # factors of 5-10× on the b-channel and pushes pixels far out of
    # CIE-LAB gamut — those clamp on lab2rgb to extreme blue/yellow.
    tgt_std = np.maximum(tgt_std, _STD_FLOOR)
    scale = np.clip(ref_std / tgt_std, _SCALE_MIN, _SCALE_MAX)

    # Adaptive application based on how light the reference fabric is.
    # For light / near-white fabrics (ivory, cream, off-white) we MUST
    # apply globally — otherwise the highlight side of the fabric stays
    # above the threshold, gets skipped, and the result reads as whiter
    # than the reference. For darker fabrics we restrict application to
    # the fabric mask and restore the backdrop byte-exact — otherwise
    # the white backdrop would pick up the fabric's hue (navy fabric →
    # blue backdrop). The LAB-distance cutoff (~25 units) is between
    # "cream" and "light beige".
    ref_distance_from_white = float(
        np.linalg.norm(ref_mean - np.array([100.0, 0.0, 0.0]))
    )
    light_fabric = ref_distance_from_white < _LIGHT_FABRIC_LAB_DISTANCE

    if light_fabric or not mask_white_bg:
        # Light-fabric (or no-mask) mode: CHROMA-ONLY transfer applied
        # globally. We preserve the target's L (luminance) channel and
        # only shift a/b (the color cast). Two reasons this matters for
        # near-white refs:
        #   1) Full LAB transfer on light targets pushes highlight
        #      pixels past L=100, which clips back to pure white and
        #      discards the ivory cast on the lightest part of the
        #      fabric — making the result read whiter than the ref.
        #   2) Preserving target L keeps the AI-generated lighting /
        #      shadows intact; only the COLOR is matched to the ref.
        out_lab = tgt_lab.copy()
        out_lab[..., 1] = (tgt_lab[..., 1] - tgt_mean[1]) * scale[1] + ref_mean[1]
        out_lab[..., 2] = (tgt_lab[..., 2] - tgt_mean[2]) * scale[2] + ref_mean[2]
        return _lab_to_rgb(out_lab)
    else:
        # Dark / colored fabric — full LAB transform on fabric pixels,
        # restore the backdrop bit-exact (avoids RGB→LAB→RGB roundtrip
        # noise on pixels we didn't intend to change, and keeps a navy
        # fabric's color cast from tinting the white backdrop blue).
        out_lab = tgt_lab.copy()
        out_lab[tgt_mask] = (tgt_lab[tgt_mask] - tgt_mean) * scale + ref_mean
        out_rgb = _lab_to_rgb(out_lab)
        out_rgb[~tgt_mask] = target_rgb[~tgt_mask]
        return out_rgb


# ---------------------------------------------------------------------------
# Restore-from-debug flow
# ---------------------------------------------------------------------------

def restore_from_debug(
    *,
    sku_folder_id: str,
    photos_subdir: str,
    logger=print,
) -> int:
    """Roll back a bad refine run by promoting every `_pre_refine_<name>.jpg`
    in `<photos>/_debug/` back to `<name>` in `<photos>/`. The corrupted
    file at the destination is trashed first.

    Returns the number of files restored. Idempotent — re-running after a
    successful restore is a no-op (the _debug folder is empty)."""
    photos_id = _resolve_subfolder(sku_folder_id, photos_subdir)
    if not photos_id:
        logger(f"  ⚠ photos folder not found: {photos_subdir!r}")
        return 0
    debug_id = drive.list_folders(photos_id).get("_debug")
    if not debug_id:
        logger("  ⚠ no _debug folder — nothing to restore")
        return 0

    debug_files = drive.list_files(debug_id)
    pre_refine = [f for f in debug_files if f["name"].startswith("_pre_refine_")]
    if not pre_refine:
        logger("  ⚠ _debug folder has no _pre_refine_* files")
        return 0

    current_by_name = {f["name"]: f for f in drive.list_files(photos_id)}
    restored = 0
    for f in pre_refine:
        original_name = f["name"][len("_pre_refine_"):]  # strip the prefix
        # Trash the corrupted file at the destination, if present.
        if original_name in current_by_name:
            drive.trash_item(current_by_name[original_name]["id"])
        # Move the debug copy out of _debug/ and rename it back.
        drive.move_item(f["id"], photos_id)
        drive.rename_item(f["id"], original_name)
        logger(f"  ✓ restored {original_name}")
        restored += 1
    return restored


# ---------------------------------------------------------------------------
# Drive-orchestrated refine flow
# ---------------------------------------------------------------------------

class RefineProgress(NamedTuple):
    target_name: str
    file_index: int
    file_total: int
    skipped: bool
    error: str = ""


def _resolve_subfolder(parent_id: str, rel_path: str) -> str | None:
    current = parent_id
    for part in rel_path.split("/"):
        children = drive.list_folders(current)
        if part not in children:
            return None
        current = children[part]
    return current


def execute(
    *,
    sku_folder_id: str,
    photos_subdir: str,
    reference_name: str,
    target_names: list[str] | None = None,
    mask_white_bg: bool = True,
    white_threshold: int = 245,
    jpeg_quality: int = 92,
    debug: bool = False,
    logger=print,
) -> Generator[RefineProgress, None, None]:
    """Download the reference + every target from <sku_folder>/<photos_subdir>/,
    apply LAB color transfer, and overwrite the targets on Drive.

    If `debug=True`, the originals are uploaded to
    `<sku_folder>/<photos_subdir>/_debug/_pre_refine_<name>.jpg` BEFORE
    being replaced — recoverable inspection without relying on Drive's
    trash. Without `debug`, originals just go to Drive trash (recoverable
    for ~30 days).

    `target_names=None` (default) means "everything in the folder except
    the reference"."""
    photos_id = _resolve_subfolder(sku_folder_id, photos_subdir)
    if not photos_id:
        yield RefineProgress("", 0, 0, skipped=True,
                             error=f"photos folder not found: {photos_subdir!r}")
        return

    photo_files = [f for f in drive.list_files(photos_id) if not f["name"].startswith(".")]
    by_name = {f["name"]: f for f in photo_files}

    if reference_name not in by_name:
        yield RefineProgress(reference_name, 0, 0, skipped=True,
                             error=f"reference photo {reference_name!r} not found in "
                                   f"photos folder; have: {sorted(by_name)}")
        return

    if target_names:
        # User-specified targets: skip the ref + ignore unknowns (with warning).
        targets: list[str] = []
        for n in target_names:
            n = n.strip()
            if not n:
                continue
            if n == reference_name:
                continue
            if n not in by_name:
                yield RefineProgress(n, 0, 0, skipped=True,
                                     error="target not found in photos folder")
                continue
            targets.append(n)
    else:
        targets = sorted(n for n in by_name if n != reference_name)

    total = len(targets)
    if total == 0:
        logger("  nothing to refine — no targets to apply transfer to")
        return

    with tempfile.TemporaryDirectory(prefix=f"refine_{reference_name}_") as tmp:
        tmp_path = Path(tmp)

        ref_local = tmp_path / reference_name
        drive.download_file(by_name[reference_name]["id"], str(ref_local))
        with _PILImage.open(ref_local) as img:
            reference_rgb = np.array(img.convert("RGB"))
        logger(f"  reference: {reference_name} ({reference_rgb.shape[1]}x{reference_rgb.shape[0]})")

        debug_folder_id: str | None = None

        for i, name in enumerate(targets, 1):
            tgt_local = tmp_path / name
            try:
                drive.download_file(by_name[name]["id"], str(tgt_local))
                with _PILImage.open(tgt_local) as img:
                    target_rgb = np.array(img.convert("RGB"))

                out_rgb = color_transfer(
                    reference_rgb, target_rgb,
                    mask_white_bg=mask_white_bg,
                    white_threshold=white_threshold,
                )
                out_local = tmp_path / f"_out_{name}"
                _PILImage.fromarray(out_rgb).save(
                    out_local, "JPEG", quality=jpeg_quality, optimize=True,
                )

                if debug:
                    if debug_folder_id is None:
                        debug_folder_id = drive.find_or_create_folder("_debug", photos_id)
                    drive.upload_file(
                        str(tgt_local), debug_folder_id,
                        f"_pre_refine_{name}", "image/jpeg",
                    )

                # Replace on Drive: trash the original, upload the refined
                # version under the same name. Drive duplicates by name by
                # default if we skip the trash step, which would confuse
                # any downstream tool that iterates the folder.
                drive.trash_item(by_name[name]["id"])
                drive.upload_file(
                    str(out_local), photos_id, name, "image/jpeg",
                )
                yield RefineProgress(name, i, total, skipped=False)

            except Exception as exc:
                yield RefineProgress(name, i, total, skipped=False,
                                     error=f"refinement failed: {exc}")
