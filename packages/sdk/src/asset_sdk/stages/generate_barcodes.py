"""Generate Code-128 barcode label images from a master sheet and upload them
to each SKU's barcode subfolder on Drive.

Rendering is lifted (and lightly cleaned up) from the standalone
generate_product_barcodes.py script. The output filename per SKU is `<sku>.jpg`
to match what was already produced locally.
"""
from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from io import BytesIO
from typing import Generator, NamedTuple, Optional

from PIL import Image, ImageDraw, ImageFont
from barcode import Code128
from barcode.writer import ImageWriter

from asset_sdk.adapters import drive

# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

CM_TO_IN = 0.3937007874
DEFAULT_DPI = 300
DEFAULT_WIDTH_CM = 10.0
DEFAULT_PAGE_HEIGHT_CM = 35.6  # the original script computed block height as page_h/5

_SYSTEM_FONT_CANDIDATES = [
    "/System/Library/Fonts/Helvetica.ttc",
    "/System/Library/Fonts/Arial.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "C:\\Windows\\Fonts\\arial.ttf",
    "C:\\Windows\\Fonts\\calibri.ttf",
]


def _cm_to_px(cm: float, dpi: int) -> int:
    return int(round(cm * CM_TO_IN * dpi))


def _load_fonts(width_px: int, font_path: Optional[str] = None) -> tuple:
    title_size = max(24, int(width_px * 0.055))
    barcode_text_size = max(16, int(width_px * 0.045))
    candidates = ([font_path] if font_path else []) + _SYSTEM_FONT_CANDIDATES
    for path in candidates:
        if path and os.path.isfile(path):
            try:
                return (
                    ImageFont.truetype(path, size=title_size),
                    ImageFont.truetype(path, size=barcode_text_size),
                )
            except Exception:
                continue
    return ImageFont.load_default(), ImageFont.load_default()


def _render_code128(barcode_text: str, height_px: int) -> Image.Image:
    options = {
        "module_height": height_px,
        "write_text": False,
        "quiet_zone": 1,
        "module_width": 2.0,
    }
    code = Code128(barcode_text, writer=ImageWriter())
    buf = BytesIO()
    code.write(buf, options)
    buf.seek(0)
    return Image.open(buf).convert("RGB")


def _draw_label(
    canvas: Image.Image, draw: ImageDraw.ImageDraw,
    width: int, height: int,
    title: str, barcode_text: str,
    title_font, barcode_text_font,
) -> None:
    # Border.
    draw.rectangle([0, 0, width - 1, height - 1], outline=(0, 0, 0), width=4)

    pad = int(0.03 * height)
    title_y = pad
    tw, th = draw.textbbox((0, 0), title, font=title_font)[2:]
    draw.text(((width - tw) // 2, title_y), title, fill=(0, 0, 0), font=title_font)

    bar_top = title_y + th + int(0.08 * height)
    barcode_text_area = int(0.25 * height)
    bar_bottom = height - barcode_text_area - int(0.03 * height)
    bar_height = max(25, int((bar_bottom - bar_top) * 0.8))
    target_w = int(0.8 * width)
    side_pad = int(0.1 * width)

    bc = _render_code128(barcode_text, bar_height)
    bc = bc.resize((target_w, bar_height), Image.LANCZOS)

    canvas.paste(bc, (side_pad, bar_top + (bar_height - bar_height) // 2))

    bt_y = bar_top + bar_height + int(0.02 * height)
    bt_w, bt_h = draw.textbbox((0, 0), barcode_text, font=barcode_text_font)[2:]
    draw.text(((width - bt_w) // 2, bt_y), barcode_text, fill=(0, 0, 0), font=barcode_text_font)


def render_barcode_image(
    sku: str,
    barcode_text: str,
    out_path: str,
    *,
    dpi: int = DEFAULT_DPI,
    width_cm: float = DEFAULT_WIDTH_CM,
    page_height_cm: float = DEFAULT_PAGE_HEIGHT_CM,
    font_path: Optional[str] = None,
) -> None:
    """Render a single SKU label and write it as JPG at out_path."""
    width_px = _cm_to_px(width_cm, dpi)
    page_height_px = _cm_to_px(page_height_cm, dpi)
    block_h = page_height_px // 5  # one block, same proportions as the original script

    title_font, barcode_text_font = _load_fonts(width_px, font_path)
    img = Image.new("RGB", (width_px, block_h), color="white")
    draw = ImageDraw.Draw(img)
    _draw_label(img, draw, width_px, block_h, sku, barcode_text, title_font, barcode_text_font)
    img.save(out_path, dpi=(dpi, dpi))


# ---------------------------------------------------------------------------
# Plan / execute
# ---------------------------------------------------------------------------

@dataclass
class BarcodePlan:
    sku: str
    supplier: str
    barcode: str
    has_existing_file: bool
    action: str               # GENERATE | SKIP
    notes: str = ""
    target_sku_folder_id: str = ""


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


def build_plan(
    category_folder_id: str,
    structure: str,
    sheet_rows: list[dict[str, str]],
    sku_col: str,
    supplier_col: str,
    barcode_col: str,
    barcode_subdir: str,
) -> list[BarcodePlan]:
    """One plan per SKU in the sheet that has a barcode column populated.

    SKIP rules:
      - Empty barcode → SKIP, note 'no barcode in sheet'.
      - SKU folder not found in Drive → SKIP, note 'sku folder not found'.
      - <sku>.jpg already exists in the barcode subfolder → SKIP, note 'already exists'.
    """
    sku_index = _build_sku_index(category_folder_id, structure)
    plans: list[BarcodePlan] = []

    for row in sheet_rows:
        sku = (row.get(sku_col) or "").strip()
        sup_sheet = (row.get(supplier_col) or "").strip()
        barcode_text = (row.get(barcode_col) or "").strip()
        if not sku:
            continue

        if not barcode_text:
            plans.append(BarcodePlan(
                sku=sku, supplier=sup_sheet, barcode="",
                has_existing_file=False, action="SKIP",
                notes="no barcode in sheet",
            ))
            continue

        if sku not in sku_index:
            plans.append(BarcodePlan(
                sku=sku, supplier=sup_sheet, barcode=barcode_text,
                has_existing_file=False, action="SKIP",
                notes="sku folder not found in Drive",
            ))
            continue

        target_sup, target_sku_id = sku_index[sku]
        existing = False
        bc_id = _resolve_optional(target_sku_id, barcode_subdir)
        if bc_id:
            for f in drive.list_files(bc_id):
                if f["name"] == f"{sku}.jpg":
                    existing = True
                    break

        plans.append(BarcodePlan(
            sku=sku, supplier=target_sup or sup_sheet, barcode=barcode_text,
            has_existing_file=existing,
            action="SKIP" if existing else "GENERATE",
            notes="already exists" if existing else "",
            target_sku_folder_id=target_sku_id,
        ))

    return plans


def to_sheet_rows(plans: list[BarcodePlan]) -> tuple[list[str], list[list]]:
    headers = ["SKU", "Supplier", "Barcode", "Has Existing File", "Action", "Notes"]
    rows: list[list] = []
    for p in plans:
        rows.append([
            p.sku, p.supplier, p.barcode,
            "TRUE" if p.has_existing_file else "FALSE",
            p.action, p.notes,
        ])
    return headers, rows


class GenProgress(NamedTuple):
    sku: str
    barcode: str
    skipped: bool
    error: str = ""


def execute(
    report_rows: list[dict[str, str]],
    category_folder_id: str,
    structure: str,
    barcode_subdir: str,
) -> Generator[GenProgress, None, None]:
    """Read the (possibly edited) report and render+upload one barcode per Action=GENERATE row."""
    sku_index = _build_sku_index(category_folder_id, structure)

    for row in report_rows:
        action = (row.get("Action") or "").strip().upper()
        if action != "GENERATE":
            continue

        sku = (row.get("SKU") or "").strip()
        barcode_text = (row.get("Barcode") or "").strip()
        if not sku or not barcode_text:
            yield GenProgress(sku=sku, barcode=barcode_text, skipped=True,
                              error="missing SKU or Barcode in report row")
            continue
        if sku not in sku_index:
            yield GenProgress(sku=sku, barcode=barcode_text, skipped=True,
                              error=f"target SKU folder not found: {sku}")
            continue

        _, target_sku_id = sku_index[sku]
        try:
            current = target_sku_id
            for part in barcode_subdir.split("/"):
                current = drive.find_or_create_folder(part, current)
            bc_folder_id = current
        except Exception as exc:
            yield GenProgress(sku=sku, barcode=barcode_text, skipped=False,
                              error=f"could not create barcode folder: {exc}")
            continue

        try:
            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
                tmp_path = tmp.name
            try:
                render_barcode_image(sku, barcode_text, tmp_path)
                drive.upload_file(tmp_path, bc_folder_id, f"{sku}.jpg", "image/jpeg")
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            yield GenProgress(sku=sku, barcode=barcode_text, skipped=False)
        except Exception as exc:
            yield GenProgress(sku=sku, barcode=barcode_text, skipped=False, error=str(exc))


def summarise(plans: list[BarcodePlan]) -> dict[str, int]:
    return {
        "total": len(plans),
        "to_generate": sum(1 for p in plans if p.action == "GENERATE"),
        "skipped": sum(1 for p in plans if p.action == "SKIP"),
    }
