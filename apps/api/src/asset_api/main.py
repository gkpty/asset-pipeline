"""FastAPI service backing the `asset organize` web UI.

Endpoints:
  GET  /api/skus                  → list every SKU with a photos/ folder + first photo URL
  GET  /api/skus/{sku}/photos     → list all photos for a SKU with current saved order if any
  POST /api/skus/{sku}/order      → save a new ordering (writes to photo_orders)
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Load .env before importing the SDK (which reads OAuth creds at module import).
load_dotenv()

from asset_db.models import PhotoOrder
from asset_db.session import get_sessionmaker
from asset_sdk.adapters import drive
from asset_sdk.config import PipelineConfig


class _AppState:
    cfg: PipelineConfig | None = None
    skus_cache: list[dict] | None = None


state = _AppState()


def _load_cfg() -> PipelineConfig:
    if state.cfg is None:
        path = Path(os.environ.get("PIPELINE_CONFIG_PATH", "pipeline.config.toml"))
        state.cfg = PipelineConfig.load(path)
    return state.cfg


def _root_folder_id() -> str:
    rf = os.environ.get("GOOGLE_DRIVE_ROOT_FOLDER_ID")
    if not rf:
        raise HTTPException(500, "GOOGLE_DRIVE_ROOT_FOLDER_ID is not set")
    return rf


def _resolve_subfolder(parent_id: str, rel_path: str) -> str | None:
    current = parent_id
    for part in rel_path.split("/"):
        children = drive.list_folders(current)
        if part not in children:
            return None
        current = children[part]
    return current


def _resize_thumbnail(url: str | None, size_px: int) -> str | None:
    """Optionally bump the size suffix on a Drive thumbnailLink (=sNNN).

    We're conservative here — only modify when the URL clearly ends in a numeric size
    suffix; otherwise return as-is. Tokens inside Drive thumbnail URLs can contain '=s'
    sequences, so naive splitting breaks them.
    """
    import re
    if not url:
        return None
    m = re.search(r"=s\d+(-[a-zA-Z0-9]+)?$", url)
    if m:
        return url[: m.start()] + f"=s{size_px}"
    return url


def _scan_one_sku(sku: str, sku_id: str, supplier: str, photos_subdir: str) -> dict:
    """Resolve <sku>/<photos_subdir>, count files, pick the first as the thumbnail."""
    photos_id = _resolve_subfolder(sku_id, photos_subdir)
    first_photo_id: str | None = None
    first_thumb_url: str | None = None
    photo_count = 0
    if photos_id:
        files = [f for f in drive.list_files(photos_id) if not f["name"].startswith(".")]
        files.sort(key=lambda f: f["name"])
        photo_count = len(files)
        if files:
            first_photo_id = files[0]["id"]
            first_thumb_url = files[0].get("thumbnailLink")
    return {
        "sku": sku,
        "supplier": supplier,
        "sku_folder_id": sku_id,
        "photos_folder_id": photos_id,
        "photo_count": photo_count,
        "first_photo_id": first_photo_id,
        "first_thumb_url": first_thumb_url,
    }


def _scan_skus() -> list[dict]:
    """Walk the products drive in parallel, capturing every SKU + its photos folder."""
    from concurrent.futures import ThreadPoolExecutor
    cfg = _load_cfg()
    photos_subdir = cfg.paths.product_photos

    # Phase 1 (sequential, fast): enumerate SKU folders.
    sku_tuples: list[tuple[str, str, str]] = []  # (sku, supplier, sku_folder_id)
    if cfg.drive.structure == "flat":
        for sku, sid in drive.list_folders(_root_folder_id()).items():
            sku_tuples.append((sku, "", sid))
    else:
        for sup_name, sup_id in drive.list_folders(_root_folder_id()).items():
            for sku, sid in drive.list_folders(sup_id).items():
                sku_tuples.append((sku, sup_name, sid))

    print(f"[skus] Found {len(sku_tuples)} SKU folders, scanning photos in parallel…", flush=True)

    # Phase 2 (parallel, slow): per-SKU resolve photos folder + count files.
    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [
            ex.submit(_scan_one_sku, s, sid, sup, photos_subdir)
            for (s, sup, sid) in sku_tuples
        ]
        for i, fut in enumerate(futures, 1):
            try:
                out.append(fut.result())
            except Exception as exc:
                print(f"[skus]  ! {sku_tuples[i-1][0]}: {exc}", flush=True)
            if i % 50 == 0:
                print(f"[skus]   {i}/{len(sku_tuples)} done", flush=True)

    out.sort(key=lambda x: (x["supplier"], x["sku"]))
    with_thumb = sum(1 for x in out if x.get("first_thumb_url"))
    print(
        f"[skus] Done. {with_thumb}/{len(out)} SKUs have a Drive thumbnailLink",
        flush=True,
    )
    return out


app = FastAPI(title="Asset Pipeline API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class SkuListItem(BaseModel):
    sku: str
    supplier: str
    photo_count: int
    first_photo_url: str | None


class PhotoItem(BaseModel):
    file_id: str
    name: str
    url: str


class SkuPhotosResponse(BaseModel):
    sku: str
    supplier: str
    photos: list[PhotoItem]
    has_saved_order: bool
    saved_at: str | None


class OrderItem(BaseModel):
    file_id: str
    name: str


class OrderRequest(BaseModel):
    items: list[OrderItem]


def _fallback_thumb(file_id: str, w: int = 400) -> str:
    """Last-resort URL — works only if the file is public OR the browser is logged into Drive."""
    return f"https://drive.google.com/thumbnail?id={file_id}&sz=w{w}"


@app.get("/api/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "cache_size": len(state.skus_cache or [])}


@app.get("/api/skus", response_model=list[SkuListItem])
async def list_skus(supplier: str | None = None) -> list[SkuListItem]:
    if state.skus_cache is None:
        try:
            print("[skus] Scanning products drive…", flush=True)
            state.skus_cache = _scan_skus()
            print(f"[skus] Cached {len(state.skus_cache)} SKUs", flush=True)
        except Exception as exc:
            import traceback
            traceback.print_exc()
            raise HTTPException(500, f"Drive scan failed: {exc}")
    out: list[SkuListItem] = []
    for x in state.skus_cache:
        if supplier and x["supplier"].lower() != supplier.lower():
            continue
        thumb = _resize_thumbnail(x.get("first_thumb_url"), 400)
        if not thumb and x["first_photo_id"]:
            thumb = _fallback_thumb(x["first_photo_id"], 400)
        out.append(SkuListItem(
            sku=x["sku"], supplier=x["supplier"],
            photo_count=x["photo_count"],
            first_photo_url=thumb,
        ))
    return out


@app.post("/api/refresh", response_model=dict)
async def refresh() -> dict[str, Any]:
    state.skus_cache = _scan_skus()
    return {"sku_count": len(state.skus_cache)}


@app.get("/api/skus/{sku}/photos", response_model=SkuPhotosResponse)
async def get_sku_photos(sku: str) -> SkuPhotosResponse:
    items = state.skus_cache or _scan_skus()
    sku_entry = next((x for x in items if x["sku"] == sku), None)
    if not sku_entry:
        raise HTTPException(404, f"SKU '{sku}' not found")
    if not sku_entry["photos_folder_id"]:
        raise HTTPException(404, f"SKU '{sku}' has no photos/ folder")

    files = drive.list_files(sku_entry["photos_folder_id"])
    files = [f for f in files if not f["name"].startswith(".")]
    files_by_id = {f["id"]: f for f in files}

    Session = get_sessionmaker()
    saved_at: str | None = None
    has_order = False
    async with Session() as session:
        rec = await session.get(PhotoOrder, (sku, "product_photo"))
        if rec is not None:
            has_order = True
            saved_at = rec.saved_at.isoformat() if rec.saved_at else None
            ordered: list[dict] = []
            seen: set[str] = set()
            for item in rec.items:
                fid = item.get("file_id")
                if fid in files_by_id:
                    ordered.append(files_by_id[fid])
                    seen.add(fid)
            for f in files:
                if f["id"] not in seen:
                    ordered.append(f)
            files = ordered
        else:
            files.sort(key=lambda f: f["name"])

    photo_items: list[PhotoItem] = []
    for f in files:
        thumb = _resize_thumbnail(f.get("thumbnailLink"), 600)
        if not thumb:
            thumb = _fallback_thumb(f["id"], 600)
        photo_items.append(PhotoItem(file_id=f["id"], name=f["name"], url=thumb))

    return SkuPhotosResponse(
        sku=sku,
        supplier=sku_entry["supplier"],
        photos=photo_items,
        has_saved_order=has_order,
        saved_at=saved_at,
    )


@app.post("/api/skus/{sku}/order", response_model=dict)
async def save_sku_order(sku: str, body: OrderRequest) -> dict[str, Any]:
    Session = get_sessionmaker()
    async with Session() as session:
        rec = await session.get(PhotoOrder, (sku, "product_photo"))
        items_payload = [{"file_id": i.file_id, "name": i.name} for i in body.items]
        if rec is None:
            rec = PhotoOrder(sku=sku, asset_kind="product_photo", items=items_payload)
            session.add(rec)
        else:
            rec.items = items_payload
        await session.commit()
    return {"sku": sku, "count": len(body.items)}
