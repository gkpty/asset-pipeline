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

from sqlalchemy import select

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


def _category() -> str:
    """Category subfolder under the parent root. Defaults to 'products'."""
    return os.environ.get("GOOGLE_DRIVE_CATEGORY", "products").strip() or "products"


def _root_folder_id() -> str:
    rf = os.environ.get("GOOGLE_DRIVE_ROOT_FOLDER_ID")
    if not rf:
        raise HTTPException(500, "GOOGLE_DRIVE_ROOT_FOLDER_ID is not set")
    return rf


def _category_folder_id() -> str:
    """Resolve <parent>/<category> Drive folder ID. Cached on `state` after first lookup."""
    cached = getattr(state, "category_folder_id", None)
    if cached:
        return cached
    try:
        fid = drive.resolve_category_folder(_root_folder_id(), _category())
    except Exception as exc:
        raise HTTPException(500, f"Could not resolve category folder: {exc}")
    state.category_folder_id = fid
    return fid


def _resolve_subfolder(parent_id: str, rel_path: str) -> str | None:
    current = parent_id
    for part in rel_path.split("/"):
        children = drive.list_folders(current)
        if part not in children:
            return None
        current = children[part]
    return current


def _thumb_url(file_id: str, size_px: int = 400) -> str:
    """Drive's simple thumbnail URL — works when the browser is logged into Google
    or when the file is publicly readable."""
    return f"https://drive.google.com/thumbnail?id={file_id}&sz=w{size_px}"


def _scan_one_sku(sku: str, sku_id: str, supplier: str, photos_subdir: str) -> dict:
    """Resolve <sku>/<photos_subdir>, count files, pick the first as the thumbnail."""
    photos_id = _resolve_subfolder(sku_id, photos_subdir)
    first_photo_id: str | None = None
    photo_count = 0
    if photos_id:
        files = [f for f in drive.list_files(photos_id) if not f["name"].startswith(".")]
        files.sort(key=lambda f: f["name"])
        photo_count = len(files)
        if files:
            first_photo_id = files[0]["id"]
    return {
        "sku": sku,
        "supplier": supplier,
        "sku_folder_id": sku_id,
        "photos_folder_id": photos_id,
        "photo_count": photo_count,
        "first_photo_id": first_photo_id,
    }


def _scan_skus() -> list[dict]:
    """Walk the category drive in parallel, capturing every SKU + its photos folder."""
    from concurrent.futures import ThreadPoolExecutor
    cfg = _load_cfg()
    photos_subdir = cfg.paths.product_photos
    category_id = _category_folder_id()

    # Phase 1 (sequential, fast): enumerate SKU folders.
    sku_tuples: list[tuple[str, str, str]] = []  # (sku, supplier, sku_folder_id)
    if cfg.structure_for(_category()) == "flat":
        for sku, sid in drive.list_folders(category_id).items():
            sku_tuples.append((sku, "", sid))
    else:
        for sup_name, sup_id in drive.list_folders(category_id).items():
            for sku, sid in drive.list_folders(sup_id).items():
                sku_tuples.append((sku, sup_name, sid))

    print(f"[skus] Found {len(sku_tuples)} SKU folders in {_category()}, scanning photos in parallel…", flush=True)

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
    with_photos = sum(1 for x in out if x["first_photo_id"])
    print(f"[skus] Done. {with_photos}/{len(out)} SKUs have at least one photo", flush=True)
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


@app.get("/api/health")
async def health() -> dict[str, Any]:
    return {"status": "ok", "cache_size": len(state.skus_cache or [])}


async def _apply_saved_orders(items: list[dict]) -> None:
    """Override each entry's first_photo_id with the saved order's first item, if any."""
    try:
        Session = get_sessionmaker()
        async with Session() as session:
            rows = (await session.execute(select(PhotoOrder))).scalars().all()
        by_sku = {o.sku: o for o in rows}
        for entry in items:
            order = by_sku.get(entry["sku"])
            if order and order.items:
                first_id = order.items[0].get("file_id")
                if first_id:
                    entry["first_photo_id"] = first_id
    except Exception as exc:
        print(f"[skus] Couldn't apply saved orders (DB unreachable?): {exc}", flush=True)


async def _populate_cache() -> None:
    print(f"[skus] Scanning {_category()} drive…", flush=True)
    items = _scan_skus()
    await _apply_saved_orders(items)
    state.skus_cache = items
    print(f"[skus] Cached {len(items)} SKUs", flush=True)


@app.get("/api/skus", response_model=list[SkuListItem])
async def list_skus(supplier: str | None = None) -> list[SkuListItem]:
    if state.skus_cache is None:
        try:
            await _populate_cache()
        except Exception as exc:
            import traceback
            traceback.print_exc()
            raise HTTPException(500, f"Drive scan failed: {exc}")
    out: list[SkuListItem] = []
    for x in state.skus_cache:
        if supplier and x["supplier"].lower() != supplier.lower():
            continue
        thumb = _thumb_url(x["first_photo_id"], 400) if x["first_photo_id"] else None
        out.append(SkuListItem(
            sku=x["sku"], supplier=x["supplier"],
            photo_count=x["photo_count"],
            first_photo_url=thumb,
        ))
    return out


@app.post("/api/refresh", response_model=dict)
async def refresh() -> dict[str, Any]:
    await _populate_cache()
    return {"sku_count": len(state.skus_cache or [])}


@app.get("/api/skus/{sku}/photos", response_model=SkuPhotosResponse)
async def get_sku_photos(sku: str) -> SkuPhotosResponse:
    if state.skus_cache is None:
        try:
            state.skus_cache = _scan_skus()
        except Exception as exc:
            import traceback
            traceback.print_exc()
            raise HTTPException(500, f"Drive scan failed: {exc}")
    sku_entry = next((x for x in state.skus_cache if x["sku"] == sku), None)
    if not sku_entry:
        raise HTTPException(
            404,
            f"SKU '{sku}' not in the cache. "
            f"If it was just uploaded, POST /api/refresh to re-scan.",
        )
    if not sku_entry["photos_folder_id"]:
        raise HTTPException(
            404,
            f"SKU '{sku}' has no photos/ folder under '{_load_cfg().paths.product_photos}/'.",
        )

    files = drive.list_files(sku_entry["photos_folder_id"])
    files = [f for f in files if not f["name"].startswith(".")]
    files_by_id = {f["id"]: f for f in files}

    saved_at: str | None = None
    has_order = False
    try:
        Session = get_sessionmaker()
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
    except Exception as exc:
        # DB unreachable — show photos in their natural order so the user can still view them.
        # Saving will fail with a clearer error, which is acceptable.
        print(f"[photos] DB unavailable, returning unordered: {exc}", flush=True)
        files.sort(key=lambda f: f["name"])
        has_order = False
        saved_at = None

    photo_items: list[PhotoItem] = [
        PhotoItem(file_id=f["id"], name=f["name"], url=_thumb_url(f["id"], 600))
        for f in files
    ]

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

    # Reflect the new "first photo" on the home grid immediately.
    if state.skus_cache and items_payload:
        for entry in state.skus_cache:
            if entry["sku"] == sku:
                entry["first_photo_id"] = items_payload[0]["file_id"]
                break

    return {"sku": sku, "count": len(body.items)}


@app.delete("/api/files/{file_id}", response_model=dict)
async def delete_file(file_id: str) -> dict[str, Any]:
    """Move a single Drive file to trash. Updates the cache so the grid stays accurate."""
    try:
        drive.trash_item(file_id)
    except Exception as exc:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Drive trash failed: {exc}")

    # Drop the file from any saved orders + adjust the home-grid cache.
    if state.skus_cache:
        for entry in state.skus_cache:
            if entry.get("first_photo_id") == file_id:
                entry["first_photo_id"] = None
                entry["photo_count"] = max(0, entry.get("photo_count", 0) - 1)
    try:
        Session = get_sessionmaker()
        async with Session() as session:
            rows = (await session.execute(select(PhotoOrder))).scalars().all()
            for rec in rows:
                if any(it.get("file_id") == file_id for it in rec.items):
                    rec.items = [it for it in rec.items if it.get("file_id") != file_id]
            await session.commit()
    except Exception as exc:
        # DB might be down; the trash already happened.
        print(f"[delete] Couldn't update saved orders: {exc}", flush=True)

    return {"file_id": file_id, "trashed": True}
