"use client";

import {
  DndContext,
  PointerSensor,
  closestCenter,
  useSensor,
  useSensors,
  type DragEndEvent,
} from "@dnd-kit/core";
import {
  SortableContext,
  arrayMove,
  rectSortingStrategy,
  useSortable,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { useRouter } from "next/navigation";
import { use, useCallback, useEffect, useState } from "react";
import {
  deletePhoto,
  fetchSkuPhotos,
  saveOrder,
  type PhotoItem,
  type SkuPhotosResponse,
} from "../../../lib/api";

function PhotoCard({
  photo,
  position,
  onDelete,
}: {
  photo: PhotoItem;
  position: number;
  onDelete: (fileId: string) => void;
}) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } =
    useSortable({ id: photo.file_id });

  const style: React.CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
  };

  return (
    <div
      ref={setNodeRef}
      style={style}
      {...attributes}
      {...listeners}
      className={`photo-card${isDragging ? " dragging" : ""}`}
    >
      <div className="position">{position}</div>
      <button
        type="button"
        className="delete-btn"
        title={`Delete ${photo.name}`}
        // Stop the drag from picking this up on click.
        onPointerDown={(e) => e.stopPropagation()}
        onClick={(e) => {
          e.stopPropagation();
          if (window.confirm(`Move "${photo.name}" to Drive trash?`)) {
            onDelete(photo.file_id);
          }
        }}
      >
        ×
      </button>
      <div className="thumb">
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img src={photo.url} alt={photo.name} referrerPolicy="no-referrer" />
      </div>
      <div className="name">{photo.name}</div>
    </div>
  );
}

export default function SkuPage({ params }: { params: Promise<{ sku: string }> }) {
  const { sku } = use(params);
  const decodedSku = decodeURIComponent(sku);
  const router = useRouter();

  const [data, setData] = useState<SkuPhotosResponse | null>(null);
  const [photos, setPhotos] = useState<PhotoItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const [toast, setToast] = useState<string | null>(null);

  useEffect(() => {
    fetchSkuPhotos(decodedSku)
      .then((d) => {
        setData(d);
        setPhotos(d.photos);
      })
      .catch((e) => setError(String(e)));
  }, [decodedSku]);

  useEffect(() => {
    if (!toast) return;
    const t = setTimeout(() => setToast(null), 2500);
    return () => clearTimeout(t);
  }, [toast]);

  const sensors = useSensors(useSensor(PointerSensor, { activationConstraint: { distance: 5 } }));

  const handleDragEnd = useCallback((event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id) return;
    setPhotos((items) => {
      const oldIndex = items.findIndex((p) => p.file_id === active.id);
      const newIndex = items.findIndex((p) => p.file_id === over.id);
      if (oldIndex === -1 || newIndex === -1) return items;
      return arrayMove(items, oldIndex, newIndex);
    });
    setDirty(true);
  }, []);

  const handleSave = useCallback(async () => {
    setSaving(true);
    try {
      await saveOrder(
        decodedSku,
        photos.map((p) => ({ file_id: p.file_id, name: p.name })),
      );
      setDirty(false);
      // Drop home-grid cache so the saved-order's first photo is shown on return.
      try {
        sessionStorage.removeItem("skus_cache_v1");
      } catch {
        /* ignore */
      }
      setToast("Order saved");
    } catch (e) {
      setToast(`Save failed: ${e}`);
    } finally {
      setSaving(false);
    }
  }, [decodedSku, photos]);

  const handleReset = useCallback(() => {
    if (data) {
      setPhotos(data.photos);
      setDirty(false);
    }
  }, [data]);

  const handleDelete = useCallback(async (fileId: string) => {
    try {
      await deletePhoto(fileId);
      setPhotos((items) => items.filter((p) => p.file_id !== fileId));
      // Also drop from the cached `data` so Reset won't bring it back.
      setData((d) =>
        d ? { ...d, photos: d.photos.filter((p) => p.file_id !== fileId) } : d,
      );
      // Invalidate the home grid's sessionStorage cache so its thumbnail refreshes
      // when the user goes back (next mount triggers a fresh fetchSkus anyway).
      try {
        sessionStorage.removeItem("skus_cache_v1");
      } catch {
        /* ignore */
      }
      setToast("Photo deleted");
    } catch (e) {
      setToast(`Delete failed: ${e}`);
    }
  }, []);

  return (
    <>
      <header className="header">
        <div>
          <a
            href="/"
            onClick={(e) => {
              // Prefer router.back() so the home page restores from cache + scroll;
              // fall through to a normal nav if there's no history (direct URL load).
              if (window.history.length > 1) {
                e.preventDefault();
                router.back();
              }
            }}
            style={{ fontSize: 13, color: "#666", cursor: "pointer" }}
          >
            ← All SKUs
          </a>
          <h1 style={{ marginTop: 4 }}>{decodedSku}</h1>
          {data && (
            <div className="meta">
              {data.supplier} · {photos.length} photo{photos.length === 1 ? "" : "s"}
              {data.has_saved_order && data.saved_at && (
                <> · saved order from {new Date(data.saved_at).toLocaleString()}</>
              )}
            </div>
          )}
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <button onClick={handleReset} disabled={!dirty || saving}>Reset</button>
          <button className="primary" onClick={handleSave} disabled={!dirty || saving}>
            {saving ? "Saving…" : "Save order"}
          </button>
        </div>
      </header>

      {!data && !error && (
        <div className="loading-overlay">
          <div className="loading-bar" />
          <div className="label">Loading photos for {decodedSku}…</div>
        </div>
      )}

      <main className="container">
        {error && <div className="error-box">Error: {error}</div>}

        {data && photos.length === 0 && (
          <div className="empty">No photos in this SKU's photos/ folder.</div>
        )}

        {photos.length > 0 && (
          <DndContext
            sensors={sensors}
            collisionDetection={closestCenter}
            onDragEnd={handleDragEnd}
          >
            <SortableContext
              items={photos.map((p) => p.file_id)}
              strategy={rectSortingStrategy}
            >
              <div className="photo-grid">
                {photos.map((p, i) => (
                  <PhotoCard
                    key={p.file_id}
                    photo={p}
                    position={i + 1}
                    onDelete={handleDelete}
                  />
                ))}
              </div>
            </SortableContext>
          </DndContext>
        )}
      </main>

      {toast && <div className="toast">{toast}</div>}
    </>
  );
}
