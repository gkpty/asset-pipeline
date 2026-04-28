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
import Link from "next/link";
import { use, useCallback, useEffect, useState } from "react";
import {
  fetchSkuPhotos,
  saveOrder,
  type PhotoItem,
  type SkuPhotosResponse,
} from "../../../lib/api";

function PhotoCard({
  photo,
  position,
}: {
  photo: PhotoItem;
  position: number;
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
      <div className="thumb">
        {/* eslint-disable-next-line @next/next/no-img-element */}
        <img src={photo.url} alt={photo.name} />
      </div>
      <div className="name">{photo.name}</div>
    </div>
  );
}

export default function SkuPage({ params }: { params: Promise<{ sku: string }> }) {
  const { sku } = use(params);
  const decodedSku = decodeURIComponent(sku);

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

  return (
    <>
      <header className="header">
        <div>
          <Link href="/" style={{ fontSize: 13, color: "#666" }}>← All SKUs</Link>
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

      <main className="container">
        {error && <p style={{ color: "red" }}>Error: {error}</p>}
        {!data && !error && <p>Loading…</p>}

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
                  <PhotoCard key={p.file_id} photo={p} position={i + 1} />
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
