const API_BASE =
  process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

export type SkuListItem = {
  sku: string;
  supplier: string;
  photo_count: number;
  first_photo_url: string | null;
};

export type PhotoItem = {
  file_id: string;
  name: string;
  url: string;
};

export type SkuPhotosResponse = {
  sku: string;
  supplier: string;
  photos: PhotoItem[];
  has_saved_order: boolean;
  saved_at: string | null;
};

export async function fetchSkus(): Promise<SkuListItem[]> {
  const res = await fetch(`${API_BASE}/api/skus`, { cache: "no-store" });
  if (!res.ok) throw new Error(`Failed to fetch SKUs: ${res.status}`);
  return res.json();
}

export async function fetchSkuPhotos(sku: string): Promise<SkuPhotosResponse> {
  const res = await fetch(
    `${API_BASE}/api/skus/${encodeURIComponent(sku)}/photos`,
    { cache: "no-store" },
  );
  if (!res.ok) throw new Error(`Failed to fetch photos: ${res.status}`);
  return res.json();
}

export async function saveOrder(
  sku: string,
  items: { file_id: string; name: string }[],
): Promise<void> {
  const res = await fetch(
    `${API_BASE}/api/skus/${encodeURIComponent(sku)}/order`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ items }),
    },
  );
  if (!res.ok) throw new Error(`Failed to save order: ${res.status}`);
}
