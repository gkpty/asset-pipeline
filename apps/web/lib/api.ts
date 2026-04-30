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

async function _bodyOrEmpty(res: Response): Promise<string> {
  try {
    const t = await res.text();
    return t.length > 400 ? t.slice(0, 400) + "…" : t;
  } catch {
    return "";
  }
}

export async function fetchSkus(): Promise<SkuListItem[]> {
  const res = await fetch(`${API_BASE}/api/skus`, { cache: "no-store" });
  if (!res.ok) {
    const body = await _bodyOrEmpty(res);
    throw new Error(`GET /api/skus → ${res.status} ${res.statusText}\n${body}`);
  }
  return res.json();
}

export async function fetchSkuPhotos(sku: string): Promise<SkuPhotosResponse> {
  const url = `${API_BASE}/api/skus/${encodeURIComponent(sku)}/photos`;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    const body = await _bodyOrEmpty(res);
    throw new Error(`GET ${url} → ${res.status} ${res.statusText}\n${body}`);
  }
  return res.json();
}

export async function saveOrder(
  sku: string,
  items: { file_id: string; name: string }[],
): Promise<void> {
  const url = `${API_BASE}/api/skus/${encodeURIComponent(sku)}/order`;
  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ items }),
  });
  if (!res.ok) {
    const body = await _bodyOrEmpty(res);
    throw new Error(`POST ${url} → ${res.status} ${res.statusText}\n${body}`);
  }
}

export async function deletePhoto(fileId: string): Promise<void> {
  const url = `${API_BASE}/api/files/${encodeURIComponent(fileId)}`;
  const res = await fetch(url, { method: "DELETE" });
  if (!res.ok) {
    const body = await _bodyOrEmpty(res);
    throw new Error(`DELETE ${url} → ${res.status} ${res.statusText}\n${body}`);
  }
}
