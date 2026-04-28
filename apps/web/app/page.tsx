"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { fetchSkus, type SkuListItem } from "../lib/api";

export default function Home() {
  const [skus, setSkus] = useState<SkuListItem[] | null>(null);
  const [filter, setFilter] = useState("");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchSkus()
      .then(setSkus)
      .catch((e) => setError(String(e)));
  }, []);

  const filtered = useMemo(() => {
    if (!skus) return [];
    const q = filter.trim().toLowerCase();
    if (!q) return skus;
    return skus.filter(
      (s) =>
        s.sku.toLowerCase().includes(q) ||
        s.supplier.toLowerCase().includes(q),
    );
  }, [skus, filter]);

  return (
    <>
      <header className="header">
        <h1>Organize · SKU Grid</h1>
        <div style={{ display: "flex", gap: 12, alignItems: "center" }}>
          <input
            type="text"
            placeholder="Filter by SKU or supplier…"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
          <span className="meta">
            {skus ? `${filtered.length}/${skus.length} SKUs` : "loading…"}
          </span>
        </div>
      </header>

      <main className="container">
        {error && <p style={{ color: "red" }}>Error: {error}</p>}
        {!skus && !error && <p>Loading…</p>}

        {skus && filtered.length === 0 && (
          <div className="empty">No SKUs match.</div>
        )}

        <div className="sku-grid">
          {filtered.map((s) => (
            <Link
              key={`${s.supplier}/${s.sku}`}
              href={`/sku/${encodeURIComponent(s.sku)}`}
              className="sku-card"
            >
              <div className="thumb">
                {s.first_photo_url ? (
                  // eslint-disable-next-line @next/next/no-img-element
                  <img src={s.first_photo_url} alt={s.sku} />
                ) : (
                  <span>no photos</span>
                )}
              </div>
              <div className="meta">
                <div className="sku-name">{s.sku}</div>
                <div className="sub">
                  {s.supplier} · {s.photo_count} photo{s.photo_count === 1 ? "" : "s"}
                </div>
              </div>
            </Link>
          ))}
        </div>
      </main>
    </>
  );
}
