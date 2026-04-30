"use client";

import Link from "next/link";
import { useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { fetchSkus, type SkuListItem } from "../lib/api";

const CACHE_KEY = "skus_cache_v1";
const SCROLL_KEY = "skus_scroll_y";
const FILTER_KEY = "skus_filter";

function readCache(): SkuListItem[] | null {
  if (typeof window === "undefined") return null;
  try {
    const raw = sessionStorage.getItem(CACHE_KEY);
    return raw ? (JSON.parse(raw) as SkuListItem[]) : null;
  } catch {
    return null;
  }
}

// Use layoutEffect on the client, plain effect on the server (Next.js suppresses
// the warning, and effects don't run during SSR anyway).
const useIsoLayoutEffect =
  typeof window !== "undefined" ? useLayoutEffect : useEffect;

export default function Home() {
  // Initial state must match between SSR and client to avoid hydration errors.
  // We hydrate from sessionStorage in a layout effect immediately after mount —
  // that runs before the first paint, so the grid still appears without flicker.
  const [skus, setSkus] = useState<SkuListItem[] | null>(null);
  const [filter, setFilter] = useState<string>("");
  const [error, setError] = useState<string | null>(null);
  const restored = useRef(false);

  // First-paint hydration of cheap state (filter + cached SKU list, if any).
  // Scroll restoration happens in a separate effect that waits for SKUs.
  useIsoLayoutEffect(() => {
    const savedFilter = sessionStorage.getItem(FILTER_KEY);
    if (savedFilter) setFilter(savedFilter);
    const cached = readCache();
    if (cached) setSkus(cached);
  }, []);

  // Background refetch on every mount (catches new uploads / saves / deletes).
  useEffect(() => {
    fetchSkus()
      .then((data) => {
        setSkus(data);
        try {
          sessionStorage.setItem(CACHE_KEY, JSON.stringify(data));
        } catch {
          /* quota exceeded — ignore */
        }
      })
      .catch((e) => setError(String(e)));
  }, []);

  // Restore scroll once SKUs are on the page — works both for cache hits
  // (synchronously, before paint) AND for cache misses (after the fetch resolves).
  // The `restored` ref ensures this only fires once per mount.
  useIsoLayoutEffect(() => {
    if (restored.current) return;
    if (!skus) return;
    const y = sessionStorage.getItem(SCROLL_KEY);
    if (y) window.scrollTo(0, parseInt(y, 10) || 0);
    restored.current = true;
  }, [skus]);

  // Persist filter so it survives navigation too.
  useEffect(() => {
    if (typeof window === "undefined") return;
    sessionStorage.setItem(FILTER_KEY, filter);
  }, [filter]);

  // Save scroll position whenever the user scrolls (throttled via rAF).
  useEffect(() => {
    if (typeof window === "undefined") return;
    let pending = false;
    const onScroll = () => {
      if (pending) return;
      pending = true;
      requestAnimationFrame(() => {
        sessionStorage.setItem(SCROLL_KEY, String(window.scrollY));
        pending = false;
      });
    };
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
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

      {!skus && !error && (
        <div className="loading-overlay">
          <div className="loading-bar" />
          <div className="label">Loading SKUs from Drive…</div>
          <div className="sub">
            First load can take 30–60s while the API walks every supplier folder.
            Subsequent loads come from the cache.
          </div>
        </div>
      )}

      <main className="container">
        {error && <div className="error-box">Error: {error}</div>}

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
                  <img
                    src={s.first_photo_url}
                    alt={s.sku}
                    referrerPolicy="no-referrer"
                  />
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
