"""Diagnose Replicate connectivity issues for the inpaint flow.

Hits Replicate's REST API directly with the configured REPLICATE_API_TOKEN to
verify:
  1. Whose account/team owns the token, and what shows up at /v1/account.
  2. Whether the configured SAM model exists / is accessible (200 vs 404).
  3. What candidate SAM-2 model slugs ARE accessible to this token.
  4. Whether a tiny prediction call hits a 429 throttle and what the
     reset-window says.

Usage:
  uv run python scripts/test_replicate.py

If `pipeline.config.toml` has `[generate.photos.inpaint] sam_model = "..."`,
that's what we test. Otherwise tries the default `meta/sam-2`.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "packages" / "sdk" / "src"))

from dotenv import load_dotenv

load_dotenv(_REPO_ROOT / ".env")

import httpx

from asset_sdk.config import PipelineConfig

CANDIDATE_SAM_SLUGS = [
    "meta/sam-2",
    "lucataco/segment-anything-2",
    "lucataco/sam-2",
    "yyjim/segment-anything-everything",
    "mhrachev/segment-anything",
    "schananas/grounded_sam",
]

# Reference-image-aware inpainting models. We need one that accepts
# (source, mask, EXAMPLE_IMAGE) and paints the masked region with the example.
# black-forest-labs/flux-fill-dev is text-only (current bug).
CANDIDATE_REF_INPAINT_SLUGS = [
    "cjwbw/paint-by-example",
    "lucataco/paint-by-example",
    "geekyutao/inpaint-anything",
    "lucataco/sdxl-inpaint-ip-adapter",
    "fofr/flux-controlnet-inpaint",
    "lucataco/flux-controlnet-inpaint",
    "zsxkib/flux-dev-inpainting",
    "fofr/realistic-inpainting-with-image",
]


def _api_get(token: str, path: str) -> tuple[int, dict | str]:
    url = f"https://api.replicate.com/v1{path}"
    with httpx.Client(timeout=15.0) as c:
        r = c.get(url, headers={"Authorization": f"Bearer {token}"})
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, r.text


def main() -> int:
    token = os.environ.get("REPLICATE_API_TOKEN")
    if not token:
        print("REPLICATE_API_TOKEN not set in .env")
        return 1
    print(f"Token prefix: {token[:8]}…  (loaded from .env)")
    print()

    # 1. Account info
    print("=== /v1/account ===")
    code, body = _api_get(token, "/account")
    if code != 200:
        print(f"  HTTP {code}: {body}")
        print("  → token is invalid or revoked.")
        return 1
    print(f"  type:     {body.get('type')}")
    print(f"  username: {body.get('username')}")
    print(f"  name:     {body.get('name')}")
    print()

    # 2. Configured SAM model
    print("=== Configured SAM model ===")
    cfg = PipelineConfig.load(_REPO_ROOT / "pipeline.config.toml")
    configured = cfg.generate.photos.inpaint.sam_model
    print(f"  {configured}")
    code, body = _api_get(token, f"/models/{configured}")
    if code == 200:
        latest = (body or {}).get("latest_version") or {}
        print(f"  ✓ accessible. latest version: {latest.get('id', '?')[:16]}…")
    else:
        msg = body.get("detail") if isinstance(body, dict) else body
        print(f"  ✗ HTTP {code}: {msg}")
        if code == 404:
            print("  → this model slug is wrong / inaccessible. Update")
            print("    [generate.photos.inpaint] sam_model in pipeline.config.toml")
            print("    to one of the working candidates below.")
    print()

    # 3. Candidate SAM models
    print("=== Trying candidate SAM-2 / SAM model slugs ===")
    for slug in CANDIDATE_SAM_SLUGS:
        code, body = _api_get(token, f"/models/{slug}")
        if code == 200:
            latest = (body or {}).get("latest_version") or {}
            ver = latest.get("id", "?")[:16]
            print(f"  ✓ {slug:<45s}  latest={ver}…")
        else:
            msg = body.get("detail") if isinstance(body, dict) else body
            print(f"  ✗ {slug:<45s}  HTTP {code} {str(msg)[:60]}")
    print()

    # 3b. Candidate reference-image inpainting models — these accept a
    # source + mask + EXAMPLE IMAGE so the masked area is painted using
    # the example as visual guidance. This is what we need to swap text-only
    # FLUX-Fill with so the actual material reference photo gets used.
    print("=== Trying candidate reference-image inpainting models ===")
    for slug in CANDIDATE_REF_INPAINT_SLUGS:
        code, body = _api_get(token, f"/models/{slug}")
        if code == 200:
            latest = (body or {}).get("latest_version") or {}
            ver = latest.get("id", "?")[:16]
            print(f"  ✓ {slug:<45s}  latest={ver}…")
        else:
            msg = body.get("detail") if isinstance(body, dict) else body
            print(f"  ✗ {slug:<45s}  HTTP {code} {str(msg)[:60]}")
    print()

    # 4. Tiny throttle probe — if rate-limited, log details.
    # (We use the configured FLUX model since it should be cheap to hit metadata.)
    print("=== Rate-limit probe ===")
    flux = cfg.generate.photos.inpaint.inpaint_model
    code, body = _api_get(token, f"/models/{flux}")
    print(f"  GET /models/{flux} → {code}")
    if code == 200:
        print("  ✓ Replicate API access is functional from this token.")
    else:
        msg = body.get("detail") if isinstance(body, dict) else body
        print(f"  ✗ {msg}")
    print()
    print("If you're still seeing 429 with 'less than $5.0 in credit' AFTER")
    print("topping up, that's a Replicate billing/rate-limit propagation issue")
    print("on their side. Reach out to support@replicate.com with your account")
    print("username + the date you funded — they can force-clear the throttle.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
