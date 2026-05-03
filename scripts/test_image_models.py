"""Smoke-test image generation across every model in the configured cycle.

Synthesises a tiny stand-in product photo (a cube on a white background) and
calls each model in `cfg.generate.photos.models` with a minimal prompt, so you
can verify your API keys + quotas + model IDs are all working before kicking
off a real `asset generate --type photos` run.

Usage:
  uv run python scripts/test_image_models.py                       # all configured models
  uv run python scripts/test_image_models.py --models gpt-image-2-2026-04-21,gemini-3.1-flash-image-preview
  uv run python scripts/test_image_models.py --image path/to/real.jpg

Outputs each model's response as `tmp/test_<provider>_<model>.jpg` in the repo
root so you can eyeball them. Reports per-model latency, bytes received, and
any error so you know exactly which side broke.
"""
from __future__ import annotations

import argparse
import io
import os
import sys
import tempfile
import time
from pathlib import Path

# Ensure the project's SDK is importable when this runs as a plain script.
_REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO_ROOT / "packages" / "sdk" / "src"))

from dotenv import load_dotenv

load_dotenv(_REPO_ROOT / ".env")

from PIL import Image, ImageDraw

from asset_sdk.config import PipelineConfig
from asset_sdk.stages.generate_photos import (
    _generate_dispatch,
    _provider_for,
)


def _make_test_image(out_path: str, w: int = 1024, h: int = 1024) -> None:
    """Synthesise a recognisable little 'product photo' so the model has
    something concrete to edit (a tan cube on a white background)."""
    img = Image.new("RGB", (w, h), (255, 255, 255))
    d = ImageDraw.Draw(img)
    # Cube: front face + side face for a 3/4 perspective look.
    cx, cy, s = w // 2, h // 2, min(w, h) // 4
    front = [(cx - s, cy - s), (cx + s, cy - s), (cx + s, cy + s), (cx - s, cy + s)]
    side = [(cx + s, cy - s), (cx + s + s // 2, cy - s - s // 4),
            (cx + s + s // 2, cy + s - s // 4), (cx + s, cy + s)]
    top = [(cx - s, cy - s), (cx - s + s // 2, cy - s - s // 4),
           (cx + s + s // 2, cy - s - s // 4), (cx + s, cy - s)]
    d.polygon(front, fill=(180, 130, 90))
    d.polygon(side, fill=(140, 100, 70))
    d.polygon(top, fill=(210, 160, 110))
    img.save(out_path, "JPEG", quality=92)


def _run_one(model: str, image_path: str, prompt: str, quality: str, size: str) -> dict:
    """Call one model, return a dict with status / bytes / latency / error."""
    provider = _provider_for(model)
    clients: dict = {}
    start = time.time()
    try:
        result_bytes = _generate_dispatch(
            clients=clients,
            model=model,
            quality=quality,
            size=size,
            sibling_photo_path=image_path,
            material_paths=[],
            prompt=prompt,
        )
        elapsed = time.time() - start
        return {
            "model": model,
            "provider": provider,
            "ok": True,
            "bytes": len(result_bytes),
            "elapsed_s": elapsed,
            "data": result_bytes,
            "error": "",
        }
    except Exception as exc:
        return {
            "model": model,
            "provider": provider,
            "ok": False,
            "bytes": 0,
            "elapsed_s": time.time() - start,
            "data": b"",
            "error": str(exc),
        }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--models",
        help="Comma-separated model ids. Defaults to cfg.generate.photos.models.",
    )
    ap.add_argument(
        "--image",
        help="Path to a real source image. If omitted, a synthetic cube is used.",
    )
    ap.add_argument(
        "--prompt",
        default=(
            "Photograph of a wooden cube on a clean white seamless background. "
            "Photorealistic studio product photography. No text or watermark."
        ),
        help="Prompt sent to each model. Default is a generic product-photo prompt.",
    )
    ap.add_argument("--quality", default=None, help="OpenAI quality: low|medium|high")
    ap.add_argument("--size", default=None, help="OpenAI size string or 'auto'")
    ap.add_argument(
        "--out-dir",
        default="tmp",
        help="Where to save successful outputs (default: tmp/).",
    )
    args = ap.parse_args()

    cfg = PipelineConfig.load(_REPO_ROOT / "pipeline.config.toml").generate.photos
    if args.models:
        models = [m.strip() for m in args.models.split(",") if m.strip()]
    else:
        models = list(cfg.models)
    if not models:
        print("No models configured.", file=sys.stderr)
        return 1
    quality = (args.quality or cfg.quality).strip().lower()
    size = (args.size or cfg.size).strip()

    out_dir = _REPO_ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    # Source image (real or synthetic).
    if args.image:
        image_path = str(Path(args.image).expanduser().resolve())
        if not os.path.exists(image_path):
            print(f"Image not found: {image_path}", file=sys.stderr)
            return 1
    else:
        synth = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
        _make_test_image(synth.name)
        image_path = synth.name
        print(f"[synth] Wrote test image: {image_path}")

    print()
    print(f"Testing {len(models)} model(s): {models}")
    print(f"Quality: {quality}    Size: {size}")
    print(f"Source:  {image_path}")
    print(f"Prompt:  {args.prompt[:80]}{'…' if len(args.prompt) > 80 else ''}")
    print()

    results: list[dict] = []
    for m in models:
        provider = _provider_for(m)
        print(f"  → {provider:<7s} {m} …", flush=True)
        r = _run_one(m, image_path, args.prompt, quality, size)
        results.append(r)
        if r["ok"]:
            out_path = out_dir / f"test_{r['provider']}_{m.replace('/', '-')}.jpg"
            with open(out_path, "wb") as fh:
                fh.write(r["data"])
            print(
                f"    [OK]   {r['bytes']:>8,} bytes in {r['elapsed_s']:.1f}s  → {out_path}"
            )
        else:
            print(f"    [FAIL] {r['elapsed_s']:.1f}s")
            for line in str(r["error"]).splitlines()[:6]:
                print(f"           {line}")

    print()
    ok = sum(1 for r in results if r["ok"])
    fail = len(results) - ok
    print(f"Summary: {ok} passed, {fail} failed")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
