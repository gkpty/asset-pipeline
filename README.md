# Asset Optimization Pipeline

A local, single-user pipeline for managing and optimizing product assets. It reads a master product list from Google Sheets, audits asset folders in Google Drive, applies bulk fixes, optimizes photos and 3D models, and produces a clean canonical structure ready for downstream publishing.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
  - [Environment variables](#environment-variables)
  - [Pipeline config](#pipeline-config)
  - [Google OAuth setup](#google-oauth-setup)
- [Google Drive folder structure](#google-drive-folder-structure)
- [Pipeline workflow](#pipeline-workflow)
- [CLI reference](#cli-reference)
- [Image optimization details](#image-optimization-details)
- [3D model optimization details](#3d-model-optimization-details)
- [Development](#development)

---

## Overview

The pipeline works against two sources of truth:

| Source | What it owns |
|--------|-------------|
| **Google Sheets** | Which products exist (SKUs, suppliers, product names, parent products, supplier refs) |
| **Google Drive** | Which asset files exist and their folder structure |

A typical run moves through several stages — diagnosing structure problems, renaming orphans against the sheet, removing duplicates, scaffolding missing folders, uploading any missing local files, generating derived assets, organizing for publish, and finally optimizing photos and 3D models. Each stage is its own CLI command and is independently re-runnable.

Reports are always written to a Google Sheets tab so they can be edited in place. Several commands (`rename`, `dedupe`) read the diagnose report's edited columns directly, making the sheet the source of truth for in-flight decisions.

---

## Architecture

```
asset_pipeline/
├── packages/
│   ├── db/       Python — SQLAlchemy models + Alembic migrations (Postgres 16)
│   └── sdk/      Python — storage adapters, stage logic, config, image/3D pipelines
├── apps/
│   ├── cli/      Typer CLI  (entry point: `asset`)
│   ├── api/      FastAPI service  (read-only views, v2)
│   └── web/      Next.js 15 front-end  (v2)
├── pipeline.config.toml   Per-project standards (folder names, optimize settings)
└── docker-compose.yml     Local Postgres 16
```

Python packages are tied together with a **uv workspace**. The JS app is managed by **pnpm + Turborepo**.

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Python | 3.12+ | [python.org](https://www.python.org/downloads/) or `brew install python` |
| uv | latest | See below |
| Node.js | 20+ | [nodejs.org](https://nodejs.org/) or `brew install node` |
| pnpm | 9+ | See below |
| Docker Desktop | any recent | [docker.com](https://www.docker.com/products/docker-desktop/) |

---

## Installation

### 1. Install uv

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# macOS via Homebrew
brew install uv
```

Reload your shell, then verify:

```bash
source ~/.zshrc
uv --version
```

### 2. Install pnpm

```bash
curl -fsSL https://get.pnpm.io/install.sh | sh
# or: brew install pnpm
# or: npm install -g pnpm
pnpm --version
```

### 3. Set up the project

```bash
git clone <repo-url>
cd asset_pipeline

cp env.example .env
# Edit .env — see Configuration below.

make install            # uv sync + pnpm install
make up                 # start Postgres in Docker
make migrate            # apply Alembic migrations
```

The `asset` CLI is now available:

```bash
uv run asset --help
```

---

## Configuration

### Environment variables

Copy `env.example` to `.env` and fill in the values. The file is gitignored.

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | Postgres connection string. Defaults to the local Docker instance. |
| `GOOGLE_OAUTH_CREDENTIALS` | Yes | Path to the OAuth Desktop-app client JSON downloaded from Google Cloud Console. See [Google OAuth setup](#google-oauth-setup). |
| `GOOGLE_OAUTH_TOKEN_PATH` | No | Where the refreshable user token is cached. Defaults to `./.secrets/oauth_token.json`. |
| `GOOGLE_SHEETS_MASTER_ID` | Yes | ID of the Google Sheet that holds the master product list. From the sheet URL: `https://docs.google.com/spreadsheets/d/**<ID>**/`. |
| `GOOGLE_DRIVE_ROOT_FOLDER_ID` | Yes | ID of the Google Drive folder that contains all SKU folders. From the folder URL: `https://drive.google.com/drive/folders/**<ID>**`. |
| `GOOGLE_DRIVE_LIFESTYLE_FOLDER_ID` | For `rename-lifestyle-photos` | Drive folder containing lifestyle photo folders (named by parent product). |
| `GOOGLE_DRIVE_MODELS_FOLDER_ID` | For `copy-models-into-products` | Shared Drive folder containing per-SKU model folders. |
| `PIPELINE_CONFIG_PATH` | No | Path to `pipeline.config.toml`. Defaults to `./pipeline.config.toml`. |
| `AWS_REGION` / `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `S3_BUCKET` / `S3_PREFIX` | For S3 publishing (v2) | AWS credentials and destination. |
| `LOG_LEVEL` | No | `INFO` (default), `DEBUG`, etc. |

### Pipeline config

`pipeline.config.toml` controls folder naming conventions, sheet column headers, and per-command tuning knobs. Defaults are sensible — you usually only edit `[csv]` (sheet column headers) and `[paths.input]` (Drive subfolder names) when adapting to a new project.

Key sections:

```toml
[csv]
tab_name              = "products"
sku_column            = "sku"
name_column           = "name"
supplier_column       = "supplier"
parent_product_column = "parent product"
supplier_ref_column   = "supplier ref"

[drive]
# "supplier" → root / <supplier> / <sku>   (default)
# "flat"     → root / <sku>
structure = "supplier"

[paths.input]
product_photos        = "photos"
lifestyle_photos      = "lifestyle"
thumbnails_website    = "thumbnails/website_thumbnail"
thumbnails_system     = "thumbnails/system_thumbnail"
videos                = "videos"
diagram               = "diagram"
models_dwg            = "models/dwg"
models_obj            = "models/obj"
models_gltf           = "models/gltf"
models_skp            = "models/skp"
assembly_instructions = "assembly_instructions"
carton_layout         = "carton_layout"
barcode               = "barcode"

[diagnose]   report_tab = "Diagnose Report"
[lifestyle]  report_tab = "Lifestyle Rename"
[models]     report_tab = "Models Report"

[scaffold]
report_tab        = "Scaffold Report"
moved_folder_name = "MOVED_FOLDER"
typo_cutoff       = 0.65

[optimize]
# Photos
target_size           = 2000
target_padding_pct    = 8
white_threshold       = 245
jpg_quality           = 85
max_file_mb           = 2.0
output_subdir_suffix  = "_optimized"
report_tab            = "Optimize Report"

# 3D models
model_dest_subdir         = "models_optimized"
model_target_texture_px   = 1024
model_decim_target_fine   = 0.25
model_decim_target_med    = 0.55
model_decim_target_coarse = 0.85
model_decim_max_stretch   = 0.10
model_unit_name           = "millimeter"
model_unit_meter          = 0.001
model_up_axis             = "Z_UP"
```

### Google OAuth setup

The pipeline authenticates to Google as **you** using OAuth (Desktop-app flow). Files created or modified by the CLI are owned by your Google account, which means the regular My Drive quota applies and there's no need for a Shared Drive. The first run opens a browser tab to authorize; the refresh token is then cached so subsequent runs are silent.

#### Step 1 — Create a Google Cloud project (skip if you already have one)

1. Go to [console.cloud.google.com](https://console.cloud.google.com/).
2. Click the project dropdown → **New Project** → name it (e.g. `asset-pipeline`) → **Create**.

#### Step 2 — Enable the required APIs

In the left sidebar go to **APIs & Services → Library** and enable:

- **Google Sheets API**
- **Google Drive API**

#### Step 3 — Configure the OAuth consent screen

1. **APIs & Services → OAuth consent screen**.
2. User type: **External**, click **Create**.
3. Fill in app name (e.g. `Asset Pipeline`), support email, developer email → **Save and continue** through the rest of the wizard.
4. Under **Test users**, add your own Google email so you can log in while the app is in "Testing" mode.

#### Step 4 — Create the OAuth client

1. **APIs & Services → Credentials → + Create credentials → OAuth client ID**.
2. **Application type: Desktop app**, give it a name → **Create**.
3. Click **Download JSON** on the new credential.
4. Save it as `.secrets/oauth_client.json` in the project root (gitignored).

#### Step 5 — Share the resources with your Google account

Make sure your Google account (the one you'll authorize with) has at least **Editor** access on:

- The master Google Sheet
- The products Drive folder (`GOOGLE_DRIVE_ROOT_FOLDER_ID`)
- The lifestyle / models folders, if you'll use those commands

If you own the resources, you already have access.

#### Step 6 — First run

```bash
uv run asset init
```

A browser tab opens asking you to log in and approve the requested scopes (Drive + Sheets read/write). After you click **Allow**, the token is written to `.secrets/oauth_token.json` and every subsequent run is silent. The token auto-refreshes whenever it expires.

To re-authenticate (e.g. switch accounts), delete `.secrets/oauth_token.json` and run `asset init` again.

---

## Google Drive folder structure

The pipeline expects the products root to be organised by supplier:

```
<Drive root folder>/
└── <Supplier>/
    └── <SKU>/
        ├── photos/
        ├── lifestyle/
        ├── thumbnails/
        │   ├── website_thumbnail/
        │   └── system_thumbnail/
        ├── videos/
        ├── diagram/
        ├── models/
        │   ├── dwg/
        │   ├── obj/
        │   ├── gltf/
        │   └── skp/
        ├── assembly_instructions/
        ├── carton_layout/
        └── barcode/
```

Use `[drive] structure = "flat"` if your layout has SKUs directly under the root (no supplier level).

The `scaffold` command will create any missing folders (and create missing SKU folders from the sheet). Subfolder names come from `[paths.input]` and can be renamed there without changing any code.

---

## Pipeline workflow

The recommended order for a clean run from scratch:

```
 ┌──────────┐    ┌────────┐    ┌────────┐    ┌──────────┐
 │ Diagnose │ -> │ Rename │ -> │ Dedupe │ -> │ Scaffold │
 └──────────┘    └────────┘    └────────┘    └──────────┘
                                                  │
                                                  v
                            ┌──────────────┐    ┌──────────────────┐
                            │  Optimize    │ <- │ Upload missing   │
                            │  (photos +   │    │ files            │
                            │   models)    │    └──────────────────┘
                            └──────────────┘
```

The full intended order is:

| # | Stage | Command | Status | What it does |
|---|---|---|---|---|
| 1 | **Diagnose** | `asset diagnose` | available | Audit Drive vs sheet, write report with `Suggested Rename` (orphans) and `Suggested Action` (duplicates) columns |
| 2 | **Rename** | `asset rename` | available | Apply the report's `Suggested Rename` column to rename orphan folders to their canonical SKU names |
| 3 | **Deduplicate** | `asset dedupe` | available | Apply the report's `Suggested Action` column — DELETE redundant duplicates, MERGE folders with unique content |
| 4 | **Scaffold** | `asset scaffold [--fix] [--clean] [--move]` | available | Create missing SKUs from sheet, ensure canonical subdir structure, fix loose files / typos, clean junk |
| 5 | **Upload missing files** | `asset upload-local-files --type X --input ./dir` | available | Match local files to SKUs (filename / supplier ref / name / PDF content / fuzzy) and upload to the right subdir |
| 6 | **Generate** | — | planned | Generate derived assets (thumbnails, barcodes, system thumbnails, PDF catalog) |
| 7 | **Organize** | — | planned | Enumerate and rename within each SKU's subfolders to the canonical numbering scheme |
| 8 | **Optimize** | `asset optimize --type {photo, model}` | available | Standardise photos (format/dimensions/padding/background/file size); decimate + re-export 3D models as OBJ/GLB/DAE |

> **Re-run `diagnose` between stages.** Almost every other command operates on the diagnose report or on the live Drive structure, so refreshing the report after each step keeps decisions accurate.

Special-purpose commands (run as needed, not part of the linear pipeline):

| Command | Purpose |
|---|---|
| `asset rename-lifestyle-photos` | Map a separate "lifestyle photos" folder (named by parent product) into each SKU's `lifestyle/` subdir |
| `asset copy-models-into-products` | Pull a shared 3D models folder into each SKU's `models/<type>/` and `diagram/` subdirs (PDF datasheets routed to diagram) |

---

## CLI reference

All commands run via `uv run asset <command>` from the repo root (or just `asset <command>` if the venv is activated).

```bash
uv run asset --help
```

Common conventions:

- Almost every command has a **dry-run mode** (no flag) and an **execute mode** (`--execute`). Dry runs always write a sheet report you can review before applying.
- IDs are read from `.env` automatically — most invocations need no arguments beyond the optional `--execute` and any per-command filters.
- Reports are written to specific tabs (`Diagnose Report`, `Lifestyle Rename`, `Models Report`, etc.) — see `pipeline.config.toml` to rename them.

---

### `asset init`

Verifies OAuth credentials and resource access. On the first run it triggers the browser flow.

```bash
uv run asset init
```

---

### `asset diagnose`

Compares Drive against the sheet and writes a row-per-folder report:

```bash
uv run asset diagnose
```

**Report columns**: `SKU | Supplier | Status | isDuplicate | Suggested Rename | Suggested Action | Issues | <13 dir-count columns>`

**Status values**: `OK`, `INCOMPLETE` (subdir missing), `MISSING DIR` (sheet SKU has no folder), `ORPHAN DIR` (folder has no sheet SKU).

**Duplicate detection**: when the same SKU folder name appears in multiple locations (cross-supplier or within-parent), the most-structured occurrence is treated as the primary; secondaries get `isDuplicate=TRUE`. The duplicate's content is compared to the primary using a tiered heuristic that auto-suggests `DELETE` (identical content under different filenames or re-encoded with same dimensions) or `MERGE` (some unique content). The `Issues` column always names the tier that fired.

**Suggested Rename**: only on `ORPHAN DIR` rows — fuzzy-matches against `MISSING DIR` SKUs to suggest the canonical name.

---

### `asset rename`

Applies the `Suggested Rename` column from the diagnose report.

```bash
uv run asset rename                # dry run — prints rename table
uv run asset rename --execute      # rename folders in Drive
```

Skips duplicate rows (resolve them with `dedupe` first), skips collisions (two sources targeting the same name, or a target that already exists in Drive). Warns about anything skipped.

---

### `asset dedupe`

Applies the `Suggested Action` column from the diagnose report.

```bash
uv run asset dedupe                # dry run — prints action table
uv run asset dedupe --execute      # apply DELETE/MERGE
```

- `DELETE` trashes the duplicate folder (recoverable from Drive's bin).
- `MERGE` recursively copies any files in the duplicate that aren't already present at the same relative path in the primary, then trashes the duplicate.

---

### `asset scaffold`

Creates missing SKU folders from the sheet and ensures the canonical subdir structure inside every SKU. Optional flags handle loose files, typos, and clutter.

```bash
uv run asset scaffold                            # dry run, baseline (just create-missing plan)
uv run asset scaffold --fix                      # + loose-file routing + typo fixes
uv run asset scaffold --clean                    # + delete junk + delete non-canonical dirs
uv run asset scaffold --clean --move             # + quarantine non-canonical dirs in MOVED_FOLDER
uv run asset scaffold --fix --clean --move --execute   # apply everything
```

Flag effects:

| Flag | Action types added |
|------|------|
| (none) | `CREATE_SKU`, `CREATE_SUBDIR` |
| `--fix` | + `RENAME_DIR` (typo/case fixes), `MOVE_FILE` (images → photos/, PDFs → diagram/assembly_instructions/carton_layout based on filename keywords), `DUPLICATE_DIR` (informational) |
| `--clean` | + `DELETE_FILE` (.DS_Store, Thumbs.db, desktop.ini), `DELETE_DIR` (any non-canonical subfolder) |
| `--clean --move` | replaces `DELETE_DIR` with `MOVE_DIR` → `MOVED_FOLDER/<orig_dir>/<sku>/` |
| `--execute` | actually apply (otherwise dry-run + report only) |

Execution order is deterministic: `CREATE_SKU` → `RENAME_DIR` → `CREATE_SUBDIR` → `MOVE_FILE` → `DELETE_FILE` → `MOVE_DIR`/`DELETE_DIR`.

Typo detection uses lowercased difflib similarity ≥ `scaffold.typo_cutoff` (default 0.65) against canonical top-level dir names — fires only when the canonical name doesn't already exist in the SKU.

---

### `asset upload-local-files`

Generic local-file uploader with smart SKU matching.

```bash
# Dry run — scan, infer SKU per file, write report to "Upload - photo" tab
uv run asset upload-local-files --input ./photos --type photo

# Edit the tab to fix any wrong SKUs, then:
uv run asset upload-local-files --input ./photos --type photo --execute
```

**Type aliases** map to subdirectories from `[paths.input]`:

```
photo / photos / product_photos  → product_photos
lifestyle / lifestyle_photos     → lifestyle_photos
video / videos                   → videos
diagram                          → diagram
assembly / assembly_instructions → assembly_instructions
carton / carton_layout           → carton_layout
barcode                          → barcode
obj / skp / dwg / gltf           → models_*
thumbnails_website / thumbnails_system → respective paths
```

**Matching strategy** (first hit wins, decision recorded in the `Match Decision` column):

| # | Strategy | Confidence |
|---|---|---|
| 1 | Filename contains exact SKU | HIGH |
| 2 | Filename contains exact supplier ref | HIGH |
| 3 | Filename contains exact product name | HIGH |
| 4 | Filename has ≥70% of name tokens (e.g. `Mansa Crest Leather Sofa Reel.mp4` matches `Crest Leather Sofa`) | MEDIUM |
| 5 | Filename has ≥70% of supplier-ref tokens | MEDIUM |
| 6 | PDF text contains SKU/ref/name | MEDIUM |
| 7 | Fuzzy filename ≈ SKU / ref / name (highest scoring) | LOW |
| 8 | None | NONE |

Pass `--supplier <name>` to restrict matching to one supplier's SKUs (faster, fewer false positives).

The report's `Destination SKU` column is the only one that drives upload behavior on `--execute` — edit a SKU, leave it blank to skip, or trust the inferred value. Already-uploaded files are skipped (idempotent).

---

### `asset optimize`

Standardises product photos *or* 3D models, dispatched by `--type`.

```bash
# Photos: dry-run report
uv run asset optimize --type photo
# Photos: execute
uv run asset optimize --type photo --execute

# 3D models: dry-run report
uv run asset optimize --type model
# 3D models: execute
uv run asset optimize --type model --execute

# Filters work for both
uv run asset optimize --type photo --supplier mansa --execute
uv run asset optimize --type model --sku sofa-crest --execute
```

See [Image optimization details](#image-optimization-details) and [3D model optimization details](#3d-model-optimization-details) below.

---

### `asset rename-lifestyle-photos`

Maps a separate lifestyle-photos folder (named by parent product, not SKU) into each SKU's `lifestyle/` subdir. The first SKU per parent product is the destination.

```bash
uv run asset rename-lifestyle-photos                # dry run + report
uv run asset rename-lifestyle-photos --execute      # copy files
```

Idempotent — files already at the destination by name are skipped. Only image extensions are copied (`.jpg`, `.png`, `.webp`, etc.); `.DS_Store` and other non-images are filtered out.

---

### `asset copy-models-into-products`

Pulls a shared 3D models folder into each SKU's product folder. Compares against the products drive (not the sheet) and creates orphan SKU folders if necessary.

```bash
uv run asset copy-models-into-products              # dry run + report
uv run asset copy-models-into-products --execute    # copy files
```

Source layout (per SKU, with case-insensitive matching and typo recovery for `GITF` → `gltf`):

```
<models_root>/<supplier>/<sku>/
├── OBJ/   → copied to <products>/<supplier>/<sku>/models/obj/
├── SKP/   → models/skp/
├── DWG/   → models/dwg/
├── CAD/   → models/dwg/    (same as DWG)
├── GLTF/  → models/gltf/
└── PDF/   → diagram/        (PDFs go to /diagram, not models/pdf)
```

Also supports a nested layout where the type folders live inside an inner `models/` subdir of the SKU. The report flags missing types, unexpected items, orphan SKUs, and SKUs in products with no matching models folder.

---

## Image optimization details

`asset optimize --type photo` reads photos from `<sku>/photos/`, applies a standardisation pipeline, and writes optimized JPEGs to `<sku>/photos_optimized/`. Originals are preserved.

### Dry run report

The dry run downloads each image, analyzes it, and writes a report to the `Optimize Report` tab with embedded image previews. Slow (~1-2 s per image) but informative. Columns:

| Column | Content |
|---|---|
| SKU / Supplier | identifies the product |
| File | original filename |
| Preview | `=IMAGE(...)` formula pulling a 100×100 thumbnail from Drive |
| Format | `PNG → JPG` |
| Dimensions | `3000×2400 → 2000×2000` |
| Aspect | `1:1` / `tall (3:4)` / `wide (16:9)` |
| File Size | `4.2MB → ≤2.0MB` |
| Padding | `12% → 8%` (avg of 4 margins) |
| Background | `Clean (98% pure white)` / `Subtle off-white (62% pure / 95% near)` / `Has background (45% near-white)` |
| Actions | semicolon-list of planned operations |

To make previews legible after the first write, manually bump the report tab's row height to ~110px (Sheets remembers it).

### What the pipeline does

| Step | Purpose |
|------|---------|
| 1. RGB conversion | flattens any alpha channel onto a white background |
| 2. Background cleanup | clamps every pixel with all RGB ≥ `white_threshold` (245) to pure `(255,255,255)` — fixes "subtle off-white" |
| 3. Product bbox detection | finds the bounding box of non-white pixels |
| 4. Square canvas | crops to product, pads to a square white canvas with the longest side at `(1 - 2*target_padding_pct/100)` of the canvas |
| 5. Resize | LANCZOS resize to `target_size × target_size` (default 2000) |
| 6. JPEG encode | quality 85, then auto-drops to 80/75/.../50 until the file fits under `max_file_mb` (2.0) |

Re-runs are idempotent — files already in the destination are skipped.

### Background classification

Uses two thresholds:

- **Pure white** = pixels exactly `(255,255,255)`
- **Near white** = pixels with all RGB ≥ `white_threshold` (245)

Status lines on the report:

- `Clean (X% pure white)` — both percentages high and similar
- `Subtle off-white (X% pure / Y% near)` — near-white high, pure-white materially lower → cleanup will clamp to pure white
- `Has background (X% near-white)` — near-white < 80% → there's actual non-white content beyond the product

### Not in v1

- Color/saturation/white-balance correction (yellowing) — auto-correction can make things worse without scene knowledge
- Background removal of non-white backgrounds — `rembg` is in deps and could plug in as a `--remove-bg` mode

---

## 3D model optimization details

`asset optimize --type model` runs a 5-stage pipeline ported from `3d-model-optimization/`. It takes a heavy `.obj` exported from Rhino (or any DCC tool that produces over-tessellated NURBS-derived meshes), shrinks it 60-80% without visible loss, and emits both a Collada `.dae` (for SketchUp import) and a `.glb` (for web/three.js).

Sources from `<sku>/models/obj/`. Output goes to `<sku>/models_optimized/`.

### Dry-run report (no OBJ download)

Light-weight: only the (small) MTL is downloaded; texture stats come from Drive metadata. Columns:

| Column | Content |
|---|---|
| SKU / Supplier | identifies the product |
| OBJ File | filename |
| OBJ Size | current size in MB |
| Materials | count from MTL |
| Textures | count of texture files in the OBJ folder |
| Texture Size | total bytes of textures |
| Duplicates | groups of byte-identical textures (e.g. `fabric.jpg=fabric_1.jpg`) |
| Oversized | textures over 1024 px (with current dimensions) |
| Unused | textures present in folder but not referenced by any material in MTL |
| Actions | concise list of planned operations |

### Pipeline stages

| Stage | Purpose | Notes |
|---|---|---|
| 1. Sanitize | rename materials (strip parens/spaces — Collada XML doesn't allow them), dedupe textures by md5 | Rhino exports often have `Plastic (2)`, duplicated texture files |
| 2. Split by material | streaming parser splits the master OBJ into one self-contained sub-OBJ per material | Necessary so each piece fits comfortably in RAM during decimation |
| 3. Decimate | per-mesh **adaptive** quadric edge collapse with bridging-triangle sanity check | Two safety mechanisms; see below |
| 4. Merge | concat decimated sub-OBJs back into one OBJ + MTL with proper material grouping and texture references | |
| 5a. Resize textures | downscale any texture whose longer edge exceeds `model_target_texture_px` (1024) | Often saves more bytes than geometry decimation |
| 5b. Export GLB | `trimesh.Scene.export()` — small, web-friendly | |
| 5c. Export DAE | hand-built `pycollada` scene (images → effects → materials → geometries → scene nodes) for SketchUp | |

The **adaptive decimation** uses median + p99 edge-length ratios (relative to bbox diagonal) to decide how aggressively to reduce each sub-mesh:

| Median edge ratio | Action |
|---|---|
| `> 0.005` (0.5%) | Pass through — already coarse |
| `0.005 – 0.01` | Mild reduction (55% retention) |
| `< 0.005` | Aggressive reduction (25% retention) |
| Plus: p99 edge > 5% of bbox | Pass through regardless |

The **stretched-triangle sanity check** measures the longest output edge after decimation and rejects the result (passing the original through unchanged) if it exceeds `model_decim_max_stretch` (10%) of the bounding box diagonal — this catches edges that bridge across implicit feature boundaries (cushion seams, panel separations) and would visibly tear the model.

### After execute — SketchUp final step

```
1. Open SketchUp → File → Import…
2. Filter: COLLADA File (*.dae)
3. Select <sku>/models_optimized/model.dae
4. After import, File → Save As… → SketchUp File (.skp)
```

Make sure the textures (`*.jpeg`) are in the same folder as the `.dae` — SketchUp resolves them by relative path.

### Dependencies (already in the SDK)

`fast-simplification` (pure-C++ decimator, no GL/Qt), `trimesh`, `pycollada`, `numpy`, `Pillow`.

---

## Development

### Project layout

```
packages/db/
  alembic/                        Alembic migration environment + versions
  src/asset_db/
    models.py                     SQLAlchemy ORM models (typed Mapped[...] style)
    session.py                    Async session factory

packages/sdk/
  src/asset_sdk/
    config.py                     PipelineConfig dataclasses
    adapters/
      drive.py                    OAuth, list/move/copy/upload/download
      sheets.py                   gspread wrapper
    stages/
      diagnose.py                 Drive vs sheet audit + duplicate analysis
      rename_skus.py              Apply Suggested Rename column
      dedupe.py                   Apply Suggested Action column (DELETE/MERGE)
      scaffold.py                 Create missing folders, fix loose files, clean clutter
      upload_local_files.py       Generic local-file uploader with fuzzy SKU match
      rename_lifestyle.py         Lifestyle-folder → SKU lifestyle/ subdir
      copy_models.py              Shared models drive → SKU models/<type>/ + diagram/
      optimize_photos.py          Photo standardisation pipeline
      optimize_models.py          5-stage 3D-model optimization pipeline

apps/cli/
  src/asset_cli/
    main.py                       Typer app — every CLI command lives here

apps/api/                         FastAPI service (v2)
apps/web/                         Next.js 15 front-end (v2)
```

### Common make targets

```bash
make up          # Start Postgres in Docker
make down        # Stop Postgres
make install     # uv sync + pnpm install
make migrate     # Apply pending Alembic migrations
make test        # pytest
make lint        # ruff check + mypy
make format      # ruff format + ruff --fix
make psql        # psql shell on the dev database
make clean       # Stop containers, clear caches
```

### Adding a migration

After changing models in `packages/db/src/asset_db/models.py`:

```bash
make migration NAME=add_foo_column
# Review the generated file in packages/db/alembic/versions/
make migrate
```
