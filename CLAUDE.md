# Asset Optimization Pipeline

A monorepo for an asset optimization pipeline. Single-user, runs locally.

## Architecture

- `packages/sdk` — Python SDK (Pillow, rembg, reportlab). Storage adapters,
  pipeline orchestrator, image/doc ops, repo layer. Domain models separate
  from DB models.
- `packages/db` — SQLAlchemy models + Alembic migrations. Postgres 16.
- `apps/cli` — Typer CLI. Imports SDK directly.
- `apps/api` — FastAPI. Imports SDK directly. Web app talks only to this.
- `apps/web` — Next.js 15 App Router. Reads via generated TS client.

uv workspace ties Python packages together; Turborepo handles JS.

## Pipeline stages (run in order)

1. `init` — pull master CSV from Google Sheets, upsert entities
2. `scaffold` — create per-product directories in source storage
3. `inventory` — scan source, classify by subfolder, hash, diff
4. `diagnose` — standards checks, write findings, emit review CSV
5. (human review of CSV)
6. `remediate` — bulk image fixes from reviewed CSV
7. `organize` — enumeration, both-side renames
8. `publish` — push canonical assets to S3
9. `finalize` — bump diff_date, mark run complete

`derive` (thumbnails, barcodes, PDF catalog, CMYK) is v2.

## Key decisions

- Storage adapters: Local, GoogleDrive, S3 — same interface
- PathMapper outside adapters, translates input layout (supplier/sku/kind)
  to output layout (sku/kind)
- MD5 hashes for diff detection (parity with S3 ETags + Drive md5Checksum)
- CSV is authoritative for entity existence; storage is authoritative for
  asset existence
- Diagnostics config in pipeline.config.toml, keyed by asset kind
- Drafts: deferred-commit for reorder/rename only; single-asset edits
  bypass drafts (v2 feature anyway)
- Globally unique SKUs

## Build order

Step 1: repo skeleton + DB migrations running (DONE)
Step 2: storage adapters + repo layer
Step 3: Sheets adapter + init + scaffold stages
Step 4: inventory stage
Step 5: diagnose stage + report CSV
Step 6: remediate + organize + publish + finalize
Step 7: FastAPI + Next.js read-only views
Step 8: drafts + drag-to-reorder

## Conventions

- Python 3.12+, ruff, mypy strict, pytest
- SQLAlchemy 2.x typed style (Mapped[...])
- Async everywhere in SDK and API
- Domain models are dataclasses; ORM models stay in asset_db
- Each pipeline stage runs in its own DB transaction
- Stages must be idempotent