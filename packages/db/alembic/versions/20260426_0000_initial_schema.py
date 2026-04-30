"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-04-26 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Define the PG enum types with their values + create_type=False so SQLAlchemy doesn't
# try to (re-)create them when columns of these types are added to tables. We create
# them explicitly via op.execute below so the values are guaranteed correct.
ENTITY_KIND = postgresql.ENUM(
    "product", "category", "designer", "collection",
    name="entitykind", create_type=False,
)
ASSET_KIND = postgresql.ENUM(
    "product_photo", "lifestyle_photo", "website_thumbnail", "system_thumbnail",
    "video", "diagram", "model_dwg", "model_obj", "model_gltf", "model_skp",
    "assembly_instructions", "carton_layout", "barcode",
    name="assetkind", create_type=False,
)
ASSET_STATUS = postgresql.ENUM(
    "pending", "ok", "failed", "sync_pending",
    name="assetstatus", create_type=False,
)
RUN_STATUS = postgresql.ENUM(
    "running", "succeeded", "failed", "partial",
    name="runstatus", create_type=False,
)
STAGE_STATUS = postgresql.ENUM(
    "running", "succeeded", "failed", "skipped",
    name="stagestatus", create_type=False,
)


def upgrade() -> None:
    op.execute("CREATE TYPE entitykind AS ENUM ('product', 'category', 'designer', 'collection')")
    op.execute(
        "CREATE TYPE assetkind AS ENUM ("
        "'product_photo', 'lifestyle_photo', 'website_thumbnail', 'system_thumbnail', "
        "'video', 'diagram', 'model_dwg', 'model_obj', 'model_gltf', 'model_skp', "
        "'assembly_instructions', 'carton_layout', 'barcode')"
    )
    op.execute("CREATE TYPE assetstatus AS ENUM ('pending', 'ok', 'failed', 'sync_pending')")
    op.execute("CREATE TYPE runstatus AS ENUM ('running', 'succeeded', 'failed', 'partial')")
    op.execute("CREATE TYPE stagestatus AS ENUM ('running', 'succeeded', 'failed', 'skipped')")

    op.create_table(
        "entities",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("kind", ENTITY_KIND, nullable=False),
        sa.Column("sku", sa.String(128), nullable=False),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("supplier", sa.String(128), nullable=True),
        sa.Column("csv_metadata", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("sku"),
    )

    op.create_table(
        "assets",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("kind", ASSET_KIND, nullable=False),
        sa.Column("sequence", sa.Integer(), nullable=False),
        sa.Column("source_path", sa.Text(), nullable=True),
        sa.Column("dest_path", sa.Text(), nullable=True),
        sa.Column("source_hash", sa.String(64), nullable=True),
        sa.Column("dest_hash", sa.String(64), nullable=True),
        sa.Column("width", sa.Integer(), nullable=True),
        sa.Column("height", sa.Integer(), nullable=True),
        sa.Column("format", sa.String(16), nullable=True),
        sa.Column("bytes", sa.BigInteger(), nullable=True),
        sa.Column("status", ASSET_STATUS, nullable=False, server_default="pending"),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["entity_id"], ["entities.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("entity_id", "kind", "sequence", name="uq_assets_entity_kind_seq"),
    )
    op.create_index("ix_assets_entity_id", "assets", ["entity_id"])

    op.create_table(
        "pipeline_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", RUN_STATUS, nullable=False, server_default="running"),
        sa.Column("triggered_by", sa.String(32), nullable=True),
        sa.Column("source_config", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("dest_config", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("diff_date_used", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "stages_run",
            postgresql.ARRAY(sa.String()),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("summary", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "stage_runs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("stage", sa.String(64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", STAGE_STATUS, nullable=False, server_default="running"),
        sa.Column("metrics", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.ForeignKeyConstraint(["run_id"], ["pipeline_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_stage_runs_run_id", "stage_runs", ["run_id"])

    op.create_table(
        "diagnostic_findings",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("run_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("asset_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("check_name", sa.String(64), nullable=False),
        sa.Column("severity", sa.String(16), nullable=False),
        sa.Column("details", postgresql.JSONB(), nullable=False, server_default="{}"),
        sa.Column("dismissed", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_by_run", postgresql.UUID(as_uuid=True), nullable=True),
        sa.ForeignKeyConstraint(["asset_id"], ["assets.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["entity_id"], ["entities.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["resolved_by_run"], ["pipeline_runs.id"]),
        sa.ForeignKeyConstraint(["run_id"], ["pipeline_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_diagnostic_findings_run_id", "diagnostic_findings", ["run_id"])
    op.create_index("ix_diagnostic_findings_entity_id", "diagnostic_findings", ["entity_id"])

    op.create_table(
        "entity_drafts",
        sa.Column("entity_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("ops", postgresql.JSONB(), nullable=False, server_default="[]"),
        sa.Column("entity_version_at_load", sa.Integer(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["entity_id"], ["entities.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("entity_id"),
    )


def downgrade() -> None:
    op.drop_table("entity_drafts")
    op.drop_index("ix_diagnostic_findings_entity_id", table_name="diagnostic_findings")
    op.drop_index("ix_diagnostic_findings_run_id", table_name="diagnostic_findings")
    op.drop_table("diagnostic_findings")
    op.drop_index("ix_stage_runs_run_id", table_name="stage_runs")
    op.drop_table("stage_runs")
    op.drop_table("pipeline_runs")
    op.drop_index("ix_assets_entity_id", table_name="assets")
    op.drop_table("assets")
    op.drop_table("entities")
    op.execute("DROP TYPE stagestatus")
    op.execute("DROP TYPE runstatus")
    op.execute("DROP TYPE assetstatus")
    op.execute("DROP TYPE assetkind")
    op.execute("DROP TYPE entitykind")
