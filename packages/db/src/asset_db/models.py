"""SQLAlchemy ORM models for the asset pipeline.

These are the persistence-layer types. The SDK exposes its own domain models
(see asset_sdk.models) and translates at the repo boundary so consumers don't
depend on SQLAlchemy session lifecycle.
"""
from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# --- Enums ---

class EntityKind(str, enum.Enum):
    product = "product"
    category = "category"
    designer = "designer"
    collection = "collection"


class AssetKind(str, enum.Enum):
    product_photo = "product_photo"
    lifestyle_photo = "lifestyle_photo"
    website_thumbnail = "website_thumbnail"
    system_thumbnail = "system_thumbnail"
    video = "video"
    diagram = "diagram"
    model_dwg = "model_dwg"
    model_obj = "model_obj"
    model_gltf = "model_gltf"
    model_skp = "model_skp"
    assembly_instructions = "assembly_instructions"
    carton_layout = "carton_layout"
    barcode = "barcode"


class AssetStatus(str, enum.Enum):
    pending = "pending"
    ok = "ok"
    failed = "failed"
    sync_pending = "sync_pending"


class RunStatus(str, enum.Enum):
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    partial = "partial"


class StageStatus(str, enum.Enum):
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    skipped = "skipped"


# --- Core tables ---

class Entity(Base):
    __tablename__ = "entities"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    kind: Mapped[EntityKind] = mapped_column(Enum(EntityKind), nullable=False)
    sku: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    name: Mapped[str | None] = mapped_column(Text)
    supplier: Mapped[str | None] = mapped_column(String(128))
    csv_metadata: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    assets: Mapped[list[Asset]] = relationship(back_populates="entity", cascade="all, delete-orphan")
    draft: Mapped[EntityDraft | None] = relationship(back_populates="entity", uselist=False)


class Asset(Base):
    __tablename__ = "assets"
    __table_args__ = (
        UniqueConstraint("entity_id", "kind", "sequence", name="uq_assets_entity_kind_seq"),
        Index("ix_assets_entity_id", "entity_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    entity_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("entities.id", ondelete="CASCADE"), nullable=False
    )
    kind: Mapped[AssetKind] = mapped_column(Enum(AssetKind), nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    source_path: Mapped[str | None] = mapped_column(Text)
    dest_path: Mapped[str | None] = mapped_column(Text)
    source_hash: Mapped[str | None] = mapped_column(String(64))
    dest_hash: Mapped[str | None] = mapped_column(String(64))
    width: Mapped[int | None] = mapped_column(Integer)
    height: Mapped[int | None] = mapped_column(Integer)
    format: Mapped[str | None] = mapped_column(String(16))
    bytes: Mapped[int | None] = mapped_column(BigInteger)
    status: Mapped[AssetStatus] = mapped_column(
        Enum(AssetStatus), default=AssetStatus.pending, nullable=False
    )
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    asset_metadata: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSONB, default=dict
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    entity: Mapped[Entity] = relationship(back_populates="assets")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[RunStatus] = mapped_column(
        Enum(RunStatus), default=RunStatus.running, nullable=False
    )
    triggered_by: Mapped[str | None] = mapped_column(String(32))
    source_config: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    dest_config: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    diff_date_used: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stages_run: Mapped[list[str]] = mapped_column(ARRAY(String), default=list)
    summary: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    stage_runs: Mapped[list[StageRun]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


class StageRun(Base):
    __tablename__ = "stage_runs"
    __table_args__ = (Index("ix_stage_runs_run_id", "run_id"),)

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    stage: Mapped[str] = mapped_column(String(64), nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[StageStatus] = mapped_column(
        Enum(StageStatus), default=StageStatus.running, nullable=False
    )
    metrics: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)

    run: Mapped[PipelineRun] = relationship(back_populates="stage_runs")


class DiagnosticFinding(Base):
    __tablename__ = "diagnostic_findings"
    __table_args__ = (
        Index("ix_diagnostic_findings_run_id", "run_id"),
        Index("ix_diagnostic_findings_entity_id", "entity_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False
    )
    entity_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("entities.id", ondelete="CASCADE"), nullable=False
    )
    asset_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("assets.id", ondelete="CASCADE")
    )
    check_name: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    dismissed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolved_by_run: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("pipeline_runs.id")
    )


class PhotoOrder(Base):
    """Saved photo order for a SKU's product_photos folder.

    Written by the `organize` web UI after a drag-and-drop reorder, then read
    by `asset organize --rename --execute` to rename files in Drive sequentially.
    """

    __tablename__ = "photo_orders"

    sku: Mapped[str] = mapped_column(String(128), primary_key=True)
    asset_kind: Mapped[str] = mapped_column(
        String(32), primary_key=True, default="product_photo"
    )
    # [{"file_id": "...", "name": "..."}] in display order (position = index).
    items: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)
    saved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class EntityDraft(Base):
    """Pending UI changeset for an entity. MVP: single-user, one draft per entity."""

    __tablename__ = "entity_drafts"

    entity_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("entities.id", ondelete="CASCADE"),
        primary_key=True,
    )
    ops: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, default=list)
    entity_version_at_load: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    entity: Mapped[Entity] = relationship(back_populates="draft")
