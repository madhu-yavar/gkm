from __future__ import annotations

import enum
from datetime import datetime, date

from sqlalchemy import JSON, Boolean, Date, DateTime, Enum, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class UserRole(str, enum.Enum):
    admin = "admin"
    analyst = "analyst"
    client_viewer = "client_viewer"


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[UserRole] = mapped_column(Enum(UserRole), index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class Client(Base):
    __tablename__ = "clients"
    __table_args__ = (UniqueConstraint("external_id", name="uq_clients_external_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    external_id: Mapped[str] = mapped_column(String(64), index=True)  # e.g. "CH"
    client_type: Mapped[str] = mapped_column(String(64), default="CPA")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    snapshots: Mapped[list["ClientSnapshot"]] = relationship(back_populates="client", cascade="all, delete-orphan")


class Snapshot(Base):
    __tablename__ = "snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_filename: Mapped[str] = mapped_column(String(512))
    as_of_date: Mapped[date] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    client_rows: Mapped[list["ClientSnapshot"]] = relationship(back_populates="snapshot", cascade="all, delete-orphan")
    staff_rows: Mapped[list["StaffSnapshot"]] = relationship(back_populates="snapshot", cascade="all, delete-orphan")
    pii_fields: Mapped[list["SnapshotPiiField"]] = relationship(back_populates="snapshot", cascade="all, delete-orphan")
    pii_token_mappings: Mapped[list["SnapshotPiiTokenMapping"]] = relationship(back_populates="snapshot", cascade="all, delete-orphan")
    schema_profile: Mapped["WorkbookSchemaProfile | None"] = relationship(back_populates="snapshot", cascade="all, delete-orphan", uselist=False)
    dashboard_proposal: Mapped["DashboardProposal | None"] = relationship(back_populates="snapshot", cascade="all, delete-orphan", uselist=False)
    analytics_bundle: Mapped["SnapshotAnalyticsBundle | None"] = relationship(back_populates="snapshot", cascade="all, delete-orphan", uselist=False)
    analysis_set_memberships: Mapped[list["AnalysisSetMember"]] = relationship(back_populates="snapshot", cascade="all, delete-orphan")


class ClientSnapshot(Base):
    __tablename__ = "client_snapshots"
    __table_args__ = (UniqueConstraint("snapshot_id", "client_id", name="uq_client_snapshot"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("snapshots.id", ondelete="CASCADE"), index=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.id", ondelete="CASCADE"), index=True)

    contracted_ind: Mapped[int] = mapped_column(Integer, default=0)
    contracted_bus: Mapped[int] = mapped_column(Integer, default=0)
    contracted_total: Mapped[int] = mapped_column(Integer, default=0)

    received_ind: Mapped[int] = mapped_column(Integer, default=0)
    received_bus: Mapped[int] = mapped_column(Integer, default=0)
    received_total: Mapped[int] = mapped_column(Integer, default=0)

    pending_ind: Mapped[int] = mapped_column(Integer, default=0)
    pending_bus: Mapped[int] = mapped_column(Integer, default=0)
    pending_total: Mapped[int] = mapped_column(Integer, default=0)

    snapshot: Mapped["Snapshot"] = relationship(back_populates="client_rows")
    client: Mapped["Client"] = relationship(back_populates="snapshots")


class StaffSnapshot(Base):
    __tablename__ = "staff_snapshots"
    __table_args__ = (UniqueConstraint("snapshot_id", "staff_external_id", name="uq_staff_snapshot"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("snapshots.id", ondelete="CASCADE"), index=True)

    name: Mapped[str] = mapped_column(String(255))
    staff_external_id: Mapped[str] = mapped_column(String(64))  # e.g. "TM"
    staff_type: Mapped[str] = mapped_column(String(64))  # e.g. "2 FTE"

    received_ind: Mapped[int] = mapped_column(Integer, default=0)
    received_bus: Mapped[int] = mapped_column(Integer, default=0)
    received_total: Mapped[int] = mapped_column(Integer, default=0)

    snapshot: Mapped["Snapshot"] = relationship(back_populates="staff_rows")


class WorkbookSchemaProfile(Base):
    __tablename__ = "workbook_schema_profiles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("snapshots.id", ondelete="CASCADE"), unique=True, index=True)
    schema_signature: Mapped[str] = mapped_column(String(64), index=True)
    workbook_type: Mapped[str] = mapped_column(String(64), index=True)
    profile_json: Mapped[dict] = mapped_column(JSON)
    source_filename: Mapped[str] = mapped_column(String(512))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    snapshot: Mapped["Snapshot"] = relationship(back_populates="schema_profile")
    proposals: Mapped[list["DashboardProposal"]] = relationship(back_populates="schema_profile")


class DashboardBlueprint(Base):
    __tablename__ = "dashboard_blueprints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    blueprint_key: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str] = mapped_column(Text)
    schema_signature: Mapped[str] = mapped_column(String(64), index=True)
    workbook_type: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(32), default="approved", index=True)
    config_json: Mapped[dict] = mapped_column(JSON)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    approved_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_by: Mapped["User | None"] = relationship(foreign_keys=[created_by_user_id])
    approved_by: Mapped["User | None"] = relationship(foreign_keys=[approved_by_user_id])
    proposals: Mapped[list["DashboardProposal"]] = relationship(back_populates="matched_blueprint", foreign_keys="DashboardProposal.matched_blueprint_id")
    approved_proposals: Mapped[list["DashboardProposal"]] = relationship(back_populates="approved_blueprint", foreign_keys="DashboardProposal.approved_blueprint_id")


class DashboardProposal(Base):
    __tablename__ = "dashboard_proposals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("snapshots.id", ondelete="CASCADE"), unique=True, index=True)
    schema_profile_id: Mapped[int] = mapped_column(ForeignKey("workbook_schema_profiles.id", ondelete="CASCADE"), index=True)
    matched_blueprint_id: Mapped[int | None] = mapped_column(ForeignKey("dashboard_blueprints.id", ondelete="SET NULL"), index=True, nullable=True)
    approved_blueprint_id: Mapped[int | None] = mapped_column(ForeignKey("dashboard_blueprints.id", ondelete="SET NULL"), index=True, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    match_mode: Mapped[str] = mapped_column(String(32), default="inferred")
    confidence_score: Mapped[float] = mapped_column(Float, default=0.0)
    title: Mapped[str] = mapped_column(String(255))
    summary: Mapped[str] = mapped_column(Text)
    rationale: Mapped[str] = mapped_column(Text)
    proposal_json: Mapped[dict] = mapped_column(JSON)
    created_by_system: Mapped[str] = mapped_column(String(64), default="schema_profiler")
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    approved_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    snapshot: Mapped["Snapshot"] = relationship(back_populates="dashboard_proposal")
    schema_profile: Mapped["WorkbookSchemaProfile"] = relationship(back_populates="proposals")
    matched_blueprint: Mapped["DashboardBlueprint | None"] = relationship(back_populates="proposals", foreign_keys=[matched_blueprint_id])
    approved_blueprint: Mapped["DashboardBlueprint | None"] = relationship(back_populates="approved_proposals", foreign_keys=[approved_blueprint_id])
    created_by: Mapped["User | None"] = relationship(foreign_keys=[created_by_user_id])
    approved_by: Mapped["User | None"] = relationship(foreign_keys=[approved_by_user_id])


class SnapshotAnalyticsBundle(Base):
    __tablename__ = "snapshot_analytics_bundles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("snapshots.id", ondelete="CASCADE"), unique=True, index=True)
    blueprint_id: Mapped[int | None] = mapped_column(ForeignKey("dashboard_blueprints.id", ondelete="SET NULL"), index=True, nullable=True)
    proposal_id: Mapped[int | None] = mapped_column(ForeignKey("dashboard_proposals.id", ondelete="SET NULL"), index=True, nullable=True)
    bundle_version: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(32), default="ready", index=True)
    generation_mode: Mapped[str] = mapped_column(String(32), default="fallback")
    stale: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    payload_json: Mapped[dict] = mapped_column(JSON)
    diagnostics_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    snapshot: Mapped["Snapshot"] = relationship(back_populates="analytics_bundle")
    blueprint: Mapped["DashboardBlueprint | None"] = relationship(foreign_keys=[blueprint_id])
    proposal: Mapped["DashboardProposal | None"] = relationship(foreign_keys=[proposal_id])


class AppLog(Base):
    __tablename__ = "app_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_key: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    level: Mapped[str] = mapped_column(String(16), index=True)
    state: Mapped[str | None] = mapped_column(String(32), index=True, nullable=True)
    category: Mapped[str] = mapped_column(String(64), index=True)
    event: Mapped[str] = mapped_column(String(128), index=True)
    agent_name: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    workflow: Mapped[str | None] = mapped_column(String(128), nullable=True)
    tool_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    model_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    message: Mapped[str] = mapped_column(String(512))
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    request_path: Mapped[str | None] = mapped_column(String(255), nullable=True)
    snapshot_id: Mapped[int | None] = mapped_column(ForeignKey("snapshots.id", ondelete="SET NULL"), index=True, nullable=True)
    proposal_id: Mapped[int | None] = mapped_column(ForeignKey("dashboard_proposals.id", ondelete="SET NULL"), index=True, nullable=True)
    blueprint_id: Mapped[int | None] = mapped_column(ForeignKey("dashboard_blueprints.id", ondelete="SET NULL"), index=True, nullable=True)
    user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, index=True)

    snapshot: Mapped["Snapshot | None"] = relationship(foreign_keys=[snapshot_id])
    proposal: Mapped["DashboardProposal | None"] = relationship(foreign_keys=[proposal_id])
    blueprint: Mapped["DashboardBlueprint | None"] = relationship(foreign_keys=[blueprint_id])
    user: Mapped["User | None"] = relationship(foreign_keys=[user_id])


class AnalysisSet(Base):
    __tablename__ = "analysis_sets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    summary: Mapped[str] = mapped_column(Text)
    intent: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="draft", index=True)
    relationship_type: Mapped[str] = mapped_column(String(64), default="comparison", index=True)
    confidence_score: Mapped[float] = mapped_column(Float, default=0.0)
    proposal_json: Mapped[dict] = mapped_column(JSON)
    confirmed_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    confirmed_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    members: Mapped[list["AnalysisSetMember"]] = relationship(back_populates="analysis_set", cascade="all, delete-orphan")
    created_by: Mapped["User | None"] = relationship(foreign_keys=[created_by_user_id])
    confirmed_by: Mapped["User | None"] = relationship(foreign_keys=[confirmed_by_user_id])


class AnalysisSetMember(Base):
    __tablename__ = "analysis_set_members"
    __table_args__ = (UniqueConstraint("analysis_set_id", "snapshot_id", name="uq_analysis_set_member"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    analysis_set_id: Mapped[int] = mapped_column(ForeignKey("analysis_sets.id", ondelete="CASCADE"), index=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("snapshots.id", ondelete="CASCADE"), index=True)
    member_order: Mapped[int] = mapped_column(Integer, default=1)
    role_label: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_filename: Mapped[str] = mapped_column(String(512))
    as_of_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    workbook_type: Mapped[str | None] = mapped_column(String(64), nullable=True)

    analysis_set: Mapped["AnalysisSet"] = relationship(back_populates="members")
    snapshot: Mapped["Snapshot"] = relationship(back_populates="analysis_set_memberships")


class UploadedWorkbook(Base):
    __tablename__ = "uploaded_workbooks"
    __table_args__ = (UniqueConstraint("upload_token", name="uq_uploaded_workbooks_upload_token"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    upload_token: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    source_filename: Mapped[str] = mapped_column(String(512))
    stored_path: Mapped[str] = mapped_column(String(1024))
    workbook_family: Mapped[str] = mapped_column(String(64), index=True)
    family_label: Mapped[str] = mapped_column(String(128))
    family_mode: Mapped[str] = mapped_column(String(64))
    preview_json: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(32), default="ready", index=True)
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    created_by: Mapped["User | None"] = relationship(foreign_keys=[created_by_user_id])
    jobs: Mapped[list["DocumentProcessingJob"]] = relationship(back_populates="upload", cascade="all, delete-orphan")


class DocumentProcessingJob(Base):
    __tablename__ = "document_processing_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    upload_id: Mapped[int] = mapped_column(ForeignKey("uploaded_workbooks.id", ondelete="CASCADE"), index=True)
    snapshot_id: Mapped[int | None] = mapped_column(ForeignKey("snapshots.id", ondelete="SET NULL"), index=True, nullable=True)
    workbook_family: Mapped[str] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(32), default="queued", index=True)
    stage: Mapped[str] = mapped_column(String(64), default="queued")
    progress_percent: Mapped[int] = mapped_column(Integer, default=0)
    message: Mapped[str] = mapped_column(String(255), default="Queued")
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    pii_config_json: Mapped[list | None] = mapped_column(JSON, nullable=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    upload: Mapped["UploadedWorkbook"] = relationship(back_populates="jobs")
    snapshot: Mapped["Snapshot | None"] = relationship(foreign_keys=[snapshot_id])
    created_by: Mapped["User | None"] = relationship(foreign_keys=[created_by_user_id])


class SnapshotPiiField(Base):
    __tablename__ = "snapshot_pii_fields"
    __table_args__ = (
        UniqueConstraint("snapshot_id", "sheet_name", "section_key", "header_label", name="uq_snapshot_pii_field"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("snapshots.id", ondelete="CASCADE"), index=True)
    sheet_name: Mapped[str] = mapped_column(String(255))
    section_key: Mapped[str] = mapped_column(String(64), index=True)
    header_label: Mapped[str] = mapped_column(String(255))
    normalized_header: Mapped[str] = mapped_column(String(255), index=True)
    pii_type: Mapped[str] = mapped_column(String(32))
    masking_strategy: Mapped[str] = mapped_column(String(32), default="tokenize")
    selection_source: Mapped[str] = mapped_column(String(32), default="user_selected")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    updated_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    snapshot: Mapped["Snapshot"] = relationship(back_populates="pii_fields")
    created_by: Mapped["User | None"] = relationship(foreign_keys=[created_by_user_id])
    updated_by: Mapped["User | None"] = relationship(foreign_keys=[updated_by_user_id])
    token_mappings: Mapped[list["SnapshotPiiTokenMapping"]] = relationship(back_populates="pii_field")


class SnapshotPiiTokenMapping(Base):
    __tablename__ = "snapshot_pii_token_mappings"
    __table_args__ = (
        UniqueConstraint("snapshot_id", "masked_token", name="uq_snapshot_pii_masked_token"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("snapshots.id", ondelete="CASCADE"), index=True)
    pii_field_id: Mapped[int | None] = mapped_column(ForeignKey("snapshot_pii_fields.id", ondelete="SET NULL"), index=True, nullable=True)
    pii_type: Mapped[str] = mapped_column(String(32))
    masking_strategy: Mapped[str] = mapped_column(String(32), default="tokenize")
    source_sheet_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_section_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_header_label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    original_value: Mapped[str] = mapped_column(Text)
    original_value_hash: Mapped[str] = mapped_column(String(64), index=True)
    masked_token: Mapped[str] = mapped_column(String(64))
    created_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), index=True, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    snapshot: Mapped["Snapshot"] = relationship(back_populates="pii_token_mappings")
    pii_field: Mapped["SnapshotPiiField | None"] = relationship(back_populates="token_mappings")
    created_by: Mapped["User | None"] = relationship(foreign_keys=[created_by_user_id])
