from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app import models
from app.app_logs import log_app_event
from app.dashboard_orchestrator import run_dashboard_orchestrator
from app.gemini_reasoning import GeminiRequestSettings
from app.ingest_excel import PreviewSheet, preview_contracted_vs_actual_xlsx
from app.raw_data_store import load_or_extract_snapshot_raw_tables
from app.settings import settings
from app.workbook_families import detect_workbook_family_from_profile


logger = logging.getLogger(__name__)


def _normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _profile_dict_from_preview(sheets: list[PreviewSheet], source_filename: str) -> dict[str, Any]:
    return {
        "source_filename": source_filename,
        "sheets": [
            {
                "sheet_name": sheet.sheet_name,
                "sections": [
                    {
                        "section_key": section.section_key,
                        "section_label": section.section_label,
                        "header_row": section.header_row,
                        "fields": [
                            {
                                "column": field.column,
                                "header_label": field.header_label,
                                "normalized_header": _normalize(field.header_label),
                                "sample_value": field.sample_value,
                                "suggested_pii_type": field.suggested_pii_type,
                            }
                            for field in section.headers
                        ],
                    }
                    for section in sheet.sections
                ],
            }
            for sheet in sheets
        ],
    }


def _schema_signature(profile: dict[str, Any]) -> str:
    compact = {
        "sheets": [
            {
                "sheet_name": sheet["sheet_name"],
                "sections": [
                    {
                        "section_key": section["section_key"],
                        "fields": [field["normalized_header"] for field in section["fields"]],
                    }
                    for section in sheet["sections"]
                ],
            }
            for sheet in profile["sheets"]
        ]
    }
    raw = json.dumps(compact, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _infer_workbook_type(profile: dict[str, Any]) -> str:
    return detect_workbook_family_from_profile(profile)


def _storage_candidates(snapshot: models.Snapshot) -> list[Path]:
    storage = Path(settings.storage_dir).resolve()
    candidates = [
        storage / f"upload_{snapshot.source_filename}",
        storage / f"preview_{snapshot.source_filename}",
        Path(snapshot.source_filename),
    ]
    candidates.extend(sorted(storage.glob(f"upload_*_{snapshot.source_filename}"), reverse=True))
    candidates.extend(sorted(storage.glob(f"preview_*_{snapshot.source_filename}"), reverse=True))
    seen: set[Path] = set()
    unique: list[Path] = []
    for path in candidates:
        if path in seen:
            continue
        seen.add(path)
        unique.append(path)
    return unique


def ensure_schema_profile(
    db: Session,
    *,
    snapshot: models.Snapshot,
    preview_sheets: list[PreviewSheet] | None = None,
) -> models.WorkbookSchemaProfile:
    existing = (
        db.query(models.WorkbookSchemaProfile)
        .filter(models.WorkbookSchemaProfile.snapshot_id == snapshot.id)
        .first()
    )
    if existing:
        return existing

    sheets = preview_sheets
    if sheets is None:
        for path in _storage_candidates(snapshot):
            if path.exists():
                try:
                    sheets = preview_contracted_vs_actual_xlsx(path)
                    break
                except Exception:
                    continue
    if sheets is None:
        raise ValueError("Unable to profile workbook schema for this snapshot")

    profile_json = _profile_dict_from_preview(sheets, snapshot.source_filename)
    return ensure_schema_profile_from_profile_json(db, snapshot=snapshot, profile_json=profile_json)


def ensure_schema_profile_from_profile_json(
    db: Session,
    *,
    snapshot: models.Snapshot,
    profile_json: dict[str, Any],
) -> models.WorkbookSchemaProfile:
    existing = (
        db.query(models.WorkbookSchemaProfile)
        .filter(models.WorkbookSchemaProfile.snapshot_id == snapshot.id)
        .first()
    )
    if existing:
        return existing
    schema_signature = _schema_signature(profile_json)
    workbook_type = _infer_workbook_type(profile_json)

    profile = models.WorkbookSchemaProfile(
        snapshot_id=snapshot.id,
        schema_signature=schema_signature,
        workbook_type=workbook_type,
        profile_json=profile_json,
        source_filename=snapshot.source_filename,
    )
    db.add(profile)
    db.flush()
    return profile


def _section(
    key: str,
    label: str,
    description: str,
    *,
    renderer: str = "panel",
    slot: str | None = None,
    widget_type: str | None = None,
    bindings: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "key": key,
        "label": label,
        "description": description,
        "renderer": renderer,
        "slot": slot,
        "widget_type": widget_type or renderer,
        "bindings": bindings or {},
    }


def _build_standard_tax_blueprint(profile: models.WorkbookSchemaProfile) -> tuple[str, str, dict[str, Any], str, float]:
    config = {
        "dashboard_family": "variance_dashboard",
        "layout_template": "executive_variance",
        "title": "Tax Returns — Contracted vs Received",
        "subtitle": "Operational variance dashboard for contracted, received, pending, staff workload, and risk.",
        "tabs": [
            {
                "key": "overview",
                "label": "Overview",
                "description": "Portfolio KPIs and distribution.",
                "sections": [
                    _section("overall_progress", "Overall Progress", "Receipt progress across all contracted returns.", slot="hero", widget_type="progress_hero"),
                    _section("client_distribution", "Client Distribution", "Clients grouped by receipt-rate band.", slot="main_left", widget_type="distribution_bands"),
                    _section("top_clients", "Top Clients by Volume", "Largest contracted portfolios.", slot="main_left", widget_type="ranked_list"),
                    _section("zero_received", "Zero Received", "Highest-volume clients with no receipts.", slot="sidebar_top", widget_type="exception_list"),
                    _section("critical_clients", "Critical Clients", "Clients under the critical threshold.", slot="sidebar_middle", widget_type="exception_list"),
                    _section("ahead_clients", "Ahead Clients", "Strong performers above target pace.", slot="sidebar_bottom", widget_type="exception_list"),
                ],
            },
            {
                "key": "clients",
                "label": "Client Table",
                "description": "Detailed client-level delivery view.",
                "sections": [
                    _section("client_filters", "Filters", "Search and risk filtering controls.", renderer="control_bar", slot="hero", widget_type="filter_bar"),
                    _section("client_table", "Client Table", "Sortable client metrics table.", renderer="table", slot="main_left", widget_type="data_table"),
                    _section("client_detail", "Client Detail", "Focused client drill-down panel.", renderer="detail_panel", slot="sidebar_top", widget_type="detail_panel"),
                ],
            },
            {
                "key": "staff",
                "label": "Staff Workload",
                "description": "Team throughput and concentration.",
                "sections": [
                    _section("staff_workload", "Staff Workload", "Returns received per staff member.", renderer="table", slot="full_width", widget_type="workload_bars"),
                ],
            },
            {
                "key": "risk",
                "label": "Risk Flags",
                "description": "Operational exceptions and follow-up priorities.",
                "sections": [
                    _section("risk_zero", "Zero Received", "Contracted clients with no receipts.", renderer="list", slot="main_left", widget_type="exception_list"),
                    _section("risk_critical", "Critical", "Receipt rate under critical threshold.", renderer="list", slot="main_left", widget_type="exception_list"),
                    _section("risk_anomalies", "Anomalies", "Over-delivered or uncontracted anomalies.", renderer="list", slot="main_left", widget_type="exception_list"),
                    _section("risk_summary", "Risk Summary", "Risk-band distribution summary.", renderer="summary", slot="sidebar_top", widget_type="summary_panel"),
                ],
            },
        ],
        "kpi_cards": [
            {"key": "total_contracted", "label": "Total Contracted"},
            {"key": "total_received", "label": "Received to Date"},
            {"key": "total_pending", "label": "Still Outstanding"},
            {"key": "total_received_ind", "label": "Individual Returns"},
            {"key": "total_received_bus", "label": "Business Returns"},
            {"key": "zero_received_clients", "label": "Not Yet Started"},
            {"key": "over_delivered_clients", "label": "Over Contracted"},
        ],
        "schema_fields": profile.profile_json["sheets"],
    }
    return (
        "Tax Returns Operational Dashboard",
        "Standard 4-view operating dashboard for contracted vs received tax-return tracking.",
        config,
        "Matched the known contracted-vs-actual workbook shape and generated the standard 4-tab dashboard blueprint.",
        0.98,
    )


def _build_status_pipeline_blueprint(profile: models.WorkbookSchemaProfile) -> tuple[str, str, dict[str, Any], str, float]:
    notes_present = any(
        field["normalized_header"] in {"cpa notes", "gkm notes"}
        for sheet in profile.profile_json["sheets"]
        for section in sheet["sections"]
        for field in section["fields"]
    )
    queue_sections = [
        _section("queue_table", "Open Return Queue", "Operational queue of returns that still need action.", renderer="table", slot="main_left", widget_type="queue_table"),
        _section("status_mix", "Status Mix", "Relative volume by status.", renderer="summary", slot="sidebar_top", widget_type="status_mix"),
    ]
    notes_sections = [
        _section("notes_panel", "Notes Review", "Rows carrying operational notes that may need follow-up.", renderer="list", slot="main_left", widget_type="notes_panel")
    ] if notes_present else [
        _section("exceptions_panel", "Operational Exceptions", "Rows that are not yet complete and need attention.", renderer="list", slot="main_left", widget_type="exception_list")
    ]
    config = {
        "dashboard_family": "status_pipeline_dashboard",
        "layout_template": "pipeline_ops",
        "title": "Return Status Operations Dashboard",
        "subtitle": "Pipeline-oriented dashboard for return status, queue health, and operational notes.",
        "tabs": [
            {
                "key": "overview",
                "label": "Overview",
                "description": "Topline pipeline health and status mix.",
                "sections": [
                    _section("pipeline_hero", "Pipeline Health", "Operational summary of the current return pipeline.", slot="hero", widget_type="status_hero"),
                    _section("status_distribution", "Status Distribution", "Returns grouped by current status.", slot="main_left", widget_type="status_distribution"),
                    _section("return_type_mix", "Return Type Mix", "Split between business and individual returns.", slot="main_right", widget_type="type_mix"),
                    _section("stale_items", "Stale Items", "Open items likely to need follow-up.", slot="sidebar_top", widget_type="exception_list"),
                ],
            },
            {
                "key": "pipeline",
                "label": "Status Pipeline",
                "description": "Detailed status-stage composition.",
                "sections": [
                    _section("pipeline_board", "Pipeline Board", "Stage-by-stage status cards for the active queue.", slot="full_width", widget_type="pipeline_board"),
                ],
            },
            {
                "key": "queue",
                "label": "Return Queue",
                "description": "Open work queue and bottleneck review.",
                "sections": queue_sections,
            },
            {
                "key": "notes",
                "label": "Operational Notes",
                "description": "Review notes and follow-up text from source rows.",
                "sections": notes_sections,
            },
        ],
        "kpi_cards": [
            {"key": "total_returns", "label": "Total Returns"},
            {"key": "completed_returns", "label": "Completed"},
            {"key": "open_returns", "label": "Open Queue"},
            {"key": "awaiting_answers", "label": "Awaiting Answers"},
            {"key": "under_review", "label": "Under Review"},
            {"key": "in_process", "label": "In Process"},
        ],
        "schema_fields": profile.profile_json["sheets"],
    }
    return (
        "Return Status Operations Dashboard",
        "Status-driven operational dashboard tailored to client return workflow and queue movement.",
        config,
        "Detected a status-tracking workbook shape and proposed a pipeline-style dashboard instead of a contracted-vs-actual variance view.",
        0.9,
    )


def _build_product_master_blueprint(profile: models.WorkbookSchemaProfile) -> tuple[str, str, dict[str, Any], str, float]:
    config = {
        "dashboard_family": "product_catalog_dashboard",
        "layout_template": "catalog_ops",
        "title": "Product Master Dashboard",
        "subtitle": "Catalog-level view of products, types, units of measure, and category structure.",
        "tabs": [
            {
                "key": "overview",
                "label": "Overview",
                "description": "Topline product catalog health.",
                "sections": [
                    _section("catalog_hero", "Catalog Overview", "High-level counts and product mix.", slot="hero", widget_type="catalog_hero"),
                    _section("type_mix", "Product Type Mix", "Distribution by product type.", slot="main_left", widget_type="distribution_bands"),
                    _section("uom_mix", "Unit Mix", "Distribution by base unit of measure.", slot="main_right", widget_type="distribution_bands"),
                    _section("category_top", "Top Categories", "Largest product category buckets.", slot="sidebar_top", widget_type="ranked_list"),
                ],
            },
            {
                "key": "catalog",
                "label": "Product Catalog",
                "description": "Detailed product list.",
                "sections": [
                    _section("catalog_table", "Product Catalog", "Main product listing with the source attributes.", renderer="table", slot="full_width", widget_type="data_table"),
                ],
            },
            {
                "key": "categories",
                "label": "Category Analysis",
                "description": "Category concentration and hierarchy summary.",
                "sections": [
                    _section("category_distribution", "Category Distribution", "Product counts by category code.", renderer="summary", slot="main_left", widget_type="distribution_bands"),
                    _section("type_category_cross", "Type and Category Signals", "Mix of product type and category patterns.", renderer="summary", slot="sidebar_top", widget_type="summary_panel"),
                ],
            },
            {
                "key": "quality",
                "label": "Data Quality",
                "description": "Missing attributes and formatting checks.",
                "sections": [
                    _section("quality_gaps", "Missing Attributes", "Rows with blank key attributes.", renderer="list", slot="main_left", widget_type="exception_list"),
                    _section("quality_summary", "Quality Summary", "Catalog integrity observations.", renderer="summary", slot="sidebar_top", widget_type="summary_panel"),
                ],
            },
        ],
        "kpi_cards": [
            {"key": "total_products", "label": "Products"},
            {"key": "product_type_count", "label": "Product Types"},
            {"key": "uom_count", "label": "Units of Measure"},
            {"key": "category_count", "label": "Categories"},
        ],
        "schema_fields": profile.profile_json["sheets"],
    }
    return (
        "Product Master Dashboard",
        "Catalog-oriented dashboard for product master workbooks and master-data review.",
        config,
        "Detected a product master workbook shape and proposed a catalog-style dashboard instead of a tax operations dashboard.",
        0.88,
    )


def _build_generic_blueprint(profile: models.WorkbookSchemaProfile) -> tuple[str, str, dict[str, Any], str, float]:
    tabs = []
    for idx, sheet in enumerate(profile.profile_json["sheets"], start=1):
        sections = []
        for section in sheet["sections"]:
            sections.append(
                _section(
                    f"{section['section_key']}_{idx}",
                    section["section_label"],
                    f"Review {len(section['fields'])} fields from {sheet['sheet_name']}.",
                    renderer="schema_table",
                    slot="full_width",
                    widget_type="schema_table",
                )
            )
        tabs.append(
            {
                "key": f"sheet_{idx}",
                "label": sheet["sheet_name"],
                "description": f"Inferred dashboard view for {sheet['sheet_name']}.",
                "sections": sections or [_section(f"sheet_{idx}_summary", "Sheet Summary", "No sections detected yet.", renderer="schema_table")],
            }
        )
    config = {
        "dashboard_family": "generic_review_dashboard",
        "layout_template": "schema_review",
        "title": "Proposed Adaptive Dashboard",
        "subtitle": "Schema-first review view for a workbook that does not match an approved dashboard family yet.",
        "tabs": tabs,
        "kpi_cards": [
            {"key": "sheet_count", "label": "Sheets Detected"},
            {"key": "field_count", "label": "Fields Detected"},
        ],
        "schema_fields": profile.profile_json["sheets"],
    }
    return (
        "Proposed Adaptive Dashboard",
        "Initial schema-driven dashboard proposal for a workbook that does not match an approved blueprint yet.",
        config,
        "No approved blueprint matched this schema exactly, so the system proposed tabs directly from the detected sheet and section structure.",
        0.62,
    )


def _run_dashboard_agent(
    *,
    profile: models.WorkbookSchemaProfile,
    snapshot: models.Snapshot,
    user_guidance: str | None,
    base_title: str,
    base_summary: str,
    base_config: dict[str, Any],
    base_rationale: str,
    base_confidence: float,
    runtime_summary: dict[str, Any] | None,
    gemini: GeminiRequestSettings | None,
) -> tuple[str, str, dict[str, Any], str, float]:
    result = run_dashboard_orchestrator(
        profile=profile,
        runtime_payload=runtime_summary,
        raw_tables=load_or_extract_snapshot_raw_tables(snapshot, profile.workbook_type),
        user_guidance=user_guidance,
        base_title=base_title,
        base_summary=base_summary,
        base_config=base_config,
        base_rationale=base_rationale,
        base_confidence=base_confidence,
        gemini=gemini,
    )
    return result.title, result.summary, result.config, result.rationale, result.confidence_score


def _proposal_payload(profile: models.WorkbookSchemaProfile) -> tuple[str, str, dict[str, Any], str, float, str]:
    exact = (
        "exact"
        if profile.workbook_type == "contracted_actual_v1"
        else "inferred"
    )
    if profile.workbook_type == "contracted_actual_v1":
        title, summary, config, rationale, confidence = _build_standard_tax_blueprint(profile)
    elif profile.workbook_type == "client_status_report_v1":
        title, summary, config, rationale, confidence = _build_status_pipeline_blueprint(profile)
    elif profile.workbook_type == "product_master_v1":
        title, summary, config, rationale, confidence = _build_product_master_blueprint(profile)
    else:
        title, summary, config, rationale, confidence = _build_generic_blueprint(profile)
    return title, summary, config, rationale, confidence, exact


def _has_rich_layout_spec(config: dict[str, Any] | None) -> bool:
    if not isinstance(config, dict):
        return False
    if str(config.get("dashboard_family") or "") == "generic_review_dashboard":
        return bool(config.get("adaptive_dashboard_enabled") and config.get("adaptive_dashboard_spec"))
    return bool(config.get("dashboard_family") and config.get("layout_template") and config.get("title"))


def _is_snapshot_specific_blueprint(config: dict[str, Any] | None) -> bool:
    if not isinstance(config, dict):
        return False
    family = str(config.get("dashboard_family") or "").strip().lower()
    layout = str(config.get("layout_template") or "").strip().lower()
    if family == "generic_review_dashboard":
        return True
    if layout == "adaptive_semantic":
        return True
    if bool(config.get("adaptive_dashboard_enabled")):
        return True
    return False


def _can_reuse_blueprint(
    *,
    profile: models.WorkbookSchemaProfile,
    blueprint: models.DashboardBlueprint | None,
) -> bool:
    if blueprint is None:
        return False
    if blueprint.status != "approved":
        return False
    if blueprint.workbook_type != profile.workbook_type:
        return False
    if blueprint.schema_signature != profile.schema_signature:
        return False
    if _is_snapshot_specific_blueprint(blueprint.config_json):
        return False
    return True


def _find_reusable_blueprint(
    db: Session,
    *,
    profile: models.WorkbookSchemaProfile,
) -> models.DashboardBlueprint | None:
    candidates = (
        db.query(models.DashboardBlueprint)
        .filter(
            models.DashboardBlueprint.schema_signature == profile.schema_signature,
            models.DashboardBlueprint.status == "approved",
        )
        .order_by(models.DashboardBlueprint.last_used_at.desc(), models.DashboardBlueprint.id.desc())
        .all()
    )
    for blueprint in candidates:
        if _can_reuse_blueprint(profile=profile, blueprint=blueprint):
            return blueprint
    return None


def get_effective_blueprint(db: Session, *, snapshot: models.Snapshot) -> models.DashboardBlueprint | None:
    profile = ensure_schema_profile(db, snapshot=snapshot)
    approved_proposal = snapshot.dashboard_proposal
    if approved_proposal and approved_proposal.status == "approved" and approved_proposal.approved_blueprint is not None:
        blueprint = approved_proposal.approved_blueprint
        blueprint.last_used_at = datetime.now(timezone.utc)
        db.flush()
        return blueprint

    blueprint = _find_reusable_blueprint(db, profile=profile)
    if blueprint:
        blueprint.last_used_at = datetime.now(timezone.utc)
        db.flush()
        return blueprint
    return None


def build_or_refresh_proposal(
    db: Session,
    *,
    snapshot: models.Snapshot,
    actor_user_id: int | None,
    gemini: GeminiRequestSettings | None = None,
    user_guidance: str | None = None,
) -> models.DashboardProposal:
    log_app_event(
        level="info",
        state="started",
        category="dashboard_proposal",
        event="build_or_refresh_started",
        agent_name="dashboard_blueprints",
        workflow="dashboard_proposal_v1",
        message="Started building or refreshing dashboard proposal",
        payload={"snapshot_id": snapshot.id, "user_guidance": user_guidance},
        snapshot_id=snapshot.id,
        user_id=actor_user_id,
    )
    profile = ensure_schema_profile(db, snapshot=snapshot)
    matched_blueprint = _find_reusable_blueprint(db, profile=profile)
    title, summary, config, rationale, confidence, match_mode = _proposal_payload(profile)
    runtime_payload: dict[str, Any] | None = None
    if matched_blueprint and not user_guidance and _has_rich_layout_spec(matched_blueprint.config_json):
        title = matched_blueprint.name
        summary = matched_blueprint.description
        config = matched_blueprint.config_json
        rationale = "Found an approved dashboard blueprint with an exact schema signature match."
        confidence = 1.0
        match_mode = "exact"
    else:
        from app.dashboard_runtime import get_dashboard_runtime_payload

        if matched_blueprint and user_guidance:
            title = matched_blueprint.name
            summary = matched_blueprint.description
            config = matched_blueprint.config_json
            rationale = "Found an approved dashboard blueprint with an exact schema signature match, then reopened proposal generation because the user supplied new dashboard guidance."
            confidence = 0.96
            match_mode = "guided"
        elif matched_blueprint and not _has_rich_layout_spec(matched_blueprint.config_json):
            title = matched_blueprint.name
            summary = matched_blueprint.description
            config = matched_blueprint.config_json
            rationale = "Found an approved dashboard blueprint with an exact schema signature match, but reopened proposal generation because the stored generic layout is not adaptive enough for current semantic dashboarding."
            confidence = 0.9
            match_mode = "refresh"
        runtime_payload = get_dashboard_runtime_payload(db, snapshot, profile.workbook_type)
        logger.info(
            "dashboard proposal: running dashboard LangGraph workflow for snapshot %s with workbook type %s",
            snapshot.id,
            profile.workbook_type,
        )
        title, summary, config, rationale, confidence = _run_dashboard_agent(
            profile=profile,
            snapshot=snapshot,
            user_guidance=user_guidance,
            base_title=title,
            base_summary=summary,
            base_config=config,
            base_rationale=rationale,
            base_confidence=confidence,
            runtime_summary=runtime_payload,
            gemini=gemini,
        )

    proposal = (
        db.query(models.DashboardProposal)
        .filter(models.DashboardProposal.snapshot_id == snapshot.id)
        .first()
    )
    if proposal is None:
        proposal = models.DashboardProposal(
            snapshot_id=snapshot.id,
            schema_profile_id=profile.id,
            created_by_user_id=actor_user_id,
            title=title,
            summary=summary,
            rationale=rationale,
            proposal_json=config,
            confidence_score=confidence,
            match_mode=match_mode,
            matched_blueprint_id=matched_blueprint.id if matched_blueprint else None,
            status="pending",
        )
        db.add(proposal)
    else:
        proposal.schema_profile_id = profile.id
        proposal.title = title
        proposal.summary = summary
        proposal.rationale = rationale
        proposal.proposal_json = config
        proposal.confidence_score = confidence
        proposal.match_mode = match_mode
        proposal.matched_blueprint_id = matched_blueprint.id if matched_blueprint else None
        if proposal.status != "approved":
            proposal.status = "pending"
    db.flush()
    log_app_event(
        level="info",
        state="completed",
        category="dashboard_proposal",
        event="build_or_refresh_completed",
        agent_name="dashboard_blueprints",
        workflow=str(config.get("proposal_workflow") or config.get("orchestrator_workflow") or "dashboard_proposal_v1"),
        message="Dashboard proposal build or refresh completed",
        payload={
            "proposal_id": proposal.id,
            "match_mode": match_mode,
            "confidence_score": confidence,
            "title": title,
        },
        snapshot_id=snapshot.id,
        proposal_id=proposal.id,
        user_id=actor_user_id,
    )
    return proposal


def approve_proposal(
    db: Session,
    *,
    proposal: models.DashboardProposal,
    actor_user_id: int | None,
) -> models.DashboardBlueprint:
    reusable_match = _can_reuse_blueprint(profile=proposal.schema_profile, blueprint=proposal.matched_blueprint)
    if proposal.matched_blueprint_id and reusable_match:
        blueprint = proposal.matched_blueprint
        assert blueprint is not None
        blueprint.name = proposal.title
        blueprint.description = proposal.summary
        blueprint.config_json = proposal.proposal_json
        blueprint.approved_by_user_id = actor_user_id
        blueprint.approved_at = datetime.now(timezone.utc)
    else:
        profile = proposal.schema_profile
        blueprint_key = (
            f"{profile.workbook_type}:{profile.schema_signature[:12]}"
            if not _is_snapshot_specific_blueprint(proposal.proposal_json)
            else f"{profile.workbook_type}:{profile.schema_signature[:12]}:snapshot:{proposal.snapshot_id}"
        )
        blueprint = db.query(models.DashboardBlueprint).filter(models.DashboardBlueprint.blueprint_key == blueprint_key).first()
        if blueprint is not None:
            blueprint.name = proposal.title
            blueprint.description = proposal.summary
            blueprint.schema_signature = profile.schema_signature
            blueprint.workbook_type = profile.workbook_type
            blueprint.status = "approved"
            blueprint.config_json = proposal.proposal_json
            blueprint.approved_by_user_id = actor_user_id
            blueprint.approved_at = datetime.now(timezone.utc)
        else:
            blueprint = models.DashboardBlueprint(
                blueprint_key=blueprint_key,
                name=proposal.title,
                description=proposal.summary,
                schema_signature=profile.schema_signature,
                workbook_type=profile.workbook_type,
                status="approved",
                config_json=proposal.proposal_json,
                created_by_user_id=actor_user_id,
                approved_by_user_id=actor_user_id,
                approved_at=datetime.now(timezone.utc),
                last_used_at=datetime.now(timezone.utc),
            )
            db.add(blueprint)
            db.flush()

    proposal.approved_blueprint_id = blueprint.id
    proposal.approved_by_user_id = actor_user_id
    proposal.approved_at = datetime.now(timezone.utc)
    proposal.status = "approved"
    blueprint.last_used_at = datetime.now(timezone.utc)
    db.flush()
    return blueprint
