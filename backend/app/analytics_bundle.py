from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app import models
from app.app_logs import exception_detail, log_app_event
from app.field_roles import classify_table_fields
from app.gemini_reasoning import GeminiRequestSettings
from app.raw_data_store import load_or_extract_snapshot_raw_tables


BUNDLE_VERSION = 1


def _semantic_label(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in (
            "measure_name",
            "dimension_name",
            "entity_name",
            "business_domain",
            "name",
            "column_name",
            "label",
            "title",
            "business_meaning",
            "description",
        ):
            raw = value.get(key)
            if isinstance(raw, str) and raw.strip():
                return raw.strip()
    return str(value or "").strip()


def _sanitize_for_json(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return _sanitize_for_json(value.model_dump())
    if is_dataclass(value):
        return _sanitize_for_json(asdict(value))
    if isinstance(value, dict):
        return {str(key): _sanitize_for_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_for_json(item) for item in value]
    return value


def _normalize_dashboard_config(config: dict[str, Any] | None) -> dict[str, Any]:
    next_config = deepcopy(config or {})
    spec = dict(next_config.get("adaptive_dashboard_spec") or {})
    if spec:
        spec["domain"] = _semantic_label(spec.get("domain"))
        spec["primary_entity"] = _semantic_label(spec.get("primary_entity"))
        spec["primary_measure"] = _semantic_label(spec.get("primary_measure"))
        spec["secondary_measures"] = [
            label
            for label in (_semantic_label(item) for item in list(spec.get("secondary_measures") or []))
            if label
        ][:6]
        spec["chart_preferences"] = [
            label
            for label in (_semantic_label(item) for item in list(spec.get("chart_preferences") or []))
            if label
        ][:6]
        spec["questions"] = [
            label
            for label in (_semantic_label(item) for item in list(spec.get("questions") or []))
            if label
        ][:6]
        spec["evidence_titles"] = [
            label
            for label in (_semantic_label(item) for item in list(spec.get("evidence_titles") or []))
            if label
        ][:8]
        next_config["adaptive_dashboard_spec"] = spec
    details = next_config.get("semantic_details")
    if isinstance(details, dict):
        business_domain = _semantic_label(
            details.get("business_domain")
            or details.get("dominant_business_domain")
            or details.get("dominant_domain")
        )
        description = _semantic_label(
            details.get("description")
            or details.get("inferred_meaning")
            or details.get("workbook_business_meaning")
            or details.get("semantic_summary")
        )
        if business_domain and description:
            next_config["semantic_summary"] = f"{business_domain}: {description}"
        elif description:
            next_config["semantic_summary"] = description
        elif business_domain:
            next_config["semantic_summary"] = business_domain
    for key in ("title", "subtitle", "semantic_summary"):
        if key in next_config:
            next_config[key] = _semantic_label(next_config.get(key))
    return _sanitize_for_json(next_config)


def _field_roles_payload(snapshot: models.Snapshot, workbook_type: str | None) -> list[dict[str, Any]]:
    if not workbook_type:
        return []
    tables = load_or_extract_snapshot_raw_tables(snapshot, workbook_type)
    payload: list[dict[str, Any]] = []
    for table in tables:
        profiles = classify_table_fields(table)
        payload.append(
            {
                "table_name": table.name,
                "sheet_name": table.name,
                "section_key": table.name,
                "row_count": len(table.rows),
                "fields": [
                    {
                        "header": header,
                        "normalized_header": profile.normalized_header,
                        "role": profile.role,
                        "data_kind": profile.data_kind,
                        "confidence": profile.confidence,
                    }
                    for header, profile in profiles.items()
                ],
            }
        )
    return payload


def _generation_mode(config: dict[str, Any]) -> str:
    workflows = [
        str(config.get("orchestrator_workflow") or "").strip(),
        str(config.get("eda_workflow") or "").strip(),
        str(config.get("proposal_workflow") or "").strip(),
    ]
    workflows = [item for item in workflows if item]
    if not workflows:
        return "fallback"
    has_fallback = any("fallback" in item.lower() for item in workflows)
    has_llm = any("fallback" not in item.lower() for item in workflows)
    if has_fallback and has_llm:
        return "mixed"
    if has_fallback:
        return "fallback"
    return "llm"


def _bundle_diagnostics(
    *,
    workbook_type: str | None,
    dashboard_config: dict[str, Any],
    runtime_payload: dict[str, Any] | None,
    field_roles: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "workbook_type": workbook_type,
        "orchestrator_workflow": dashboard_config.get("orchestrator_workflow"),
        "eda_workflow": dashboard_config.get("eda_workflow"),
        "proposal_workflow": dashboard_config.get("proposal_workflow"),
        "raw_semantic_details": _sanitize_for_json(dashboard_config.get("semantic_details")),
        "raw_eda_plan": _sanitize_for_json(dashboard_config.get("eda_plan")),
        "raw_eda_evidence": _sanitize_for_json(dashboard_config.get("eda_evidence")),
        "runtime_keys": sorted(list((runtime_payload or {}).keys())),
        "field_roles": field_roles,
    }


def _variance_surface_payloads(ctx: Any) -> dict[str, Any]:
    total_contracted = sum(int(item.contracted_total or 0) for item in ctx.clients)
    total_received = sum(int(item.received_total or 0) for item in ctx.clients)
    total_pending = sum(int(item.pending_total or 0) for item in ctx.clients)
    total_contracted_ind = sum(int(item.contracted_ind or 0) for item in ctx.clients)
    total_contracted_bus = sum(int(item.contracted_bus or 0) for item in ctx.clients)
    total_received_ind = sum(int(item.received_ind or 0) for item in ctx.clients)
    total_received_bus = sum(int(item.received_bus or 0) for item in ctx.clients)
    overall_receipt_rate = (total_received / total_contracted) if total_contracted else 0.0
    active_clients = sum(1 for item in ctx.clients if (item.contracted_total > 0 or item.received_total > 0))
    zero_received_clients = sum(1 for item in ctx.clients if (item.contracted_total > 0 and item.received_total == 0))
    over_delivered_clients = sum(1 for item in ctx.clients if item.received_total > item.contracted_total)
    staff_total_received = sum(int(item.received_total or 0) for item in ctx.staff)
    return {
        "kpis": {
            "snapshot": {
                "id": ctx.snapshot.id,
                "as_of_date": ctx.snapshot.as_of_date.isoformat(),
                "source_filename": ctx.snapshot.source_filename,
            },
            "total_contracted": total_contracted,
            "total_received": total_received,
            "total_pending": total_pending,
            "total_contracted_ind": total_contracted_ind,
            "total_contracted_bus": total_contracted_bus,
            "total_received_ind": total_received_ind,
            "total_received_bus": total_received_bus,
            "overall_receipt_rate": overall_receipt_rate,
            "active_clients": active_clients,
            "zero_received_clients": zero_received_clients,
            "over_delivered_clients": over_delivered_clients,
            "staff_total_received": staff_total_received,
        },
        "clients": [
            {
                "client_name": item.client_name,
                "client_id": item.client_external_id,
                "client_type": item.client_type,
                "contracted_ind": item.contracted_ind,
                "contracted_bus": item.contracted_bus,
                "contracted_total": item.contracted_total,
                "received_ind": item.received_ind,
                "received_bus": item.received_bus,
                "received_total": item.received_total,
                "pending_ind": item.pending_ind,
                "pending_bus": item.pending_bus,
                "pending_total": item.pending_total,
                "receipt_rate": item.receipt_rate,
            }
            for item in ctx.clients
        ],
        "staff": [
            {
                "name": item.name,
                "staff_id": item.staff_id,
                "staff_type": item.staff_type,
                "received_ind": item.received_ind,
                "received_bus": item.received_bus,
                "received_total": item.received_total,
            }
            for item in ctx.staff
        ],
    }


def get_snapshot_analytics_bundle(db: Session, snapshot_id: int) -> models.SnapshotAnalyticsBundle | None:
    return (
        db.query(models.SnapshotAnalyticsBundle)
        .filter(models.SnapshotAnalyticsBundle.snapshot_id == snapshot_id)
        .first()
    )


def bundle_is_current(
    bundle: models.SnapshotAnalyticsBundle | None,
    *,
    blueprint: models.DashboardBlueprint | None,
    proposal: models.DashboardProposal | None,
) -> bool:
    if bundle is None:
        return False
    if bundle.bundle_version != BUNDLE_VERSION or bundle.stale:
        return False
    if blueprint and bundle.blueprint_id != blueprint.id:
        return False
    if proposal and proposal.status == "approved" and bundle.proposal_id != proposal.id:
        return False
    if blueprint and blueprint.approved_at and bundle.generated_at < blueprint.approved_at:
        return False
    if proposal and proposal.status == "approved" and proposal.approved_at and bundle.generated_at < proposal.approved_at:
        return False
    return True


def mark_snapshot_analytics_bundle_stale(db: Session, snapshot_id: int) -> None:
    bundle = get_snapshot_analytics_bundle(db, snapshot_id)
    if bundle is None:
        return
    bundle.stale = True
    bundle.status = "stale"
    bundle.updated_at = datetime.now(timezone.utc)
    db.flush()


def _write_bundle_row(
    db: Session,
    *,
    snapshot_id: int,
    blueprint_id: int | None,
    proposal_id: int | None,
    payload: dict[str, Any],
    diagnostics: dict[str, Any],
    bundle_version: int,
    generation_mode: str,
    generated_at: datetime,
) -> tuple[models.SnapshotAnalyticsBundle, str]:
    table = models.SnapshotAnalyticsBundle.__table__
    values = {
        "snapshot_id": snapshot_id,
        "blueprint_id": blueprint_id,
        "proposal_id": proposal_id,
        "bundle_version": bundle_version,
        "status": "ready",
        "generation_mode": generation_mode,
        "stale": False,
        "payload_json": payload,
        "diagnostics_json": diagnostics,
        "generated_at": generated_at,
        "updated_at": generated_at,
    }
    existing = get_snapshot_analytics_bundle(db, snapshot_id)
    write_mode = "update" if existing is not None else "insert"

    if db.bind is not None and db.bind.dialect.name == "postgresql":
        stmt = (
            pg_insert(table)
            .values(**values)
            .on_conflict_do_update(
                index_elements=[table.c.snapshot_id],
                set_={
                    "blueprint_id": blueprint_id,
                    "proposal_id": proposal_id,
                    "bundle_version": bundle_version,
                    "status": "ready",
                    "generation_mode": generation_mode,
                    "stale": False,
                    "payload_json": payload,
                    "diagnostics_json": diagnostics,
                    "generated_at": generated_at,
                    "updated_at": generated_at,
                },
            )
        )
        db.execute(stmt)
        db.flush()
        bundle = get_snapshot_analytics_bundle(db, snapshot_id)
        if bundle is None:
            raise RuntimeError(f"Snapshot analytics bundle was not found after upsert for snapshot {snapshot_id}")
        return bundle, ("upsert_update" if existing is not None else "upsert_insert")

    if existing is None:
        existing = models.SnapshotAnalyticsBundle(**values)
        db.add(existing)
    else:
        existing.blueprint_id = blueprint_id
        existing.proposal_id = proposal_id
        existing.bundle_version = bundle_version
        existing.status = "ready"
        existing.generation_mode = generation_mode
        existing.stale = False
        existing.payload_json = payload
        existing.diagnostics_json = diagnostics
        existing.generated_at = generated_at
        existing.updated_at = generated_at
    db.flush()
    return existing, write_mode


def generate_snapshot_analytics_bundle(
    db: Session,
    *,
    snapshot: models.Snapshot,
    blueprint: models.DashboardBlueprint | None = None,
    proposal: models.DashboardProposal | None = None,
    gemini: GeminiRequestSettings | None = None,
    force: bool = False,
) -> models.SnapshotAnalyticsBundle:
    from app.dashboard_blueprints import build_or_refresh_proposal, ensure_schema_profile, get_effective_blueprint
    from app.dashboard_runtime import get_dashboard_runtime_payload
    from app.reporting import build_agentic_chat_context, build_summary_reasoning_bundle, load_snapshot_report_context

    profile = ensure_schema_profile(db, snapshot=snapshot)
    blueprint = blueprint or get_effective_blueprint(db, snapshot=snapshot)
    proposal = proposal or snapshot.dashboard_proposal or build_or_refresh_proposal(db, snapshot=snapshot, actor_user_id=None)
    existing = get_snapshot_analytics_bundle(db, snapshot.id)
    if not force and bundle_is_current(existing, blueprint=blueprint, proposal=proposal):
        log_app_event(
            level="info",
            state="skipped",
            category="analytics_bundle",
            event="bundle_reused_current",
            agent_name="analytics_bundle_generator",
            workflow="snapshot_bundle_v1",
            message="Current snapshot analytics bundle reused",
            payload={
                "snapshot_id": snapshot.id,
                "proposal_id": proposal.id if proposal else None,
                "blueprint_id": blueprint.id if blueprint else None,
            },
            snapshot_id=snapshot.id,
            proposal_id=proposal.id if proposal else None,
            blueprint_id=blueprint.id if blueprint else None,
        )
        return existing
    log_app_event(
        level="info",
        state="started",
        category="analytics_bundle",
        event="bundle_generation_started",
        agent_name="analytics_bundle_generator",
        workflow="snapshot_bundle_v1",
        message="Snapshot analytics bundle generation started",
        payload={
            "snapshot_id": snapshot.id,
            "proposal_id": proposal.id if proposal else None,
            "blueprint_id": blueprint.id if blueprint else None,
            "force": force,
        },
        snapshot_id=snapshot.id,
        proposal_id=proposal.id if proposal else None,
        blueprint_id=blueprint.id if blueprint else None,
    )

    source_config = blueprint.config_json if blueprint else (proposal.proposal_json if proposal else {})
    dashboard_config = _normalize_dashboard_config(source_config)
    runtime_payload = get_dashboard_runtime_payload(db, snapshot, profile.workbook_type, dashboard_config)
    field_roles = _field_roles_payload(snapshot, profile.workbook_type)
    ctx = load_snapshot_report_context(
        db,
        snapshot.id,
        use_bundle=False,
        precomputed_dashboard_config=dashboard_config,
        precomputed_runtime_payload=runtime_payload,
    )
    reasoning_bundle = build_summary_reasoning_bundle(ctx, gemini=gemini)
    chat_context = build_agentic_chat_context(ctx)
    surface_payloads = _variance_surface_payloads(ctx) if profile.workbook_type == "contracted_actual_v1" else {}
    generated_at = datetime.now(timezone.utc)
    payload = {
        "metadata": {
            "bundle_version": BUNDLE_VERSION,
            "generated_at": generated_at.isoformat(),
            "source_snapshot_id": snapshot.id,
            "blueprint_id": blueprint.id if blueprint else None,
            "proposal_id": proposal.id if proposal else None,
            "status": "ready",
            "generation_mode": _generation_mode(dashboard_config),
            "workbook_type": profile.workbook_type,
        },
        "semantic": {
            "summary": _semantic_label(dashboard_config.get("semantic_summary")),
            "details": _sanitize_for_json(dashboard_config.get("semantic_details") or {}),
            "business_questions": [_semantic_label(item) for item in list(dashboard_config.get("business_questions") or []) if _semantic_label(item)],
            "ambiguities": [_semantic_label(item) for item in list(dashboard_config.get("ambiguities") or []) if _semantic_label(item)],
            "confidence": dashboard_config.get("semantic_confidence"),
        },
        "field_roles": field_roles,
        "eda": {
            "plan": _sanitize_for_json(dashboard_config.get("eda_plan") or []),
            "evidence": _sanitize_for_json(dashboard_config.get("eda_evidence") or []),
            "workflow": dashboard_config.get("eda_workflow"),
        },
        "dashboard": {
            "config": dashboard_config,
            "runtime_payload": _sanitize_for_json(runtime_payload),
            "surface_payloads": _sanitize_for_json(surface_payloads),
        },
        "reasoning_bundle": {
            "plan": {
                "family": reasoning_bundle.plan.family,
                "mode": reasoning_bundle.plan.mode,
                "entity_name": reasoning_bundle.plan.entity_name,
                "required_section_keys": list(reasoning_bundle.plan.required_section_keys),
                "title_overrides": dict(reasoning_bundle.plan.title_overrides),
                "evidence": [_sanitize_for_json(item) for item in reasoning_bundle.plan.evidence],
            },
            "evidence": [_sanitize_for_json(item) for item in reasoning_bundle.evidence],
            "packet": {
                "family": reasoning_bundle.packet.family,
                "plan_summary": reasoning_bundle.packet.plan_summary,
                "steps": [_sanitize_for_json(item) for item in reasoning_bundle.packet.steps],
                "findings": [_sanitize_for_json(item) for item in reasoning_bundle.packet.findings],
                "actions": [_sanitize_for_json(item) for item in reasoning_bundle.packet.actions],
                "limitations": [_sanitize_for_json(item) for item in reasoning_bundle.packet.limitations],
            },
        },
        "chat_context": {
            "family": chat_context.family,
            "dashboard_title": chat_context.dashboard_title,
            "family_description": chat_context.family_description,
            "evidence": [_sanitize_for_json(item) for item in chat_context.evidence],
        },
    }

    diagnostics = _bundle_diagnostics(
        workbook_type=profile.workbook_type,
        dashboard_config=dashboard_config,
        runtime_payload=runtime_payload,
        field_roles=field_roles,
    )
    try:
        bundle, write_mode = _write_bundle_row(
            db,
            snapshot_id=snapshot.id,
            blueprint_id=blueprint.id if blueprint else None,
            proposal_id=proposal.id if proposal else None,
            payload=payload,
            diagnostics=diagnostics,
            bundle_version=BUNDLE_VERSION,
            generation_mode=payload["metadata"]["generation_mode"],
            generated_at=generated_at,
        )
    except Exception as exc:
        log_app_event(
            level="error",
            state="failed",
            category="analytics_bundle",
            event="bundle_generation_failed",
            agent_name="analytics_bundle_generator",
            workflow="snapshot_bundle_v1",
            message="Snapshot analytics bundle generation failed",
            detail=exception_detail(exc),
            payload={
                "snapshot_id": snapshot.id,
                "proposal_id": proposal.id if proposal else None,
                "blueprint_id": blueprint.id if blueprint else None,
                "workbook_type": profile.workbook_type,
            },
            snapshot_id=snapshot.id,
            proposal_id=proposal.id if proposal else None,
            blueprint_id=blueprint.id if blueprint else None,
        )
        raise
    log_app_event(
        level="info",
        state="completed",
        category="analytics_bundle",
        event="bundle_generation_completed",
        agent_name="analytics_bundle_generator",
        workflow="snapshot_bundle_v1",
        message="Snapshot analytics bundle generation completed",
        payload={
            "bundle_version": BUNDLE_VERSION,
            "generation_mode": payload["metadata"]["generation_mode"],
            "workbook_type": profile.workbook_type,
            "write_mode": write_mode,
        },
        snapshot_id=snapshot.id,
        proposal_id=proposal.id if proposal else None,
        blueprint_id=blueprint.id if blueprint else None,
    )
    return bundle


def get_or_generate_snapshot_analytics_bundle(
    db: Session,
    *,
    snapshot: models.Snapshot,
    blueprint: models.DashboardBlueprint | None = None,
    proposal: models.DashboardProposal | None = None,
    gemini: GeminiRequestSettings | None = None,
) -> models.SnapshotAnalyticsBundle:
    bundle = get_snapshot_analytics_bundle(db, snapshot.id)
    if bundle_is_current(bundle, blueprint=blueprint, proposal=proposal):
        log_app_event(
            level="info",
            state="skipped",
            category="analytics_bundle",
            event="bundle_reused_current",
            agent_name="analytics_bundle_generator",
            workflow="snapshot_bundle_v1",
            message="Current snapshot analytics bundle reused",
            payload={
                "snapshot_id": snapshot.id,
                "bundle_version": bundle.bundle_version,
                "generation_mode": bundle.generation_mode,
            },
            snapshot_id=snapshot.id,
            proposal_id=proposal.id if proposal else None,
            blueprint_id=blueprint.id if blueprint else None,
        )
        return bundle
    log_app_event(
        level="info",
        state="started",
        category="analytics_bundle",
        event="bundle_regeneration_requested",
        agent_name="analytics_bundle_generator",
        workflow="snapshot_bundle_v1",
        message="Snapshot analytics bundle regeneration requested",
        payload={
            "snapshot_id": snapshot.id,
            "has_existing_bundle": bundle is not None,
            "existing_bundle_version": bundle.bundle_version if bundle else None,
            "existing_generation_mode": bundle.generation_mode if bundle else None,
        },
        snapshot_id=snapshot.id,
        proposal_id=proposal.id if proposal else None,
        blueprint_id=blueprint.id if blueprint else None,
    )
    return generate_snapshot_analytics_bundle(
        db,
        snapshot=snapshot,
        blueprint=blueprint,
        proposal=proposal,
        gemini=gemini,
        force=True,
    )
