from __future__ import annotations

import ast
import re

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request
from sqlalchemy.orm import Session

from app import models
from app.analytics_bundle import (
    generate_snapshot_analytics_bundle,
    get_or_generate_snapshot_analytics_bundle,
    get_snapshot_analytics_bundle,
    mark_snapshot_analytics_bundle_stale,
)
from app.app_logs import exception_detail, log_app_event, reset_log_context, set_log_context
from app.dashboard_blueprints import approve_proposal, build_or_refresh_proposal, ensure_schema_profile, get_effective_blueprint
from app.dashboard_runtime import get_dashboard_runtime_payload
from app.gemini_reasoning import GeminiReasoningError, gemini_settings_from_headers
from app.db import get_db
from app.deps import get_current_user
from app.schemas import (
    AppLogResponse,
    ClientRow,
    DashboardBlueprintResponse,
    DashboardChatRequest,
    DashboardChatResponse,
    DashboardLayoutPreferencesRequest,
    DashboardProposalRequest,
    DashboardProposalResponse,
    DashboardRefinementDiffResponse,
    DashboardRefinementResultResponse,
    KpiResponse,
    SnapshotSummary,
    StaffRow,
)


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def _get_snapshot(db: Session, snapshot_id: int | None) -> models.Snapshot:
    if snapshot_id is None:
        snap = db.query(models.Snapshot).order_by(models.Snapshot.as_of_date.desc(), models.Snapshot.id.desc()).first()
    else:
        snap = db.query(models.Snapshot).filter(models.Snapshot.id == snapshot_id).first()
    if not snap:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return snap


def _set_dashboard_read_context(*, request: Request, snapshot_id: int, route_name: str):
    return set_log_context(
        run_key=f"{route_name}:{snapshot_id}",
        request_path=str(request.url.path),
        snapshot_id=snapshot_id,
        agent_name="dashboard_router",
        workflow=route_name,
    )


def _normalize_config(config: dict | None) -> dict:
    cfg = dict(config or {})
    cfg.setdefault("dashboard_family", "generic_review_dashboard")
    cfg.setdefault("layout_template", "schema_review")
    cfg.setdefault("title", "Proposed Adaptive Dashboard")
    cfg.setdefault("subtitle", "Schema-first adaptive dashboard proposal.")
    cfg.setdefault("tabs", [])
    cfg.setdefault("kpi_cards", [])
    cfg.setdefault("schema_fields", [])
    cfg.setdefault("customization_prompts", [])
    cfg.setdefault("semantic_summary", None)
    cfg.setdefault("semantic_details", None)
    cfg.setdefault("business_questions", [])
    cfg.setdefault("ambiguities", [])
    cfg.setdefault("semantic_confidence", None)
    cfg.setdefault("eda_plan", [])
    cfg.setdefault("eda_evidence", [])
    cfg.setdefault("eda_workflow", None)
    cfg.setdefault("orchestrator_workflow", None)
    cfg.setdefault("investigation_plan", [])
    cfg.setdefault("investigation_evidence", [])
    cfg.setdefault("proposal_workflow", None)
    prefs = dict(cfg.get("dashboard_preferences") or {})
    prefs.setdefault("hidden_cards", [])
    prefs.setdefault("card_orders", {})
    cfg["dashboard_preferences"] = prefs
    raw_summary = cfg.get("semantic_summary")
    parsed_details = None
    if isinstance(raw_summary, dict):
        parsed_details = raw_summary
    elif isinstance(raw_summary, str):
        text = raw_summary.strip()
        if text.startswith("{") and text.endswith("}"):
            try:
                candidate = ast.literal_eval(text)
                if isinstance(candidate, dict):
                    parsed_details = candidate
            except Exception:
                parsed_details = None
    if isinstance(cfg.get("semantic_details"), dict):
        parsed_details = cfg["semantic_details"]
    if isinstance(parsed_details, dict):
        business_domain = str(
            parsed_details.get("business_domain")
            or parsed_details.get("dominant_business_domain")
            or parsed_details.get("dominant_domain")
            or ""
        ).strip()
        description = str(
            parsed_details.get("description")
            or parsed_details.get("inferred_meaning")
            or parsed_details.get("workbook_business_meaning")
            or parsed_details.get("semantic_summary")
            or ""
        ).strip()
        if description and business_domain:
            cfg["semantic_summary"] = f"{business_domain}: {description}"
        elif description:
            cfg["semantic_summary"] = description
        elif business_domain:
            cfg["semantic_summary"] = business_domain
        cfg["semantic_details"] = parsed_details
    return cfg


def _proposal_summary_text(proposal: models.DashboardProposal, normalized_config: dict) -> str:
    summary = str(proposal.summary or "").strip()
    if summary.startswith("{") and summary.endswith("}"):
        summary = ""
    if summary.startswith("[") and summary.endswith("]"):
        summary = ""
    semantic_summary = str(normalized_config.get("semantic_summary") or "").strip()
    if semantic_summary:
        return semantic_summary
    if summary:
        return summary
    return "Adaptive dashboard proposal generated from semantic interpretation and verified workbook evidence."


def _requested_chart_types(user_guidance: str | None) -> list[str]:
    text = str(user_guidance or "").strip().lower()
    if not text:
        return []
    matches = []
    for candidate in ("scatter", "pie", "gantt", "line", "bar", "table"):
        if re.search(rf"\b{re.escape(candidate)}(?:\s+plot)?\b", text):
            matches.append(candidate)
    return list(dict.fromkeys(matches))


def _config_chart_types(config: dict | None) -> list[str]:
    cfg = dict(config or {})
    raw = []
    spec = cfg.get("adaptive_dashboard_spec")
    if isinstance(spec, dict):
        raw.extend(list(spec.get("chart_preferences") or []))
    raw.extend(list(cfg.get("chart_preferences") or []))
    return list(
        dict.fromkeys(
            str(item).strip().lower()
            for item in raw
            if str(item or "").strip()
        )
    )


def _config_tabs(config: dict | None) -> list[str]:
    return [
        str(item.get("key") or "").strip()
        for item in list((config or {}).get("tabs") or [])
        if str(item.get("key") or "").strip()
    ]


def _config_sections(config: dict | None) -> list[tuple[str, str, str]]:
    sections: list[tuple[str, str, str]] = []
    for tab in list((config or {}).get("tabs") or []):
        tab_key = str(tab.get("key") or "").strip()
        for section in list(tab.get("sections") or []):
            key = str(section.get("key") or "").strip()
            label = str(section.get("label") or "").strip()
            if key:
                sections.append((tab_key, key, label))
    return sections


def _build_refinement_result(
    *,
    previous_config: dict | None,
    previous_title: str | None,
    previous_summary: str | None,
    next_config: dict,
    next_title: str,
    next_summary: str,
    user_guidance: str | None,
) -> DashboardRefinementResultResponse | None:
    guidance = str(user_guidance or "").strip()
    if not guidance:
        return DashboardRefinementResultResponse(
            status="not_requested",
            message="No widget refinement request was submitted.",
        )

    requested_chart_types = _requested_chart_types(guidance)
    next_chart_types = _config_chart_types(next_config)
    accepted_chart_types = [item for item in requested_chart_types if item in next_chart_types]
    missing_chart_types = [item for item in requested_chart_types if item not in next_chart_types]
    lowered_guidance = guidance.lower()
    requests_odr = any(token in lowered_guidance for token in ("odr", "default rate"))
    requests_month_select = "month" in lowered_guidance and any(token in lowered_guidance for token in ("select", "selector", "choose"))
    requests_top5 = any(token in lowered_guidance for token in ("top 5", "top five", "top-5"))

    previous_tabs = set(_config_tabs(previous_config))
    next_tabs = set(_config_tabs(next_config))
    added_tabs = sorted(next_tabs - previous_tabs)

    previous_sections = {(tab_key, section_key) for tab_key, section_key, _label in _config_sections(previous_config)}
    next_sections_full = _config_sections(next_config)
    added_sections = [(tab_key, label) for tab_key, section_key, label in next_sections_full if (tab_key, section_key) not in previous_sections]
    added_section_labels = [label for _tab_key, label in added_sections[:8]]

    changed_title = str(previous_title or "").strip() != str(next_title or "").strip()
    changed_summary = str(previous_summary or "").strip() != str(next_summary or "").strip()
    next_sections_by_key = {section_key: label for tab_key, section_key, label in next_sections_full}
    next_spec = dict(next_config.get("adaptive_dashboard_spec") or {})
    next_features = {str(item).strip() for item in list(next_spec.get("requested_features") or []) if str(item).strip()}
    next_controls = {str(item).strip() for item in list(next_spec.get("controls") or []) if str(item).strip()}

    accepted_requests: list[str] = []
    if accepted_chart_types:
        accepted_requests.append(f"Accepted chart types: {', '.join(accepted_chart_types)}")
    if added_tabs:
        accepted_requests.append(f"Added tabs: {', '.join(added_tabs)}")
    if added_section_labels:
        accepted_requests.append(f"Added widgets/sections: {', '.join(added_section_labels)}")

    unsupported_requests: list[str] = []
    if missing_chart_types:
        unsupported_requests.append(f"Requested chart types not reflected in the proposal: {', '.join(missing_chart_types)}")
    if requests_odr and "odr_top5_by_month" in next_features and "odr_top5_month" in next_sections_by_key:
        accepted_requests.append("Accepted metric feature: ODR top 5 by month")
    elif requests_odr:
        unsupported_requests.append("Requested metric feature not reflected: ODR top 5 by month")
    if requests_month_select and "month_select" in next_controls:
        accepted_requests.append("Accepted control: month selector")
    elif requests_month_select:
        unsupported_requests.append("Requested control not reflected: month selector")
    if requests_top5 and "odr_top5_month" in next_sections_by_key:
        accepted_requests.append("Accepted ranking intent: top 5")
    elif requests_top5:
        unsupported_requests.append("Requested ranking not reflected: top 5")

    warnings: list[str] = []
    if not accepted_requests:
        warnings.append("The proposal refresh completed, but no structural widget change was detected from the current request.")

    if accepted_requests and not unsupported_requests:
        status = "fulfilled"
        message = "The widget refinement request was incorporated into the proposal."
    elif accepted_requests and unsupported_requests:
        status = "partially_fulfilled"
        message = "Part of the widget refinement request was incorporated, but some requested elements were not reflected."
    else:
        status = "rejected"
        message = "The widget refinement request was not reflected in the regenerated proposal."

    return DashboardRefinementResultResponse(
        status=status,
        message=message,
        accepted_requests=accepted_requests,
        unsupported_requests=unsupported_requests,
        warnings=warnings,
        diff=DashboardRefinementDiffResponse(
            added_tabs=added_tabs,
            added_section_count=len(added_sections),
            added_section_labels=added_section_labels,
            accepted_chart_types=accepted_chart_types,
            missing_chart_types=missing_chart_types,
            changed_title=changed_title,
            changed_summary=changed_summary,
        ),
    )


def _serialize_blueprint(blueprint: models.DashboardBlueprint) -> DashboardBlueprintResponse:
    return DashboardBlueprintResponse(
        id=blueprint.id,
        blueprint_key=blueprint.blueprint_key,
        name=blueprint.name,
        description=blueprint.description,
        schema_signature=blueprint.schema_signature,
        workbook_type=blueprint.workbook_type,
        status=blueprint.status,
        config=_normalize_config(blueprint.config_json),
    )


def _serialize_proposal(
    proposal: models.DashboardProposal,
    *,
    refinement_result: DashboardRefinementResultResponse | None = None,
) -> DashboardProposalResponse:
    normalized_config = _normalize_config(proposal.proposal_json)
    return DashboardProposalResponse(
        id=proposal.id,
        snapshot_id=proposal.snapshot_id,
        status=proposal.status,
        match_mode=proposal.match_mode,
        confidence_score=proposal.confidence_score,
        title=proposal.title,
        summary=_proposal_summary_text(proposal, normalized_config),
        rationale=proposal.rationale,
        schema_signature=proposal.schema_profile.schema_signature,
        workbook_type=proposal.schema_profile.workbook_type,
        matched_blueprint_id=proposal.matched_blueprint_id,
        approved_blueprint_id=proposal.approved_blueprint_id,
        proposal=normalized_config,
        refinement_result=refinement_result,
    )


def _serialize_app_log(log: models.AppLog) -> AppLogResponse:
    return AppLogResponse(
        id=log.id,
        run_key=log.run_key,
        level=log.level,
        state=log.state,
        category=log.category,
        event=log.event,
        agent_name=log.agent_name,
        workflow=log.workflow,
        tool_name=log.tool_name,
        model_name=log.model_name,
        message=log.message,
        detail=log.detail,
        payload_json=log.payload_json,
        request_path=log.request_path,
        snapshot_id=log.snapshot_id,
        proposal_id=log.proposal_id,
        blueprint_id=log.blueprint_id,
        user_id=log.user_id,
        created_at=log.created_at.isoformat() if log.created_at else "",
    )


@router.get("/blueprint", response_model=DashboardBlueprintResponse | None)
def get_dashboard_blueprint(
    snapshot_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    snapshot = _get_snapshot(db, snapshot_id)
    ensure_schema_profile(db, snapshot=snapshot)
    blueprint = get_effective_blueprint(db, snapshot=snapshot)
    return _serialize_blueprint(blueprint) if blueprint else None


@router.post("/proposals", response_model=DashboardProposalResponse)
def propose_dashboard_blueprint(
    request: Request,
    snapshot_id: int | None = Query(default=None),
    payload: DashboardProposalRequest | None = None,
    x_gemini_api_key: str | None = Header(default=None),
    x_gemini_model: str | None = Header(default=None),
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    snapshot = _get_snapshot(db, snapshot_id)
    context_token = set_log_context(
        run_key=f"proposal:{snapshot.id}:{user.id}:{snapshot.dashboard_proposal.id if snapshot.dashboard_proposal else 'new'}",
        request_path=str(request.url.path),
        snapshot_id=snapshot.id,
        user_id=user.id,
    )
    current_blueprint = get_effective_blueprint(db, snapshot=snapshot)
    current_proposal = snapshot.dashboard_proposal
    previous_config = None
    previous_title = None
    previous_summary = None
    if current_blueprint is not None:
        previous_config = _normalize_config(current_blueprint.config_json)
        previous_title = current_blueprint.name
        previous_summary = current_blueprint.description
    elif current_proposal is not None:
        previous_config = _normalize_config(current_proposal.proposal_json)
        previous_title = current_proposal.title
        previous_summary = _proposal_summary_text(current_proposal, previous_config)
    user_guidance = (payload.user_guidance or "").strip() or None if payload else None
    try:
        log_app_event(
            level="info",
            state="started",
            category="dashboard_proposal",
            event="proposal_generation_started",
            agent_name="dashboard_proposal_router",
            workflow="dashboard_proposal_v1",
            message="Dashboard proposal generation started",
            payload={"user_guidance": user_guidance},
        )
        proposal = build_or_refresh_proposal(
            db,
            snapshot=snapshot,
            actor_user_id=user.id,
            gemini=gemini_settings_from_headers(x_gemini_api_key, x_gemini_model),
            user_guidance=user_guidance,
        )
    except Exception as exc:
        log_app_event(
            level="error",
            state="failed",
            category="dashboard_proposal",
            event="proposal_generation_failed",
            agent_name="dashboard_proposal_router",
            workflow="dashboard_proposal_v1",
            message="Dashboard proposal generation failed",
            detail={**exception_detail(exc), "user_guidance": user_guidance},
        )
        reset_log_context(context_token)
        raise
    normalized_config = _normalize_config(proposal.proposal_json)
    refinement_result = _build_refinement_result(
        previous_config=previous_config,
        previous_title=previous_title,
        previous_summary=previous_summary,
        next_config=normalized_config,
        next_title=proposal.title,
        next_summary=_proposal_summary_text(proposal, normalized_config),
        user_guidance=user_guidance,
    )
    log_app_event(
        level="info",
        state="completed",
        category="dashboard_proposal",
        event="proposal_generated",
        agent_name="dashboard_proposal_router",
        workflow="dashboard_proposal_v1",
        message="Dashboard proposal generated",
        payload={
            "refinement_status": refinement_result.status if refinement_result else "not_requested",
            "user_guidance": user_guidance,
            "proposal_id": proposal.id,
        },
        proposal_id=proposal.id,
    )
    reset_log_context(context_token)
    return _serialize_proposal(proposal, refinement_result=refinement_result)


@router.post("/proposals/{proposal_id}/approve", response_model=DashboardBlueprintResponse)
def approve_dashboard_blueprint(
    proposal_id: int,
    request: Request,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    proposal = db.query(models.DashboardProposal).filter(models.DashboardProposal.id == proposal_id).first()
    if not proposal:
        raise HTTPException(status_code=404, detail="Dashboard proposal not found")
    context_token = set_log_context(
        run_key=f"approve:{proposal.snapshot_id}:{proposal.id}:{user.id}",
        request_path=str(request.url.path),
        snapshot_id=proposal.snapshot_id,
        proposal_id=proposal.id,
        user_id=user.id,
    )
    try:
        log_app_event(
            level="info",
            state="started",
            category="dashboard_proposal",
            event="proposal_approval_started",
            agent_name="dashboard_proposal_router",
            workflow="dashboard_approval_v1",
            message="Dashboard proposal approval started",
        )
        blueprint = approve_proposal(db, proposal=proposal, actor_user_id=user.id)
    except Exception as exc:
        log_app_event(
            level="error",
            state="failed",
            category="dashboard_proposal",
            event="proposal_approval_failed",
            agent_name="dashboard_proposal_router",
            workflow="dashboard_approval_v1",
            message="Dashboard proposal approval failed",
            detail=exception_detail(exc),
        )
        reset_log_context(context_token)
        raise

    bundle_error: Exception | None = None
    try:
        generate_snapshot_analytics_bundle(
            db,
            snapshot=proposal.snapshot,
            blueprint=blueprint,
            proposal=proposal,
            force=True,
        )
    except Exception as exc:
        bundle_error = exc
        mark_snapshot_analytics_bundle_stale(db, proposal.snapshot_id)
        log_app_event(
            level="error",
            state="failed",
            category="analytics_bundle",
            event="bundle_generation_failed_after_approval",
            agent_name="analytics_bundle_generator",
            workflow="snapshot_bundle_v1",
            message="Dashboard proposal approved, but analytics bundle generation failed",
            detail=exception_detail(exc),
            blueprint_id=blueprint.id,
        )
    log_app_event(
        level="warning" if bundle_error else "info",
        state="completed" if bundle_error is None else "completed_with_warning",
        category="dashboard_proposal",
        event="proposal_approved",
        agent_name="dashboard_proposal_router",
        workflow="dashboard_approval_v1",
        message="Dashboard proposal approved" if bundle_error is None else "Dashboard proposal approved with bundle regeneration warning",
        payload={"bundle_generation": "failed" if bundle_error else "succeeded"},
        blueprint_id=blueprint.id,
    )
    reset_log_context(context_token)
    return _serialize_blueprint(blueprint)


@router.patch("/blueprint/preferences", response_model=DashboardBlueprintResponse)
def update_dashboard_blueprint_preferences(
    payload: DashboardLayoutPreferencesRequest,
    snapshot_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    snapshot = _get_snapshot(db, snapshot_id)
    ensure_schema_profile(db, snapshot=snapshot)
    blueprint = get_effective_blueprint(db, snapshot=snapshot)
    if not blueprint:
        raise HTTPException(status_code=404, detail="Approved dashboard blueprint not found for this snapshot")
    config = _normalize_config(blueprint.config_json)
    hidden_cards: list[str] = []
    seen_hidden: set[str] = set()
    for item in payload.hidden_cards:
        value = str(item or "").strip()
        if not value or value in seen_hidden:
            continue
        seen_hidden.add(value)
        hidden_cards.append(value)
    card_orders: dict[str, list[str]] = {}
    for zone, items in payload.card_orders.items():
        zone_key = str(zone or "").strip()
        if not zone_key:
            continue
        ordered: list[str] = []
        seen_order: set[str] = set()
        for item in items:
            value = str(item or "").strip()
            if not value or value in seen_order:
                continue
            seen_order.add(value)
            ordered.append(value)
        if ordered:
            card_orders[zone_key] = ordered
    config["dashboard_preferences"] = {
        "hidden_cards": hidden_cards,
        "card_orders": card_orders,
    }
    blueprint.config_json = config
    db.flush()
    proposal = snapshot.dashboard_proposal
    generate_snapshot_analytics_bundle(
        db,
        snapshot=snapshot,
        blueprint=blueprint,
        proposal=proposal,
        force=True,
    )
    return _serialize_blueprint(blueprint)


@router.get("/runtime")
def get_dashboard_runtime(
    request: Request,
    snapshot_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    snapshot = _get_snapshot(db, snapshot_id)
    context_token = _set_dashboard_read_context(request=request, snapshot_id=snapshot.id, route_name="dashboard_runtime_v1")
    profile = ensure_schema_profile(db, snapshot=snapshot)
    blueprint = get_effective_blueprint(db, snapshot=snapshot)
    proposal = snapshot.dashboard_proposal
    try:
        log_app_event(
            level="info",
            state="started",
            category="dashboard_read",
            event="runtime_requested",
            message="Dashboard runtime requested",
        )
        bundle = get_or_generate_snapshot_analytics_bundle(
            db,
            snapshot=snapshot,
            blueprint=blueprint,
            proposal=proposal,
        )
        bundle_payload = dict(bundle.payload_json or {})
        dashboard_payload = dict(bundle_payload.get("dashboard") or {})
        payload = dashboard_payload.get("runtime_payload")
        if payload is None:
            payload = get_dashboard_runtime_payload(
                db,
                snapshot,
                profile.workbook_type,
                blueprint.config_json if blueprint else None,
            )
        log_app_event(
            level="info",
            state="completed",
            category="dashboard_read",
            event="runtime_loaded",
            message="Dashboard runtime loaded",
            payload={"bundle_version": bundle.bundle_version, "generation_mode": bundle.generation_mode},
        )
        return {
            "snapshot_id": snapshot.id,
            "workbook_type": profile.workbook_type,
            "payload": payload,
            "bundle_version": bundle.bundle_version,
            "generated_at": bundle.generated_at.isoformat() if bundle.generated_at else None,
            "stale": bundle.stale,
            "generation_mode": bundle.generation_mode,
        }
    except Exception as exc:
        log_app_event(
            level="error",
            state="failed",
            category="dashboard_read",
            event="runtime_failed",
            message="Dashboard runtime load failed",
            detail=exception_detail(exc),
        )
        raise
    finally:
        reset_log_context(context_token)


@router.get("/analytics-bundle/debug")
def get_dashboard_analytics_bundle_debug(
    snapshot_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    if user.role != models.UserRole.admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    snapshot = _get_snapshot(db, snapshot_id)
    bundle = get_snapshot_analytics_bundle(db, snapshot.id)
    if bundle is None:
        raise HTTPException(status_code=404, detail="Analytics bundle not found")
    return {
        "snapshot_id": snapshot.id,
        "bundle_version": bundle.bundle_version,
        "status": bundle.status,
        "generation_mode": bundle.generation_mode,
        "stale": bundle.stale,
        "generated_at": bundle.generated_at.isoformat() if bundle.generated_at else None,
        "updated_at": bundle.updated_at.isoformat() if bundle.updated_at else None,
        "payload": bundle.payload_json,
        "diagnostics": bundle.diagnostics_json,
    }


@router.get("/logs", response_model=list[AppLogResponse])
def list_dashboard_logs(
    snapshot_id: int | None = Query(default=None),
    proposal_id: int | None = Query(default=None),
    run_key: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    if user.role != models.UserRole.admin:
        raise HTTPException(status_code=403, detail="Admin access required")
    query = db.query(models.AppLog)
    if snapshot_id is not None:
        query = query.filter(models.AppLog.snapshot_id == snapshot_id)
    if proposal_id is not None:
        query = query.filter(models.AppLog.proposal_id == proposal_id)
    if run_key:
        query = query.filter(models.AppLog.run_key == run_key)
    rows = query.order_by(models.AppLog.created_at.desc(), models.AppLog.id.desc()).limit(limit).all()
    return [_serialize_app_log(item) for item in rows]


@router.get("/kpis", response_model=KpiResponse)
def get_kpis(
    request: Request,
    snapshot_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    snap = _get_snapshot(db, snapshot_id)
    context_token = _set_dashboard_read_context(request=request, snapshot_id=snap.id, route_name="dashboard_kpis_v1")
    profile = ensure_schema_profile(db, snapshot=snap)
    blueprint = get_effective_blueprint(db, snapshot=snap)
    proposal = snap.dashboard_proposal
    try:
        log_app_event(
            level="info",
            state="started",
            category="dashboard_read",
            event="kpis_requested",
            message="Dashboard KPI payload requested",
        )
        bundle = get_or_generate_snapshot_analytics_bundle(
            db,
            snapshot=snap,
            blueprint=blueprint,
            proposal=proposal,
        )
        bundle_dashboard = dict((bundle.payload_json or {}).get("dashboard") or {})
        surface_payloads = dict(bundle_dashboard.get("surface_payloads") or {})
        kpi_payload = surface_payloads.get("kpis")
        if isinstance(kpi_payload, dict):
            response = KpiResponse.model_validate(kpi_payload)
            log_app_event(
                level="info",
                state="completed",
                category="dashboard_read",
                event="kpis_loaded",
                message="Dashboard KPI payload loaded from analytics bundle",
                payload={"bundle_version": bundle.bundle_version},
            )
            return response

        rows = (
            db.query(models.ClientSnapshot, models.Client)
            .join(models.Client, models.Client.id == models.ClientSnapshot.client_id)
            .filter(models.ClientSnapshot.snapshot_id == snap.id)
            .all()
        )

        tot_con_ind = sum(r.ClientSnapshot.contracted_ind for r in rows)
        tot_con_bus = sum(r.ClientSnapshot.contracted_bus for r in rows)
        tot_con = tot_con_ind + tot_con_bus
        tot_rec_ind = sum(r.ClientSnapshot.received_ind for r in rows)
        tot_rec_bus = sum(r.ClientSnapshot.received_bus for r in rows)
        tot_rec = tot_rec_ind + tot_rec_bus
        tot_pend = tot_con - tot_rec
        overall_rate = (tot_rec / tot_con) if tot_con > 0 else 0.0

        active_clients = sum(1 for r in rows if (r.ClientSnapshot.contracted_total > 0 or r.ClientSnapshot.received_total > 0))
        zero_clients = sum(1 for r in rows if (r.ClientSnapshot.contracted_total > 0 and r.ClientSnapshot.received_total == 0))
        over_clients = sum(1 for r in rows if (r.ClientSnapshot.received_total > r.ClientSnapshot.contracted_total))

        staff_total = (
            db.query(models.StaffSnapshot)
            .filter(models.StaffSnapshot.snapshot_id == snap.id)
            .all()
        )
        staff_tot_received = sum(s.received_total for s in staff_total)

        response = KpiResponse(
            snapshot=SnapshotSummary(id=snap.id, as_of_date=snap.as_of_date, source_filename=snap.source_filename),
            total_contracted=tot_con,
            total_received=tot_rec,
            total_pending=tot_pend,
            total_contracted_ind=tot_con_ind,
            total_contracted_bus=tot_con_bus,
            total_received_ind=tot_rec_ind,
            total_received_bus=tot_rec_bus,
            overall_receipt_rate=overall_rate,
            active_clients=active_clients,
            zero_received_clients=zero_clients,
            over_delivered_clients=over_clients,
            staff_total_received=staff_tot_received,
        )
        log_app_event(
            level="warning",
            state="completed_with_warning",
            category="dashboard_read",
            event="kpis_loaded_legacy_fallback",
            message="Dashboard KPI payload fell back to legacy query path",
        )
        return response
    except Exception as exc:
        log_app_event(
            level="error",
            state="failed",
            category="dashboard_read",
            event="kpis_failed",
            message="Dashboard KPI payload load failed",
            detail=exception_detail(exc),
        )
        raise
    finally:
        reset_log_context(context_token)


@router.get("/clients", response_model=list[ClientRow])
def list_clients(
    request: Request,
    snapshot_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    snap = _get_snapshot(db, snapshot_id)
    context_token = _set_dashboard_read_context(request=request, snapshot_id=snap.id, route_name="dashboard_clients_v1")
    profile = ensure_schema_profile(db, snapshot=snap)
    blueprint = get_effective_blueprint(db, snapshot=snap)
    proposal = snap.dashboard_proposal
    try:
        log_app_event(
            level="info",
            state="started",
            category="dashboard_read",
            event="clients_requested",
            message="Dashboard client payload requested",
        )
        bundle = get_or_generate_snapshot_analytics_bundle(
            db,
            snapshot=snap,
            blueprint=blueprint,
            proposal=proposal,
        )
        bundle_dashboard = dict((bundle.payload_json or {}).get("dashboard") or {})
        surface_payloads = dict(bundle_dashboard.get("surface_payloads") or {})
        client_payload = surface_payloads.get("clients")
        if isinstance(client_payload, list):
            response = [ClientRow.model_validate(item) for item in client_payload if isinstance(item, dict)]
            log_app_event(
                level="info",
                state="completed",
                category="dashboard_read",
                event="clients_loaded",
                message="Dashboard client payload loaded from analytics bundle",
                payload={"row_count": len(response), "bundle_version": bundle.bundle_version},
            )
            return response

        rows = (
            db.query(models.ClientSnapshot, models.Client)
            .join(models.Client, models.Client.id == models.ClientSnapshot.client_id)
            .filter(models.ClientSnapshot.snapshot_id == snap.id)
            .all()
        )

        out: list[ClientRow] = []
        for cs, c in rows:
            rate = (cs.received_total / cs.contracted_total) if cs.contracted_total > 0 else (None if cs.received_total > 0 else 0.0)
            out.append(
                ClientRow(
                    client_name=c.name,
                    client_id=c.external_id,
                    client_type=c.client_type,
                    contracted_ind=cs.contracted_ind,
                    contracted_bus=cs.contracted_bus,
                    contracted_total=cs.contracted_total,
                    received_ind=cs.received_ind,
                    received_bus=cs.received_bus,
                    received_total=cs.received_total,
                    pending_ind=cs.pending_ind,
                    pending_bus=cs.pending_bus,
                    pending_total=cs.pending_total,
                    receipt_rate=rate,
                )
            )
        log_app_event(
            level="warning",
            state="completed_with_warning",
            category="dashboard_read",
            event="clients_loaded_legacy_fallback",
            message="Dashboard client payload fell back to legacy query path",
            payload={"row_count": len(out)},
        )
        return out
    except Exception as exc:
        log_app_event(
            level="error",
            state="failed",
            category="dashboard_read",
            event="clients_failed",
            message="Dashboard client payload load failed",
            detail=exception_detail(exc),
        )
        raise
    finally:
        reset_log_context(context_token)


@router.get("/staff", response_model=list[StaffRow])
def list_staff(
    request: Request,
    snapshot_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    snap = _get_snapshot(db, snapshot_id)
    context_token = _set_dashboard_read_context(request=request, snapshot_id=snap.id, route_name="dashboard_staff_v1")
    profile = ensure_schema_profile(db, snapshot=snap)
    blueprint = get_effective_blueprint(db, snapshot=snap)
    proposal = snap.dashboard_proposal
    try:
        log_app_event(
            level="info",
            state="started",
            category="dashboard_read",
            event="staff_requested",
            message="Dashboard staff payload requested",
        )
        bundle = get_or_generate_snapshot_analytics_bundle(
            db,
            snapshot=snap,
            blueprint=blueprint,
            proposal=proposal,
        )
        bundle_dashboard = dict((bundle.payload_json or {}).get("dashboard") or {})
        surface_payloads = dict(bundle_dashboard.get("surface_payloads") or {})
        staff_payload = surface_payloads.get("staff")
        if isinstance(staff_payload, list):
            response = [StaffRow.model_validate(item) for item in staff_payload if isinstance(item, dict)]
            log_app_event(
                level="info",
                state="completed",
                category="dashboard_read",
                event="staff_loaded",
                message="Dashboard staff payload loaded from analytics bundle",
                payload={"row_count": len(response), "bundle_version": bundle.bundle_version},
            )
            return response

        rows = (
            db.query(models.StaffSnapshot)
            .filter(models.StaffSnapshot.snapshot_id == snap.id)
            .order_by(models.StaffSnapshot.received_total.desc())
            .all()
        )
        response = [
            StaffRow(
                name=r.name,
                staff_id=r.staff_external_id,
                staff_type=r.staff_type,
                received_ind=r.received_ind,
                received_bus=r.received_bus,
                received_total=r.received_total,
            )
            for r in rows
        ]
        log_app_event(
            level="warning",
            state="completed_with_warning",
            category="dashboard_read",
            event="staff_loaded_legacy_fallback",
            message="Dashboard staff payload fell back to legacy query path",
            payload={"row_count": len(response)},
        )
        return response
    except Exception as exc:
        log_app_event(
            level="error",
            state="failed",
            category="dashboard_read",
            event="staff_failed",
            message="Dashboard staff payload load failed",
            detail=exception_detail(exc),
        )
        raise
    finally:
        reset_log_context(context_token)


@router.post("/chat", response_model=DashboardChatResponse)
def chat_about_dashboard(
    payload: DashboardChatRequest,
    x_gemini_api_key: str | None = Header(default=None),
    x_gemini_model: str | None = Header(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    gemini = gemini_settings_from_headers(x_gemini_api_key, x_gemini_model)
    if gemini is None:
        from app.settings import settings

        if not settings.openai_api_key:
            raise HTTPException(status_code=400, detail="Agentic chat requires an active model provider. Enable Gemini in Settings or configure the backend report agent.")

    from app.reporting import generate_agentic_chat_response, load_snapshot_report_context

    snapshot = _get_snapshot(db, payload.snapshot_id)
    ctx = load_snapshot_report_context(db, snapshot.id)
    try:
        return generate_agentic_chat_response(ctx=ctx, question=payload.question, gemini=gemini)
    except GeminiReasoningError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
