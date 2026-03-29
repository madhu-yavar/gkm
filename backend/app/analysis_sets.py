from __future__ import annotations

from io import BytesIO
from math import fabs
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from sqlalchemy.orm import Session

from app import models
from app.analytics_bundle import get_or_generate_snapshot_analytics_bundle
from app.dashboard_blueprints import ensure_schema_profile, get_effective_blueprint
from app.gemini_reasoning import GeminiReasoningError, GeminiRequestSettings, gemini_generate_structured
from app.reporting import (
    ChartInterpretationSpec,
    EvidenceItem,
    _line_chart,
    _render_chart_interpretation_block,
    _run_chart_interpretation_graph,
    _vertical_bar_chart,
)
from app.schemas import AnalysisSetMemberResponse, AnalysisSetProposalResponse


GENERIC_JOIN_KEYS = {
    "client id",
    "client name",
    "client type",
    "staff id",
    "staff name",
    "product id",
    "product",
    "category",
    "status",
    "owner name",
    "unit",
    "house",
    "number",
    "tower",
    "pools",
    "machine",
    "process",
    "operator",
}


def _combined_risk_label(rate: float | None) -> str:
    if rate is None:
        return "Uncontracted"
    if rate == 0:
        return "Not Started"
    if rate < 0.15:
        return "Critical"
    if rate < 0.35:
        return "At Risk"
    if rate < 0.6:
        return "On Track"
    return "Ahead"


class AnalysisSetAgentDraft(BaseModel):
    name: str
    summary: str
    relationship_type: str
    comparability: str
    rationale: str
    suggested_join_keys: list[str] = Field(default_factory=list, max_length=8)
    dashboard_hypothesis: list[str] = Field(default_factory=list, max_length=6)
    conflicts: list[str] = Field(default_factory=list, max_length=8)


def _snapshot_headers(profile_json: dict[str, Any]) -> set[str]:
    headers: set[str] = set()
    for sheet in list(profile_json.get("sheets") or []):
        for section in list(sheet.get("sections") or []):
            for field in list(section.get("fields") or []):
                normalized = str(field.get("normalized_header") or "").strip().lower()
                if normalized:
                    headers.add(normalized)
    return headers


def _bundle_semantic_domain(bundle: models.SnapshotAnalyticsBundle | None) -> str:
    if bundle is None:
        return ""
    payload = dict(bundle.payload_json or {})
    semantic = dict(payload.get("semantic") or {})
    details = dict(semantic.get("details") or {})
    return str(details.get("business_domain") or semantic.get("summary") or "").strip()


def _member_response(
    *,
    snapshot: models.Snapshot,
    workbook_type: str | None,
    order: int,
    role_label: str | None = None,
) -> AnalysisSetMemberResponse:
    return AnalysisSetMemberResponse(
        snapshot_id=snapshot.id,
        source_filename=snapshot.source_filename,
        as_of_date=snapshot.as_of_date,
        workbook_type=workbook_type,
        member_order=order,
        role_label=role_label,
    )


def _deterministic_relationship_type(workbook_types: list[str], signature_match: bool, dates: list[Any]) -> str:
    unique_types = {item for item in workbook_types if item}
    if len(unique_types) == 1 and len(set(dates)) > 1:
        return "time_series"
    if len(unique_types) == 1 and signature_match:
        return "scenario_comparison"
    if len(unique_types) == 1:
        return "portfolio_comparison"
    return "semantic_comparison"


def _comparability_label(score: float) -> str:
    if score >= 0.9:
        return "high"
    if score >= 0.7:
        return "medium"
    return "low"


def _similarity_prompt(
    *,
    intent: str | None,
    member_summaries: list[dict[str, Any]],
    deterministic: AnalysisSetAgentDraft,
) -> str:
    return (
        "You are the multi-document similarity agent for processed analytical workbooks.\n"
        "Decide whether the selected snapshots should be compared as a time series, scenario set, portfolio comparison, or semantic comparison.\n"
        "Keep the response grounded in the provided processed metadata only.\n"
        "Return JSON that matches the required schema.\n\n"
        f"User intent: {intent or 'No extra intent provided.'}\n\n"
        f"Processed snapshot summaries:\n{member_summaries}\n\n"
        f"Deterministic baseline:\n{deterministic.model_dump()}\n"
    )


def _analysis_set_draft(
    *,
    snapshots: list[models.Snapshot],
    profiles: list[models.WorkbookSchemaProfile],
    bundles: list[models.SnapshotAnalyticsBundle | None],
    intent: str | None,
    title: str | None,
    gemini: GeminiRequestSettings | None,
) -> tuple[AnalysisSetAgentDraft, float]:
    workbook_types = [str(profile.workbook_type or "").strip() for profile in profiles]
    signatures = [str(profile.schema_signature or "").strip() for profile in profiles]
    signature_match = len(set(signatures)) == 1 if signatures else False
    dates = [snapshot.as_of_date for snapshot in snapshots]
    shared_headers = set.intersection(*[_snapshot_headers(profile.profile_json) for profile in profiles]) if profiles else set()
    join_keys = [header.title() for header in sorted(shared_headers & GENERIC_JOIN_KEYS)][:6]
    domains = [domain for domain in (_bundle_semantic_domain(bundle) for bundle in bundles) if domain]
    common_domain = domains[0] if domains and len(set(domains)) == 1 else ""
    score = 0.45
    if len(set(workbook_types)) == 1:
        score += 0.2
    if signature_match:
        score += 0.2
    if join_keys:
        score += 0.1
    if common_domain:
        score += 0.1
    score = min(score, 0.98)
    relationship_type = _deterministic_relationship_type(workbook_types, signature_match, dates)
    comparability = _comparability_label(score)
    conflicts: list[str] = []
    if len(set(workbook_types)) > 1:
        conflicts.append("Workbook families differ, so comparisons must rely on semantic alignment rather than exact schema stacking.")
    if not join_keys:
        conflicts.append("No strong shared join key was detected automatically. User confirmation is required before merging.")
    if len(set(dates)) != len(dates):
        conflicts.append("At least two selected documents share the same as-of date; confirm whether they are scenarios or duplicates.")
    member_summaries = [
        {
            "snapshot_id": snapshot.id,
            "source_filename": snapshot.source_filename,
            "as_of_date": snapshot.as_of_date.isoformat(),
            "workbook_type": profile.workbook_type,
            "business_domain": _bundle_semantic_domain(bundle),
            "shared_headers": sorted((_snapshot_headers(profile.profile_json) & GENERIC_JOIN_KEYS))[:10],
        }
        for snapshot, profile, bundle in zip(snapshots, profiles, bundles)
    ]
    base_name = title or f"{snapshots[0].source_filename.rsplit('.', 1)[0]} Combined Analysis"
    deterministic = AnalysisSetAgentDraft(
        name=base_name,
        summary=f"Combined {relationship_type.replace('_', ' ')} across {len(snapshots)} processed documents.",
        relationship_type=relationship_type,
        comparability=comparability,
        rationale=(
            f"The proposal is based on {len(snapshots)} processed snapshots, shared workbook family `{workbook_types[0]}`." if len(set(workbook_types)) == 1
            else f"The proposal spans {len(set(workbook_types))} workbook families and will need semantic alignment."
        ),
        suggested_join_keys=join_keys or ["User confirmation required"],
        dashboard_hypothesis=[
            "Surface cross-document trends, deltas, and concentration shifts.",
            "Let the user compare the selected documents side by side before drilling into merged insights.",
            "Carry forward only user-confirmed join keys into the combined analytics pipeline.",
        ],
        conflicts=conflicts,
    )
    if gemini is None:
        return deterministic, score
    try:
        refined = gemini_generate_structured(
            prompt=_similarity_prompt(intent=intent, member_summaries=member_summaries, deterministic=deterministic),
            schema=AnalysisSetAgentDraft,
            settings=gemini,
        )
        return refined, score
    except GeminiReasoningError:
        return deterministic, score


def propose_analysis_set(
    db: Session,
    *,
    snapshot_ids: list[int],
    actor_user_id: int | None,
    intent: str | None = None,
    title: str | None = None,
    gemini: GeminiRequestSettings | None = None,
) -> models.AnalysisSet:
    unique_ids = list(dict.fromkeys(snapshot_ids))
    snapshots = (
        db.query(models.Snapshot)
        .filter(models.Snapshot.id.in_(unique_ids))
        .order_by(models.Snapshot.as_of_date.asc(), models.Snapshot.id.asc())
        .all()
    )
    if len(snapshots) < 2:
        raise ValueError("Select at least two processed documents for combined analysis.")

    profiles = [ensure_schema_profile(db, snapshot=snapshot) for snapshot in snapshots]
    bundles = [
        get_or_generate_snapshot_analytics_bundle(
            db,
            snapshot=snapshot,
            blueprint=get_effective_blueprint(db, snapshot=snapshot),
            proposal=snapshot.dashboard_proposal,
        )
        for snapshot in snapshots
    ]
    draft, score = _analysis_set_draft(
        snapshots=snapshots,
        profiles=profiles,
        bundles=bundles,
        intent=intent,
        title=title,
        gemini=gemini,
    )
    analysis_set = models.AnalysisSet(
        name=draft.name,
        summary=draft.summary,
        intent=intent,
        status="draft",
        relationship_type=draft.relationship_type,
        confidence_score=score,
        proposal_json={
            "comparability": draft.comparability,
            "rationale": draft.rationale,
            "suggested_join_keys": draft.suggested_join_keys,
            "conflicts": draft.conflicts,
            "dashboard_hypothesis": draft.dashboard_hypothesis,
        },
        created_by_user_id=actor_user_id,
    )
    db.add(analysis_set)
    db.flush()
    for order, (snapshot, profile) in enumerate(zip(snapshots, profiles), start=1):
        db.add(
            models.AnalysisSetMember(
                analysis_set_id=analysis_set.id,
                snapshot_id=snapshot.id,
                member_order=order,
                role_label=f"Document {order}",
                source_filename=snapshot.source_filename,
                as_of_date=snapshot.as_of_date,
                workbook_type=profile.workbook_type,
            )
        )
    db.flush()
    return analysis_set


def confirm_analysis_set(
    db: Session,
    *,
    analysis_set: models.AnalysisSet,
    actor_user_id: int | None,
    title: str | None = None,
    intent: str | None = None,
    relationship_type: str | None = None,
    join_keys: list[str] | None = None,
    member_labels: dict[int, str] | None = None,
) -> models.AnalysisSet:
    analysis_set.name = (title or analysis_set.name).strip() or analysis_set.name
    analysis_set.intent = (intent or analysis_set.intent or "").strip() or analysis_set.intent
    analysis_set.relationship_type = (relationship_type or analysis_set.relationship_type).strip() or analysis_set.relationship_type
    proposal_json = dict(analysis_set.proposal_json or {})
    if join_keys is not None:
        proposal_json["confirmed_join_keys"] = [str(item).strip() for item in join_keys if str(item).strip()]
    labels = member_labels or {}
    for member in analysis_set.members:
        override = labels.get(member.snapshot_id)
        if isinstance(override, str) and override.strip():
            member.role_label = override.strip()
    proposal_json["member_labels"] = {str(member.snapshot_id): member.role_label for member in analysis_set.members if member.role_label}
    analysis_set.proposal_json = proposal_json
    analysis_set.confirmed_json = {
        "confirmed_join_keys": proposal_json.get("confirmed_join_keys") or proposal_json.get("suggested_join_keys") or [],
        "member_labels": proposal_json.get("member_labels") or {},
        "dashboard_hypothesis": proposal_json.get("dashboard_hypothesis") or [],
    }
    analysis_set.status = "confirmed"
    analysis_set.confirmed_by_user_id = actor_user_id
    analysis_set.confirmed_at = datetime.now(timezone.utc)
    db.flush()
    return analysis_set


def serialize_analysis_set(analysis_set: models.AnalysisSet) -> AnalysisSetProposalResponse:
    proposal = dict(analysis_set.proposal_json or {})
    members = [
        AnalysisSetMemberResponse(
            snapshot_id=member.snapshot_id,
            source_filename=member.source_filename,
            as_of_date=member.as_of_date,
            workbook_type=member.workbook_type,
            member_order=member.member_order,
            role_label=member.role_label,
        )
        for member in sorted(analysis_set.members, key=lambda item: (item.member_order, item.id))
    ]
    return AnalysisSetProposalResponse(
        id=analysis_set.id,
        status=analysis_set.status,
        name=analysis_set.name,
        summary=analysis_set.summary,
        intent=analysis_set.intent,
        relationship_type=analysis_set.relationship_type,
        confidence_score=analysis_set.confidence_score,
        comparability=str(proposal.get("comparability") or "low"),
        rationale=str(proposal.get("rationale") or ""),
        suggested_join_keys=[str(item) for item in list(proposal.get("confirmed_join_keys") or proposal.get("suggested_join_keys") or []) if str(item).strip()],
        suggested_period_order=members,
        conflicts=[str(item) for item in list(proposal.get("conflicts") or []) if str(item).strip()],
        dashboard_hypothesis=[str(item) for item in list(proposal.get("dashboard_hypothesis") or []) if str(item).strip()],
        members=members,
    )


def list_analysis_sets(db: Session) -> list[models.AnalysisSet]:
    return (
        db.query(models.AnalysisSet)
        .order_by(models.AnalysisSet.confirmed_at.desc().nullslast(), models.AnalysisSet.created_at.desc(), models.AnalysisSet.id.desc())
        .all()
    )


def _bundle_member_label(member: models.AnalysisSetMember) -> str:
    if member.role_label and member.role_label.strip():
        return member.role_label.strip()
    if member.as_of_date:
        return member.as_of_date.isoformat()
    return member.source_filename.rsplit(".", 1)[0]


def _analysis_set_bundle_payload(analysis_set: models.AnalysisSet) -> dict[str, Any] | None:
    payload = dict(analysis_set.confirmed_json or {})
    bundle = payload.get("bundle")
    return bundle if isinstance(bundle, dict) else None


def _build_pdf_styles():
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="PdfTitle", parent=styles["Heading1"], fontName="Helvetica-Bold", fontSize=21, leading=25, textColor=colors.HexColor("#1f1636"), spaceAfter=10))
    styles.add(ParagraphStyle(name="PdfSubtitle", parent=styles["BodyText"], fontName="Helvetica", fontSize=10, leading=14, textColor=colors.HexColor("#5f6170"), spaceAfter=10))
    styles.add(ParagraphStyle(name="PdfSection", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=13, leading=17, textColor=colors.HexColor("#2f2548"), spaceBefore=10, spaceAfter=6))
    styles.add(ParagraphStyle(name="PdfBody", parent=styles["BodyText"], fontName="Helvetica", fontSize=9.5, leading=14, textColor=colors.HexColor("#374151")))
    styles.add(ParagraphStyle(name="ChartCaption", parent=styles["BodyText"], fontName="Helvetica", fontSize=8.8, leading=13, textColor=colors.HexColor("#5f6170")))
    return styles


def _report_table(rows: list[list[str]], widths: list[float] | None = None) -> Table:
    table = Table(rows, colWidths=widths, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f3f0ff")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#4b3b80")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, 0), 8),
                ("BACKGROUND", (0, 1), (-1, -1), colors.white),
                ("TEXTCOLOR", (0, 1), (-1, -1), colors.HexColor("#334155")),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 1), (-1, -1), 8.5),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#e5e7eb")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#fafafa")]),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    return table


def _analysis_set_evidence(bundle: dict[str, Any]) -> list[EvidenceItem]:
    evidence_payload = list(bundle.get("chat_context", {}).get("evidence") or [])
    evidence: list[EvidenceItem] = []
    for item in evidence_payload:
        if not isinstance(item, dict):
            continue
        evidence.append(
            EvidenceItem(
                id=str(item.get("id") or f"AS-{len(evidence)+1:03d}"),
                tab=str(item.get("tab") or "overview"),
                title=str(item.get("title") or "Combined finding"),
                detail=str(item.get("detail") or ""),
            )
        )
    return evidence


def _trend_direction(values: list[float]) -> str:
    if len(values) < 2:
        return "stable"
    delta = values[-1] - values[0]
    if delta > 0:
        return "rising"
    if delta < 0:
        return "falling"
    return "stable"


def _variance_analysis_set_bundle(
    analysis_set: models.AnalysisSet,
    member_payloads: list[dict[str, Any]],
) -> dict[str, Any]:
    labels = [item["label"] for item in member_payloads]
    kpis_series = [item["surface_payloads"]["kpis"] for item in member_payloads]
    staff_series = [list(item["surface_payloads"].get("staff") or []) for item in member_payloads]
    latest_kpis = kpis_series[-1]
    earliest_kpis = kpis_series[0]
    receipt_rate_values = [round(float(item.get("overall_receipt_rate") or 0.0) * 100, 2) for item in kpis_series]
    received_values = [float(item.get("total_received") or 0) for item in kpis_series]
    pending_values = [float(item.get("total_pending") or 0) for item in kpis_series]
    contracted_values = [float(item.get("total_contracted") or 0) for item in kpis_series]
    forecast_receipt_rate = receipt_rate_values[-1] + (receipt_rate_values[-1] - receipt_rate_values[-2] if len(receipt_rate_values) >= 2 else 0.0)
    forecast_pending = max(pending_values[-1] + (pending_values[-1] - pending_values[-2] if len(pending_values) >= 2 else 0.0), 0.0)

    latest_clients = list(member_payloads[-1]["surface_payloads"].get("clients") or [])
    earliest_clients = list(member_payloads[0]["surface_payloads"].get("clients") or [])
    earliest_by_id = {str(item.get("client_id") or item.get("client_name")): item for item in earliest_clients}
    delta_rows: list[dict[str, Any]] = []
    for client in latest_clients:
        key = str(client.get("client_id") or client.get("client_name"))
        previous = earliest_by_id.get(key)
        if not previous:
            continue
        latest_rate = float(client.get("receipt_rate") or 0.0)
        previous_rate = float(previous.get("receipt_rate") or 0.0)
        latest_pending = int(client.get("pending_total") or 0)
        previous_pending = int(previous.get("pending_total") or 0)
        delta_rows.append(
            {
                "client_name": str(client.get("client_name") or key),
                "previous_rate": previous_rate,
                "latest_rate": latest_rate,
                "rate_delta": latest_rate - previous_rate,
                "pending_delta": latest_pending - previous_pending,
                "latest_pending": latest_pending,
            }
        )
    delta_rows.sort(key=lambda item: (fabs(item["rate_delta"]) + fabs(item["pending_delta"]) / 1000.0), reverse=True)
    top_backlog_clients = sorted(latest_clients, key=lambda item: int(item.get("pending_total") or 0), reverse=True)[:8]
    improving_clients = sorted(delta_rows, key=lambda item: item["rate_delta"], reverse=True)[:6]
    deteriorating_clients = sorted(delta_rows, key=lambda item: item["rate_delta"])[:6]
    total_latest_pending = sum(int(item.get("pending_total") or 0) for item in latest_clients) or 1
    backlog_share_rows = [
        {
            "label": str(item.get("client_name") or "Client"),
            "value": float(item.get("pending_total") or 0),
            "share": float(item.get("pending_total") or 0) / total_latest_pending,
        }
        for item in top_backlog_clients
    ]
    band_labels = ["Ahead", "On Track", "At Risk", "Critical", "Not Started", "Uncontracted"]
    earliest_band_counts = {label: 0 for label in band_labels}
    latest_band_counts = {label: 0 for label in band_labels}
    for item in earliest_clients:
      earliest_band_counts[_combined_risk_label(item.get("receipt_rate"))] += 1
    for item in latest_clients:
      latest_band_counts[_combined_risk_label(item.get("receipt_rate"))] += 1
    latest_staff = sorted(staff_series[-1], key=lambda item: int(item.get("received_total") or 0), reverse=True)[:8] if staff_series else []

    findings = [
        f"Receipt rate moved from {receipt_rate_values[0]:.1f}% in {labels[0]} to {receipt_rate_values[-1]:.1f}% in {labels[-1]}.",
        f"Pending work changed from {int(pending_values[0]):,} to {int(pending_values[-1]):,} across the selected periods.",
        f"The largest current backlog sits with {top_backlog_clients[0]['client_name']} at {int(top_backlog_clients[0]['pending_total']):,} pending items."
        if top_backlog_clients
        else "No client backlog concentration was available in the latest selected period.",
        f"The deterministic next-point outlook projects receipt rate at {forecast_receipt_rate:.1f}% and pending work near {int(forecast_pending):,} if the current direction persists.",
        f"Backlog concentration is led by the top {min(len(top_backlog_clients), 5)} clients, who hold {sum(row['share'] for row in backlog_share_rows[:5]) * 100:.1f}% of the latest pending workload." if backlog_share_rows else "No backlog concentration profile was available.",
    ]

    widgets = [
        {
            "key": "combined-receipt-rate-trend",
            "tab": "overview",
            "title": "Receipt Rate Trend",
            "description": "How receipt progress moved across the selected processed documents.",
            "chart_type": "line",
            "items": [{"label": label, "value": value} for label, value in zip(labels, receipt_rate_values)],
            "insight": f"Receipt progress is {_trend_direction(receipt_rate_values)} across the confirmed document order.",
        },
        {
            "key": "combined-receipt-rate-comparison",
            "tab": "overview",
            "title": "Receipt Rate Comparison",
            "description": "Period-by-period receipt-rate comparison across the selected documents.",
            "chart_type": "bar",
            "items": [{"label": label, "value": value} for label, value in zip(labels, receipt_rate_values)],
            "insight": "This chart makes it easier to compare each selected period directly, instead of inferring movement from the trend line alone.",
        },
        {
            "key": "combined-pending-trend",
            "tab": "trends",
            "title": "Pending Work Trend",
            "description": "Total pending work across the selected documents.",
            "chart_type": "line",
            "items": [{"label": label, "value": value} for label, value in zip(labels, pending_values)],
            "insight": f"Pending exposure is {_trend_direction(pending_values)} across the combined analysis set.",
        },
        {
            "key": "combined-received-trend",
            "tab": "trends",
            "title": "Received Volume Trend",
            "description": "Total received workload across the selected documents.",
            "chart_type": "line",
            "items": [{"label": label, "value": value} for label, value in zip(labels, received_values)],
            "insight": f"Received volume is {_trend_direction(received_values)} across the confirmed comparison sequence.",
        },
        {
            "key": "combined-forecast",
            "tab": "trends",
            "title": "Deterministic Forecast Outlook",
            "description": "Current versus projected next-point receipt rate outlook from the merged time-order.",
            "chart_type": "line",
            "items": [
                {"label": labels[-1], "value": receipt_rate_values[-1]},
                {"label": "Projected Next", "value": forecast_receipt_rate},
            ],
            "insight": "This forecast is deterministic and should be used for planning, not as a commitment-grade prediction.",
        },
        {
            "key": "combined-top-backlog",
            "tab": "clients",
            "title": "Top Backlog Clients In Latest Period",
            "description": "Clients carrying the highest pending workload in the latest selected document.",
            "chart_type": "bar",
            "items": [
                {
                    "label": str(item.get("client_name") or "Client"),
                    "value": float(item.get("pending_total") or 0),
                    "meta": f"Receipt rate {(float(item.get('receipt_rate') or 0) * 100):.1f}%",
                }
                for item in top_backlog_clients
            ],
            "insight": "The latest document should drive operational follow-up because it shows current backlog concentration by client.",
        },
        {
            "key": "combined-backlog-concentration",
            "tab": "overview",
            "title": "Backlog Concentration",
            "description": "How much of the latest pending workload sits in the most exposed clients.",
            "chart_type": "bar",
            "items": [{"label": row["label"], "value": row["value"]} for row in backlog_share_rows],
            "insight": "A steep concentration shape means a small client cohort is dominating the current backlog risk.",
        },
        {
            "key": "combined-client-shift-table",
            "tab": "clients",
            "title": "Client Receipt Change",
            "description": "Earliest versus latest receipt movement for the highest-signal clients.",
            "chart_type": "table",
            "rows": [
                {
                    "Client": row["client_name"],
                    f"{labels[0]} Rate": f"{row['previous_rate'] * 100:.1f}%",
                    f"{labels[-1]} Rate": f"{row['latest_rate'] * 100:.1f}%",
                    "Rate Delta": f"{row['rate_delta'] * 100:+.1f} pts",
                    "Pending Delta": f"{row['pending_delta']:+,}",
                }
                for row in delta_rows[:10]
            ],
            "insight": "This table highlights which client relationships improved or deteriorated the most between the earliest and latest selected documents.",
        },
        {
            "key": "combined-risk-band-shift",
            "tab": "overview",
            "title": "Risk-Band Shift",
            "description": "Client distribution across receipt-progress bands in the earliest versus latest selected period.",
            "chart_type": "table",
            "rows": [
                {
                    "Risk Band": label,
                    f"{labels[0]}": str(earliest_band_counts[label]),
                    f"{labels[-1]}": str(latest_band_counts[label]),
                    "Delta": f"{latest_band_counts[label] - earliest_band_counts[label]:+d}",
                }
                for label in band_labels
            ],
            "insight": "Use this table to see whether risk is moving into healthier bands or accumulating in the critical end of the portfolio.",
        },
        {
            "key": "combined-improving-clients",
            "tab": "clients",
            "title": "Top Improving Clients",
            "description": "Clients with the strongest receipt-rate improvement between the earliest and latest selected documents.",
            "chart_type": "table",
            "rows": [
                {
                    "Client": row["client_name"],
                    "Rate Delta": f"{row['rate_delta'] * 100:+.1f} pts",
                    "Pending Delta": f"{row['pending_delta']:+,}",
                    "Latest Pending": f"{row['latest_pending']:,}",
                }
                for row in improving_clients
            ],
            "insight": "These clients improved the most and can help separate portfolio progress from concentration risk elsewhere.",
        },
        {
            "key": "combined-deteriorating-clients",
            "tab": "clients",
            "title": "Top Deteriorating Clients",
            "description": "Clients with the weakest receipt-rate movement between the earliest and latest selected documents.",
            "chart_type": "table",
            "rows": [
                {
                    "Client": row["client_name"],
                    "Rate Delta": f"{row['rate_delta'] * 100:+.1f} pts",
                    "Pending Delta": f"{row['pending_delta']:+,}",
                    "Latest Pending": f"{row['latest_pending']:,}",
                }
                for row in deteriorating_clients
            ],
            "insight": "These clients deserve immediate review because they are contributing to deterioration instead of absorbing backlog.",
        },
        {
            "key": "combined-pending-comparison",
            "tab": "trends",
            "title": "Pending Work Comparison",
            "description": "Side-by-side backlog comparison across the confirmed document order.",
            "chart_type": "bar",
            "items": [{"label": label, "value": value} for label, value in zip(labels, pending_values)],
            "insight": "This view shows whether backlog is being reduced consistently or simply moving between reporting periods.",
        },
        {
            "key": "combined-staff-load",
            "tab": "trends",
            "title": "Latest Staff Load Distribution",
            "description": "Received workload concentration across staff in the latest selected document.",
            "chart_type": "bar",
            "items": [
                {"label": str(item.get("name") or "Staff"), "value": float(item.get("received_total") or 0)}
                for item in latest_staff
            ],
            "insight": "This chart shows whether throughput is broadly distributed or concentrated in a small staff cohort in the latest period.",
        },
    ]

    executive_lines = [
        findings[0],
        findings[1],
        findings[3],
    ]
    analytics_lines = [
        "The combined analysis stacks the confirmed documents in the user-approved order and compares portfolio-level progress, backlog, and client movement.",
        findings[0],
        findings[1],
        findings[4],
    ]
    evidence = [
        {"id": f"AS-{index+1:03d}", "tab": "overview", "title": f"{label} portfolio summary", "detail": f"Contracted {int(contracted):,}, received {int(received):,}, pending {int(pending):,}, receipt rate {rate:.1f}%."}
        for index, (label, contracted, received, pending, rate) in enumerate(zip(labels, contracted_values, received_values, pending_values, receipt_rate_values))
    ]
    if top_backlog_clients:
        evidence.append(
            {
                "id": f"AS-{len(evidence)+1:03d}",
                "tab": "clients",
                "title": "Latest backlog concentration",
                "detail": f"{top_backlog_clients[0]['client_name']} carries the highest pending backlog at {int(top_backlog_clients[0]['pending_total']):,}.",
            }
        )

    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "ready",
            "generation_mode": "bundle_merge_v1",
            "member_count": len(member_payloads),
            "workbook_type": "contracted_actual_v1",
        },
        "dashboard": {
            "config": {
                "dashboard_family": "generic_review_dashboard",
                "layout_template": "adaptive_semantic",
                "title": analysis_set.name,
                "subtitle": f"Combined {analysis_set.relationship_type.replace('_', ' ')} across {len(member_payloads)} processed documents.",
                "tabs": [
                    {"key": "overview", "label": "Overview", "description": "Combined portfolio trends and headline metrics.", "sections": []},
                    {"key": "trends", "label": "Trends", "description": "Cross-period movement across the selected documents.", "sections": []},
                    {"key": "clients", "label": "Clients", "description": "Backlog concentration and client change analysis.", "sections": []},
                ],
                "kpi_cards": [],
                "schema_fields": [],
                "customization_prompts": [],
                "semantic_summary": "Combined portfolio analysis across multiple processed documents.",
                "business_questions": [
                    "Which clients improved or deteriorated most across the confirmed document order?",
                    "Where is backlog concentration building or easing over time?",
                ],
                "dashboard_preferences": {"hidden_cards": [], "card_orders": {}},
            },
            "runtime_payload": {
                "tabular_sheet_count": len(member_payloads),
                "total_rows": int(sum(contracted_values)),
                "numeric_measure_count": 4,
                "adaptive_dashboard": {
                    "domain": "Combined Portfolio Analytics",
                    "primary_entity": "Client",
                    "primary_measure": "Receipt Rate",
                    "chart_preferences": ["line", "bar", "table"],
                    "kpis": [
                        {"key": "documents", "label": "Documents", "value": str(len(member_payloads)), "meta": "Processed documents in the confirmed analysis set"},
                        {"key": "latest_contracted", "label": "Latest Contracted", "value": f"{int(latest_kpis.get('total_contracted') or 0):,}", "meta": labels[-1]},
                        {"key": "latest_pending", "label": "Latest Pending", "value": f"{int(latest_kpis.get('total_pending') or 0):,}", "meta": labels[-1]},
                        {"key": "receipt_rate_delta", "label": "Receipt Rate Delta", "value": f"{receipt_rate_values[-1] - receipt_rate_values[0]:+.1f} pts", "meta": f"{labels[0]} to {labels[-1]}"},
                    ],
                    "widgets": widgets,
                    "supporting_notes": [
                        "The combined dashboard is built from the confirmed join logic and document order.",
                        "All dashboard widgets use the persisted analysis-set bundle rather than recomputing each snapshot independently.",
                    ],
                },
            },
        },
        "executive_summary": {"title": analysis_set.name, "lines": executive_lines},
        "analytics_summary": {"title": f"{analysis_set.name} Analytics Summary", "lines": analytics_lines, "findings": findings, "widgets": widgets},
        "chat_context": {"title": analysis_set.name, "summary": analytics_lines[0], "evidence": evidence},
    }


def _generic_analysis_set_bundle(
    analysis_set: models.AnalysisSet,
    member_payloads: list[dict[str, Any]],
) -> dict[str, Any]:
    labels = [item["label"] for item in member_payloads]
    row_values: list[float] = []
    measure_values: list[float] = []
    sheet_values: list[float] = []
    member_rows: list[dict[str, str]] = []
    for item in member_payloads:
        runtime = dict(item.get("runtime_payload") or {})
        row_value = float(runtime.get("total_rows") or 0)
        measure_value = float(runtime.get("numeric_measure_count") or 0)
        sheet_value = float(runtime.get("tabular_sheet_count") or 0)
        row_values.append(row_value)
        measure_values.append(measure_value)
        sheet_values.append(sheet_value)
        member_rows.append(
            {
                "Document": item["label"],
                "Workbook": str(item.get("workbook_type") or "Unknown"),
                "Rows": f"{int(row_value):,}",
                "Measures": f"{int(measure_value)}",
                "Sheets": f"{int(sheet_value)}",
            }
        )

    findings = [
        f"The combined set includes {len(member_payloads)} processed documents across {len({item.get('workbook_type') for item in member_payloads})} workbook family patterns.",
        f"Visible row volume moves from {int(row_values[0]):,} in {labels[0]} to {int(row_values[-1]):,} in {labels[-1]}.",
        f"Numeric measure coverage ranges from {int(min(measure_values or [0]))} to {int(max(measure_values or [0]))} across the selected documents.",
    ]
    widgets = [
        {
            "key": "combined-rows-trend",
            "tab": "overview",
            "title": "Visible Rows Across Documents",
            "description": "A simple scale comparison across the confirmed document sequence.",
            "chart_type": "line",
            "items": [{"label": label, "value": value} for label, value in zip(labels, row_values)],
            "insight": f"Visible row volume is {_trend_direction(row_values)} across the selected processed documents.",
        },
        {
            "key": "combined-measure-coverage",
            "tab": "overview",
            "title": "Numeric Measure Coverage",
            "description": "How many numeric fields each processed document contributes to merged analytics.",
            "chart_type": "bar",
            "items": [{"label": label, "value": value} for label, value in zip(labels, measure_values)],
            "insight": "This chart shows which documents are richer analytical inputs before deeper merged EDA begins.",
        },
        {
            "key": "combined-document-table",
            "tab": "details",
            "title": "Combined Document Inventory",
            "description": "High-level inventory of the confirmed analysis-set members.",
            "chart_type": "table",
            "rows": member_rows,
            "insight": "This table confirms the document order, family classification, and basic analytical scale for each member in the combined set.",
        },
    ]
    evidence = [
        {"id": f"AS-{index+1:03d}", "tab": "overview", "title": f"{label} member profile", "detail": f"Rows {int(rows):,}, numeric measures {int(measures)}, tabular sheets {int(sheets)}."}
        for index, (label, rows, measures, sheets) in enumerate(zip(labels, row_values, measure_values, sheet_values))
    ]
    return {
        "metadata": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "status": "ready",
            "generation_mode": "bundle_merge_v1",
            "member_count": len(member_payloads),
            "workbook_type": "multi_document_generic",
        },
        "dashboard": {
            "config": {
                "dashboard_family": "generic_review_dashboard",
                "layout_template": "adaptive_semantic",
                "title": analysis_set.name,
                "subtitle": f"Combined {analysis_set.relationship_type.replace('_', ' ')} across {len(member_payloads)} processed documents.",
                "tabs": [
                    {"key": "overview", "label": "Overview", "description": "Combined document scale and analytical readiness.", "sections": []},
                    {"key": "details", "label": "Details", "description": "Member-level inventory and merge context.", "sections": []},
                ],
                "kpi_cards": [],
                "schema_fields": [],
                "customization_prompts": [],
                "semantic_summary": "Combined analysis across multiple processed documents.",
                "business_questions": [
                    "Which selected documents are strongest candidates for merged analytics?",
                    "Does the combined set behave more like a time series, scenario comparison, or portfolio comparison?",
                ],
                "dashboard_preferences": {"hidden_cards": [], "card_orders": {}},
            },
            "runtime_payload": {
                "tabular_sheet_count": int(sum(sheet_values)),
                "total_rows": int(sum(row_values)),
                "numeric_measure_count": int(sum(measure_values)),
                "adaptive_dashboard": {
                    "domain": "Combined Document Analytics",
                    "primary_entity": "Document",
                    "primary_measure": "Visible Rows",
                    "chart_preferences": ["line", "bar", "table"],
                    "kpis": [
                        {"key": "documents", "label": "Documents", "value": str(len(member_payloads)), "meta": "Confirmed analysis-set members"},
                        {"key": "total_rows", "label": "Total Rows", "value": f"{int(sum(row_values)):,}", "meta": "Across all selected processed documents"},
                        {"key": "measure_range", "label": "Measure Range", "value": f"{int(min(measure_values or [0]))}–{int(max(measure_values or [0]))}", "meta": "Numeric field coverage across the set"},
                    ],
                    "widgets": widgets,
                    "supporting_notes": [
                        "This combined dashboard uses the persisted analysis-set order and join confirmation.",
                        "Merged EDA can be expanded further once the combined bundle gains domain-specific cross-document operators.",
                    ],
                },
            },
        },
        "executive_summary": {"title": analysis_set.name, "lines": findings[:2]},
        "analytics_summary": {"title": f"{analysis_set.name} Analytics Summary", "lines": findings, "findings": findings, "widgets": widgets},
        "chat_context": {"title": analysis_set.name, "summary": findings[0], "evidence": evidence},
    }


def generate_analysis_set_bundle(
    db: Session,
    *,
    analysis_set: models.AnalysisSet,
    force: bool = False,
) -> dict[str, Any]:
    existing = _analysis_set_bundle_payload(analysis_set)
    if existing and not force:
        return existing
    members = sorted(analysis_set.members, key=lambda item: (item.member_order, item.id))
    if len(members) < 2:
        raise ValueError("Analysis set requires at least two members.")
    member_payloads: list[dict[str, Any]] = []
    workbook_types: set[str] = set()
    for member in members:
        snapshot = member.snapshot
        if snapshot is None:
            continue
        profile = ensure_schema_profile(db, snapshot=snapshot)
        workbook_type = str(profile.workbook_type or "").strip()
        workbook_types.add(workbook_type)
        bundle = get_or_generate_snapshot_analytics_bundle(
            db,
            snapshot=snapshot,
            blueprint=get_effective_blueprint(db, snapshot=snapshot),
            proposal=snapshot.dashboard_proposal,
        )
        payload = dict(bundle.payload_json or {})
        member_payloads.append(
            {
                "snapshot_id": snapshot.id,
                "label": _bundle_member_label(member),
                "workbook_type": workbook_type,
                "runtime_payload": dict(payload.get("dashboard", {}).get("runtime_payload") or {}),
                "surface_payloads": dict(payload.get("dashboard", {}).get("surface_payloads") or {}),
                "payload": payload,
            }
        )
    if not member_payloads:
        raise ValueError("No processed member bundles were available for the selected analysis set.")
    if len(workbook_types) == 1 and "contracted_actual_v1" in workbook_types:
        bundle = _variance_analysis_set_bundle(analysis_set, member_payloads)
    else:
        bundle = _generic_analysis_set_bundle(analysis_set, member_payloads)
    confirmed = dict(analysis_set.confirmed_json or {})
    confirmed["bundle"] = bundle
    analysis_set.confirmed_json = confirmed
    db.flush()
    return bundle


def get_or_generate_analysis_set_bundle(db: Session, analysis_set: models.AnalysisSet) -> dict[str, Any]:
    return generate_analysis_set_bundle(db, analysis_set=analysis_set, force=False)


def build_analysis_set_dashboard_view(db: Session, analysis_set: models.AnalysisSet) -> dict[str, Any]:
    bundle = get_or_generate_analysis_set_bundle(db, analysis_set)
    dashboard = dict(bundle.get("dashboard") or {})
    return {
        "analysis_set_id": analysis_set.id,
        "name": analysis_set.name,
        "summary": analysis_set.summary,
        "relationship_type": analysis_set.relationship_type,
        "confidence_score": analysis_set.confidence_score,
        "dashboard_config": dashboard.get("config") or {},
        "runtime_payload": dashboard.get("runtime_payload") or {},
        "generated_at": dict(bundle.get("metadata") or {}).get("generated_at"),
    }


def build_analysis_set_chat_context(db: Session, analysis_set: models.AnalysisSet) -> dict[str, Any]:
    bundle = get_or_generate_analysis_set_bundle(db, analysis_set)
    return dict(bundle.get("chat_context") or {})


def _analysis_set_chart_blocks(bundle: dict[str, Any], styles, gemini: GeminiRequestSettings | None) -> list[Any]:
    widgets = list(bundle.get("analytics_summary", {}).get("widgets") or [])
    evidence = _analysis_set_evidence(bundle)
    story: list[Any] = []
    for widget in widgets:
        if not isinstance(widget, dict):
            continue
        chart_type = str(widget.get("chart_type") or "")
        title = str(widget.get("title") or "Chart")
        description = str(widget.get("description") or "")
        story.append(Paragraph(title, styles["PdfSection"]))
        if description:
            story.append(Paragraph(description, styles["PdfSubtitle"]))
        if chart_type == "line":
            items = list(widget.get("items") or [])
            labels = [str(item.get("label") or "") for item in items]
            values = [float(item.get("value") or 0) for item in items]
            story.append(_line_chart(title, description, labels, values, accent=colors.HexColor("#7c3aed"), width=6.3 * inch))
            spec_kind = "generic_trend"
            spec_payload = {"labels": labels, "values": values}
        elif chart_type == "bar":
            items = list(widget.get("items") or [])
            labels = [str(item.get("label") or "") for item in items]
            values = [float(item.get("value") or 0) for item in items]
            story.append(_vertical_bar_chart(title, description, labels, values, accent=colors.HexColor("#8b5cf6"), width=6.3 * inch))
            spec_kind = "generic_ranked"
            total = sum(values) or 1.0
            spec_payload = {
                "rows": [
                    {
                        "label": label,
                        "value": value,
                        "share": value / total,
                    }
                    for label, value in zip(labels, values)
                ]
            }
        elif chart_type == "table":
            rows = list(widget.get("rows") or [])
            if rows:
                columns = list(rows[0].keys())
                table_rows = [columns] + [[str(row.get(column) or "") for column in columns] for row in rows]
                story.append(_report_table(table_rows))
            spec_kind = "generic_ranked"
            spec_payload = {"rows": []}
        else:
            items = list(widget.get("items") or [])
            table_rows = [["Label", "Value", "Meta"]] + [[str(item.get("label") or ""), str(item.get("value") or ""), str(item.get("meta") or "")] for item in items]
            story.append(_report_table(table_rows))
            spec_kind = "generic_ranked"
            total = sum(float(item.get("value") or 0) for item in items) or 1.0
            spec_payload = {
                "rows": [
                    {
                        "label": str(item.get("label") or ""),
                        "value": float(item.get("value") or 0),
                        "share": float(item.get("value") or 0) / total,
                    }
                    for item in items
                ]
            }
        if chart_type in {"line", "bar"}:
            items = list(widget.get("items") or [])
            spec = ChartInterpretationSpec(
                key=str(widget.get("key") or title),
                title=title,
                subtitle=description,
                chart_kind=spec_kind,
                evidence_ids=[item.id for item in evidence[:4]],
                payload=spec_payload,
            )
            draft = _run_chart_interpretation_graph(spec, evidence, gemini=gemini)
            fallback = str(widget.get("insight") or "This chart summarizes the strongest directional pattern in the combined analysis set.")
            story.append(Spacer(1, 6))
            story.append(_render_chart_interpretation_block(draft, fallback, styles))
        elif chart_type == "table":
            fallback = str(widget.get("insight") or "This table summarizes the most important combined document comparison rows.")
            story.append(Spacer(1, 6))
            story.append(Paragraph(fallback, styles["ChartCaption"]))
        story.append(Spacer(1, 10))
    return story


def build_analysis_set_executive_pdf(
    db: Session,
    *,
    analysis_set: models.AnalysisSet,
    gemini: GeminiRequestSettings | None = None,
) -> bytes:
    bundle = get_or_generate_analysis_set_bundle(db, analysis_set)
    styles = _build_pdf_styles()
    story: list[Any] = [
        Paragraph(analysis_set.name, styles["PdfTitle"]),
        Paragraph(f"Combined executive summary for a confirmed {analysis_set.relationship_type.replace('_', ' ')} across {len(analysis_set.members)} processed documents.", styles["PdfSubtitle"]),
        Spacer(1, 6),
        Paragraph("Executive Summary", styles["PdfSection"]),
    ]
    for line in list(bundle.get("executive_summary", {}).get("lines") or []):
        story.append(Paragraph(f"• {str(line)}", styles["PdfBody"]))
    story.append(Spacer(1, 12))
    story.extend(_analysis_set_chart_blocks(bundle, styles, gemini))
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=40, bottomMargin=30)
    doc.build(story)
    return buffer.getvalue()


def build_analysis_set_analytics_pdf(
    db: Session,
    *,
    analysis_set: models.AnalysisSet,
    gemini: GeminiRequestSettings | None = None,
) -> bytes:
    bundle = get_or_generate_analysis_set_bundle(db, analysis_set)
    styles = _build_pdf_styles()
    analytics = dict(bundle.get("analytics_summary") or {})
    story: list[Any] = [
        Paragraph(analytics.get("title") or analysis_set.name, styles["PdfTitle"]),
        Paragraph(f"Analyst-facing combined report for {len(analysis_set.members)} processed documents linked through the confirmed merge logic.", styles["PdfSubtitle"]),
        Spacer(1, 6),
        Paragraph("Analytical Findings", styles["PdfSection"]),
    ]
    for line in list(analytics.get("lines") or []):
        story.append(Paragraph(f"• {str(line)}", styles["PdfBody"]))
    findings = list(analytics.get("findings") or [])
    if findings:
        story.append(Spacer(1, 8))
        story.append(_report_table([["Finding"]] + [[str(item)] for item in findings], widths=[6.7 * inch]))
        story.append(Spacer(1, 10))
    story.extend(_analysis_set_chart_blocks(bundle, styles, gemini))
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=40, bottomMargin=30)
    doc.build(story)
    return buffer.getvalue()
