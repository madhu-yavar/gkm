from __future__ import annotations

from dataclasses import dataclass
from html import escape
from io import BytesIO
from math import ceil
import re
from typing import Any, Callable, Literal, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from openai import OpenAI
from reportlab.lib import colors
from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.linecharts import HorizontalLineChart
from reportlab.graphics.shapes import Drawing, Line, Rect, String
from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app import models
from app.analysis_engine import AnalysisReport, build_analysis_report
from app.dashboard_semantics import SemanticWorkbookInterpretation, interpret_workbook_semantics
from app.eda_agent import EDAAgentResult, run_eda_agent
from app.gemini_reasoning import GeminiReasoningError, GeminiRequestSettings, gemini_generate_structured
from app.pii import PiiMaskLookup, load_snapshot_pii_lookup, mask_text, unmask_text
from app.raw_data_store import load_or_extract_snapshot_raw_tables
from app.schemas import DashboardChatResponse
from app.settings import settings


BRAND = HexColor("#7c3aed")
BRAND_DARK = HexColor("#1f1b2e")
INK = HexColor("#18181b")
MUTED = HexColor("#5b5e70")
BORDER = HexColor("#ddd6fe")
SURFACE = HexColor("#ffffff")
SURFACE_ALT = HexColor("#f7f7fb")
SUCCESS = HexColor("#0f9d58")
WARNING = HexColor("#d97706")
DANGER = HexColor("#dc2626")
INFO = HexColor("#2563eb")


@dataclass
class ClientMetrics:
    db_client_id: int
    client_name: str
    client_external_id: str
    client_type: str
    contracted_ind: int
    contracted_bus: int
    contracted_total: int
    received_ind: int
    received_bus: int
    received_total: int
    pending_ind: int
    pending_bus: int
    pending_total: int
    receipt_rate: float | None


@dataclass
class StaffMetrics:
    name: str
    staff_id: str
    staff_type: str
    received_ind: int
    received_bus: int
    received_total: int


@dataclass
class SnapshotReportContext:
    snapshot: models.Snapshot
    previous_snapshot: models.Snapshot | None
    clients: list[ClientMetrics]
    previous_clients_by_db_id: dict[int, ClientMetrics]
    staff: list[StaffMetrics]
    previous_total_received: int | None
    pii_lookup: PiiMaskLookup | None
    schema_profile: models.WorkbookSchemaProfile | None
    workbook_type: str | None
    dashboard_config: dict[str, Any] | None
    runtime_payload: dict[str, Any] | None
    analytics_bundle: models.SnapshotAnalyticsBundle | None
    analytics_bundle_payload: dict[str, Any] | None


class EvidenceItem(BaseModel):
    id: str
    tab: str
    title: str
    detail: str


class CitedLine(BaseModel):
    text: str = Field(min_length=12, max_length=360)
    citations: list[str] = Field(min_length=1, max_length=4)


class CitedAction(BaseModel):
    action: str = Field(min_length=12, max_length=220)
    rationale: str = Field(min_length=12, max_length=320)
    citations: list[str] = Field(min_length=1, max_length=4)
    confidence: Literal["high", "medium", "low"]


class ReportSection(BaseModel):
    key: str
    title: str
    bullets: list[CitedLine] = Field(min_length=2, max_length=5)


class AgenticReportDraft(BaseModel):
    sections: list[ReportSection] = Field(min_length=4, max_length=10)
    actions: list[CitedAction] = Field(min_length=3, max_length=6)
    limitations: list[CitedLine] = Field(min_length=1, max_length=3)


class AgenticReportResult(BaseModel):
    draft: AgenticReportDraft
    evidence: list[EvidenceItem]


class AgenticReportState(TypedDict, total=False):
    mode: str
    report_style: str
    prompt: str
    review_prompt: str
    evidence: list[EvidenceItem]
    required_section_keys: list[str]
    draft: AgenticReportDraft
    reviewed_draft: AgenticReportDraft
    final_draft: AgenticReportDraft
    validation_errors: list[str]


@dataclass(frozen=True)
class ChartInterpretationSpec:
    key: str
    chart_kind: str
    title: str
    subtitle: str
    evidence_ids: list[str]
    payload: dict[str, Any]


class ChartInterpretationDraft(BaseModel):
    sentences: list[str] = Field(min_length=2, max_length=3)
    citations: list[str] = Field(min_length=1, max_length=4)


class ChartInterpretationState(TypedDict, total=False):
    spec: ChartInterpretationSpec
    selected_tool: str
    tool_output: str
    prompt: str
    draft: ChartInterpretationDraft
    validation_errors: list[str]


@dataclass(frozen=True)
class AgenticOverallPlan:
    family: str
    mode: str
    entity_name: str
    required_section_keys: list[str]
    title_overrides: dict[str, str]
    evidence: list[EvidenceItem]


@dataclass(frozen=True)
class AgenticChatContext:
    family: str
    dashboard_title: str
    family_description: str
    evidence: list[EvidenceItem]


@dataclass(frozen=True)
class InvestigationStep:
    key: str
    title: str
    objective: str
    tool: str
    rationale: str
    evidence_ids: list[str]


@dataclass(frozen=True)
class VerifiedFinding:
    key: str
    title: str
    insight: str
    implication: str
    priority: Literal["critical", "high", "medium", "low"]
    confidence: Literal["high", "medium", "low"]
    evidence_ids: list[str]


@dataclass(frozen=True)
class SummaryReasoningPacket:
    family: str
    plan_summary: str
    steps: list[InvestigationStep]
    findings: list[VerifiedFinding]
    actions: list[CitedAction]
    limitations: list[CitedLine]


@dataclass(frozen=True)
class SummaryReasoningBundle:
    plan: AgenticOverallPlan
    evidence: list[EvidenceItem]
    packet: SummaryReasoningPacket


def _fmt_num(value: int | float) -> str:
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return f"{value:,}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "—"
    return f"{value * 100:.1f}%"


def _safe_rate(contracted: int, received: int) -> float | None:
    if contracted > 0:
        return received / contracted
    if received > 0:
        return None
    return 0.0


def _risk_label(rate: float | None) -> str:
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


def _risk_target_rate(label: str) -> float:
    targets = {
        "Ahead": 0.92,
        "On Track": 0.78,
        "At Risk": 0.58,
        "Critical": 0.34,
        "Not Started": 0.16,
        "Uncontracted": 0.0,
    }
    return targets[label]


def _find_snapshot(db: Session, snapshot_id: int | None) -> models.Snapshot:
    query = db.query(models.Snapshot)
    if snapshot_id is not None:
        snap = query.filter(models.Snapshot.id == snapshot_id).first()
    else:
        snap = query.order_by(models.Snapshot.as_of_date.desc(), models.Snapshot.id.desc()).first()
    if not snap:
        raise ValueError("Snapshot not found")
    return snap


def _find_previous_snapshot(db: Session, snapshot: models.Snapshot) -> models.Snapshot | None:
    return (
        db.query(models.Snapshot)
        .filter(
            (models.Snapshot.as_of_date < snapshot.as_of_date)
            | ((models.Snapshot.as_of_date == snapshot.as_of_date) & (models.Snapshot.id < snapshot.id))
        )
        .order_by(models.Snapshot.as_of_date.desc(), models.Snapshot.id.desc())
        .first()
    )


def _deserialize_evidence_item(payload: Any) -> EvidenceItem | None:
    if not isinstance(payload, dict):
        return None
    try:
        return EvidenceItem(
            id=str(payload.get("id") or "").strip(),
            tab=str(payload.get("tab") or "").strip(),
            title=str(payload.get("title") or "").strip(),
            detail=str(payload.get("detail") or "").strip(),
        )
    except Exception:
        return None


def _deserialize_summary_reasoning_bundle(payload: dict[str, Any] | None) -> SummaryReasoningBundle | None:
    if not isinstance(payload, dict):
        return None
    bundle_payload = payload.get("reasoning_bundle")
    if not isinstance(bundle_payload, dict):
        return None
    plan_payload = bundle_payload.get("plan")
    packet_payload = bundle_payload.get("packet")
    evidence_payload = bundle_payload.get("evidence")
    if not isinstance(plan_payload, dict) or not isinstance(packet_payload, dict) or not isinstance(evidence_payload, list):
        return None

    plan_evidence = [item for item in (_deserialize_evidence_item(entry) for entry in list(plan_payload.get("evidence") or [])) if item is not None]
    reasoning_evidence = [item for item in (_deserialize_evidence_item(entry) for entry in evidence_payload) if item is not None]
    try:
        return SummaryReasoningBundle(
            plan=AgenticOverallPlan(
                family=str(plan_payload.get("family") or "").strip() or "variance_dashboard",
                mode=str(plan_payload.get("mode") or "").strip() or "canonical_bundle_v1",
                entity_name=str(plan_payload.get("entity_name") or "").strip() or "the workbook",
                required_section_keys=[str(item).strip() for item in list(plan_payload.get("required_section_keys") or []) if str(item).strip()],
                title_overrides={str(key): str(value) for key, value in dict(plan_payload.get("title_overrides") or {}).items()},
                evidence=plan_evidence,
            ),
            evidence=reasoning_evidence,
            packet=SummaryReasoningPacket(
                family=str(packet_payload.get("family") or "").strip() or "variance_dashboard",
                plan_summary=str(packet_payload.get("plan_summary") or "").strip(),
                steps=[
                    InvestigationStep(
                        key=str(item.get("key") or "").strip(),
                        title=str(item.get("title") or "").strip(),
                        objective=str(item.get("objective") or "").strip(),
                        tool=str(item.get("tool") or "").strip(),
                        rationale=str(item.get("rationale") or "").strip(),
                        evidence_ids=[str(entry).strip() for entry in list(item.get("evidence_ids") or []) if str(entry).strip()],
                    )
                    for item in list(packet_payload.get("steps") or [])
                    if isinstance(item, dict)
                ],
                findings=[
                    VerifiedFinding(
                        key=str(item.get("key") or "").strip(),
                        title=str(item.get("title") or "").strip(),
                        insight=str(item.get("insight") or "").strip(),
                        implication=str(item.get("implication") or "").strip(),
                        priority=str(item.get("priority") or "medium").strip(),  # type: ignore[arg-type]
                        confidence=str(item.get("confidence") or "medium").strip(),  # type: ignore[arg-type]
                        evidence_ids=[str(entry).strip() for entry in list(item.get("evidence_ids") or []) if str(entry).strip()],
                    )
                    for item in list(packet_payload.get("findings") or [])
                    if isinstance(item, dict)
                ],
                actions=[CitedAction.model_validate(item) for item in list(packet_payload.get("actions") or []) if isinstance(item, dict)],
                limitations=[CitedLine.model_validate(item) for item in list(packet_payload.get("limitations") or []) if isinstance(item, dict)],
            ),
        )
    except Exception:
        return None


def _deserialize_chat_context(payload: dict[str, Any] | None) -> AgenticChatContext | None:
    if not isinstance(payload, dict):
        return None
    chat_payload = payload.get("chat_context")
    if not isinstance(chat_payload, dict):
        return None
    evidence = [item for item in (_deserialize_evidence_item(entry) for entry in list(chat_payload.get("evidence") or [])) if item is not None]
    return AgenticChatContext(
        family=str(chat_payload.get("family") or "").strip() or "variance_dashboard",
        dashboard_title=str(chat_payload.get("dashboard_title") or "").strip() or "Dashboard",
        family_description=str(chat_payload.get("family_description") or "").strip(),
        evidence=evidence,
    )


def load_snapshot_report_context(
    db: Session,
    snapshot_id: int | None,
    *,
    use_bundle: bool = True,
    precomputed_dashboard_config: dict[str, Any] | None = None,
    precomputed_runtime_payload: dict[str, Any] | None = None,
) -> SnapshotReportContext:
    from app.dashboard_blueprints import build_or_refresh_proposal, ensure_schema_profile, get_effective_blueprint
    from app.dashboard_runtime import get_dashboard_runtime_payload
    from app.analytics_bundle import get_or_generate_snapshot_analytics_bundle

    snapshot = _find_snapshot(db, snapshot_id)
    previous_snapshot = _find_previous_snapshot(db, snapshot)
    profile = ensure_schema_profile(db, snapshot=snapshot)
    blueprint = get_effective_blueprint(db, snapshot=snapshot)
    proposal = snapshot.dashboard_proposal or build_or_refresh_proposal(db, snapshot=snapshot, actor_user_id=None)
    analytics_bundle = None
    analytics_bundle_payload: dict[str, Any] | None = None
    dashboard_config = precomputed_dashboard_config
    runtime_payload = precomputed_runtime_payload
    if use_bundle:
        analytics_bundle = get_or_generate_snapshot_analytics_bundle(
            db,
            snapshot=snapshot,
            blueprint=blueprint,
            proposal=proposal,
        )
        analytics_bundle_payload = dict(analytics_bundle.payload_json or {})
        dashboard_payload = dict(analytics_bundle_payload.get("dashboard") or {})
        dashboard_config = dashboard_payload.get("config") or dashboard_config
        runtime_payload = dashboard_payload.get("runtime_payload") or runtime_payload
    if dashboard_config is None:
        dashboard_config = blueprint.config_json if blueprint else (proposal.proposal_json if proposal else None)
    if runtime_payload is None:
        runtime_payload = get_dashboard_runtime_payload(db, snapshot, profile.workbook_type, dashboard_config)

    current_rows = (
        db.query(models.ClientSnapshot, models.Client)
        .join(models.Client, models.Client.id == models.ClientSnapshot.client_id)
        .filter(models.ClientSnapshot.snapshot_id == snapshot.id)
        .all()
    )

    clients = [
        ClientMetrics(
            db_client_id=client.id,
            client_name=client.name,
            client_external_id=client.external_id,
            client_type=client.client_type,
            contracted_ind=row.contracted_ind,
            contracted_bus=row.contracted_bus,
            contracted_total=row.contracted_total,
            received_ind=row.received_ind,
            received_bus=row.received_bus,
            received_total=row.received_total,
            pending_ind=row.pending_ind,
            pending_bus=row.pending_bus,
            pending_total=row.pending_total,
            receipt_rate=_safe_rate(row.contracted_total, row.received_total),
        )
        for row, client in current_rows
    ]
    clients.sort(key=lambda item: (-item.contracted_total, item.client_name.lower()))

    previous_clients_by_db_id: dict[int, ClientMetrics] = {}
    previous_total_received: int | None = None
    if previous_snapshot:
        previous_rows = (
            db.query(models.ClientSnapshot, models.Client)
            .join(models.Client, models.Client.id == models.ClientSnapshot.client_id)
            .filter(models.ClientSnapshot.snapshot_id == previous_snapshot.id)
            .all()
        )
        previous_total_received = 0
        for row, client in previous_rows:
            metrics = ClientMetrics(
                db_client_id=client.id,
                client_name=client.name,
                client_external_id=client.external_id,
                client_type=client.client_type,
                contracted_ind=row.contracted_ind,
                contracted_bus=row.contracted_bus,
                contracted_total=row.contracted_total,
                received_ind=row.received_ind,
                received_bus=row.received_bus,
                received_total=row.received_total,
                pending_ind=row.pending_ind,
                pending_bus=row.pending_bus,
                pending_total=row.pending_total,
                receipt_rate=_safe_rate(row.contracted_total, row.received_total),
            )
            previous_clients_by_db_id[client.id] = metrics
            previous_total_received += metrics.received_total

    staff_rows = (
        db.query(models.StaffSnapshot)
        .filter(models.StaffSnapshot.snapshot_id == snapshot.id)
        .order_by(models.StaffSnapshot.received_total.desc(), models.StaffSnapshot.name.asc())
        .all()
    )
    staff = [
        StaffMetrics(
            name=row.name,
            staff_id=row.staff_external_id,
            staff_type=row.staff_type,
            received_ind=row.received_ind,
            received_bus=row.received_bus,
            received_total=row.received_total,
        )
        for row in staff_rows
    ]

    return SnapshotReportContext(
        snapshot=snapshot,
        previous_snapshot=previous_snapshot,
        clients=clients,
        previous_clients_by_db_id=previous_clients_by_db_id,
        staff=staff,
        previous_total_received=previous_total_received,
        pii_lookup=load_snapshot_pii_lookup(db, snapshot.id),
        schema_profile=profile,
        workbook_type=profile.workbook_type,
        dashboard_config=dashboard_config,
        runtime_payload=runtime_payload,
        analytics_bundle=analytics_bundle,
        analytics_bundle_payload=analytics_bundle_payload,
    )


def _build_styles():
    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="ReportTitle",
            parent=styles["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=22,
            leading=26,
            textColor=colors.white,
            spaceAfter=8,
        )
    )
    styles.add(
        ParagraphStyle(
            name="ReportSubtitle",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            textColor=colors.white,
        )
    )
    styles.add(
        ParagraphStyle(
            name="SectionTitle",
            parent=styles["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=15,
            textColor=INK,
            spaceAfter=8,
            spaceBefore=4,
        )
    )
    styles.add(
        ParagraphStyle(
            name="BodyMuted",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=9.5,
            leading=14,
            textColor=MUTED,
        )
    )
    styles.add(
        ParagraphStyle(
            name="BodyStrong",
            parent=styles["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=9.5,
            leading=14,
            textColor=INK,
        )
    )
    styles.add(
        ParagraphStyle(
            name="KpiLabel",
            parent=styles["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=8,
            leading=10,
            textColor=MUTED,
            uppercase=True,
        )
    )
    styles.add(
        ParagraphStyle(
            name="KpiValue",
            parent=styles["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=20,
            textColor=INK,
        )
    )
    styles.add(
        ParagraphStyle(
            name="KpiMeta",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=8.5,
            leading=11,
            textColor=MUTED,
        )
    )
    styles.add(
        ParagraphStyle(
            name="TableHeaderCell",
            parent=styles["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=8.5,
            leading=10.5,
            textColor=colors.white,
        )
    )
    styles.add(
        ParagraphStyle(
            name="TableCell",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=8.2,
            leading=10.5,
            textColor=INK,
        )
    )
    styles.add(
        ParagraphStyle(
            name="CalloutTitle",
            parent=styles["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=10,
            leading=12,
            textColor=INK,
        )
    )
    styles.add(
        ParagraphStyle(
            name="CalloutBody",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=8.8,
            leading=12,
            textColor=MUTED,
        )
    )
    styles.add(
        ParagraphStyle(
            name="ChartCaption",
            parent=styles["BodyText"],
            fontName="Helvetica",
            fontSize=8.3,
            leading=11,
            textColor=MUTED,
        )
    )
    return styles


def _metric_card(label: str, value: str, meta: str, accent: colors.Color, styles) -> Table:
    table = Table(
        [
            [Paragraph(label, styles["KpiLabel"])],
            [Paragraph(value, styles["KpiValue"])],
            [Paragraph(meta, styles["KpiMeta"])],
        ],
        colWidths=[2.35 * inch],
    )
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), SURFACE),
                ("BOX", (0, 0), (-1, -1), 0.8, BORDER),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
                ("LINEBELOW", (0, 0), (-1, 0), 2, accent),
            ]
        )
    )
    return table


def _metric_grid(cards: list[Table]) -> Table:
    rows: list[list[Table | str]] = []
    for idx in range(0, len(cards), 2):
        row: list[Table | str] = cards[idx : idx + 2]
        if len(row) == 1:
            row.append("")
        rows.append(row)
    table = Table(rows, colWidths=[3.0 * inch, 3.0 * inch], hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
            ]
        )
    )
    return table


def _callout_box(title: str, lines: list[str], accent: colors.Color, styles) -> Table:
    content = [Paragraph(title, styles["CalloutTitle"])]
    content.extend(Paragraph(f"• {line}", styles["CalloutBody"]) for line in lines)
    table = Table([[item] for item in content], colWidths=[3.05 * inch], hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), SURFACE),
                ("BOX", (0, 0), (-1, -1), 0.8, BORDER),
                ("LINEBELOW", (0, 0), (-1, 0), 2, accent),
                ("LEFTPADDING", (0, 0), (-1, -1), 12),
                ("RIGHTPADDING", (0, 0), (-1, -1), 12),
                ("TOPPADDING", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    return table


def _callout_row(left: Table, right: Table) -> Table:
    table = Table([[left, right]], colWidths=[3.12 * inch, 3.12 * inch], hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 0),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                ("TOPPADDING", (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
            ]
        )
    )
    return table


def _data_table(headers: list[str], rows: list[list[str]], col_widths: list[float], styles=None) -> Table:
    styles = styles or _build_styles()
    wrapped_headers = [Paragraph(escape(str(cell)), styles["TableHeaderCell"]) for cell in headers]
    wrapped_rows = [
        [Paragraph(escape(str(cell)), styles["TableCell"]) for cell in row]
        for row in rows
    ]
    table = Table([wrapped_headers, *wrapped_rows], colWidths=col_widths, repeatRows=1, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), BRAND_DARK),
                ("BACKGROUND", (0, 1), (-1, -1), SURFACE),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [SURFACE, SURFACE_ALT]),
                ("GRID", (0, 0), (-1, -1), 0.6, BORDER),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
            ]
        )
    )
    return table


def _ascii_rate_bar(rate: float | None, width: int = 12) -> str:
    if rate is None:
        return "n/a"
    safe = max(0.0, min(1.0, rate))
    filled = int(round(safe * width))
    return "[" + ("#" * filled) + ("-" * (width - filled)) + "]"


def _chart_label(text: str, max_len: int = 16) -> str:
    value = str(text or "").strip()
    if len(value) <= max_len:
        return value
    return value[: max_len - 1] + "…"


def _strip_internal_tooling_text(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return value
    for source in ("DuckDB", "duckdb", "pandas", "Pandas", "Recovered from Gemini plan repair output.", "Recovered from Gemini repair output."):
        value = value.replace(source, "")
    value = re.sub(r"\btool=[^·\]\n]+", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip(" .;,-")
    return value or str(text or "").strip()


def _sanitized_evidence_items(evidence: list[EvidenceItem]) -> list[EvidenceItem]:
    return [
        EvidenceItem(
            id=item.id,
            tab=item.tab,
            title=_strip_internal_tooling_text(item.title),
            detail=_strip_internal_tooling_text(item.detail),
        )
        for item in evidence
    ]


def _safe_series_max(values: list[float], *, minimum: float = 1.0) -> float:
    filtered = [float(value) for value in values if value is not None]
    if not filtered:
        return minimum
    return max(max(filtered), minimum)


def _value_step(max_value: float) -> float:
    if max_value <= 5:
        return 1
    rough = max_value / 5
    if rough <= 10:
        return ceil(rough)
    magnitude = 10 ** max(len(str(int(rough))) - 1, 0)
    return ceil(rough / magnitude) * magnitude


def _chart_box(
    title: str,
    subtitle: str,
    chart,
    *,
    width: float = 6.0 * inch,
    height: float = 2.4 * inch,
) -> Drawing:
    drawing = Drawing(width, height)
    drawing.add(Rect(0, 0, width, height, fillColor=SURFACE, strokeColor=BORDER, strokeWidth=0.8, rx=10, ry=10))
    drawing.add(String(14, height - 18, title, fontName="Helvetica-Bold", fontSize=11, fillColor=INK))
    drawing.add(String(14, height - 31, subtitle, fontName="Helvetica", fontSize=8.2, fillColor=MUTED))
    chart.x = 22
    chart.y = 20
    chart.width = width - 46
    chart.height = height - 64
    drawing.add(chart)
    return drawing


def _vertical_bar_chart(
    title: str,
    subtitle: str,
    labels: list[str],
    values: list[float],
    *,
    accent: colors.Color = BRAND,
    percent: bool = False,
    width: float = 6.0 * inch,
    height: float = 2.4 * inch,
) -> Drawing:
    chart = VerticalBarChart()
    chart.data = [[float(value or 0.0) for value in values]]
    chart.strokeColor = BORDER
    chart.valueAxis.valueMin = 0
    max_value = _safe_series_max([float(value or 0.0) for value in values], minimum=100.0 if percent else 1.0)
    chart.valueAxis.valueMax = 100.0 if percent else max_value * 1.15
    chart.valueAxis.valueStep = 20.0 if percent else _value_step(chart.valueAxis.valueMax)
    chart.categoryAxis.categoryNames = [_chart_label(label, 14) for label in labels]
    chart.categoryAxis.labels.fontName = "Helvetica"
    chart.categoryAxis.labels.fontSize = 7
    chart.categoryAxis.labels.angle = 22
    chart.categoryAxis.labels.dy = -10
    chart.categoryAxis.strokeColor = BORDER
    chart.valueAxis.strokeColor = BORDER
    chart.valueAxis.gridStrokeColor = colors.HexColor("#ede9fe")
    chart.valueAxis.visibleGrid = 1
    chart.bars[0].fillColor = accent
    chart.bars[0].strokeColor = accent
    chart.barLabelFormat = "%0.0f%%" if percent else "%0.0f"
    chart.barLabels.fontName = "Helvetica"
    chart.barLabels.fontSize = 7
    chart.barLabels.fillColor = MUTED
    chart.barLabels.nudge = 7
    chart.barLabels.boxAnchor = "s"
    chart.barLabels.visible = True
    return _chart_box(title, subtitle, chart, width=width, height=height)


def _line_chart(
    title: str,
    subtitle: str,
    labels: list[str],
    values: list[float],
    *,
    accent: colors.Color = INFO,
    width: float = 6.0 * inch,
    height: float = 2.4 * inch,
) -> Drawing:
    chart = HorizontalLineChart()
    chart.data = [[float(value or 0.0) for value in values]]
    chart.joinedLines = 1
    chart.categoryAxis.categoryNames = [_chart_label(label, 12) for label in labels]
    chart.categoryAxis.labels.fontName = "Helvetica"
    chart.categoryAxis.labels.fontSize = 7
    chart.categoryAxis.strokeColor = BORDER
    chart.valueAxis.valueMin = 0
    chart.valueAxis.valueMax = _safe_series_max([float(value or 0.0) for value in values], minimum=1.0) * 1.15
    chart.valueAxis.valueStep = _value_step(chart.valueAxis.valueMax)
    chart.valueAxis.strokeColor = BORDER
    chart.valueAxis.gridStrokeColor = colors.HexColor("#ede9fe")
    chart.valueAxis.visibleGrid = 1
    chart.lines[0].strokeWidth = 2
    chart.lines[0].strokeColor = accent
    return _chart_box(title, subtitle, chart, width=width, height=height)


def _eda_scope_rows(packet: SummaryReasoningPacket) -> list[list[str]]:
    rows: list[list[str]] = []
    for step in packet.steps[:8]:
        rows.append(
            [
                step.title,
                step.tool,
                step.objective,
                step.rationale,
                _citation_label(step.evidence_ids),
            ]
        )
    return rows or [["No executed steps", "—", "No EDA operator steps were recorded for this report.", "—", "—"]]


def _verified_finding_rows(packet: SummaryReasoningPacket) -> list[list[str]]:
    rows: list[list[str]] = []
    for finding in packet.findings[:8]:
        rows.append(
            [
                finding.title,
                finding.priority.title(),
                finding.confidence.title(),
                finding.insight,
                finding.implication,
                _citation_label(finding.evidence_ids),
            ]
        )
    return rows or [["No verified findings", "—", "—", "No verified EDA findings were promoted into the report.", "—", "—"]]


def _user_friendly_finding_rows(packet: SummaryReasoningPacket) -> list[list[str]]:
    rows: list[list[str]] = []
    for finding in packet.findings[:6]:
        rows.append(
            [
                finding.title,
                finding.insight,
                finding.implication,
                finding.confidence.title(),
            ]
        )
    return rows or [["No promoted findings", "No analytical findings were promoted into the report yet.", "Refine the dashboard proposal or semantic mapping to promote stronger EDA findings into the visible report.", "—"]]


def _variance_trend_chart_data(ctx: SnapshotReportContext) -> tuple[list[str], list[float], list[float]]:
    total_contracted = sum(item.contracted_total for item in ctx.clients)
    total_received = sum(item.received_total for item in ctx.clients)
    total_pending = sum(item.pending_total for item in ctx.clients)
    predictive = _overall_predictive_summary(ctx)
    labels: list[str] = []
    received_points: list[float] = []
    rate_points: list[float] = []

    if ctx.previous_total_received is not None and total_contracted:
        labels.append("Prior")
        received_points.append(float(ctx.previous_total_received))
        rate_points.append(_share(ctx.previous_total_received, total_contracted) * 100.0)

    labels.append("Current")
    received_points.append(float(total_received))
    rate_points.append(_share(total_received, total_contracted) * 100.0 if total_contracted else 0.0)

    projected_14_text = str(predictive.get("projected_14_day_received") or "").replace(",", "")
    if projected_14_text.isdigit():
        projected_14 = int(projected_14_text)
        labels.append("+14d")
        received_points.append(float(projected_14))
        rate_points.append(_share(projected_14, total_contracted) * 100.0 if total_contracted else 0.0)

    heuristic_final_text = str(predictive.get("heuristic_final_received") or "").replace(",", "")
    if heuristic_final_text.isdigit():
        heuristic_final = int(heuristic_final_text)
        labels.append("Season Close")
        received_points.append(float(heuristic_final))
        rate_points.append(_share(heuristic_final, total_contracted) * 100.0 if total_contracted else 0.0)

    if not labels:
        labels = ["Current"]
        received_points = [float(total_received)]
        rate_points = [_share(total_received, total_contracted) * 100.0 if total_contracted else 0.0]
    return labels, received_points, rate_points


def _pick_evidence_ids(evidence: list[EvidenceItem], *prefixes: str, fallback: list[str] | None = None) -> list[str]:
    chosen: list[str] = []
    for item in evidence:
        if any(item.id.startswith(prefix) for prefix in prefixes):
            chosen.append(item.id)
    if chosen:
        return chosen[:4]
    return (fallback or [])[:4]


def _distribution_chart_tool(spec: ChartInterpretationSpec) -> str:
    rows = list(spec.payload.get("rows") or [])
    if not rows:
        return "No distribution rows were available for interpretation."
    ordered = sorted(rows, key=lambda item: float(item.get("client_count") or 0), reverse=True)
    top = ordered[0]
    laggards = [item for item in rows if float(item.get("rate") or 0.0) <= 0.15]
    return (
        f"Largest cohort={top.get('label')} with {int(top.get('client_count') or 0)} clients at "
        f"{float(top.get('rate') or 0.0) * 100:.1f}% receipt rate. "
        f"Low-conversion cohorts (<=15%) account for {sum(int(item.get('client_count') or 0) for item in laggards)} clients "
        f"and {sum(int(item.get('pending') or 0) for item in laggards):,} pending returns."
    )


def _concentration_chart_tool(spec: ChartInterpretationSpec) -> str:
    rows = list(spec.payload.get("rows") or [])
    total_pending = int(spec.payload.get("total_pending") or 0)
    if not rows:
        return "No backlog concentration rows were available for interpretation."
    top = rows[0]
    top5 = rows[:5]
    top5_pending = sum(int(item.get("pending") or 0) for item in top5)
    return (
        f"Top backlog client={top.get('label')} with {int(top.get('pending') or 0):,} pending returns at "
        f"{float(top.get('rate') or 0.0) * 100:.1f}% receipt rate. "
        f"The top five backlog clients hold {(_share(top5_pending, total_pending) * 100):.1f}% of total pending workload."
    )


def _trend_chart_tool(spec: ChartInterpretationSpec) -> str:
    labels = list(spec.payload.get("labels") or [])
    received = list(spec.payload.get("received") or [])
    rates = list(spec.payload.get("rates") or [])
    if not labels or not received:
        return "No trend points were available for interpretation."
    first_received = float(received[0])
    last_received = float(received[-1])
    first_rate = float(rates[0]) if rates else 0.0
    last_rate = float(rates[-1]) if rates else 0.0
    direction = "improving" if last_rate >= first_rate else "weakening"
    return (
        f"Received volume moves from {first_received:,.0f} at {first_rate:.1f}% to {last_received:,.0f} at {last_rate:.1f}% across "
        f"{', '.join(labels)}. "
        f"The directional trend is {direction}, with the final plotted point representing the deterministic planning forecast rather than observed throughput."
    )


def _forecast_chart_tool(spec: ChartInterpretationSpec) -> str:
    labels = list(spec.payload.get("labels") or [])
    rates = list(spec.payload.get("rates") or [])
    if not labels or not rates:
        return "No forecast-rate points were available for interpretation."
    current_rate = float(rates[0])
    projected_rate = float(rates[-1])
    return (
        f"Current plotted receipt rate is {current_rate:.1f}%, while the final deterministic forecast point reaches {projected_rate:.1f}%. "
        f"This forecast should be treated as directional planning support because it is derived from heuristic operating targets, not a statistical commitment model."
    )


def _anomaly_chart_tool(spec: ChartInterpretationSpec) -> str:
    rows = list(spec.payload.get("rows") or [])
    if not rows:
        return "No anomaly categories were detected in the selected snapshot."
    top = max(rows, key=lambda item: int(item.get("count") or 0))
    return (
        f"Most frequent anomaly category={top.get('label')} with {int(top.get('count') or 0)} flagged rows. "
        f"The anomaly panel should be read as operational exception density rather than sheer workload, because these categories distort interpretation even when their absolute count is small."
    )


def _staff_chart_tool(spec: ChartInterpretationSpec) -> str:
    rows = list(spec.payload.get("rows") or [])
    total_received = int(spec.payload.get("total_received") or 0)
    if not rows:
        return "No staff rows were available for workload interpretation."
    top = rows[0]
    return (
        f"Top workload owner={top.get('label')} with {int(top.get('received') or 0):,} received returns, representing "
        f"{(_share(int(top.get('received') or 0), total_received) * 100):.1f}% of total received volume. "
        f"If the first two or three staff bars dominate the chart, the operating model is carrying concentration risk rather than balanced throughput."
    )


def _generic_ranked_chart_tool(spec: ChartInterpretationSpec) -> str:
    rows = list(spec.payload.get("rows") or [])
    if not rows:
        return "No ranked values were available for chart interpretation."
    top = rows[0]
    top_share = float(top.get("share") or 0.0) * 100.0
    return (
        f"Top contributor={top.get('label')} at {float(top.get('value') or 0.0):,.0f}, representing {top_share:.1f}% of the plotted total. "
        f"The chart should be read as concentration analysis, because the first few categories account for most of the visible exposure."
    )


def _generic_trend_chart_tool(spec: ChartInterpretationSpec) -> str:
    labels = list(spec.payload.get("labels") or [])
    values = [float(item or 0.0) for item in list(spec.payload.get("values") or [])]
    if not labels or not values:
        return "No trend points were available for interpretation."
    first_value = values[0]
    last_value = values[-1]
    direction = "rising" if last_value > first_value else "falling" if last_value < first_value else "flat"
    return (
        f"The plotted series moves from {first_value:,.0f} to {last_value:,.0f} across {', '.join(labels)}. "
        f"This is a {direction} operational trend, so the chart is useful for directional monitoring even if the workbook is not yet a fully standardized time-series model."
    )


def _generic_forecast_chart_tool(spec: ChartInterpretationSpec) -> str:
    rows = list(spec.payload.get("rows") or [])
    if not rows:
        return "No deterministic forecast points were available for interpretation."
    current = rows[0]
    projected = rows[-1]
    return (
        f"Current plotted level={float(current.get('value') or 0.0):,.0f}, while the forward point projects {float(projected.get('value') or 0.0):,.0f}. "
        f"This forecast is deterministic and readiness-gated, so it should be treated as planning support rather than a commitment-grade prediction."
    )


_CHART_INTERPRETATION_TOOLS: dict[str, tuple[str, Callable[[ChartInterpretationSpec], str]]] = {
    "distribution": ("distribution_chart_summary", _distribution_chart_tool),
    "concentration": ("concentration_chart_summary", _concentration_chart_tool),
    "trend": ("trend_chart_summary", _trend_chart_tool),
    "forecast": ("forecast_chart_summary", _forecast_chart_tool),
    "anomaly": ("anomaly_chart_summary", _anomaly_chart_tool),
    "staff_load": ("staff_load_chart_summary", _staff_chart_tool),
    "generic_ranked": ("generic_ranked_chart_summary", _generic_ranked_chart_tool),
    "generic_trend": ("generic_trend_chart_summary", _generic_trend_chart_tool),
    "generic_forecast": ("generic_forecast_chart_summary", _generic_forecast_chart_tool),
}


def _validate_chart_interpretation(draft: ChartInterpretationDraft, evidence: list[EvidenceItem]) -> list[str]:
    evidence_ids = {item.id for item in evidence}
    errors: list[str] = []
    if not (2 <= len(draft.sentences) <= 3):
        errors.append("chart interpretation must contain 2-3 sentences")
    for sentence in draft.sentences:
        text = sentence.strip()
        if len(text) < 20:
            errors.append("chart interpretation sentence too short")
        if len(text.split(".")) > 3:
            errors.append("chart interpretation sentence is too dense")
    if not all(citation in evidence_ids for citation in draft.citations):
        errors.append("chart interpretation has invalid citations")
    return errors


def _build_chart_interpretation_prompt(spec: ChartInterpretationSpec, tool_name: str, tool_output: str, evidence: list[EvidenceItem]) -> str:
    evidence_block = "\n".join(f"{item.id} | {item.title} | {item.detail}" for item in evidence)
    return (
        "You are writing a chart interpretation for an analyst-facing PDF.\n"
        "Write exactly 2 or 3 short sentences.\n"
        "Explain what the chart is showing, what pattern matters, and why it matters operationally.\n"
        "Use only the grounded tool output and evidence. Do not invent metrics or generic advice.\n"
        "Keep the tone factual, not executive.\n\n"
        f"Chart title: {spec.title}\n"
        f"Chart subtitle: {spec.subtitle}\n"
        f"Chart kind: {spec.chart_kind}\n"
        f"Summary tool: {tool_name}\n"
        f"Tool output: {tool_output}\n"
        f"Allowed citations: {', '.join(item.id for item in evidence)}\n"
        f"Evidence:\n{evidence_block}\n"
    )


def _render_chart_interpretation_block(draft: ChartInterpretationDraft | None, fallback: str, styles) -> Paragraph:
    if draft:
        text = " ".join(sentence.strip() for sentence in draft.sentences)
        citations = _citation_label(draft.citations)
        return Paragraph(f"{text} <font color='#5b5e70'>{citations}</font>", styles["ChartCaption"])
    return Paragraph(fallback, styles["ChartCaption"])


def _run_chart_interpretation_graph(
    spec: ChartInterpretationSpec,
    evidence: list[EvidenceItem],
    gemini: GeminiRequestSettings | None = None,
) -> ChartInterpretationDraft | None:
    if gemini is None and (not settings.report_agent_enabled or not settings.openai_api_key):
        return None
    tool_entry = _CHART_INTERPRETATION_TOOLS.get(spec.chart_kind)
    if tool_entry is None:
        return None
    try:
        client = OpenAI(api_key=settings.openai_api_key) if gemini is None else None

        def _parse_structured(prompt: str) -> ChartInterpretationDraft:
            if gemini is not None:
                return gemini_generate_structured(prompt=prompt, schema=ChartInterpretationDraft, settings=gemini)
            assert client is not None
            response = client.responses.parse(
                model=settings.report_agent_model,
                reasoning={"effort": settings.report_agent_reasoning_effort},
                input=prompt,
                text_format=ChartInterpretationDraft,
                store=False,
            )
            return response.output_parsed

        def plan_node(state: ChartInterpretationState) -> ChartInterpretationState:
            return {"selected_tool": tool_entry[0], "validation_errors": []}

        def tool_node(state: ChartInterpretationState) -> ChartInterpretationState:
            return {"tool_output": tool_entry[1](state["spec"])}

        def draft_node(state: ChartInterpretationState) -> ChartInterpretationState:
            prompt = _build_chart_interpretation_prompt(state["spec"], state["selected_tool"], state["tool_output"], evidence)
            return {"prompt": prompt, "draft": _parse_structured(prompt)}

        def validate_node(state: ChartInterpretationState) -> ChartInterpretationState:
            return {"validation_errors": _validate_chart_interpretation(state["draft"], evidence)}

        graph = StateGraph(ChartInterpretationState)
        graph.add_node("plan", plan_node)
        graph.add_node("run_tool", tool_node)
        graph.add_node("draft", draft_node)
        graph.add_node("validate", validate_node)
        graph.add_edge(START, "plan")
        graph.add_edge("plan", "run_tool")
        graph.add_edge("run_tool", "draft")
        graph.add_edge("draft", "validate")
        graph.add_edge("validate", END)
        compiled = graph.compile(checkpointer=InMemorySaver())
        result = compiled.invoke({"spec": spec}, config={"configurable": {"thread_id": f"chart:{spec.key}"}})
        if result.get("validation_errors"):
            return None
        return result.get("draft")
    except (Exception, GeminiReasoningError):
        return None


def _build_variance_chart_specs(
    ctx: SnapshotReportContext,
    packet: SummaryReasoningPacket,
    evidence: list[EvidenceItem],
    distribution_rows: list[list[str]],
    backlog_rows: list[list[str]],
    anomaly_rows: list[list[str]],
    staff_rows: list[list[str]],
    trend_labels: list[str],
    received_series: list[float],
    rate_series: list[float],
) -> dict[str, ChartInterpretationSpec]:
    total_pending = sum(item.pending_total for item in ctx.clients)
    total_received = sum(item.received_total for item in ctx.clients)
    packet_ids = [item_id for finding in packet.findings for item_id in finding.evidence_ids]
    fallback_ids = packet_ids[:4] or [item.id for item in evidence[:4]]
    specs = {
        "distribution": ChartInterpretationSpec(
            key="distribution",
            chart_kind="distribution",
            title="Distribution Plot",
            subtitle="Receipt-rate bands across the client portfolio.",
            evidence_ids=_pick_evidence_ids(evidence, "DST-", "DDB-", fallback=fallback_ids),
            payload={
                "rows": [
                    {
                        "label": row[0],
                        "client_count": int(str(row[1]).replace(",", "")),
                        "contracted": int(str(row[2]).replace(",", "")),
                        "received": int(str(row[3]).replace(",", "")),
                        "pending": int(str(row[4]).replace(",", "")),
                        "rate": None if row[5] == "—" else float(str(row[5]).replace("%", "")) / 100.0,
                    }
                    for row in distribution_rows
                ]
            },
        ),
        "concentration": ChartInterpretationSpec(
            key="concentration",
            chart_kind="concentration",
            title="Concentration Plot",
            subtitle="Pending workload held by the top backlog clients.",
            evidence_ids=_pick_evidence_ids(evidence, "BKL-", "DDB-", fallback=fallback_ids),
            payload={
                "total_pending": total_pending,
                "rows": [
                    {
                        "label": row[0],
                        "pending": int(str(row[1]).replace(",", "")) if str(row[1]).replace(",", "").isdigit() else 0,
                        "rate": None if row[2] == "—" else float(str(row[2]).replace("%", "")) / 100.0,
                    }
                    for row in backlog_rows
                ],
            },
        ),
        "trend": ChartInterpretationSpec(
            key="trend",
            chart_kind="trend",
            title="Trend Chart",
            subtitle="Observed and projected receipt volume across the available reporting horizon.",
            evidence_ids=_pick_evidence_ids(evidence, "OVR-003", "DDB-", fallback=fallback_ids),
            payload={"labels": trend_labels, "received": received_series, "rates": rate_series},
        ),
        "forecast": ChartInterpretationSpec(
            key="forecast",
            chart_kind="forecast",
            title="Forecast Chart",
            subtitle="Receipt-rate outlook from the deterministic forecast layer.",
            evidence_ids=_pick_evidence_ids(evidence, "OVR-003", "DDB-", fallback=fallback_ids),
            payload={"labels": trend_labels, "rates": rate_series},
        ),
        "anomaly": ChartInterpretationSpec(
            key="anomaly",
            chart_kind="anomaly",
            title="Anomaly Panel",
            subtitle="Material operational exceptions and data-quality flags detected in the snapshot.",
            evidence_ids=_pick_evidence_ids(evidence, "ANM-", "QLT-", fallback=fallback_ids),
            payload={
                "rows": [
                    {"label": key, "count": value}
                    for key, value in {
                        row[0]: sum(1 for item in anomaly_rows if item[0] == row[0])
                        for row in anomaly_rows
                    }.items()
                ]
            },
        ),
        "staff_load": ChartInterpretationSpec(
            key="staff_load",
            chart_kind="staff_load",
            title="Staff Load Distribution",
            subtitle="Received workload concentration across the most loaded staff members.",
            evidence_ids=_pick_evidence_ids(evidence, "STF-", "ANL-", fallback=fallback_ids),
            payload={
                "total_received": total_received,
                "rows": [
                    {
                        "label": row[0],
                        "received": int(str(row[2]).replace(",", "")) if str(row[2]).replace(",", "").isdigit() else 0,
                    }
                    for row in staff_rows[:8]
                ],
            },
        ),
    }
    return specs


def _bullet_lines(lines: list[str], styles) -> list[Paragraph]:
    return [Paragraph(f"• {line}", styles["BodyMuted"]) for line in lines]


def _build_page_frame(title: str, subtitle: str):
    def _draw(canvas, doc):
        canvas.saveState()
        width, height = LETTER
        canvas.setFillColor(BRAND_DARK)
        canvas.rect(0, height - 1.15 * inch, width, 1.15 * inch, fill=1, stroke=0)
        canvas.setFillColor(BRAND)
        canvas.rect(0, height - 1.15 * inch, width, 0.18 * inch, fill=1, stroke=0)
        canvas.setFillColor(colors.white)
        canvas.setFont("Helvetica-Bold", 20)
        canvas.drawString(doc.leftMargin, height - 0.58 * inch, title)
        canvas.setFont("Helvetica", 9.5)
        canvas.drawString(doc.leftMargin, height - 0.8 * inch, subtitle)
        canvas.setFont("Helvetica", 8.5)
        canvas.setFillColor(MUTED)
        canvas.drawRightString(width - doc.rightMargin, 0.45 * inch, f"Page {doc.page}")
        canvas.restoreState()

    return _draw


def _overall_predictive_summary(ctx: SnapshotReportContext) -> dict[str, str]:
    total_contracted = sum(item.contracted_total for item in ctx.clients)
    total_received = sum(item.received_total for item in ctx.clients)
    total_pending = sum(item.pending_total for item in ctx.clients)

    pace_daily: float | None = None
    backlog_days: float | None = None
    projected_14_day_received: int | None = None
    if ctx.previous_snapshot and ctx.previous_total_received is not None:
        delta_days = max((ctx.snapshot.as_of_date - ctx.previous_snapshot.as_of_date).days, 1)
        delta_received = max(total_received - ctx.previous_total_received, 0)
        pace_daily = delta_received / delta_days
        projected_14_day_received = min(total_contracted, ceil(total_received + pace_daily * 14))
        if pace_daily > 0:
            backlog_days = total_pending / pace_daily

    heuristic_projected_final = 0
    for client in ctx.clients:
        risk = _risk_label(client.receipt_rate)
        target_rate = _risk_target_rate(risk)
        if client.contracted_total > 0:
            heuristic_projected_final += min(
                client.contracted_total,
                max(client.received_total, round(client.contracted_total * target_rate)),
            )
        else:
            heuristic_projected_final += client.received_total

    heuristic_final_rate = (heuristic_projected_final / total_contracted) if total_contracted else 0.0
    projected_14_rate = (projected_14_day_received / total_contracted) if projected_14_day_received is not None and total_contracted else None

    return {
        "pace_daily": f"{pace_daily:.1f} returns/day" if pace_daily is not None else "Need 2+ snapshots",
        "projected_14_day_received": _fmt_num(projected_14_day_received) if projected_14_day_received is not None else "Need 2+ snapshots",
        "projected_14_day_rate": _fmt_pct(projected_14_rate),
        "backlog_days": f"{backlog_days:.0f} days" if backlog_days is not None else "Not yet measurable",
        "heuristic_final_received": _fmt_num(heuristic_projected_final),
        "heuristic_final_rate": _fmt_pct(heuristic_final_rate),
    }


def _overall_prescriptive_actions(ctx: SnapshotReportContext) -> list[str]:
    total_pending = sum(item.pending_total for item in ctx.clients)
    total_received = sum(item.received_total for item in ctx.clients)
    zero_clients = [client for client in ctx.clients if client.contracted_total > 0 and client.received_total == 0]
    critical_clients = [client for client in ctx.clients if client.receipt_rate is not None and 0 < client.receipt_rate < 0.15]
    at_risk_clients = [client for client in ctx.clients if client.receipt_rate is not None and 0.15 <= client.receipt_rate < 0.35]
    top_pending = sorted(ctx.clients, key=lambda item: item.pending_total, reverse=True)[:5]
    top_pending_share = (sum(item.pending_total for item in top_pending) / total_pending) if total_pending else 0.0
    top_staff_share = (ctx.staff[0].received_total / total_received) if ctx.staff and total_received else 0.0

    actions: list[str] = []
    if zero_clients:
        exposure = sum(item.pending_total for item in zero_clients[:5])
        actions.append(
            f"Launch immediate outreach on zero-receipt accounts. The top {min(5, len(zero_clients))} zero-receipt clients alone represent {_fmt_num(exposure)} pending returns."
        )
    if critical_clients:
        actions.append(
            f"Create a named recovery sprint for {len(critical_clients)} critical clients under 15% receipt rate, with twice-weekly follow-up until documents begin landing."
        )
    if at_risk_clients:
        actions.append(
            f"Move {len(at_risk_clients)} at-risk clients into a proactive review queue and confirm blockers on business returns before they drift into the critical tier."
        )
    if top_pending_share >= 0.5:
        actions.append(
            f"Prioritize the top backlog clients first. The five largest pending accounts carry {top_pending_share * 100:.0f}% of all outstanding work."
        )
    if top_staff_share >= 0.35:
        actions.append(
            f"Rebalance staff capacity. Current concentration suggests {ctx.staff[0].name} owns {top_staff_share * 100:.0f}% of received work, which creates throughput risk."
        )
    if not actions:
        actions.append("Maintain the current operating cadence, but keep weekly monitoring on receipt rate and backlog concentration to preserve momentum.")
    return actions[:5]


def _client_predictive_summary(ctx: SnapshotReportContext, client: ClientMetrics) -> dict[str, str]:
    previous = ctx.previous_clients_by_db_id.get(client.db_client_id)
    pace_daily: float | None = None
    backlog_days: float | None = None
    projected_14_day_received: int | None = None

    if previous and ctx.previous_snapshot:
        delta_days = max((ctx.snapshot.as_of_date - ctx.previous_snapshot.as_of_date).days, 1)
        delta_received = max(client.received_total - previous.received_total, 0)
        pace_daily = delta_received / delta_days
        projected_14_day_received = min(client.contracted_total, ceil(client.received_total + pace_daily * 14))
        if pace_daily > 0:
            backlog_days = client.pending_total / pace_daily

    target_rate = _risk_target_rate(_risk_label(client.receipt_rate))
    heuristic_final_received = (
        min(client.contracted_total, max(client.received_total, round(client.contracted_total * target_rate)))
        if client.contracted_total > 0
        else client.received_total
    )
    heuristic_final_rate = (heuristic_final_received / client.contracted_total) if client.contracted_total else None
    projected_14_rate = (projected_14_day_received / client.contracted_total) if projected_14_day_received is not None and client.contracted_total else None

    return {
        "pace_daily": f"{pace_daily:.1f} returns/day" if pace_daily is not None else "Need 2+ snapshots",
        "projected_14_day_received": _fmt_num(projected_14_day_received) if projected_14_day_received is not None else "Need 2+ snapshots",
        "projected_14_day_rate": _fmt_pct(projected_14_rate),
        "backlog_days": f"{backlog_days:.0f} days" if backlog_days is not None else "Not yet measurable",
        "heuristic_final_received": _fmt_num(heuristic_final_received),
        "heuristic_final_rate": _fmt_pct(heuristic_final_rate),
    }


def _client_prescriptive_actions(ctx: SnapshotReportContext, client: ClientMetrics) -> list[str]:
    actions: list[str] = []
    risk = _risk_label(client.receipt_rate)
    if client.received_total == 0 and client.contracted_total > 0:
        actions.append("Escalate this client immediately. No returns have been received against contracted scope, so outreach should happen now rather than after the next review cycle.")
    if client.pending_bus > client.pending_ind:
        actions.append("Business returns are the main blocker. Push for entity documentation first, because that is where most of the remaining backlog sits.")
    if risk in {"Critical", "At Risk"}:
        actions.append("Assign a named owner and move the account to weekly status review until the receipt rate stabilizes above the risk threshold.")
    if client.contracted_total >= 100:
        actions.append("Treat this as a high-impact account. Small improvements here create disproportionate movement in the overall dashboard.")
    if client.receipt_rate is not None and client.receipt_rate >= 0.6:
        actions.append("Protect momentum and document what is working on this account so the same playbook can be reused on slower clients.")
    if not actions:
        actions.append("Maintain the current cadence, but keep this client on the monitored list so any slowdown is visible early.")
    return actions[:4]


def _share(part: int | float, total: int | float) -> float:
    return (part / total) if total else 0.0


def _avg_rate(clients: list[ClientMetrics]) -> float:
    rated = [client.receipt_rate for client in clients if client.receipt_rate is not None]
    return (sum(rated) / len(rated)) if rated else 0.0


def _client_previous(ctx: SnapshotReportContext, client: ClientMetrics) -> ClientMetrics | None:
    return ctx.previous_clients_by_db_id.get(client.db_client_id)


def _client_delta_received(ctx: SnapshotReportContext, client: ClientMetrics) -> int | None:
    previous = _client_previous(ctx, client)
    if not previous:
        return None
    return client.received_total - previous.received_total


def _client_delta_rate(ctx: SnapshotReportContext, client: ClientMetrics) -> float | None:
    previous = _client_previous(ctx, client)
    if not previous or previous.receipt_rate is None or client.receipt_rate is None:
        return None
    return client.receipt_rate - previous.receipt_rate


def _clients_in_band(ctx: SnapshotReportContext, label: str) -> list[ClientMetrics]:
    return [client for client in ctx.clients if _risk_label(client.receipt_rate) == label]


def _distribution_rows(ctx: SnapshotReportContext) -> list[list[str]]:
    labels = ["Ahead", "On Track", "At Risk", "Critical", "Not Started", "Uncontracted"]
    rows: list[list[str]] = []
    for label in labels:
        clients = _clients_in_band(ctx, label)
        contracted = sum(client.contracted_total for client in clients)
        received = sum(client.received_total for client in clients)
        pending = sum(client.pending_total for client in clients)
        rate = _share(received, contracted) if contracted else None
        rows.append([label, _fmt_num(len(clients)), _fmt_num(contracted), _fmt_num(received), _fmt_num(pending), _fmt_pct(rate)])
    return rows


def _has_total_inconsistency(client: ClientMetrics) -> bool:
    return any(
        [
            client.contracted_ind + client.contracted_bus != client.contracted_total,
            client.received_ind + client.received_bus != client.received_total,
            client.pending_ind + client.pending_bus != client.pending_total,
            client.contracted_total - client.received_total != client.pending_total,
        ]
    )


def _overall_anomaly_rows(ctx: SnapshotReportContext) -> list[list[str]]:
    rows: list[list[str]] = []
    for client in ctx.clients:
        previous = _client_previous(ctx, client)
        delta_received = _client_delta_received(ctx, client)
        delta_rate = _client_delta_rate(ctx, client)
        if client.contracted_total > 0 and client.received_total > client.contracted_total:
            rows.append(["Over delivered", client.client_name, _fmt_num(client.received_total - client.contracted_total), "Received exceeds contracted scope", "Validate source workbook and update contract scope if the extra work is valid."])
        if client.contracted_total == 0 and client.received_total > 0:
            rows.append(["Uncontracted receipt", client.client_name, _fmt_num(client.received_total), "Returns received without contracted baseline", "Confirm whether this work should be brought into scope or reclassified."])
        if _has_total_inconsistency(client):
            rows.append(["Data mismatch", client.client_name, "Check totals", "Component totals do not reconcile", "Reconcile individual, business, total, and pending counts before using this row operationally."])
        if previous and client.pending_total > 0 and delta_received == 0:
            rows.append(["No movement", client.client_name, _fmt_num(client.pending_total), "Pending remains but no new receipts since prior snapshot", "Escalate outreach or remove blockers before backlog ages further."])
        if previous and delta_rate is not None and delta_rate < -0.10:
            rows.append(["Rate deterioration", client.client_name, f"{delta_rate * 100:.1f} pts", "Receipt rate materially worsened versus the prior snapshot", "Review intake sequencing and confirm whether new contracted scope was added without documentation intake."])
    return rows[:14]


def _overall_quality_suggestions(ctx: SnapshotReportContext) -> list[str]:
    suggestions: list[str] = []
    inconsistent = sum(1 for client in ctx.clients if _has_total_inconsistency(client))
    uncontracted = sum(1 for client in ctx.clients if client.contracted_total == 0 and client.received_total > 0)
    over_delivered = sum(1 for client in ctx.clients if client.contracted_total > 0 and client.received_total > client.contracted_total)
    if inconsistent:
        suggestions.append(f"Reconcile {inconsistent} client rows where segment totals do not match overall totals. These rows can distort risk ranking and forecast accuracy.")
    if uncontracted:
        suggestions.append(f"Review {uncontracted} uncontracted receipt rows and decide whether the work belongs in scope, needs remapping, or reflects source-file issues.")
    if over_delivered:
        suggestions.append(f"Audit {over_delivered} over-delivered rows to distinguish genuine scope expansion from data-entry or aggregation errors.")
    if not ctx.previous_snapshot:
        suggestions.append("Trend confidence is limited because there is only one snapshot. Predictive sections will become materially stronger after the next upload.")
    if not ctx.staff:
        suggestions.append("Staff workload insights are incomplete because the snapshot contains no staff rows. Add staff mapping to improve allocation recommendations.")
    if not suggestions:
        suggestions.append("No major data quality concerns were detected in this snapshot. The report can be used as a high-confidence operating view.")
    return suggestions[:5]


def _overall_benchmark_rows(ctx: SnapshotReportContext) -> list[list[str]]:
    total_contracted = sum(client.contracted_total for client in ctx.clients)
    total_received = sum(client.received_total for client in ctx.clients)
    total_pending = sum(client.pending_total for client in ctx.clients)
    top5_pending = sorted(ctx.clients, key=lambda item: item.pending_total, reverse=True)[:5]
    top10_contracted = sorted(ctx.clients, key=lambda item: item.contracted_total, reverse=True)[:10]
    stalled_clients = [
        client
        for client in ctx.clients
        if _client_previous(ctx, client) and client.pending_total > 0 and _client_delta_received(ctx, client) == 0
    ]
    business_pending = sum(client.pending_bus for client in ctx.clients)
    return [
        ["Portfolio receipt rate", _fmt_pct(_share(total_received, total_contracted)), "Weighted completion rate across all contracted returns."],
        ["Average client receipt rate", _fmt_pct(_avg_rate(ctx.clients)), "Simple mean across client rows with measurable rate."],
        ["Top 5 backlog concentration", _fmt_pct(_share(sum(client.pending_total for client in top5_pending), total_pending)), "Share of pending work held in the five largest backlog accounts."],
        ["Top 10 volume concentration", _fmt_pct(_share(sum(client.contracted_total for client in top10_contracted), total_contracted)), "Portfolio dependence on the largest contracted accounts."],
        ["Business backlog share", _fmt_pct(_share(business_pending, total_pending)), "How much remaining work sits in business returns."],
        ["Stalled clients", _fmt_num(len(stalled_clients)), "Clients with pending work but no new receipts since the previous snapshot."],
    ]


def _executive_view_lines(ctx: SnapshotReportContext) -> list[str]:
    total_contracted = sum(client.contracted_total for client in ctx.clients)
    total_received = sum(client.received_total for client in ctx.clients)
    total_pending = sum(client.pending_total for client in ctx.clients)
    top5_pending = sorted(ctx.clients, key=lambda item: item.pending_total, reverse=True)[:5]
    business_pending = sum(client.pending_bus for client in ctx.clients)
    critical = len([client for client in ctx.clients if client.receipt_rate is not None and 0 < client.receipt_rate < 0.15])
    at_risk = len([client for client in ctx.clients if client.receipt_rate is not None and 0.15 <= client.receipt_rate < 0.35])
    lines = [
        f"The portfolio has converted {_fmt_num(total_received)} of {_fmt_num(total_contracted)} contracted returns, leaving {_fmt_num(total_pending)} still outstanding at {_fmt_pct(_share(total_received, total_contracted))}.",
        f"The top five backlog accounts represent {_fmt_pct(_share(sum(client.pending_total for client in top5_pending), total_pending))} of all pending work, so leadership attention should stay concentrated there.",
        f"Business returns account for {_fmt_pct(_share(business_pending, total_pending))} of the backlog, which is where document-chasing effort will have the biggest payoff.",
        f"There are {critical} critical and {at_risk} at-risk clients, so the current story is still backlog recovery rather than late-season optimization.",
    ]
    if not ctx.previous_snapshot:
        lines.append("Forecast confidence is moderate rather than high because only one snapshot is available for this season so far.")
    return lines[:4]


def _priority_action_lines(ctx: SnapshotReportContext) -> list[str]:
    return _overall_prescriptive_actions(ctx)[:4]


def _staff_rows_with_share(ctx: SnapshotReportContext) -> list[list[str]]:
    total_received = sum(client.received_total for client in ctx.clients)
    max_received = max((member.received_total for member in ctx.staff), default=0)
    rows: list[list[str]] = []
    for member in ctx.staff[:12]:
        rows.append(
            [
                member.name,
                member.staff_type,
                _fmt_num(member.received_total),
                _fmt_pct(_share(member.received_total, total_received)),
                _fmt_num(member.received_ind),
                _fmt_num(member.received_bus),
                f"{_fmt_pct(_share(member.received_total, max_received))} of top load" if max_received else "—",
            ]
        )
    return rows


def _mover_rows(ctx: SnapshotReportContext) -> list[list[str]]:
    movers = []
    for client in ctx.clients:
        delta_received = _client_delta_received(ctx, client)
        delta_rate = _client_delta_rate(ctx, client)
        if delta_received is None:
            continue
        movers.append((client, delta_received, delta_rate))
    movers.sort(key=lambda item: (item[1], item[2] or 0.0), reverse=True)
    rows = []
    for client, delta_received, delta_rate in movers[:12]:
        rows.append(
            [
                client.client_name,
                _fmt_num(client.contracted_total),
                _fmt_num(client.received_total),
                f"{delta_received:+,}",
                f"{(delta_rate or 0.0) * 100:+.1f} pts" if delta_rate is not None else "—",
                _risk_label(client.receipt_rate),
            ]
        )
    return rows


def _client_quality_suggestions(ctx: SnapshotReportContext, client: ClientMetrics) -> list[str]:
    suggestions: list[str] = []
    previous = _client_previous(ctx, client)
    if _has_total_inconsistency(client):
        suggestions.append("The row has total mismatches between segment-level and overall figures. Reconcile the workbook before relying on this client’s trend story.")
    if client.contracted_total == 0 and client.received_total > 0:
        suggestions.append("Returns have been received without contracted baseline. Confirm whether scope is missing or the client was mapped incorrectly.")
    if client.received_total > client.contracted_total and client.contracted_total > 0:
        suggestions.append("Received volume exceeds contracted scope. Validate whether this is true scope expansion or a counting issue.")
    if previous and client.pending_total > 0 and _client_delta_received(ctx, client) == 0:
        suggestions.append("This client has pending workload but no progress since the prior snapshot. That usually indicates stalled outreach or missing documents.")
    if not suggestions:
        suggestions.append("No direct row-level quality concern is visible for this client in the selected snapshot.")
    return suggestions[:4]


def _evidence_item(item_id: str, tab: str, title: str, detail: str) -> EvidenceItem:
    return EvidenceItem(id=item_id, tab=tab, title=title, detail=detail)


def _build_overall_evidence(ctx: SnapshotReportContext) -> list[EvidenceItem]:
    total_contracted = sum(item.contracted_total for item in ctx.clients)
    total_received = sum(item.received_total for item in ctx.clients)
    total_pending = sum(item.pending_total for item in ctx.clients)
    total_rate = _share(total_received, total_contracted)
    zero_clients = [client for client in ctx.clients if client.contracted_total > 0 and client.received_total == 0]
    critical_clients = [client for client in ctx.clients if client.receipt_rate is not None and 0 < client.receipt_rate < 0.15]
    at_risk_clients = [client for client in ctx.clients if client.receipt_rate is not None and 0.15 <= client.receipt_rate < 0.35]
    ahead_clients = [client for client in ctx.clients if client.receipt_rate is not None and client.receipt_rate >= 0.6]
    top_contract = sorted(ctx.clients, key=lambda item: item.contracted_total, reverse=True)[:5]
    top_pending = sorted(ctx.clients, key=lambda item: item.pending_total, reverse=True)[:5]
    top_staff = sorted(ctx.staff, key=lambda item: item.received_total, reverse=True)[:5]
    anomalies = _overall_anomaly_rows(ctx)
    quality = _overall_quality_suggestions(ctx)
    predictive = _overall_predictive_summary(ctx)

    items = [
        _evidence_item("OVR-001", "Overview", "Portfolio totals", f"Contracted={_fmt_num(total_contracted)}, received={_fmt_num(total_received)}, pending={_fmt_num(total_pending)}, receipt_rate={_fmt_pct(total_rate)}."),
        _evidence_item("OVR-002", "Overview", "Client mix", f"Active clients={_fmt_num(sum(1 for item in ctx.clients if item.contracted_total > 0 or item.received_total > 0))}, zero_received={_fmt_num(len(zero_clients))}, ahead={_fmt_num(len(ahead_clients))}."),
        _evidence_item("OVR-003", "Overview", "Forecast", f"Current pace={predictive['pace_daily']}, projected_14_day_received={predictive['projected_14_day_received']}, backlog_clearance={predictive['backlog_days']}, heuristic_final_rate={predictive['heuristic_final_rate']}."),
        _evidence_item(
            "OVR-998",
            "Coverage",
            "Analysis boundary",
            f"Previous_snapshot={'yes' if ctx.previous_snapshot else 'no'}, staff_rows={_fmt_num(len(ctx.staff))}, signals=client_level_contract_received_pending_with_optional_staff_and_exception_views.",
        ),
        _evidence_item("RISK-001", "Risk Flags", "Risk exposure", f"Critical clients={_fmt_num(len(critical_clients))}, at_risk clients={_fmt_num(len(at_risk_clients))}, high-risk pending={_fmt_num(sum(item.pending_total for item in [*critical_clients, *at_risk_clients, *zero_clients]))}."),
        _evidence_item("RISK-002", "Risk Flags", "Zero receipt exposure", f"Zero-receipt clients={', '.join(client.client_name for client in zero_clients[:5]) or 'None'}."),
    ]

    for idx, client in enumerate(top_contract, start=1):
        items.append(_evidence_item(f"CLI-{idx:03d}", "Client Table", f"Top contracted client #{idx}", f"{client.client_name} contracted {_fmt_num(client.contracted_total)}, received {_fmt_num(client.received_total)}, pending {_fmt_num(client.pending_total)}, risk={_risk_label(client.receipt_rate)}."))
    for idx, client in enumerate(top_pending, start=1):
        items.append(_evidence_item(f"BKL-{idx:03d}", "Client Table", f"Top pending client #{idx}", f"{client.client_name} has {_fmt_num(client.pending_total)} pending returns with receipt rate {_fmt_pct(client.receipt_rate)}."))
    mover_rows = _mover_rows(ctx)
    for idx, row in enumerate(mover_rows[:5], start=1):
        items.append(_evidence_item(f"MOV-{idx:03d}", "Client Table", f"Top mover #{idx}", f"{row[0]} changed by {row[3]} received returns and {row[4]} in rate; current risk={row[5]}."))
    for idx, member in enumerate(top_staff, start=1):
        items.append(_evidence_item(f"STF-{idx:03d}", "Staff Workload", f"Top staff contributor #{idx}", f"{member.name} ({member.staff_type}) received {_fmt_num(member.received_total)} total returns, split {_fmt_num(member.received_ind)} individual and {_fmt_num(member.received_bus)} business."))
    for idx, row in enumerate(anomalies[:8], start=1):
        items.append(_evidence_item(f"ANM-{idx:03d}", "Risk Flags", row[0], f"Entity={row[1]}; impact={row[2]}; observation={row[3]}; suggested_action={row[4]}"))
    for idx, line in enumerate(quality[:5], start=1):
        items.append(_evidence_item(f"QLT-{idx:03d}", "Quality", f"Quality observation #{idx}", line))

    distribution = _distribution_rows(ctx)
    for idx, row in enumerate(distribution, start=1):
        items.append(_evidence_item(f"DST-{idx:03d}", "Overview", f"Distribution {row[0]}", f"Clients={row[1]}, contracted={row[2]}, received={row[3]}, pending={row[4]}, rate={row[5]}."))
    return items


def _build_client_evidence(ctx: SnapshotReportContext, client: ClientMetrics) -> list[EvidenceItem]:
    predictive = _client_predictive_summary(ctx, client)
    quality = _client_quality_suggestions(ctx, client)
    previous = _client_previous(ctx, client)
    delta_received = _client_delta_received(ctx, client)
    delta_rate = _client_delta_rate(ctx, client)
    volume_rank = 1 + sum(1 for item in ctx.clients if item.contracted_total > client.contracted_total)
    rate_rank = 1 + sum(1 for item in ctx.clients if (item.receipt_rate or 0) > (client.receipt_rate or 0))

    items = [
        _evidence_item("CL-001", "Overview", "Client totals", f"{client.client_name} contracted {_fmt_num(client.contracted_total)}, received {_fmt_num(client.received_total)}, pending {_fmt_num(client.pending_total)}, rate {_fmt_pct(client.receipt_rate)}."),
        _evidence_item("CL-002", "Overview", "Service mix", f"Individual progress {_fmt_num(client.received_ind)} of {_fmt_num(client.contracted_ind)}; business progress {_fmt_num(client.received_bus)} of {_fmt_num(client.contracted_bus)}."),
        _evidence_item("CL-003", "Client Table", "Portfolio standing", f"Volume rank #{volume_rank}, receipt-rate rank #{rate_rank}, current risk {_risk_label(client.receipt_rate)}."),
        _evidence_item("CL-004", "Predictive", "Forecast", f"Current pace={predictive['pace_daily']}, projected_14_day_received={predictive['projected_14_day_received']}, backlog_clearance={predictive['backlog_days']}, heuristic_final_rate={predictive['heuristic_final_rate']}."),
        _evidence_item("CL-005", "Risk Flags", "Quality and risk", f"Totals_reconcile={'no' if _has_total_inconsistency(client) else 'yes'}, over_delivered={'yes' if client.received_total > client.contracted_total and client.contracted_total > 0 else 'no'}, uncontracted_receipts={'yes' if client.contracted_total == 0 and client.received_total > 0 else 'no'}."),
    ]
    if previous:
        items.append(_evidence_item("CL-006", "Overview", "Change vs prior", f"Prior received {_fmt_num(previous.received_total)}, current delta_received {delta_received:+,}, delta_rate {((delta_rate or 0.0) * 100):+.1f} points."))
    for idx, line in enumerate(quality[:4], start=1):
        items.append(_evidence_item(f"CLQ-{idx:03d}", "Quality", f"Quality suggestion #{idx}", line))
    return items


def _build_status_pipeline_evidence(ctx: SnapshotReportContext) -> list[EvidenceItem]:
    runtime = ctx.runtime_payload or {}
    status_counts = runtime.get("status_counts") or []
    open_queue = runtime.get("open_queue") or []
    stale_items = runtime.get("stale_items") or []
    note_rows = runtime.get("note_rows") or []
    return_type_counts = runtime.get("return_type_counts") or []
    total_returns = int(runtime.get("total_returns") or 0)
    completed_returns = int(runtime.get("completed_returns") or 0)
    open_returns = int(runtime.get("open_returns") or 0)
    awaiting_answers = int(runtime.get("awaiting_answers") or 0)
    under_review = int(runtime.get("under_review") or 0)
    in_process = int(runtime.get("in_process") or 0)

    items = [
        _evidence_item("OPS-001", "Overview", "Pipeline totals", f"Total_returns={_fmt_num(total_returns)}, completed={_fmt_num(completed_returns)}, open_queue={_fmt_num(open_returns)}, awaiting_answers={_fmt_num(awaiting_answers)}, under_review={_fmt_num(under_review)}, in_process={_fmt_num(in_process)}."),
        _evidence_item("OPS-002", "Overview", "Aged and noted work", f"Stale_items={_fmt_num(len(stale_items))}, note_rows={_fmt_num(len(note_rows))}, completion_rate={_fmt_pct(_share(completed_returns, total_returns))}."),
        _evidence_item(
            "OPS-998",
            "Coverage",
            "Analysis boundary",
            "Signals available=status, return_type, client_type, assignment_age, stale_work, and free-text note indicators from the current workflow snapshot.",
        ),
    ]
    for idx, item in enumerate(status_counts[:8], start=1):
        items.append(_evidence_item(f"STS-{idx:03d}", "Pipeline", f"Status {item.get('label') or idx}", f"Count={_fmt_num(int(item.get('count') or 0))}."))
    for idx, item in enumerate(return_type_counts[:6], start=1):
        items.append(_evidence_item(f"TYP-{idx:03d}", "Queue", f"Return type {item.get('label') or idx}", f"Count={_fmt_num(int(item.get('count') or 0))}."))
    for idx, row in enumerate(open_queue[:8], start=1):
        items.append(
            _evidence_item(
                f"QUE-{idx:03d}",
                "Queue",
                f"Open queue item #{idx}",
                f"Tax_payer={row.get('tax_payer_name') or '—'}, return_code={row.get('return_code') or '—'}, status={row.get('return_status') or '—'}, type={row.get('return_type') or '—'}, age_days={row.get('age_days') if row.get('age_days') is not None else 'unknown'}, notes={'yes' if (row.get('cpa_notes') or row.get('gkm_notes')) else 'no'}.",
            )
        )
    for idx, row in enumerate(stale_items[:6], start=1):
        items.append(_evidence_item(f"STL-{idx:03d}", "Risk", f"Stale queue item #{idx}", f"Tax_payer={row.get('tax_payer_name') or '—'}, status={row.get('return_status') or '—'}, age_days={row.get('age_days') if row.get('age_days') is not None else 'unknown'}."))
    for idx, row in enumerate(note_rows[:6], start=1):
        items.append(_evidence_item(f"NTE-{idx:03d}", "Notes", f"Note-driven item #{idx}", f"Tax_payer={row.get('tax_payer_name') or '—'}, return_code={row.get('return_code') or '—'}, cpa_notes={row.get('cpa_notes') or '—'}, gkm_notes={row.get('gkm_notes') or '—'}."))
    if not items:
        items.append(_evidence_item("OPS-999", "Overview", "No runtime evidence", "The status-pipeline runtime payload is empty for this snapshot."))
    return items


def _build_product_catalog_evidence(ctx: SnapshotReportContext) -> list[EvidenceItem]:
    runtime = ctx.runtime_payload or {}
    total_products = int(runtime.get("total_products") or 0)
    product_type_count = int(runtime.get("product_type_count") or 0)
    uom_count = int(runtime.get("uom_count") or 0)
    category_count = int(runtime.get("category_count") or 0)
    product_type_counts = runtime.get("product_type_counts") or []
    uom_counts = runtime.get("uom_counts") or []
    category_counts = runtime.get("category_counts") or []
    quality_gaps = runtime.get("quality_gaps") or []

    items = [
        _evidence_item("CAT-001", "Overview", "Catalog totals", f"Products={_fmt_num(total_products)}, product_types={_fmt_num(product_type_count)}, base_uom_values={_fmt_num(uom_count)}, categories={_fmt_num(category_count)}, quality_gaps={_fmt_num(len(quality_gaps))}."),
        _evidence_item(
            "CAT-998",
            "Coverage",
            "Analysis boundary",
            "Signals available=product_type, category, base_uom, HSN, and explicit missing-attribute checks from the product-master snapshot; transactional demand is not present.",
        ),
    ]
    for idx, item in enumerate(product_type_counts[:6], start=1):
        items.append(_evidence_item(f"PTY-{idx:03d}", "Overview", f"Product type {item.get('label') or idx}", f"Count={_fmt_num(int(item.get('count') or 0))}."))
    for idx, item in enumerate(category_counts[:10], start=1):
        items.append(_evidence_item(f"PCG-{idx:03d}", "Categories", f"Category concentration #{idx}", f"Category={item.get('label') or 'Unknown'}, count={_fmt_num(int(item.get('count') or 0))}."))
    for idx, item in enumerate(uom_counts[:10], start=1):
        items.append(_evidence_item(f"UOM-{idx:03d}", "Quality", f"Base UoM #{idx}", f"UoM={item.get('label') or 'Unknown'}, count={_fmt_num(int(item.get('count') or 0))}."))
    for idx, row in enumerate(quality_gaps[:8], start=1):
        items.append(_evidence_item(f"QGP-{idx:03d}", "Quality", f"Quality gap #{idx}", f"Product_id={row.get('product_id') or '—'}, description={row.get('description') or '—'}, missing_fields={', '.join(str(item) for item in (row.get('missing_fields') or [])) or 'none'}."))
    if not items:
        items.append(_evidence_item("CAT-999", "Overview", "No runtime evidence", "The product-catalog runtime payload is empty for this snapshot."))
    return items


def _build_generic_workbook_evidence(ctx: SnapshotReportContext) -> list[EvidenceItem]:
    schema_fields = []
    if isinstance(ctx.dashboard_config, dict):
        schema_fields = ctx.dashboard_config.get("schema_fields") or []
    runtime_items: list[EvidenceItem] = []
    schema_items: list[EvidenceItem] = []
    runtime = ctx.runtime_payload or {}
    sheet_summaries = runtime.get("sheet_summaries") or []
    comparison_groups = runtime.get("comparison_groups") or []
    reference_items = runtime.get("text_reference_items") or []
    if runtime:
        runtime_items.append(
            _evidence_item(
                "GRT-001",
                "Overview",
                "Generic runtime coverage",
                f"Sheets={_fmt_num(int(runtime.get('total_sheets') or 0))}, tabular_sheets={_fmt_num(int(runtime.get('tabular_sheet_count') or 0))}, "
                f"reference_sheets={_fmt_num(int(runtime.get('reference_sheet_count') or 0))}, total_rows={_fmt_num(int(runtime.get('total_rows') or 0))}, "
                f"numeric_measures={_fmt_num(int(runtime.get('numeric_measure_count') or 0))}, comparison_groups={_fmt_num(int(runtime.get('comparison_group_count') or 0))}.",
            )
        )
        for idx, sheet in enumerate(sheet_summaries[:8], start=1):
            if sheet.get("sheet_kind") != "distribution":
                continue
            top_measure = (sheet.get("measure_totals") or [{}])[0]
            top_segment = (sheet.get("top_segments") or [{}])[0]
            runtime_items.append(
                _evidence_item(
                    f"GRT-{idx+1:03d}",
                    "Overview",
                    f"{sheet.get('sheet_name') or idx} distribution",
                    f"Dimension={sheet.get('dimension_header') or 'unknown'}, rows={_fmt_num(int(sheet.get('row_count') or 0))}, "
                    f"top_measure={top_measure.get('label') or 'unknown'}:{_fmt_num(int(top_measure.get('total') or 0))}, "
                    f"top_segment={top_segment.get('label') or 'unknown'}:{_fmt_num(int(top_segment.get('total') or 0))}.",
                )
            )
        for idx, group in enumerate(comparison_groups[:6], start=1):
            series_totals = group.get("series_totals") or []
            series_text = ", ".join(
                f"{item.get('series') or 'unknown'}={_fmt_num(int(item.get('grand_total') or 0))}"
                for item in series_totals[:4]
            ) or "no series totals"
            detail = f"Period={group.get('group_label') or idx}, totals={series_text}."
            low_rates = group.get("lowest_rate_segments") or []
            high_rates = group.get("highest_rate_segments") or []
            if group.get("rate_basis") and low_rates:
                low_text = ", ".join(
                    f"{item.get('label') or 'unknown'}={float(item.get('ratio') or 0.0):.2%}"
                    for item in low_rates[:3]
                )
                high_text = ", ".join(
                    f"{item.get('label') or 'unknown'}={float(item.get('ratio') or 0.0):.2%}"
                    for item in high_rates[:3]
                )
                detail += (
                    f" {group.get('rate_basis')} lowest={low_text}; highest={high_text}; "
                    f"matched_pools={_fmt_num(int(group.get('matched_pool_count') or 0))}, "
                    f"tc_only={_fmt_num(int(group.get('unmatched_tc_pool_count') or 0))}, "
                    f"bc_only={_fmt_num(int(group.get('unmatched_bc_pool_count') or 0))}."
                )
            runtime_items.append(_evidence_item(f"GCP-{idx:03d}", "Comparisons", f"{group.get('group_label') or idx} comparison", detail))
        for idx, line in enumerate(reference_items[:6], start=1):
            runtime_items.append(_evidence_item(f"GRF-{idx:03d}", "Reference", f"Reference note #{idx}", str(line)))
        runtime_items.append(
            _evidence_item(
                "GEN-998",
                "Coverage",
                "Analysis boundary",
                "The workbook has generic runtime coverage for sheet distributions and paired period comparisons, but it is still not mapped to a dedicated business family with a purpose-built dashboard model.",
            )
        )
    else:
        runtime_items.append(
            _evidence_item(
                "GEN-998",
                "Coverage",
                "Analysis boundary",
                "The workbook is currently profiled from schema structure only and has not yet been mapped to a mature business runtime family.",
            )
        )
    for sheet_index, sheet in enumerate(schema_fields, start=1):
        sections = sheet.get("sections") or []
        schema_items.append(
            _evidence_item(
                f"GEN-{sheet_index:03d}",
                "Schema",
                f"Sheet {sheet.get('sheet_name') or sheet_index}",
                f"Sections={_fmt_num(len(sections))}, fields={_fmt_num(sum(len(section.get('fields') or []) for section in sections))}.",
            )
        )
        for section_index, section in enumerate(sections[:2], start=1):
            fields = section.get("fields") or []
            field_labels = ", ".join(str(field.get("header_label") or "Unknown") for field in fields[:8]) or "No fields"
            schema_items.append(
                _evidence_item(
                    f"GSC-{sheet_index:02d}-{section_index:02d}",
                    "Schema",
                    f"{section.get('section_label') or section.get('section_key') or 'Section'}",
                    f"Header_row={section.get('header_row') or 'unknown'}, fields={field_labels}.",
                )
            )
    if not schema_items:
        schema_items.append(_evidence_item("GEN-999", "Schema", "No schema profile", "No schema fields were available for this workbook, so business interpretation is limited."))
    return runtime_items + schema_items


def _generic_runtime_ready(ctx: SnapshotReportContext) -> bool:
    if _dashboard_family(ctx) != "generic_review_dashboard":
        return False
    runtime = ctx.runtime_payload or {}
    return bool(
        int(runtime.get("comparison_group_count") or 0) >= 1
        or int(runtime.get("tabular_sheet_count") or 0) >= 2
        or int(runtime.get("total_rows") or 0) >= 25
    )


def _runtime_summary_for_semantics(runtime: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(runtime, dict):
        return {}
    summary: dict[str, Any] = {}
    for key in (
        "total_sheets",
        "tabular_sheet_count",
        "reference_sheet_count",
        "total_rows",
        "numeric_measure_count",
        "comparison_group_count",
    ):
        if key in runtime:
            summary[key] = runtime.get(key)
    if isinstance(runtime.get("sheet_summaries"), list):
        summary["sheet_summaries"] = runtime["sheet_summaries"][:6]
    if isinstance(runtime.get("comparison_groups"), list):
        summary["comparison_groups"] = runtime["comparison_groups"][:6]
    if isinstance(runtime.get("text_reference_items"), list):
        summary["text_reference_items"] = runtime["text_reference_items"][:4]
    return summary


def _parse_runtime_period(label: str) -> tuple[int, int] | None:
    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    text = str(label or "").strip()
    if not text:
        return None
    parts = text[:3].lower()
    month = month_map.get(parts)
    digits = "".join(ch for ch in text if ch.isdigit())
    if month is None or not digits:
        return None
    year = int(digits)
    if year < 100:
        year += 2000 if year < 70 else 1900
    return year, month


def _linear_projection(values: list[float]) -> tuple[float, float]:
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return values[0], 0.0
    count = len(values)
    x_values = list(range(count))
    x_mean = sum(x_values) / count
    y_mean = sum(values) / count
    denominator = sum((x - x_mean) ** 2 for x in x_values)
    if denominator == 0:
        return values[-1], 0.0
    slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, values)) / denominator
    intercept = y_mean - slope * x_mean
    return intercept + slope * count, slope


def _generic_runtime_forecast(runtime: dict[str, Any] | None) -> dict[str, Any]:
    comparison_groups = list((runtime or {}).get("comparison_groups") or [])
    ratio_points: list[tuple[tuple[int, int], float]] = []
    tc_points: list[tuple[tuple[int, int], float]] = []
    bc_points: list[tuple[tuple[int, int], float]] = []
    for group in comparison_groups:
        period_key = _parse_runtime_period(str(group.get("group_label") or ""))
        if period_key is None:
            continue
        totals = {str(item.get("series") or ""): float(item.get("grand_total") or 0.0) for item in (group.get("series_totals") or [])}
        tc_total = totals.get("TC")
        bc_total = totals.get("BC")
        if tc_total is not None:
            tc_points.append((period_key, tc_total))
        if bc_total is not None:
            bc_points.append((period_key, bc_total))
        if tc_total and tc_total > 0 and bc_total is not None:
            ratio_points.append((period_key, bc_total / tc_total))
    ratio_points.sort(key=lambda item: item[0])
    tc_points.sort(key=lambda item: item[0])
    bc_points.sort(key=lambda item: item[0])
    if len(ratio_points) < 3:
        return {"ready": False, "periods": len(ratio_points)}
    next_ratio, ratio_slope = _linear_projection([value for _, value in ratio_points])
    next_tc, tc_slope = _linear_projection([value for _, value in tc_points]) if len(tc_points) >= 3 else (0.0, 0.0)
    next_bc, bc_slope = _linear_projection([value for _, value in bc_points]) if len(bc_points) >= 3 else (0.0, 0.0)
    return {
        "ready": True,
        "periods": len(ratio_points),
        "direction": "improving" if ratio_slope < 0 else "deteriorating" if ratio_slope > 0 else "stable",
        "next_ratio": max(0.0, next_ratio),
        "next_tc": max(0.0, next_tc),
        "next_bc": max(0.0, next_bc),
        "tc_trend": "up" if tc_slope > 0 else "down" if tc_slope < 0 else "flat",
        "bc_trend": "up" if bc_slope > 0 else "down" if bc_slope < 0 else "flat",
    }


def _build_generic_adaptive_evidence(
    ctx: SnapshotReportContext,
    interpretation: SemanticWorkbookInterpretation,
    eda_result: EDAAgentResult,
) -> tuple[list[EvidenceItem], dict[str, list[str]]]:
    runtime = ctx.runtime_payload or {}
    evidence: list[EvidenceItem] = []
    ids_by_key: dict[str, list[str]] = {}

    def add(item_id: str, tab: str, title: str, detail: str, *, keys: tuple[str, ...] = ()) -> None:
        evidence.append(EvidenceItem(id=item_id, tab=tab, title=title, detail=detail))
        for key in keys:
            ids_by_key.setdefault(key, []).append(item_id)

    add(
        "GEN-001",
        "Overview",
        "Workbook runtime coverage",
        (
            f"Sheets={_fmt_num(int(runtime.get('total_sheets') or 0))}, tabular_sheets={_fmt_num(int(runtime.get('tabular_sheet_count') or 0))}, "
            f"reference_sheets={_fmt_num(int(runtime.get('reference_sheet_count') or 0))}, total_rows={_fmt_num(int(runtime.get('total_rows') or 0))}, "
            f"numeric_measures={_fmt_num(int(runtime.get('numeric_measure_count') or 0))}, paired_periods={_fmt_num(int(runtime.get('comparison_group_count') or 0))}."
        ),
        keys=("runtime",),
    )
    add(
        "SEM-001",
        "Semantics",
        "Workbook interpretation",
        interpretation.semantic_summary,
        keys=("semantic",),
    )
    add(
        "SEM-002",
        "Semantics",
        "Business questions",
        "; ".join(interpretation.business_questions[:4]) or "No business questions were required after interpretation.",
        keys=("semantic",),
    )
    if interpretation.ambiguities:
        add(
            "SEM-003",
            "Semantics",
            "Remaining ambiguity",
            "; ".join(interpretation.ambiguities[:4]),
            keys=("semantic",),
        )

    comparison_groups = sorted(
        list(runtime.get("comparison_groups") or []),
        key=lambda item: _parse_runtime_period(str(item.get("group_label") or "")) or (9999, 99),
    )
    for idx, group in enumerate(comparison_groups[:6], start=1):
        period = str(group.get("group_label") or idx)
        totals = ", ".join(
            f"{item.get('series') or 'unknown'}={_fmt_num(int(float(item.get('grand_total') or 0.0)))}"
            for item in (group.get("series_totals") or [])[:4]
        ) or "no totals"
        detail = (
            f"Period={period}; totals={totals}; matched_pools={_fmt_num(int(group.get('matched_pool_count') or 0))}; "
            f"TC-only={_fmt_num(int(group.get('unmatched_tc_pool_count') or 0))}; BC-only={_fmt_num(int(group.get('unmatched_bc_pool_count') or 0))}."
        )
        high_rates = group.get("highest_rate_segments") or []
        if high_rates:
            detail += " Highest BC/TC pools="
            detail += ", ".join(
                f"{item.get('label') or 'unknown'}={float(item.get('ratio') or 0.0):.2%} (BC {_fmt_num(int(float(item.get('bad_count') or 0.0)))}/TC {_fmt_num(int(float(item.get('total_count') or 0.0)))})"
                for item in high_rates[:3]
            )
            detail += "."
        add(f"GCP-{idx:03d}", "Comparisons", f"{period} paired comparison", detail, keys=("comparisons",))

    for idx, line in enumerate((runtime.get("text_reference_items") or [])[:3], start=1):
        add(f"GRF-{idx:03d}", "Reference", f"Reference note #{idx}", str(line), keys=("reference",))

    for idx, item in enumerate(eda_result.evidence, start=1):
        evidence_id = f"EDA-{idx:03d}"
        title = _strip_internal_tooling_text(item.title)
        detail = _strip_internal_tooling_text(item.detail)
        if item.supporting_metrics:
            detail = f"{detail} Metrics: {', '.join(item.supporting_metrics[:4])}."
        add(evidence_id, "EDA", title, detail, keys=(item.tool, "eda"))

    adaptive = dict(runtime.get("adaptive_dashboard") or {})
    for idx, widget in enumerate(list(adaptive.get("widgets") or [])[:10], start=1):
        items = list(widget.get("items") or [])
        rows = list(widget.get("rows") or [])
        detail = _strip_internal_tooling_text(str(widget.get("insight") or widget.get("description") or ""))
        if not detail and items:
            top = items[0]
            detail = f"Top contributor {top.get('label') or 'unknown'} at {_fmt_num(int(float(top.get('value') or 0.0)))}."
        elif not detail and rows:
            detail = f"{len(rows)} analytical rows are available for {str(widget.get('title') or 'this view').lower()}."
        if detail:
            add(
                f"ADX-{idx:03d}",
                "Adaptive Runtime",
                _strip_internal_tooling_text(str(widget.get("title") or f"Adaptive widget #{idx}")),
                detail,
                keys=("adaptive",),
            )

    return evidence, ids_by_key


def _family_description(family: str) -> str:
    descriptions = {
        "variance_dashboard": "contracted-versus-received operational performance",
        "status_pipeline_dashboard": "workflow bottlenecks, queue health, and escalation risk",
        "product_catalog_dashboard": "catalog quality, category concentration, and master-data standardization",
        "generic_review_dashboard": "schema intent, semantic readiness, and dashboard design options",
    }
    return descriptions.get(family, "business performance and operational signals")


def _build_overall_agentic_plan(ctx: SnapshotReportContext) -> AgenticOverallPlan:
    family = _dashboard_family(ctx)
    if family == "status_pipeline_dashboard":
        return AgenticOverallPlan(
            family=family,
            mode="status_pipeline_overall_report",
            entity_name=f"status-snapshot-{ctx.snapshot.id}",
            required_section_keys=[
                "executive_overview",
                "pipeline_health",
                "queue_bottlenecks",
                "stale_and_blocked_work",
                "notes_and_escalations",
                "data_quality_and_limits",
            ],
            title_overrides={
                "executive_overview": "Executive View",
                "pipeline_health": "Pipeline Health",
                "queue_bottlenecks": "Queue Bottlenecks",
                "stale_and_blocked_work": "Stale and Blocked Work",
                "notes_and_escalations": "Notes and Escalations",
                "data_quality_and_limits": "Coverage and Data Limits",
            },
            evidence=_build_status_pipeline_evidence(ctx),
        )
    if family == "product_catalog_dashboard":
        return AgenticOverallPlan(
            family=family,
            mode="product_catalog_overall_report",
            entity_name=f"product-snapshot-{ctx.snapshot.id}",
            required_section_keys=[
                "executive_overview",
                "catalog_mix",
                "category_concentration",
                "data_quality_signals",
                "business_implications",
                "coverage_and_limits",
            ],
            title_overrides={
                "executive_overview": "Executive View",
                "catalog_mix": "Catalog Mix",
                "category_concentration": "Category Concentration",
                "data_quality_signals": "Data Quality Signals",
                "business_implications": "Business Implications",
                "coverage_and_limits": "Coverage and Data Limits",
            },
            evidence=_build_product_catalog_evidence(ctx),
        )
    if family == "generic_review_dashboard":
        if _generic_runtime_ready(ctx):
            return AgenticOverallPlan(
                family=family,
                mode="generic_adaptive_overall_report",
                entity_name=f"generic-snapshot-{ctx.snapshot.id}",
                required_section_keys=[
                    "executive_overview",
                    "period_trend_story",
                    "portfolio_hotspots",
                    "forecast_and_risk",
                    "quality_and_coverage",
                ],
                title_overrides={
                    "executive_overview": "Executive View",
                    "period_trend_story": "Period Trend Story",
                    "portfolio_hotspots": "Pool Hotspots",
                    "forecast_and_risk": "Forecast and Risk",
                    "quality_and_coverage": "Quality and Coverage",
                },
                evidence=_build_generic_workbook_evidence(ctx),
            )
        return AgenticOverallPlan(
            family=family,
            mode="generic_workbook_overall_report",
            entity_name=f"generic-snapshot-{ctx.snapshot.id}",
            required_section_keys=[
                "executive_overview",
                "schema_story",
                "semantic_gaps",
                "dashboard_design_options",
                "readiness_assessment",
            ],
            title_overrides={
                "executive_overview": "Executive View",
                "schema_story": "Schema Story",
                "semantic_gaps": "Semantic Gaps",
                "dashboard_design_options": "Dashboard Design Options",
                "readiness_assessment": "Readiness Assessment",
            },
            evidence=_build_generic_workbook_evidence(ctx),
        )
    return AgenticOverallPlan(
        family=family,
        mode="overall_report",
        entity_name=f"snapshot-{ctx.snapshot.id}",
        required_section_keys=[
            "executive_overview",
            "overview_tab",
            "client_table_tab",
            "staff_workload_tab",
            "risk_flags_tab",
            "predictive_analytics",
            "anomaly_detection",
            "quality_assessment",
        ],
        title_overrides={
            "executive_overview": "Executive View",
            "overview_tab": "Portfolio Performance",
            "client_table_tab": "Client Portfolio Review",
            "staff_workload_tab": "Team Capacity Review",
            "risk_flags_tab": "Priority Risk Review",
            "predictive_analytics": "Delivery Outlook",
            "anomaly_detection": "Exceptions Review",
            "quality_assessment": "Data Integrity Review",
        },
        evidence=_build_overall_evidence(ctx),
    )


def _build_analytics_agentic_plan(ctx: SnapshotReportContext) -> AgenticOverallPlan:
    family = _dashboard_family(ctx)
    if family == "status_pipeline_dashboard":
        return AgenticOverallPlan(
            family=family,
            mode="status_pipeline_analytics_report",
            entity_name=f"status-analytics-snapshot-{ctx.snapshot.id}",
            required_section_keys=[
                "dataset_profile",
                "pipeline_distribution_analysis",
                "exception_and_blocker_analysis",
                "data_quality_and_coverage",
            ],
            title_overrides={
                "dataset_profile": "Dataset Profile",
                "pipeline_distribution_analysis": "Pipeline Distribution Analysis",
                "exception_and_blocker_analysis": "Exception and Blocker Analysis",
                "data_quality_and_coverage": "Data Quality and Coverage",
            },
            evidence=_build_status_pipeline_evidence(ctx),
        )
    if family == "product_catalog_dashboard":
        return AgenticOverallPlan(
            family=family,
            mode="product_catalog_analytics_report",
            entity_name=f"product-analytics-snapshot-{ctx.snapshot.id}",
            required_section_keys=[
                "dataset_profile",
                "catalog_distribution_analysis",
                "quality_and_standardization_analysis",
                "coverage_and_limitations",
            ],
            title_overrides={
                "dataset_profile": "Dataset Profile",
                "catalog_distribution_analysis": "Catalog Distribution Analysis",
                "quality_and_standardization_analysis": "Quality and Standardization Analysis",
                "coverage_and_limitations": "Coverage and Limitations",
            },
            evidence=_build_product_catalog_evidence(ctx),
        )
    if family == "generic_review_dashboard":
        if _generic_runtime_ready(ctx):
            return AgenticOverallPlan(
                family=family,
                mode="generic_adaptive_analytics_report",
                entity_name=f"generic-analytics-snapshot-{ctx.snapshot.id}",
                required_section_keys=[
                    "dataset_profile",
                    "period_alignment_and_coverage",
                    "distribution_and_ratio_analysis",
                    "trend_and_forecast_analysis",
                    "data_quality_and_limitations",
                ],
                title_overrides={
                    "dataset_profile": "Dataset Profile",
                    "period_alignment_and_coverage": "Period Alignment and Coverage",
                    "distribution_and_ratio_analysis": "Distribution and Ratio Analysis",
                    "trend_and_forecast_analysis": "Trend and Forecast Analysis",
                    "data_quality_and_limitations": "Data Quality and Limitations",
                },
                evidence=_build_generic_workbook_evidence(ctx),
            )
        return AgenticOverallPlan(
            family=family,
            mode="generic_workbook_analytics_report",
            entity_name=f"generic-analytics-snapshot-{ctx.snapshot.id}",
            required_section_keys=[
                "dataset_profile",
                "semantic_evidence_analysis",
                "structural_risks",
                "coverage_and_limitations",
            ],
            title_overrides={
                "dataset_profile": "Dataset Profile",
                "semantic_evidence_analysis": "Semantic Evidence Analysis",
                "structural_risks": "Structural Risks",
                "coverage_and_limitations": "Coverage and Limitations",
            },
            evidence=_build_generic_workbook_evidence(ctx),
        )
    return AgenticOverallPlan(
        family=family,
        mode="overall_analytics_report",
        entity_name=f"analytics-snapshot-{ctx.snapshot.id}",
        required_section_keys=[
            "dataset_profile",
            "distribution_and_concentration",
            "trend_and_forecast_analysis",
            "quality_and_coverage",
        ],
        title_overrides={
            "dataset_profile": "Dataset Profile",
            "distribution_and_concentration": "Distribution and Concentration Analysis",
            "trend_and_forecast_analysis": "Trend and Forecast Analysis",
            "quality_and_coverage": "Quality and Coverage",
        },
        evidence=_build_overall_evidence(ctx),
    )


def build_agentic_chat_context(ctx: SnapshotReportContext) -> AgenticChatContext:
    stored = _deserialize_chat_context(ctx.analytics_bundle_payload)
    if stored is not None:
        return AgenticChatContext(
            family=stored.family,
            dashboard_title=stored.dashboard_title,
            family_description=stored.family_description,
            evidence=_sanitized_evidence_items(stored.evidence),
        )
    plan = _build_overall_agentic_plan(ctx)
    evidence = plan.evidence
    family_description = _family_description(plan.family)
    if plan.family == "generic_review_dashboard":
        runtime = ctx.runtime_payload or {}
        adaptive = dict(runtime.get("adaptive_dashboard") or {})
        widget_evidence: list[EvidenceItem] = []
        for idx, widget in enumerate(list(adaptive.get("widgets") or [])[:10], start=1):
            items = list(widget.get("items") or [])
            rows = list(widget.get("rows") or [])
            detail = _strip_internal_tooling_text(str(widget.get("insight") or widget.get("description") or ""))
            if not detail and items:
                top = items[0]
                detail = f"Top contributor {top.get('label') or 'unknown'} at {_fmt_num(int(float(top.get('value') or 0.0)))}."
            elif not detail and rows:
                detail = f"{len(rows)} rows are available for {str(widget.get('title') or 'this analytical view').lower()}."
            if detail:
                widget_evidence.append(
                    EvidenceItem(
                        id=f"CHAT-ADX-{idx:03d}",
                        tab="Adaptive Runtime",
                        title=_strip_internal_tooling_text(str(widget.get("title") or f"Adaptive widget #{idx}")),
                        detail=detail,
                    )
                )
        if widget_evidence:
            evidence = widget_evidence + evidence
            family_description = "adaptive business analytics with ranked exposure, concentration, trend, and forecast signals"
    return AgenticChatContext(
        family=plan.family,
        dashboard_title=_dashboard_title(ctx),
        family_description=family_description,
        evidence=_sanitized_evidence_items(evidence),
    )


def _collect_evidence_ids(evidence: list[EvidenceItem], *, prefixes: tuple[str, ...] = (), explicit: tuple[str, ...] = (), limit: int = 4) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for item_id in explicit:
        if item_id not in seen and any(item.id == item_id for item in evidence):
            ordered.append(item_id)
            seen.add(item_id)
    for item in evidence:
        if prefixes and not any(item.id.startswith(prefix) for prefix in prefixes):
            continue
        if item.id in seen:
            continue
        ordered.append(item.id)
        seen.add(item.id)
        if len(ordered) >= limit:
            break
    return ordered[:limit]


def _priority_sort_key(priority: str) -> int:
    return {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(priority, 99)


def _confidence_sort_key(confidence: str) -> int:
    return {"high": 0, "medium": 1, "low": 2}.get(confidence, 99)


def _plan_summary_text(ctx: SnapshotReportContext, family: str) -> str:
    if family == "status_pipeline_dashboard":
        return (
            f"The summary follows an investigation plan for snapshot {ctx.snapshot.as_of_date} that checks pipeline throughput, "
            "queue bottlenecks, stale or blocked work, and note-driven escalation load before drafting recommendations."
        )
    if family == "product_catalog_dashboard":
        return (
            f"The summary follows an investigation plan for snapshot {ctx.snapshot.as_of_date} that reviews catalog mix, "
            "category concentration, standardization complexity, and product-master quality risk before drafting recommendations."
        )
    if family == "generic_review_dashboard":
        return (
            f"The summary follows an investigation plan for snapshot {ctx.snapshot.as_of_date} that inspects workbook structure, "
            "semantic ambiguity, and dashboard readiness before proposing next actions."
        )
    return (
        f"The summary follows an investigation plan for snapshot {ctx.snapshot.as_of_date} that checks portfolio performance, "
        "backlog concentration, execution momentum, capacity constraints, and risk signals before drafting recommendations."
    )


def _plan_summary_steps(ctx: SnapshotReportContext, plan: AgenticOverallPlan) -> list[InvestigationStep]:
    evidence = plan.evidence
    if plan.family == "status_pipeline_dashboard":
        return [
            InvestigationStep(
                key="STEP-001",
                title="Throughput Baseline",
                objective="Measure how much work is complete versus still open and establish the current control state of the queue.",
                tool="pipeline_throughput_scan",
                rationale="Throughput is the first gate for deciding whether this is a stable queue or a backlog problem.",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("OPS-001", "OPS-002"), prefixes=("STS-",), limit=4),
            ),
            InvestigationStep(
                key="STEP-002",
                title="Bottleneck Isolation",
                objective="Identify the dominant statuses and return types absorbing the most queue volume.",
                tool="queue_bottleneck_isolation",
                rationale="The queue cannot be improved without isolating the specific stages where work is accumulating.",
                evidence_ids=_collect_evidence_ids(evidence, prefixes=("STS-", "TYP-"), explicit=("OPS-001",), limit=4),
            ),
            InvestigationStep(
                key="STEP-003",
                title="Blocker and Aging Review",
                objective="Check for stale items, external blockers, and note-heavy exceptions that can hide operational slippage.",
                tool="stale_work_probe",
                rationale="Blocked or aged work usually creates the largest hidden delivery risk in workflow dashboards.",
                evidence_ids=_collect_evidence_ids(evidence, prefixes=("STL-", "NTE-", "QUE-"), explicit=("OPS-002",), limit=4),
            ),
        ]
    if plan.family == "product_catalog_dashboard":
        return [
            InvestigationStep(
                key="STEP-001",
                title="Catalog Structure Review",
                objective="Establish the overall scale of the catalog and the breadth of type, category, and unit coverage.",
                tool="catalog_structure_scan",
                rationale="Master-data usefulness depends on both breadth and consistency of the classification structure.",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("CAT-001", "CAT-998"), prefixes=("PTY-",), limit=4),
            ),
            InvestigationStep(
                key="STEP-002",
                title="Concentration Analysis",
                objective="Determine whether a small set of categories or product types dominates the catalog.",
                tool="catalog_concentration_scan",
                rationale="Concentration changes governance priorities and can distort reporting if a few groups dominate the catalog.",
                evidence_ids=_collect_evidence_ids(evidence, prefixes=("PCG-", "PTY-"), explicit=("CAT-001",), limit=4),
            ),
            InvestigationStep(
                key="STEP-003",
                title="Quality and Standardization Review",
                objective="Assess quality gaps and unit-of-measure complexity that could weaken downstream analytics or automation.",
                tool="master_data_quality_probe",
                rationale="Master-data quality and standardization are the main determinants of whether analytical rollout is trustworthy.",
                evidence_ids=_collect_evidence_ids(evidence, prefixes=("QGP-", "UOM-"), explicit=("CAT-001", "CAT-998"), limit=4),
            ),
        ]
    if plan.family == "generic_review_dashboard":
        return [
            InvestigationStep(
                key="STEP-001",
                title="Schema Inventory",
                objective="Inspect the shape of the workbook by sheet, section, and header coverage.",
                tool="schema_inventory_scan",
                rationale="A generic workbook needs structural understanding before any executive interpretation can be trusted.",
                evidence_ids=_collect_evidence_ids(evidence, prefixes=("GEN-", "GSC-"), explicit=("GEN-998",), limit=4),
            ),
            InvestigationStep(
                key="STEP-002",
                title="Semantic Gap Review",
                objective="Identify where business meaning is still ambiguous or insufficient for stable KPI design.",
                tool="semantic_gap_probe",
                rationale="Low-confidence field semantics are the main reason generic dashboards devolve into vague summaries.",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("GEN-998",), prefixes=("GSC-",), limit=4),
            ),
            InvestigationStep(
                key="STEP-003",
                title="Readiness Assessment",
                objective="Judge whether the workbook is ready for dashboarding, or still needs a business-family mapping and metric design pass.",
                tool="readiness_assessment",
                rationale="The right next action depends on whether the workbook is structurally ready, not merely whether it has many columns.",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("GEN-998",), prefixes=("GEN-",), limit=4),
            ),
        ]
    return [
        InvestigationStep(
            key="STEP-001",
            title="Performance Baseline",
            objective="Establish contracted, received, and outstanding workload levels and the current completion baseline.",
            tool="portfolio_variance_scan",
            rationale="A reliable baseline is required before deciding whether the portfolio problem is pace, concentration, or data quality.",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("OVR-001", "OVR-002"), prefixes=("DST-",), limit=4),
        ),
        InvestigationStep(
            key="STEP-002",
            title="Backlog and Risk Concentration",
            objective="Identify where pending workload and delivery risk are concentrated across clients.",
            tool="backlog_concentration_scan",
            rationale="Leadership attention is most effective when it is focused on the accounts holding the majority of remaining work.",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("RISK-001", "RISK-002"), prefixes=("BKL-",), limit=4),
        ),
        InvestigationStep(
            key="STEP-003",
            title="Momentum and Capacity Review",
            objective="Check whether recent movement, staff concentration, and anomalies support the current recovery plan.",
            tool="momentum_capacity_review",
            rationale="Execution risk rises when backlog is concentrated, movement is weak, or delivery depends on a small number of staff.",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("OVR-003", "OVR-998"), prefixes=("MOV-", "STF-", "ANM-", "QLT-"), limit=5),
        ),
    ]


def _verify_findings(findings: list[VerifiedFinding], evidence: list[EvidenceItem]) -> list[VerifiedFinding]:
    valid_ids = {item.id for item in evidence}
    verified: list[VerifiedFinding] = []
    seen_titles: set[str] = set()
    for finding in findings:
        citations = [item_id for item_id in finding.evidence_ids if item_id in valid_ids]
        if not citations:
            continue
        title_key = finding.title.strip().lower()
        if not title_key or title_key in seen_titles:
            continue
        seen_titles.add(title_key)
        verified.append(
            VerifiedFinding(
                key=finding.key,
                title=finding.title,
                insight=finding.insight,
                implication=finding.implication,
                priority=finding.priority,
                confidence=finding.confidence,
                evidence_ids=citations[:4],
            )
        )
    verified.sort(key=lambda item: (_priority_sort_key(item.priority), _confidence_sort_key(item.confidence), item.title))
    return verified[:5]


def _variance_findings(ctx: SnapshotReportContext, evidence: list[EvidenceItem]) -> list[VerifiedFinding]:
    total_contracted = sum(item.contracted_total for item in ctx.clients)
    total_received = sum(item.received_total for item in ctx.clients)
    total_pending = sum(item.pending_total for item in ctx.clients)
    top_pending = sorted(ctx.clients, key=lambda item: item.pending_total, reverse=True)[:5]
    top_pending_total = sum(item.pending_total for item in top_pending)
    top_pending_share = _share(top_pending_total, total_pending)
    zero_clients = [client for client in ctx.clients if client.contracted_total > 0 and client.received_total == 0]
    critical_clients = [client for client in ctx.clients if client.receipt_rate is not None and 0 < client.receipt_rate < 0.15]
    at_risk_clients = [client for client in ctx.clients if client.receipt_rate is not None and 0.15 <= client.receipt_rate < 0.35]
    high_risk_pending = sum(item.pending_total for item in [*zero_clients, *critical_clients, *at_risk_clients])
    business_pending = sum(item.pending_bus for item in ctx.clients)
    predictive = _overall_predictive_summary(ctx)
    anomalies = _overall_anomaly_rows(ctx)
    total_rate = _share(total_received, total_contracted)
    findings = [
        VerifiedFinding(
            key="FIND-001",
            title="Backlog Concentration",
            insight=f"The top five backlog clients hold {_fmt_num(top_pending_total)} pending returns, representing {_fmt_pct(top_pending_share)} of the total outstanding workload.",
            implication="Recovery should be managed as a targeted backlog campaign on a small client set; broad portfolio-wide chasing will dilute impact.",
            priority="critical" if top_pending_share >= 0.6 else "high" if top_pending_share >= 0.4 else "medium",
            confidence="high",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("OVR-001",), prefixes=("BKL-",), limit=4),
        ),
        VerifiedFinding(
            key="FIND-002",
            title="Start Risk and Exposure",
            insight=f"{_fmt_num(len(zero_clients))} zero-start clients and {_fmt_num(len(critical_clients) + len(at_risk_clients))} critical or at-risk clients together account for {_fmt_num(high_risk_pending)} pending returns.",
            implication="Execution risk is not evenly distributed; the near-term portfolio outcome depends heavily on converting the zero-start and low-receipt client cohort.",
            priority="critical" if high_risk_pending >= max(total_pending * 0.5, 1) else "high",
            confidence="high",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("RISK-001", "RISK-002", "OVR-002"), prefixes=(), limit=4),
        ),
        VerifiedFinding(
            key="FIND-003",
            title="Momentum and Forecast",
            insight=(
                f"Current completion stands at {_fmt_pct(total_rate)} and the working outlook is {predictive['heuristic_final_received']} received ({predictive['heuristic_final_rate']})."
                if not ctx.previous_snapshot
                else f"Current completion stands at {_fmt_pct(total_rate)}; recent pace is {predictive['pace_daily']} with a 14-day projection to {predictive['projected_14_day_received']} received ({predictive['projected_14_day_rate']})."
            ),
            implication="The portfolio needs continued operational pressure; forecast improvement depends on converting backlog, not just maintaining current passive inflow.",
            priority="high" if total_rate < 0.6 else "medium",
            confidence="medium" if not ctx.previous_snapshot else "high",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("OVR-003", "OVR-998"), prefixes=("MOV-",), limit=4),
        ),
    ]
    if ctx.staff and total_received:
        top_staff_share = _share(ctx.staff[0].received_total, total_received)
        findings.append(
            VerifiedFinding(
                key="FIND-004",
                title="Capacity Concentration",
                insight=f"{ctx.staff[0].name} currently carries {_fmt_pct(top_staff_share)} of received work volume among mapped staff.",
                implication="Heavy delivery concentration on one person increases throughput fragility and raises the cost of any capacity disruption.",
                priority="high" if top_staff_share >= 0.35 else "medium",
                confidence="high",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("OVR-998",), prefixes=("STF-",), limit=4),
            )
        )
    if anomalies:
        findings.append(
            VerifiedFinding(
                key="FIND-005",
                title="Exception Load",
                insight=f"{_fmt_num(len(anomalies))} anomaly or data-integrity signals are already present in the current snapshot.",
                implication="Unresolved exceptions can distort prioritization and undermine confidence in the recovery story if they remain untreated.",
                priority="high" if len(anomalies) >= 5 else "medium",
                confidence="medium",
                evidence_ids=_collect_evidence_ids(evidence, prefixes=("ANM-", "QLT-"), explicit=("OVR-998",), limit=4),
            )
        )
    return findings


def _status_findings(ctx: SnapshotReportContext, evidence: list[EvidenceItem]) -> list[VerifiedFinding]:
    runtime = ctx.runtime_payload or {}
    total_returns = int(runtime.get("total_returns") or 0)
    completed_returns = int(runtime.get("completed_returns") or 0)
    open_returns = int(runtime.get("open_returns") or 0)
    awaiting_answers = int(runtime.get("awaiting_answers") or 0)
    stale_items = runtime.get("stale_items") or []
    note_rows = runtime.get("note_rows") or []
    status_counts = runtime.get("status_counts") or []
    top_status = status_counts[0] if status_counts else None
    completion_rate = _share(completed_returns, total_returns)
    findings = [
        VerifiedFinding(
            key="FIND-001",
            title="Queue Throughput",
            insight=f"{_fmt_num(completed_returns)} of {_fmt_num(total_returns)} tracked returns are complete, leaving {_fmt_num(open_returns)} items in the active queue at a completion rate of {_fmt_pct(completion_rate)}.",
            implication="The operating question is queue control rather than headline volume; open-work discipline determines whether the backlog keeps growing silently.",
            priority="critical" if open_returns > completed_returns else "high" if open_returns else "medium",
            confidence="high",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("OPS-001", "OPS-002"), prefixes=(), limit=4),
        ),
    ]
    if top_status:
        findings.append(
            VerifiedFinding(
                key="FIND-002",
                title="Primary Bottleneck",
                insight=f"The largest workflow bucket is {top_status.get('label') or 'Unknown'} with {_fmt_num(int(top_status.get('count') or 0))} items.",
                implication="Improvement will depend on resolving the dominant stage constraint first rather than spreading attention evenly across all statuses.",
                priority="high",
                confidence="high",
                evidence_ids=_collect_evidence_ids(evidence, prefixes=("STS-",), explicit=("OPS-001",), limit=4),
            )
        )
    findings.append(
        VerifiedFinding(
            key="FIND-003",
            title="Blocked and Aged Work",
            insight=f"{_fmt_num(awaiting_answers)} items are waiting on external responses and {_fmt_num(len(stale_items))} open items already appear stale.",
            implication="External blockers and aged queue items are likely to become missed commitments unless they are put under named ownership and follow-up dates.",
            priority="critical" if awaiting_answers or stale_items else "medium",
            confidence="high",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("OPS-002",), prefixes=("STL-", "QUE-"), limit=4),
        )
    )
    if note_rows:
        findings.append(
            VerifiedFinding(
                key="FIND-004",
                title="Exception-Driven Follow-Up",
                insight=f"{_fmt_num(len(note_rows))} rows already carry analyst or CPA notes.",
                implication="A meaningful part of the queue is exception-driven, so queue movement depends on translating free-text notes into explicit follow-up actions.",
                priority="high" if len(note_rows) >= 5 else "medium",
                confidence="medium",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("OPS-002",), prefixes=("NTE-",), limit=4),
            )
        )
    return findings


def _product_findings(ctx: SnapshotReportContext, evidence: list[EvidenceItem]) -> list[VerifiedFinding]:
    runtime = ctx.runtime_payload or {}
    total_products = int(runtime.get("total_products") or 0)
    category_counts = runtime.get("category_counts") or []
    product_type_counts = runtime.get("product_type_counts") or []
    uom_count = int(runtime.get("uom_count") or 0)
    quality_gaps = runtime.get("quality_gaps") or []
    top_category = category_counts[0] if category_counts else None
    top_category_count = int(top_category.get("count") or 0) if top_category else 0
    top_category_share = _share(top_category_count, total_products)
    top_type = product_type_counts[0] if product_type_counts else None
    top_type_share = _share(int(top_type.get("count") or 0), total_products) if top_type else 0.0
    quality_gap_share = _share(len(quality_gaps), total_products)
    findings = [
        VerifiedFinding(
            key="FIND-001",
            title="Category Concentration",
            insight=(
                f"The largest category is {top_category.get('label') or 'Unknown'} with {_fmt_num(top_category_count)} products, representing {_fmt_pct(top_category_share)} of the catalog."
                if top_category
                else "No dominant category could be established from the current catalog payload."
            ),
            implication="Category governance should focus first on the dominant groups because classification errors there will have the largest downstream reporting effect.",
            priority="high" if top_category_share >= 0.2 else "medium",
            confidence="high" if top_category else "medium",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("CAT-001",), prefixes=("PCG-",), limit=4),
        ),
        VerifiedFinding(
            key="FIND-002",
            title="Master Data Quality",
            insight=f"{_fmt_num(len(quality_gaps))} products currently show explicit quality gaps, equal to {_fmt_pct(quality_gap_share)} of the catalog.",
            implication="Incomplete product attributes will weaken downstream analytics, automation, and cross-system consistency until the affected records are remediated.",
            priority="high" if quality_gaps else "medium",
            confidence="high",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("CAT-001",), prefixes=("QGP-",), limit=4),
        ),
        VerifiedFinding(
            key="FIND-003",
            title="Standardization Complexity",
            insight=f"The catalog currently uses {_fmt_num(uom_count)} distinct base units of measure.",
            implication="High unit-of-measure diversity increases the effort required to standardize reporting, controls, and downstream operational rules.",
            priority="high" if uom_count >= 20 else "medium",
            confidence="high",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("CAT-001", "CAT-998"), prefixes=("UOM-",), limit=4),
        ),
    ]
    if top_type:
        findings.append(
            VerifiedFinding(
                key="FIND-004",
                title="Catalog Mix Dependence",
                insight=f"The largest product type is {top_type.get('label') or 'Unknown'} at {_fmt_pct(top_type_share)} of the catalog.",
                implication="A heavily skewed type mix can simplify governance in one area but also hides dependency on a narrow slice of the master-data model.",
                priority="medium",
                confidence="medium",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("CAT-001",), prefixes=("PTY-",), limit=4),
            )
        )
    return findings


def _generic_findings(ctx: SnapshotReportContext, evidence: list[EvidenceItem]) -> list[VerifiedFinding]:
    schema_fields = (ctx.dashboard_config or {}).get("schema_fields") if isinstance(ctx.dashboard_config, dict) else []
    sheet_count = len(schema_fields or [])
    section_count = sum(len(sheet.get("sections") or []) for sheet in (schema_fields or []))
    field_count = sum(len(section.get("fields") or []) for sheet in (schema_fields or []) for section in (sheet.get("sections") or []))
    findings = [
        VerifiedFinding(
            key="FIND-001",
            title="Workbook Breadth",
            insight=f"The workbook currently exposes {_fmt_num(sheet_count)} sheets, {_fmt_num(section_count)} profiled sections, and {_fmt_num(field_count)} mapped fields.",
            implication="There is enough structure to begin semantic design work, but not enough grounded business meaning yet to support a strong executive narrative.",
            priority="medium",
            confidence="high",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("GEN-998",), prefixes=("GEN-",), limit=4),
        ),
        VerifiedFinding(
            key="FIND-002",
            title="Semantic Ambiguity",
            insight="The workbook is still being interpreted from schema structure rather than from a mature business runtime model.",
            implication="Any summary generated before semantic grounding will drift toward generic wording because the system still lacks reliable KPI meaning and business context.",
            priority="high",
            confidence="high",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("GEN-998",), prefixes=("GSC-",), limit=4),
        ),
        VerifiedFinding(
            key="FIND-003",
            title="Dashboard Readiness",
            insight="The immediate opportunity is not more narration but clearer mapping of headers into measures, dimensions, and exceptions.",
            implication="Executive-quality outputs will improve only after the workbook is promoted from generic schema review into a stable business family with defined metrics.",
            priority="high",
            confidence="medium",
            evidence_ids=_collect_evidence_ids(evidence, explicit=("GEN-998",), prefixes=("GEN-", "GSC-"), limit=4),
        ),
    ]
    return findings


def _reasoned_actions(ctx: SnapshotReportContext, family: str, evidence: list[EvidenceItem]) -> list[CitedAction]:
    if family == "status_pipeline_dashboard":
        runtime = ctx.runtime_payload or {}
        actions: list[CitedAction] = []
        if int(runtime.get("awaiting_answers") or 0):
            actions.append(
                CitedAction(
                    action="Assign owners and due dates to the awaiting-answers queue.",
                    rationale="Externally blocked items are already visible in the pipeline and need explicit ownership to prevent silent slippage.",
                    citations=_collect_evidence_ids(evidence, explicit=("OPS-001", "OPS-002"), prefixes=("QUE-",), limit=3),
                    confidence="high",
                )
            )
        if runtime.get("stale_items"):
            actions.append(
                CitedAction(
                    action="Run a stale-item sweep before the next operating review.",
                    rationale="Aged queue items indicate hidden backlog that will not resolve through normal flow management alone.",
                    citations=_collect_evidence_ids(evidence, explicit=("OPS-002",), prefixes=("STL-",), limit=3),
                    confidence="high",
                )
            )
        actions.append(
            CitedAction(
                action="Convert note-heavy items into explicit next actions.",
                rationale="Free-text operational notes should become dated follow-ups so exception work is managed rather than merely documented.",
                citations=_collect_evidence_ids(evidence, explicit=("OPS-002",), prefixes=("NTE-",), limit=3),
                confidence="medium",
            )
        )
        return actions[:4]
    if family == "product_catalog_dashboard":
        runtime = ctx.runtime_payload or {}
        actions = [
            CitedAction(
                action="Clean product records with missing core attributes before wider analytical rollout.",
                rationale="Known quality gaps weaken the trustworthiness of master-data-driven reporting and automation.",
                citations=_collect_evidence_ids(evidence, explicit=("CAT-001",), prefixes=("QGP-",), limit=3),
                confidence="high",
            ),
            CitedAction(
                action="Prioritize governance on the highest-concentration categories first.",
                rationale="The dominant category groups carry the largest reporting and classification impact.",
                citations=_collect_evidence_ids(evidence, explicit=("CAT-001",), prefixes=("PCG-",), limit=3),
                confidence="medium",
            ),
            CitedAction(
                action="Standardize unit-of-measure usage before automating downstream controls.",
                rationale="High UoM diversity increases rule complexity and can produce inconsistent product handling across reports and processes.",
                citations=_collect_evidence_ids(evidence, explicit=("CAT-001", "CAT-998"), prefixes=("UOM-",), limit=3),
                confidence="medium",
            ),
        ]
        return actions[:4]
    if family == "generic_review_dashboard":
        return [
            CitedAction(
                action="Map headers into business measures, dimensions, and exception fields.",
                rationale="Structural profiling alone is not enough to create stable KPIs or credible executive interpretation.",
                citations=_collect_evidence_ids(evidence, explicit=("GEN-998",), prefixes=("GSC-",), limit=3),
                confidence="high",
            ),
            CitedAction(
                action="Choose the target dashboard family before expanding narrative generation.",
                rationale="The right business family determines which metrics, risks, and recommendations are actually meaningful.",
                citations=_collect_evidence_ids(evidence, explicit=("GEN-998",), prefixes=("GEN-",), limit=3),
                confidence="high",
            ),
            CitedAction(
                action="Promote the workbook into a runtime model only after semantic ambiguities are resolved.",
                rationale="Without grounded semantics, downstream dashboards and summaries will remain generic regardless of model quality.",
                citations=_collect_evidence_ids(evidence, explicit=("GEN-998",), prefixes=("GSC-",), limit=3),
                confidence="medium",
            ),
        ]
    actions: list[CitedAction] = []
    total_pending = sum(item.pending_total for item in ctx.clients)
    top_pending = sorted(ctx.clients, key=lambda item: item.pending_total, reverse=True)[:5]
    top_pending_share = _share(sum(item.pending_total for item in top_pending), total_pending)
    zero_clients = [client for client in ctx.clients if client.contracted_total > 0 and client.received_total == 0]
    if zero_clients:
        actions.append(
            CitedAction(
                action="Launch an immediate recovery sprint on zero-start clients.",
                rationale="Zero-start accounts are still carrying pending work and will continue to drag the portfolio unless outreach starts now.",
                citations=_collect_evidence_ids(evidence, explicit=("OVR-002", "RISK-002"), prefixes=("BKL-",), limit=3),
                confidence="high",
            )
        )
    if top_pending_share >= 0.4:
        actions.append(
            CitedAction(
                action="Focus leadership reviews on the top backlog clients instead of the full portfolio.",
                rationale="A small client set holds a disproportionate share of the outstanding work, so concentrated intervention will create the fastest movement.",
                citations=_collect_evidence_ids(evidence, explicit=("OVR-001",), prefixes=("BKL-",), limit=3),
                confidence="high",
            )
        )
    actions.append(
        CitedAction(
            action="Use anomaly and quality flags as gatekeepers for forecast trust.",
            rationale="Operational exceptions and data mismatches can distort both prioritization and any forward-looking narrative.",
            citations=_collect_evidence_ids(evidence, explicit=("OVR-998",), prefixes=("ANM-", "QLT-"), limit=3),
            confidence="medium",
        )
    )
    if ctx.staff:
        actions.append(
            CitedAction(
                action="Review staff load concentration before the next cycle.",
                rationale="Delivery concentration on a small number of people increases execution risk even when headline receipts are moving.",
                citations=_collect_evidence_ids(evidence, explicit=("OVR-998",), prefixes=("STF-",), limit=3),
                confidence="medium",
            )
        )
    return actions[:4]


def _reasoned_limitations(ctx: SnapshotReportContext, family: str, evidence: list[EvidenceItem]) -> list[CitedLine]:
    if family == "status_pipeline_dashboard":
        return [
            CitedLine(
                text="The current reasoning is limited to status, age, type, and note signals present in the pipeline snapshot; deeper root-cause diagnostics require richer workflow fields.",
                citations=_collect_evidence_ids(evidence, explicit=("OPS-998",), prefixes=("OPS-001",), limit=2),
            )
        ]
    if family == "product_catalog_dashboard":
        return [
            CitedLine(
                text="The current reasoning is grounded in product-master structure and quality signals; demand, transaction, or usage behavior is outside this snapshot.",
                citations=_collect_evidence_ids(evidence, explicit=("CAT-998",), prefixes=("CAT-001",), limit=2),
            )
        ]
    if family == "generic_review_dashboard":
        return [
            CitedLine(
                text="This workbook is still in semantic-readiness mode, so strong executive conclusions will remain limited until the file is mapped to a stable business family.",
                citations=_collect_evidence_ids(evidence, explicit=("GEN-998",), prefixes=("GEN-",), limit=2),
            )
        ]
    limitations: list[CitedLine] = []
    if not ctx.previous_snapshot:
        limitations.append(
            CitedLine(
                text="Trend confidence is limited because only one snapshot is currently available for this portfolio.",
                citations=_collect_evidence_ids(evidence, explicit=("OVR-998",), prefixes=("OVR-003", "QLT-"), limit=2),
            )
        )
    if not ctx.staff:
        limitations.append(
            CitedLine(
                text="Capacity findings are partial because no staff workload rows were available in the mapped snapshot.",
                citations=_collect_evidence_ids(evidence, explicit=("OVR-998",), prefixes=("QLT-",), limit=2),
            )
        )
    if not limitations:
        limitations.append(
            CitedLine(
                text="The summary is grounded in client-level contracted, received, pending, and mapped exception signals rather than task-level workflow history.",
                citations=_collect_evidence_ids(evidence, explicit=("OVR-998",), prefixes=("OVR-001",), limit=2),
            )
        )
    return limitations[:3]


def _generic_runtime_reasoning_bundle(
    ctx: SnapshotReportContext,
    gemini: GeminiRequestSettings | None = None,
) -> SummaryReasoningBundle | None:
    if not _generic_runtime_ready(ctx) or ctx.schema_profile is None or not ctx.workbook_type:
        return None
    tables = load_or_extract_snapshot_raw_tables(ctx.snapshot, ctx.workbook_type)
    if not tables:
        return None

    interpretation = interpret_workbook_semantics(
        profile=ctx.schema_profile,
        runtime_summary=_runtime_summary_for_semantics(ctx.runtime_payload),
        raw_tables=tables,
        gemini=gemini,
        user_guidance=None,
    )
    eda_result = run_eda_agent(
        profile=ctx.schema_profile,
        interpretation=interpretation,
        runtime_payload=ctx.runtime_payload,
        raw_tables=tables,
        gemini=gemini,
        user_guidance=None,
    )
    evidence, ids_by_key = _build_generic_adaptive_evidence(ctx, interpretation, eda_result)
    runtime = ctx.runtime_payload or {}
    comparison_groups = sorted(
        list(runtime.get("comparison_groups") or []),
        key=lambda item: _parse_runtime_period(str(item.get("group_label") or "")) or (9999, 99),
    )
    first_group = comparison_groups[0] if comparison_groups else None
    latest_group = comparison_groups[-1] if comparison_groups else None
    forecast = _generic_runtime_forecast(runtime)
    aligned_periods = len(comparison_groups)
    matched_pools = sum(int(group.get("matched_pool_count") or 0) for group in comparison_groups)
    tc_only = sum(int(group.get("unmatched_tc_pool_count") or 0) for group in comparison_groups)
    bc_only = sum(int(group.get("unmatched_bc_pool_count") or 0) for group in comparison_groups)
    fallback_citations = [item.id for item in evidence[:4]]
    latest_high = list((latest_group or {}).get("highest_rate_segments") or [])
    top_hotspots = ", ".join(
        f"{item.get('label') or 'unknown'} at {float(item.get('ratio') or 0.0):.2%}"
        for item in latest_high[:3]
    ) or "no high-risk pools were isolated from the latest aligned period"

    plan = AgenticOverallPlan(
        family="generic_review_dashboard",
        mode="generic_adaptive_overall_report",
        entity_name=f"generic-snapshot-{ctx.snapshot.id}",
        required_section_keys=[
            "executive_overview",
            "period_trend_story",
            "portfolio_hotspots",
            "forecast_and_risk",
            "quality_and_coverage",
        ],
        title_overrides={
            "executive_overview": "Executive View",
            "period_trend_story": "Period Trend Story",
            "portfolio_hotspots": "Pool Hotspots",
            "forecast_and_risk": "Forecast and Risk",
            "quality_and_coverage": "Quality and Coverage",
        },
        evidence=evidence,
    )

    findings = [
        VerifiedFinding(
            key="FIND-001",
            title="Paired Period Coverage",
            insight=(
                f"The workbook aligns {_fmt_num(aligned_periods)} TC/BC period pairs"
                + (
                    f" from {first_group.get('group_label') or 'the first period'} through {latest_group.get('group_label') or 'the latest period'}"
                    if first_group and latest_group
                    else ""
                )
                + f", with {_fmt_num(matched_pools)} matched pool comparisons overall."
            ),
            implication="This is sufficient to support month-over-month or quarter-over-quarter portfolio monitoring, but unmatched pools still need explicit coverage checks.",
            priority="high" if aligned_periods >= 3 else "medium",
            confidence="high",
            evidence_ids=(ids_by_key.get("period_alignment_scan") or ids_by_key.get("comparisons") or ids_by_key.get("runtime") or [])[:4],
        ),
        VerifiedFinding(
            key="FIND-002",
            title="Latest Period Hotspots",
            insight=(
                f"In {latest_group.get('group_label') or 'the latest aligned period'}, the highest BC/TC pools are {top_hotspots}."
                if latest_group
                else "The runtime did not isolate a latest aligned period with comparable BC/TC pool ratios."
            ),
            implication="The live dashboard should foreground the highest-ratio pools because they concentrate current bad-count risk more clearly than workbook-wide averages.",
            priority="critical" if latest_high else "medium",
            confidence="high" if latest_high else "medium",
            evidence_ids=(ids_by_key.get("comparisons") or ids_by_key.get("measure_by_dimension_sql_scan") or ids_by_key.get("eda") or [])[:4],
        ),
        VerifiedFinding(
            key="FIND-003",
            title="Trend and Forecast Direction",
            insight=(
                f"Observed BC/TC trend is {forecast['direction']}; the deterministic next-period projection is {forecast['next_ratio']:.2%}, "
                f"with TC volume trending {forecast['tc_trend']} to {_fmt_num(int(forecast['next_tc']))} and BC volume trending {forecast['bc_trend']} to {_fmt_num(int(forecast['next_bc']))}."
                if forecast.get("ready")
                else f"Trend analysis is only partially ready because just {_fmt_num(int(forecast.get('periods') or 0))} aligned ratio periods are available for forecasting."
            ),
            implication="Forecasts are suitable for directional decision support here, but they should be interpreted as planning signals rather than exact commitments.",
            priority="high" if forecast.get("ready") else "medium",
            confidence="medium" if forecast.get("ready") else "low",
            evidence_ids=(ids_by_key.get("forecast_signal_scan") or ids_by_key.get("comparisons") or [])[:4],
        ),
        VerifiedFinding(
            key="FIND-004",
            title="Coverage and Data Reliability",
            insight=(
                f"Across the aligned period groups, TC-only unmatched pools total {_fmt_num(tc_only)} and BC-only unmatched pools total {_fmt_num(bc_only)}. "
                "Reference sheets are available for definitions and business questions, but they are excluded from primary performance findings."
            ),
            implication="Decision-makers can trust the paired business sheets as the primary evidence source, while reference sheets should only shape interpretation and follow-up questions.",
            priority="high" if tc_only or bc_only else "medium",
            confidence="high",
            evidence_ids=(ids_by_key.get("reference") or [])[:2] + (ids_by_key.get("period_alignment_scan") or ids_by_key.get("comparisons") or [])[:2],
        ),
    ]
    findings = _verify_findings(findings, evidence)

    actions = [
        CitedAction(
            action="Prioritize the highest BC/TC pools in the latest aligned period as the primary risk watchlist.",
            rationale="Those pools carry the most concentrated bad-count pressure and are more actionable than workbook-wide averages.",
            citations=(ids_by_key.get("comparisons") or fallback_citations)[:3],
            confidence="high",
        ),
        CitedAction(
            action="Track period-over-period movement in the paired TC/BC groups before broadening the dashboard into generic descriptive charts.",
            rationale="The strongest decision value in this workbook comes from aligned period movement, not from one-time schema-level profiling.",
            citations=(ids_by_key.get("period_alignment_scan") or ids_by_key.get("comparisons") or fallback_citations)[:3],
            confidence="high",
        ),
        CitedAction(
            action="Reconcile unmatched TC-only and BC-only pools before using comparisons as hard management commitments.",
            rationale="Coverage mismatches can overstate or understate deterioration for specific pools if they remain unresolved.",
            citations=(ids_by_key.get("period_alignment_scan") or ids_by_key.get("comparisons") or fallback_citations)[:3],
            confidence="medium",
        ),
        CitedAction(
            action="Use the Definitions and Questions sheets as interpretation aids only, not as primary analytical evidence.",
            rationale="Reference sheets improve semantic context, but the decision signal is carried by the paired business sheets.",
            citations=(ids_by_key.get("reference") or ids_by_key.get("semantic") or fallback_citations)[:3],
            confidence="medium",
        ),
    ]

    limitations = [
        CitedLine(
            text="The forecast is a deterministic directional projection from aligned period totals; it is useful for planning, but not equivalent to a statistical commitment model.",
            citations=(ids_by_key.get("forecast_signal_scan") or ids_by_key.get("comparisons") or fallback_citations)[:2],
        ),
        CitedLine(
            text="This summary is grounded on paired TC/BC workbook snapshots and pool-level aggregation rather than transactional root-cause drillthrough.",
            citations=(ids_by_key.get("period_alignment_scan") or ids_by_key.get("runtime") or fallback_citations)[:2],
        ),
    ]

    packet = SummaryReasoningPacket(
        family="generic_review_dashboard",
        plan_summary=(
            f"The summary follows an adaptive investigation plan for snapshot {ctx.snapshot.as_of_date} that interprets workbook semantics, "
            "runs deterministic EDA on the paired business sheets, validates period alignment, and writes findings from verified pool-level evidence."
        ),
        steps=[
            InvestigationStep(
                key=f"STEP-{idx:03d}",
                title=step.title,
                objective=step.objective,
                tool=step.tool,
                rationale=step.rationale,
                evidence_ids=(ids_by_key.get(step.tool) or ids_by_key.get("eda") or [])[:4],
            )
            for idx, step in enumerate(eda_result.plan.steps[:8], start=1)
        ],
        findings=findings,
        actions=actions[:4],
        limitations=limitations[:3],
    )
    return SummaryReasoningBundle(plan=plan, evidence=evidence, packet=packet)


def _variance_eda_reasoning_bundle(
    ctx: SnapshotReportContext,
    gemini: GeminiRequestSettings | None = None,
) -> SummaryReasoningBundle | None:
    if _dashboard_family(ctx) != "variance_dashboard" or ctx.schema_profile is None or not ctx.workbook_type:
        return None
    tables = load_or_extract_snapshot_raw_tables(ctx.snapshot, ctx.workbook_type)
    if not tables:
        return None
    interpretation = interpret_workbook_semantics(
        profile=ctx.schema_profile,
        runtime_summary=_runtime_summary_for_semantics(ctx.runtime_payload),
        raw_tables=tables,
        gemini=gemini,
        user_guidance=None,
    )
    eda_result = run_eda_agent(
        profile=ctx.schema_profile,
        interpretation=interpretation,
        runtime_payload=ctx.runtime_payload,
        raw_tables=tables,
        gemini=gemini,
        user_guidance=None,
    )
    plan = _build_overall_agentic_plan(ctx)
    base_packet = build_summary_reasoning_packet(ctx)
    extra_evidence: list[EvidenceItem] = []
    tool_to_ids: dict[str, list[str]] = {}
    for idx, item in enumerate(eda_result.evidence, start=1):
        evidence_id = f"VED-{idx:03d}"
        detail = item.detail
        if item.supporting_metrics:
            detail = f"{detail} Metrics: {', '.join(item.supporting_metrics[:4])}."
        extra_evidence.append(EvidenceItem(id=evidence_id, tab="EDA", title=item.title, detail=detail))
        tool_to_ids.setdefault(item.tool, []).append(evidence_id)
    steps = [
        InvestigationStep(
            key=f"EDA-{idx:03d}",
            title=step.title,
            objective=step.objective,
            tool=step.tool,
            rationale=step.rationale,
            evidence_ids=(tool_to_ids.get(step.tool) or [])[:4],
        )
        for idx, step in enumerate(eda_result.plan.steps[:5], start=1)
    ]
    steps.extend(base_packet.steps[:4])
    limitations = list(base_packet.limitations)
    if extra_evidence and not any("EDA" in item.text for item in limitations):
        limitations.append(
            CitedLine(
                text="EDA checks ran on preserved client and staff tables; because the workbook is already aggregated, those checks strengthen control coverage but do not recreate transaction-level detail.",
                citations=[extra_evidence[0].id],
            )
        )
    packet = SummaryReasoningPacket(
        family=base_packet.family,
        plan_summary=(
            f"{base_packet.plan_summary} The report also uses semantic interpretation and a deterministic EDA pass over preserved client and staff tables before writing conclusions."
        ),
        steps=steps[:8],
        findings=base_packet.findings,
        actions=base_packet.actions,
        limitations=limitations[:3],
    )
    return SummaryReasoningBundle(plan=plan, evidence=[*plan.evidence, *extra_evidence], packet=packet)


def build_summary_reasoning_packet(ctx: SnapshotReportContext) -> SummaryReasoningPacket:
    plan = _build_overall_agentic_plan(ctx)
    steps = _plan_summary_steps(ctx, plan)
    if plan.family == "status_pipeline_dashboard":
        findings = _status_findings(ctx, plan.evidence)
    elif plan.family == "product_catalog_dashboard":
        findings = _product_findings(ctx, plan.evidence)
    elif plan.family == "generic_review_dashboard":
        findings = _generic_findings(ctx, plan.evidence)
    else:
        findings = _variance_findings(ctx, plan.evidence)
    return SummaryReasoningPacket(
        family=plan.family,
        plan_summary=_plan_summary_text(ctx, plan.family),
        steps=steps,
        findings=_verify_findings(findings, plan.evidence),
        actions=_reasoned_actions(ctx, plan.family, plan.evidence),
        limitations=_reasoned_limitations(ctx, plan.family, plan.evidence),
    )


def build_client_summary_reasoning_packet(ctx: SnapshotReportContext, client: ClientMetrics) -> SummaryReasoningPacket:
    evidence = _build_client_evidence(ctx, client)
    predictive = _client_predictive_summary(ctx, client)
    previous = _client_previous(ctx, client)
    delta_received = _client_delta_received(ctx, client)
    delta_rate = _client_delta_rate(ctx, client)
    pending_share = _share(client.pending_total, sum(item.pending_total for item in ctx.clients))
    findings = _verify_findings(
        [
            VerifiedFinding(
                key="FIND-001",
                title="Delivery Position",
                insight=f"{client.client_name} has received {_fmt_num(client.received_total)} of {_fmt_num(client.contracted_total)} contracted returns, leaving {_fmt_num(client.pending_total)} pending at a receipt rate of {_fmt_pct(client.receipt_rate)}.",
                implication="This account should be managed according to its current receipt position rather than its contracted size alone.",
                priority="critical" if _risk_label(client.receipt_rate) in {"Critical", "Not Started"} else "high" if _risk_label(client.receipt_rate) == "At Risk" else "medium",
                confidence="high",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("CL-001", "CL-003"), prefixes=(), limit=3),
            ),
            VerifiedFinding(
                key="FIND-002",
                title="Portfolio Exposure",
                insight=f"The client represents {pending_share * 100:.1f}% of all pending workload and currently sits in the {_risk_label(client.receipt_rate)} risk band.",
                implication="Intervention priority should reflect both the client's risk level and its share of remaining portfolio work.",
                priority="high" if pending_share >= 0.1 or _risk_label(client.receipt_rate) in {"Critical", "At Risk"} else "medium",
                confidence="high",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("CL-003",), prefixes=("CL-001",), limit=3),
            ),
            VerifiedFinding(
                key="FIND-003",
                title="Momentum and Outlook",
                insight=(
                    f"Recent pace is {predictive['pace_daily']} with a 14-day projection to {predictive['projected_14_day_received']} received ({predictive['projected_14_day_rate']})."
                    if previous
                    else f"The current heuristic outlook is {predictive['heuristic_final_received']} received ({predictive['heuristic_final_rate']})."
                ),
                implication="The client outlook should be judged by expected backlog conversion, not only by current received totals.",
                priority="high" if client.pending_total else "medium",
                confidence="high" if previous else "medium",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("CL-004",), prefixes=("CL-006",), limit=3),
            ),
            VerifiedFinding(
                key="FIND-004",
                title="Quality and Scope Risk",
                insight=(
                    f"Current row quality flags indicate totals_reconcile={'no' if _has_total_inconsistency(client) else 'yes'}, "
                    f"over_delivered={'yes' if client.received_total > client.contracted_total and client.contracted_total > 0 else 'no'}, "
                    f"uncontracted_receipts={'yes' if client.contracted_total == 0 and client.received_total > 0 else 'no'}."
                ),
                implication="Any scope or row-quality inconsistency on this account can change how its delivery story should be interpreted and acted on.",
                priority="high" if _has_total_inconsistency(client) or client.contracted_total == 0 or client.received_total > client.contracted_total else "medium",
                confidence="medium",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("CL-005",), prefixes=("CLQ-",), limit=3),
            ),
        ],
        evidence,
    )
    actions = [
        CitedAction(
            action=line,
            rationale="This recommendation is derived from the account's current receipt position, backlog mix, and risk profile.",
            citations=_collect_evidence_ids(evidence, explicit=("CL-001", "CL-003"), prefixes=("CL-004", "CL-005"), limit=3),
            confidence="high" if idx == 0 else "medium",
        )
        for idx, line in enumerate(_client_prescriptive_actions(ctx, client)[:4])
    ]
    limitations: list[CitedLine] = []
    if not previous:
        limitations.append(
            CitedLine(
                text="Trend confidence is limited because no prior snapshot is available for this client.",
                citations=_collect_evidence_ids(evidence, explicit=("CL-004",), prefixes=("CLQ-",), limit=2),
            )
        )
    if not limitations:
        limitations.append(
            CitedLine(
                text="The client summary is grounded in account-level contracted, received, pending, and exception signals rather than task-level workflow events.",
                citations=_collect_evidence_ids(evidence, explicit=("CL-001", "CL-005"), prefixes=(), limit=2),
            )
        )
    return SummaryReasoningPacket(
        family="variance_client_summary",
        plan_summary=f"The summary follows an investigation plan for {client.client_name} that checks delivery position, portfolio exposure, momentum, and data-risk signals before writing recommendations.",
        steps=[
            InvestigationStep(
                key="STEP-001",
                title="Client Baseline",
                objective="Establish the client's contracted, received, and pending position.",
                tool="client_baseline_scan",
                rationale="Account-level intervention starts with a clear view of current delivery position and risk band.",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("CL-001", "CL-003"), prefixes=(), limit=3),
            ),
            InvestigationStep(
                key="STEP-002",
                title="Momentum Review",
                objective="Check whether recent movement supports the current delivery outlook.",
                tool="client_momentum_review",
                rationale="A client with pending work but weak movement needs a different intervention than one already accelerating.",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("CL-004",), prefixes=("CL-006",), limit=3),
            ),
            InvestigationStep(
                key="STEP-003",
                title="Risk and Quality Review",
                objective="Test for scope issues, row inconsistencies, or portfolio-position risks that change the recommended action.",
                tool="client_risk_probe",
                rationale="Data and scope exceptions can invalidate an otherwise reasonable delivery interpretation.",
                evidence_ids=_collect_evidence_ids(evidence, explicit=("CL-005",), prefixes=("CLQ-",), limit=3),
            ),
        ],
        findings=findings,
        actions=actions,
        limitations=limitations,
    )


def _analysis_report_to_reasoning_bundle(ctx: SnapshotReportContext, plan: AgenticOverallPlan, report: AnalysisReport) -> SummaryReasoningBundle:
    analysis_evidence = [
        EvidenceItem(id=item.id, tab=item.table_name, title=item.title, detail=item.detail)
        for item in report.evidence
    ]
    evidence_by_table: dict[str, list[str]] = {}
    for item in analysis_evidence:
        evidence_by_table.setdefault(item.tab, []).append(item.id)

    def step_evidence_ids(table_name: str, operator: str) -> list[str]:
        operator_tokens = [token for token in operator.replace("_", " ").split() if token]
        exact_matches = [
            item.id
            for item in analysis_evidence
            if item.tab == table_name
            and any(token.lower() in f"{item.title} {item.detail}".lower() for token in operator_tokens)
        ]
        if exact_matches:
            return exact_matches[:3]
        return (evidence_by_table.get(table_name) or [])[:2]

    packet = SummaryReasoningPacket(
        family=plan.family,
        plan_summary=report.plan_summary,
        steps=[
            InvestigationStep(
                key=step.key,
                title=f"{step.operator.replace('_', ' ').title()} on {step.table_name}",
                objective=(
                    f"Evaluate {step.measure} by {step.dimension}."
                    if step.dimension and step.measure
                    else f"Evaluate {step.dimension}."
                    if step.dimension
                    else f"Run {step.operator.replace('_', ' ')}."
                ),
                tool=step.operator,
                rationale=step.rationale,
                evidence_ids=step_evidence_ids(step.table_name, step.operator),
            )
            for step in report.steps
        ],
        findings=[
            VerifiedFinding(
                key=finding.key,
                title=finding.title,
                insight=finding.insight,
                implication=finding.implication,
                priority=finding.priority,
                confidence=finding.confidence,
                evidence_ids=finding.evidence_ids,
            )
            for finding in report.findings
        ],
        actions=[
            CitedAction(
                action=item.action,
                rationale=item.rationale,
                citations=item.evidence_ids,
                confidence=item.confidence,
            )
            for item in report.actions
        ],
        limitations=[
            CitedLine(text=item.text, citations=item.evidence_ids or ([analysis_evidence[0].id] if analysis_evidence else []))
            for item in report.limitations
        ],
    )
    return SummaryReasoningBundle(plan=plan, evidence=[*plan.evidence, *analysis_evidence], packet=packet)


def build_summary_reasoning_bundle(ctx: SnapshotReportContext, gemini: GeminiRequestSettings | None = None) -> SummaryReasoningBundle:
    stored = _deserialize_summary_reasoning_bundle(ctx.analytics_bundle_payload)
    if stored is not None:
        return stored
    variance_bundle = _variance_eda_reasoning_bundle(ctx, gemini=gemini)
    if variance_bundle is not None:
        return variance_bundle
    adaptive_generic = _generic_runtime_reasoning_bundle(ctx, gemini=gemini)
    if adaptive_generic is not None:
        return adaptive_generic
    plan = _build_overall_agentic_plan(ctx)
    if ctx.workbook_type:
        tables = load_or_extract_snapshot_raw_tables(ctx.snapshot, ctx.workbook_type)
        if tables:
            report = build_analysis_report(tables, ctx.workbook_type)
            if report.findings:
                return _analysis_report_to_reasoning_bundle(ctx, plan, report)
    return SummaryReasoningBundle(plan=plan, evidence=plan.evidence, packet=build_summary_reasoning_packet(ctx))


def _mask_snapshot_evidence_if_needed(ctx: SnapshotReportContext, evidence: list[EvidenceItem]) -> tuple[list[EvidenceItem], PiiMaskLookup | None]:
    lookup = ctx.pii_lookup
    if lookup is None or not lookup.original_to_token:
        return evidence, None
    return (
        [
            EvidenceItem(
                id=item.id,
                tab=item.tab,
                title=mask_text(item.title, lookup),
                detail=mask_text(item.detail, lookup),
            )
            for item in evidence
        ],
        lookup,
    )


def _unmask_agentic_draft(draft: AgenticReportDraft, lookup) -> AgenticReportDraft:
    if lookup is None:
        return draft
    return AgenticReportDraft(
        sections=[
            ReportSection(
                key=section.key,
                title=unmask_text(section.title, lookup),
                bullets=[
                    CitedLine(text=unmask_text(line.text, lookup), citations=line.citations)
                    for line in section.bullets
                ],
            )
            for section in draft.sections
        ],
        actions=[
            CitedAction(
                action=unmask_text(action.action, lookup),
                rationale=unmask_text(action.rationale, lookup),
                citations=action.citations,
                confidence=action.confidence,
            )
            for action in draft.actions
        ],
        limitations=[
            CitedLine(text=unmask_text(line.text, lookup), citations=line.citations)
            for line in draft.limitations
        ],
    )


def _serialize_reasoning_packet(packet: SummaryReasoningPacket) -> str:
    step_lines = "\n".join(
        f"{step.key} | {step.title} | tool={step.tool} | objective={step.objective} | rationale={step.rationale} | evidence={', '.join(step.evidence_ids)}"
        for step in packet.steps
    )
    finding_lines = "\n".join(
        f"{finding.key} | {finding.title} | priority={finding.priority} | confidence={finding.confidence} | insight={finding.insight} | implication={finding.implication} | evidence={', '.join(finding.evidence_ids)}"
        for finding in packet.findings
    )
    action_lines = "\n".join(
        f"{idx+1}. action={action.action} | rationale={action.rationale} | confidence={action.confidence} | evidence={', '.join(action.citations)}"
        for idx, action in enumerate(packet.actions)
    )
    limitation_lines = "\n".join(
        f"{idx+1}. limitation={line.text} | evidence={', '.join(line.citations)}"
        for idx, line in enumerate(packet.limitations)
    )
    return (
        f"Plan summary:\n{packet.plan_summary}\n\n"
        f"Executed investigation steps:\n{step_lines}\n\n"
        f"Verified findings:\n{finding_lines}\n\n"
        f"Grounded actions:\n{action_lines}\n\n"
        f"Coverage notes:\n{limitation_lines}\n"
    )


def _build_agent_prompt(
    mode: str,
    report_style: str,
    evidence: list[EvidenceItem],
    required_section_keys: list[str],
    entity_name: str,
    reasoning_packet: SummaryReasoningPacket | None,
) -> str:
    evidence_lines = "\n".join(f"{item.id} | {item.tab} | {item.title} | {item.detail}" for item in evidence)
    reasoning_block = (
        "Verified reasoning packet:\n"
        f"{_serialize_reasoning_packet(reasoning_packet)}\n"
        if reasoning_packet is not None
        else ""
    )
    if report_style == "analytics":
        role_block = (
            f"You are the analytics-summary agent for the grounded planner/executor {mode} report for {entity_name}.\n"
            f"{'An investigation plan has already been executed and verified before you write. Use the verified findings and executed steps as the primary reasoning substrate.\\n' if reasoning_packet is not None else ''}"
            "Write for an analyst or SME audience.\n"
            "Emphasize data shape, distribution, concentration, trend, forecast readiness, anomalies, and data-quality coverage.\n"
            "Do not drift into generic business storytelling.\n"
            "If forecasting is weak or not applicable, say so explicitly from the evidence instead of implying certainty.\n"
        )
    else:
        role_block = (
            f"You are the executive-summary writing agent for the grounded planner/executor {mode} report for {entity_name}.\n"
            f"{'An investigation plan has already been executed and verified before you write. Use the verified findings and actions as the primary reasoning substrate, and use the evidence list as the factual source of record.\\n' if reasoning_packet is not None else ''}"
            "Keep the language analytical and business-facing, not generic.\n"
        )
    return (
        f"{role_block}"
        "Use only the provided evidence. Do not invent facts, numbers, trends, anomalies, or recommendations.\n"
        "Every bullet and every recommendation must cite one or more evidence IDs from the evidence list.\n"
        "Recommendations must be specific, operational, and proportional to the evidence.\n"
        "If evidence is insufficient, place the limitation in the limitations section instead of guessing.\n"
        f"Required section keys in order: {', '.join(required_section_keys)}.\n"
        "Section titles should be concise and clear for the intended audience.\n"
        "Avoid headings such as Predictive Analytics or Prescriptive Analytics unless the section contract explicitly requires forecast analysis.\n"
        "Do not cite any identifier that is not in the evidence list.\n\n"
        f"{reasoning_block}"
        "Evidence list:\n"
        f"{evidence_lines}\n"
    )


def _build_agent_review_prompt(
    draft: AgenticReportDraft,
    evidence: list[EvidenceItem],
    required_section_keys: list[str],
    validation_errors: list[str],
    reasoning_packet: SummaryReasoningPacket | None,
    report_style: str,
) -> str:
    evidence_lines = "\n".join(f"{item.id} | {item.tab} | {item.title} | {item.detail}" for item in evidence)
    reasoning_block = (
        "Verified reasoning packet:\n"
        f"{_serialize_reasoning_packet(reasoning_packet)}\n"
        if reasoning_packet is not None
        else ""
    )
    style_line = (
        "Keep the tone analyst-facing and explicit about data shape, trend, forecast readiness, anomalies, and coverage."
        if report_style == "analytics"
        else "Keep the tone executive-facing, concise, and decision-oriented."
    )
    return (
        "Review and improve the grounded report draft.\n"
        f"{'Preserve the verified investigation logic and make sure the report reflects the executed findings rather than generic commentary.\\n' if reasoning_packet is not None else ''}"
        "Keep only evidence-supported reasoning.\n"
        f"Required section keys in order: {', '.join(required_section_keys)}.\n"
        f"{style_line}\n"
        "Strengthen specificity of the prescriptive actions, improve insight density, and correct any generic wording.\n"
        "Avoid headings such as Predictive Analytics or Prescriptive Analytics unless forecast analysis is explicitly required by the section contract.\n"
        "All bullets and actions must cite valid evidence IDs.\n"
        f"Validation issues to fix: {'; '.join(validation_errors) if validation_errors else 'none'}.\n\n"
        f"{reasoning_block}"
        f"Current draft JSON:\n{draft.model_dump_json(indent=2)}\n\n"
        "Evidence list:\n"
        f"{evidence_lines}\n"
    )


def _validate_agentic_draft(draft: AgenticReportDraft, evidence: list[EvidenceItem], required_section_keys: list[str]) -> list[str]:
    errors: list[str] = []
    valid_ids = {item.id for item in evidence}
    keys = [section.key for section in draft.sections]
    if keys != required_section_keys:
        errors.append(f"section keys must exactly match required order {required_section_keys}; got {keys}")
    for section in draft.sections:
        if not section.bullets:
            errors.append(f"section {section.key} has no bullets")
        for bullet in section.bullets:
            if any(citation not in valid_ids for citation in bullet.citations):
                errors.append(f"section {section.key} has invalid citation")
    for action in draft.actions:
        if any(citation not in valid_ids for citation in action.citations):
            errors.append("prescriptive action has invalid citation")
    for line in draft.limitations:
        if any(citation not in valid_ids for citation in line.citations):
            errors.append("limitation has invalid citation")
    return errors


def _run_agentic_report_graph(
    mode: str,
    report_style: str,
    evidence: list[EvidenceItem],
    required_section_keys: list[str],
    entity_name: str,
    reasoning_packet: SummaryReasoningPacket | None = None,
    gemini: GeminiRequestSettings | None = None,
) -> AgenticReportDraft | None:
    if gemini is None and (not settings.report_agent_enabled or not settings.openai_api_key):
        return None
    try:
        client = OpenAI(api_key=settings.openai_api_key) if gemini is None else None

        def _parse_structured(prompt: str) -> AgenticReportDraft:
            if gemini is not None:
                return gemini_generate_structured(prompt=prompt, schema=AgenticReportDraft, settings=gemini)
            assert client is not None
            response = client.responses.parse(
                model=settings.report_agent_model,
                reasoning={"effort": settings.report_agent_reasoning_effort},
                input=prompt,
                text_format=AgenticReportDraft,
                store=False,
            )
            return response.output_parsed

        def prepare_node(state: AgenticReportState) -> AgenticReportState:
            return {
                "prompt": _build_agent_prompt(mode, state["report_style"], state["evidence"], state["required_section_keys"], entity_name, reasoning_packet),
                "validation_errors": [],
            }

        def draft_node(state: AgenticReportState) -> AgenticReportState:
            return {"draft": _parse_structured(state["prompt"])}

        def review_node(state: AgenticReportState) -> AgenticReportState:
            review_prompt = _build_agent_review_prompt(
                state["draft"],
                state["evidence"],
                state["required_section_keys"],
                state.get("validation_errors", []),
                reasoning_packet,
                state["report_style"],
            )
            return {"reviewed_draft": _parse_structured(review_prompt)}

        def validate_node(state: AgenticReportState) -> AgenticReportState:
            candidate = state.get("reviewed_draft") or state["draft"]
            errors = _validate_agentic_draft(candidate, state["evidence"], state["required_section_keys"])
            return {"validation_errors": errors, "final_draft": candidate}

        graph = StateGraph(AgenticReportState)
        graph.add_node("prepare", prepare_node)
        graph.add_node("draft", draft_node)
        graph.add_node("review", review_node)
        graph.add_node("validate", validate_node)
        graph.add_edge(START, "prepare")
        graph.add_edge("prepare", "draft")
        graph.add_edge("draft", "review")
        graph.add_edge("review", "validate")
        graph.add_edge("validate", END)
        compiled = graph.compile(checkpointer=InMemorySaver())

        result = compiled.invoke(
            {
                "mode": mode,
                "report_style": report_style,
                "evidence": evidence,
                "required_section_keys": required_section_keys,
            },
            config={"configurable": {"thread_id": f"{mode}:{entity_name}"}},
        )

        if result.get("validation_errors"):
            return None
        return result.get("final_draft")
    except (Exception, GeminiReasoningError):
        return None


def _render_reasoning_packet_fallback(packet: SummaryReasoningPacket, styles) -> list:
    story: list = [
        Paragraph("Context", styles["SectionTitle"]),
        Paragraph(f"• {packet.plan_summary}", styles["BodyMuted"]),
        Spacer(1, 0.1 * inch),
        Paragraph("Key Insights", styles["SectionTitle"]),
    ]
    for finding in packet.findings[:4]:
        story.append(
            Paragraph(
                f"• {finding.insight} <font color='#5b5e70'>[{', '.join(finding.evidence_ids)} · {finding.priority}/{finding.confidence}]</font>",
                styles["BodyMuted"],
            )
        )
    story.extend([Spacer(1, 0.1 * inch), Paragraph("Risks and Implications", styles["SectionTitle"])])
    for finding in packet.findings[:4]:
        story.append(
            Paragraph(
                f"• {finding.implication} <font color='#5b5e70'>[{', '.join(finding.evidence_ids)}]</font>",
                styles["BodyMuted"],
            )
        )
    story.extend([Spacer(1, 0.1 * inch), Paragraph("Priority Actions", styles["SectionTitle"])])
    for action in packet.actions[:4]:
        story.append(
            Paragraph(
                f"• <b>{action.action}</b> {action.rationale} <font color='#5b5e70'>{_citation_label(action.citations)} · confidence={action.confidence}</font>",
                styles["BodyMuted"],
            )
        )
    story.extend([Spacer(1, 0.1 * inch), Paragraph("Coverage Notes", styles["SectionTitle"])])
    for line in packet.limitations:
        story.append(Paragraph(f"• {line.text} <font color='#5b5e70'>{_citation_label(line.citations)}</font>", styles["BodyMuted"]))
    if packet.findings:
        story.extend(
            [
                Spacer(1, 0.1 * inch),
                Paragraph("Bottom Line", styles["SectionTitle"]),
                Paragraph(
                    f"• {packet.findings[0].implication} <font color='#5b5e70'>[{', '.join(packet.findings[0].evidence_ids)}]</font>",
                    styles["BodyMuted"],
                ),
            ]
        )
    return story


def _render_data_analysis_summary(packet: SummaryReasoningPacket, styles) -> list:
    story: list = [
        PageBreak(),
        Paragraph("Data Analysis Summary", styles["SectionTitle"]),
        Paragraph(
            "This section is analyst-facing. It shows the executed investigation scope, verified analytical findings, and coverage limits behind the business summary.",
            styles["BodyMuted"],
        ),
        Spacer(1, 0.1 * inch),
        Paragraph("Investigation Scope", styles["SectionTitle"]),
        Paragraph(f"• {packet.plan_summary}", styles["BodyMuted"]),
    ]
    for step in packet.steps[:6]:
        story.append(
            Paragraph(
                f"• <b>{step.title}</b> {step.objective} <font color='#5b5e70'>tool={step.tool} · {_citation_label(step.evidence_ids)}</font>",
                styles["BodyMuted"],
            )
        )
    story.extend([Spacer(1, 0.1 * inch), Paragraph("Verified Analytical Findings", styles["SectionTitle"])])
    for finding in packet.findings[:6]:
        story.append(
            Paragraph(
                f"• <b>{finding.title}</b> {finding.insight} <font color='#5b5e70'>[{', '.join(finding.evidence_ids)} · {finding.priority}/{finding.confidence}]</font>",
                styles["BodyMuted"],
            )
        )
    story.extend([Spacer(1, 0.1 * inch), Paragraph("Coverage and Data Quality Notes", styles["SectionTitle"])])
    for line in packet.limitations[:4]:
        story.append(
            Paragraph(
                f"• {line.text} <font color='#5b5e70'>{_citation_label(line.citations)}</font>",
                styles["BodyMuted"],
            )
        )
    if packet.actions:
        story.extend([Spacer(1, 0.1 * inch), Paragraph("Analytical Follow-Up", styles["SectionTitle"])])
        for action in packet.actions[:3]:
            story.append(
                Paragraph(
                    f"• <b>{action.action}</b> {action.rationale} <font color='#5b5e70'>{_citation_label(action.citations)} · confidence={action.confidence}</font>",
                    styles["BodyMuted"],
                )
            )
    return story


def generate_agentic_chat_response(
    *,
    ctx: SnapshotReportContext,
    question: str,
    gemini: GeminiRequestSettings | None = None,
) -> DashboardChatResponse:
    chat_context = build_agentic_chat_context(ctx)
    evidence = chat_context.evidence
    if chat_context.family == "generic_review_dashboard":
        priority_prefixes = ("CHAT-ADX-", "ADX-", "GCP-", "GEN-", "EDA-", "SEM-")
        prioritized: list[EvidenceItem] = []
        seen_ids: set[str] = set()
        for prefix in priority_prefixes:
            for item in evidence:
                if item.id in seen_ids:
                    continue
                if item.id == prefix or item.id.startswith(prefix):
                    prioritized.append(item)
                    seen_ids.add(item.id)
        for item in evidence:
            if item.id not in seen_ids:
                prioritized.append(item)
        evidence = prioritized
    evidence = evidence[:20]
    evidence_lines = "\n".join(f"{item.id} | {item.tab} | {item.title} | {item.detail}" for item in evidence)
    prompt = (
        f"You are a business-facing analytics copilot for the dashboard '{chat_context.dashboard_title}'.\n"
        f"The workbook family focus is {chat_context.family_description}.\n"
        "Answer only from the grounded evidence below.\n"
        "Do not invent facts, metrics, trends, causes, or recommendations.\n"
        "If the evidence is thin, say so directly.\n"
        "Keep the answer concise, analytical, and useful to a business user.\n"
        "Prefer concrete implications, anomalies, priorities, or recommended next actions when supported.\n"
        "Do not talk about parsing, uploads, files, or implementation mechanics unless the user explicitly asks about them.\n"
        "Do not mention internal tools, engines, schemas, DuckDB, pandas, or implementation details in the answer.\n"
        "Return JSON only with fields: title, summary, cards, bullets.\n"
        "cards should contain at most 3 items. Each card must include title and value, and may include meta.\n"
        "bullets should contain at most 6 items.\n\n"
        f"User question:\n{question}\n\n"
        "Evidence list:\n"
        f"{evidence_lines}\n"
    )

    if gemini is not None:
        return gemini_generate_structured(prompt=prompt, schema=DashboardChatResponse, settings=gemini)
    if settings.openai_api_key:
        client = OpenAI(api_key=settings.openai_api_key)
        response = client.responses.parse(
            model=settings.report_agent_model,
            reasoning={"effort": settings.report_agent_reasoning_effort},
            input=prompt,
            text_format=DashboardChatResponse,
            store=False,
        )
        return response.output_parsed
    raise GeminiReasoningError("No active reasoning provider is configured for agentic chat")


def _citation_label(citations: list[str]) -> str:
    return "[" + ", ".join(citations) + "]"


def _render_agentic_draft(draft: AgenticReportDraft, styles, title_overrides: dict[str, str] | None = None) -> list:
    story: list = []
    for section in draft.sections:
        title = title_overrides.get(section.key, section.title) if title_overrides else section.title
        story.append(Paragraph(title, styles["SectionTitle"]))
        for bullet in section.bullets:
            story.append(Paragraph(f"• {bullet.text} <font color='#5b5e70'>{_citation_label(bullet.citations)}</font>", styles["BodyMuted"]))
        story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("Recommended Actions", styles["SectionTitle"]))
    for action in draft.actions:
        story.append(
            Paragraph(
                f"• <b>{action.action}</b> {action.rationale} <font color='#5b5e70'>{_citation_label(action.citations)} · confidence={action.confidence}</font>",
                styles["BodyMuted"],
            )
        )
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph("Coverage Notes", styles["SectionTitle"]))
    for line in draft.limitations:
        story.append(Paragraph(f"• {line.text} <font color='#5b5e70'>{_citation_label(line.citations)}</font>", styles["BodyMuted"]))
    return story


def _render_citation_appendix(evidence: list[EvidenceItem], styles) -> list:
    rows = [[item.id, item.tab, item.title, item.detail] for item in evidence]
    return [
        PageBreak(),
        Paragraph("Citation Appendix", styles["SectionTitle"]),
        Paragraph("Every grounded narrative statement in the agentic sections cites one or more evidence identifiers from this appendix.", styles["BodyMuted"]),
        Spacer(1, 0.08 * inch),
        _data_table(["ID", "Tab", "Evidence", "Detail"], rows, [0.75 * inch, 1.1 * inch, 1.45 * inch, 3.2 * inch]),
    ]


def _render_fallback_framework(styles, sections: list[tuple[str, list[str]]]) -> list:
    story: list = []
    for title, bullets in sections:
        story.append(Paragraph(title, styles["SectionTitle"]))
        for bullet in bullets:
            story.append(Paragraph(f"• {bullet}", styles["BodyMuted"]))
        story.append(Spacer(1, 0.1 * inch))
    return story


def _variance_fallback_framework(ctx: SnapshotReportContext) -> list[tuple[str, list[str]]]:
    total_contracted = sum(item.contracted_total for item in ctx.clients)
    total_received = sum(item.received_total for item in ctx.clients)
    total_pending = sum(item.pending_total for item in ctx.clients)
    overall_rate = _share(total_received, total_contracted)
    zero_clients = [client for client in ctx.clients if client.contracted_total > 0 and client.received_total == 0]
    critical_clients = [client for client in ctx.clients if client.receipt_rate is not None and 0 < client.receipt_rate < 0.15]
    at_risk_clients = [client for client in ctx.clients if client.receipt_rate is not None and 0.15 <= client.receipt_rate < 0.35]
    top_pending_clients = sorted(ctx.clients, key=lambda item: item.pending_total, reverse=True)[:5]
    top_pending_total = sum(item.pending_total for item in top_pending_clients)
    business_backlog = sum(item.pending_bus for item in ctx.clients)
    predictive = _overall_predictive_summary(ctx)
    anomalies = _overall_anomaly_rows(ctx)
    quality = _overall_quality_suggestions(ctx)
    actions = _overall_prescriptive_actions(ctx)
    interpretation = (
        "Execution is materially behind plan and backlog recovery should be treated as the main operating priority."
        if overall_rate < 0.35
        else "Execution is moving, but the portfolio still needs active backlog control and risk prioritization."
    )
    return [
        ("Context", [f"This summary reviews contracted versus received tax-return performance for snapshot {ctx.snapshot.as_of_date} and focuses on delivery execution, backlog exposure, and recovery priorities."]),
        ("Key Metrics Snapshot", [
            f"{_fmt_num(total_received)} of {_fmt_num(total_contracted)} contracted returns have been received, leaving {_fmt_num(total_pending)} outstanding at an overall completion rate of {_fmt_pct(overall_rate)}.",
            f"{_fmt_num(len(zero_clients))} clients have not started and {_fmt_num(len(critical_clients) + len(at_risk_clients))} clients are already in critical or at-risk bands.",
        ]),
        ("Performance Interpretation", [interpretation]),
        ("Key Insights", [
            f"The top five backlog accounts hold {_fmt_num(top_pending_total)} pending returns, concentrating {_fmt_pct(_share(top_pending_total, total_pending))} of the remaining workload into a small group of clients.",
            f"Business returns account for {_fmt_pct(_share(business_backlog, total_pending))} of all pending work, so backlog reduction depends heavily on business-document collection discipline.",
            f"Forecasting is currently summarized as: {predictive['heuristic_final_received']} received ({predictive['heuristic_final_rate']}).",
        ]),
        ("Risks and Implications", [
            f"Critical and at-risk clients currently expose {_fmt_num(sum(item.pending_total for item in [*critical_clients, *at_risk_clients, *zero_clients]))} pending returns to delay risk.",
            f"{_fmt_num(len(anomalies))} anomaly or data-quality signals were detected; unresolved exceptions can distort both prioritization and forecasting.",
            quality[0],
        ]),
        ("Priority Actions", actions[:3]),
        ("Bottom Line", [
            "The portfolio has enough contracted demand, but execution is lagging; leadership attention should stay on backlog conversion, zero-start accounts, and the highest-risk client clusters."
        ]),
    ]


def _status_fallback_framework(ctx: SnapshotReportContext) -> list[tuple[str, list[str]]]:
    runtime = ctx.runtime_payload or {}
    total_returns = int(runtime.get("total_returns") or 0)
    completed_returns = int(runtime.get("completed_returns") or 0)
    open_returns = int(runtime.get("open_returns") or 0)
    awaiting_answers = int(runtime.get("awaiting_answers") or 0)
    stale_items = runtime.get("stale_items") or []
    note_rows = runtime.get("note_rows") or []
    status_counts = runtime.get("status_counts") or []
    top_status = status_counts[0] if status_counts else None
    actions = _status_pipeline_action_lines(runtime)
    return [
        ("Context", [f"This summary reviews the operational return-processing pipeline for snapshot {ctx.snapshot.as_of_date}, with emphasis on queue health, blockers, stale work, and follow-up load."]),
        ("Key Metrics Snapshot", [
            f"{_fmt_num(completed_returns)} of {_fmt_num(total_returns)} tracked returns are complete, leaving {_fmt_num(open_returns)} items in the active queue.",
            f"{_fmt_num(awaiting_answers)} items are blocked in awaiting answers, {_fmt_num(len(stale_items))} items appear stale, and {_fmt_num(len(note_rows))} rows carry analyst or CPA notes.",
        ]),
        ("Performance Interpretation", [
            "This is a control-and-flow problem more than a growth problem; the key question is whether blocked and aged work is being cleared fast enough to prevent silent backlog accumulation."
        ]),
        ("Key Insights", [
            f"The largest current workflow bucket is {top_status.get('label') if top_status else 'Unknown'} with {_fmt_num(int(top_status.get('count') or 0)) if top_status else '0'} items." if top_status else "No dominant workflow bucket could be determined from the current runtime payload.",
            f"Open work remains material at {_fmt_num(open_returns)} items, which means queue discipline and follow-up cadence matter more than headline completion alone.",
        ]),
        ("Risks and Implications", [
            f"Blocked answers and stale items can turn into missed commitments because they mask delay until late in the workflow.",
            f"Rows with notes often represent exceptions or hidden friction; {_fmt_num(len(note_rows))} such rows already exist in this snapshot.",
        ]),
        ("Priority Actions", actions[:3]),
        ("Bottom Line", [
            "Pipeline control is the priority: clear blockers, drain stale work, and convert note-heavy items into explicit follow-up actions before the queue drifts further."
        ]),
    ]


def _product_fallback_framework(ctx: SnapshotReportContext) -> list[tuple[str, list[str]]]:
    runtime = ctx.runtime_payload or {}
    total_products = int(runtime.get("total_products") or 0)
    product_type_count = int(runtime.get("product_type_count") or 0)
    uom_count = int(runtime.get("uom_count") or 0)
    category_counts = runtime.get("category_counts") or []
    quality_gaps = runtime.get("quality_gaps") or []
    top_category = category_counts[0] if category_counts else None
    top_category_share = _share(int(top_category.get("count") or 0), total_products) if top_category else 0.0
    actions = _product_catalog_action_lines(runtime)
    return [
        ("Context", [f"This summary reviews product-master readiness for snapshot {ctx.snapshot.as_of_date}, focusing on catalog breadth, concentration, data quality, and standardization risk."]),
        ("Key Metrics Snapshot", [
            f"The catalog contains {_fmt_num(total_products)} products across {_fmt_num(product_type_count)} product types, {_fmt_num(int(runtime.get('category_count') or 0))} categories, and {_fmt_num(uom_count)} units of measure.",
            f"{_fmt_num(len(quality_gaps))} rows currently show explicit quality gaps that could weaken downstream reporting or automation.",
        ]),
        ("Performance Interpretation", [
            "The dataset is large enough to support reporting and controls, but its business value depends on classification consistency, category governance, and cleanup of quality exceptions."
        ]),
        ("Key Insights", [
            f"The top category is {top_category.get('label') if top_category else 'Unknown'} with {_fmt_num(int(top_category.get('count') or 0)) if top_category else '0'} products, representing {_fmt_pct(top_category_share)} of the catalog." if top_category else "No category concentration signal was available.",
            f"Unit-of-measure diversity is high at {_fmt_num(uom_count)} distinct values, which increases standardization and reporting complexity.",
        ]),
        ("Risks and Implications", [
            "Category concentration can create hidden dependency on a few product groups and can distort mix analysis if category governance is weak.",
            f"Rows with missing or placeholder values, including the {_fmt_num(len(quality_gaps))} identified quality gaps, reduce confidence in master-data-driven analytics.",
        ]),
        ("Priority Actions", actions[:3]),
        ("Bottom Line", [
            "The catalog has scale and usable structure, but governance quality now matters more than additional volume; standardization and targeted cleanup should come before broader analytical rollout."
        ]),
    ]


def _generic_fallback_framework(ctx: SnapshotReportContext) -> list[tuple[str, list[str]]]:
    schema_fields = (ctx.dashboard_config or {}).get("schema_fields") if isinstance(ctx.dashboard_config, dict) else []
    sheet_count = len(schema_fields or [])
    field_count = sum(len(section.get("fields") or []) for sheet in (schema_fields or []) for section in (sheet.get("sections") or []))
    return [
        ("Context", [f"This summary reviews workbook readiness for snapshot {ctx.snapshot.as_of_date}. The file has not yet been mapped to a mature business family, so the immediate goal is semantic interpretation and dashboard design readiness."]),
        ("Key Metrics Snapshot", [
            f"The workbook currently exposes {_fmt_num(sheet_count)} sheets and {_fmt_num(field_count)} profiled fields for semantic mapping and dashboard design.",
        ]),
        ("Performance Interpretation", [
            "This is not yet an executive decision summary; it is a readiness assessment. The main question is whether the workbook can be translated into stable business metrics without ambiguity."
        ]),
        ("Key Insights", [
            "The current value lies in understanding sheet intent, field meaning, and how sections could map into operational or management views.",
        ]),
        ("Risks and Implications", [
            "If headers and field semantics remain ambiguous, any generated dashboard or narrative will drift into generic wording and low-confidence recommendations.",
        ]),
        ("Priority Actions", [
            "Standardize field meanings and clarify which measures are KPIs, dimensions, exceptions, and operational notes.",
            "Confirm the target business use case before expecting a high-quality executive summary.",
        ]),
        ("Bottom Line", [
            "The workbook needs semantic grounding before it can support a strong executive brief; dashboard quality depends first on mapping meaning, not layout."
        ]),
    ]


def _legacy_dynamic_overall_summary_pdf(ctx: SnapshotReportContext) -> bytes:
    """
    Build overall summary PDF using dynamic blueprint-based structure.

    This function adapts the summary structure to the actual dashboard blueprint
    instead of using hardcoded sections.

    Args:
        ctx: Snapshot report context
        db: Database session

    Returns:
        PDF bytes
    """
    from app.dashboard_blueprints import ensure_schema_profile

    # Ensure we have a schema profile and blueprint structure
    profile = ensure_schema_profile(db, snapshot=ctx.snapshot)
    blueprint_structure = get_blueprint_structure_for_snapshot(db, snapshot=ctx.snapshot)

    if blueprint_structure is None:
        # Fallback to original function if no blueprint
        return build_overall_summary_pdf(ctx)

    # Generate blueprint-aware evidence
    blueprint_evidence = generate_blueprint_aware_evidence(
        ctx, blueprint_structure, report_type="overall"
    )
    masked_evidence, lookup = _mask_snapshot_evidence_if_needed(ctx, blueprint_evidence)

    # Get dynamic section keys from blueprint
    section_keys = get_report_section_keys(blueprint_structure, report_type="overall")

    # Create dynamic section overrides for better titles
    section_overrides = create_section_overrides(blueprint_structure)

    # Run agentic report generation with dynamic structure
    agentic_draft = _run_agentic_report_graph(
        mode="overall_report",
        report_style="executive",
        evidence=masked_evidence,
        required_section_keys=section_keys,
        entity_name=f"snapshot-{ctx.snapshot.id}",
        blueprint_structure=blueprint_structure,
    )

    if agentic_draft and lookup:
        agentic_draft = _unmask_agentic_draft(agentic_draft, lookup)

    # Build PDF with dynamic structure
    return _build_dynamic_overall_pdf(ctx, blueprint_structure, agentic_draft, section_overrides)


def build_client_summary_pdf_dynamic(ctx: SnapshotReportContext, client_external_id: str, db: Session) -> tuple[bytes, ClientMetrics]:
    """
    Build client summary PDF using dynamic blueprint-based structure.

    Args:
        ctx: Snapshot report context
        client_external_id: Client external ID
        db: Database session

    Returns:
        Tuple of (PDF bytes, ClientMetrics)
    """
    from app.dashboard_blueprints import ensure_schema_profile

    client = next((item for item in ctx.clients if item.client_external_id == client_external_id), None)
    if not client:
        raise ValueError("Client not found in snapshot")

    # Ensure we have a schema profile and blueprint structure
    profile = ensure_schema_profile(db, snapshot=ctx.snapshot)
    blueprint_structure = get_blueprint_structure_for_snapshot(db, snapshot=ctx.snapshot)

    if blueprint_structure is None:
        # Fallback to original function if no blueprint
        return build_client_summary_pdf(ctx, client_external_id)

    # Generate blueprint-aware evidence for this client
    client_evidence = generate_blueprint_aware_evidence(
        ctx, blueprint_structure, report_type="client", client=client
    )
    masked_evidence, lookup = _mask_snapshot_evidence_if_needed(ctx, client_evidence)

    # Get dynamic section keys from blueprint (client-focused)
    section_keys = get_report_section_keys(blueprint_structure, report_type="client")

    # Create dynamic section overrides
    section_overrides = create_section_overrides(blueprint_structure)

    # Run agentic report generation with dynamic structure
    agentic_draft = _run_agentic_report_graph(
        mode="client_report",
        report_style="executive",
        evidence=masked_evidence,
        required_section_keys=section_keys,
        entity_name=f"client-{client.client_external_id}-snapshot-{ctx.snapshot.id}",
        blueprint_structure=blueprint_structure,
    )

    if agentic_draft and lookup:
        agentic_draft = _unmask_agentic_draft(agentic_draft, lookup)

    # Build PDF with dynamic structure
    pdf_bytes = _build_dynamic_client_pdf(ctx, client, blueprint_structure, agentic_draft, section_overrides)
    return pdf_bytes, client


def _build_dynamic_overall_pdf(
    ctx: SnapshotReportContext,
    blueprint_structure: BlueprintStructure,
    agentic_draft: AgenticReportDraft | None,
    section_overrides: dict[str, str]
) -> bytes:
    """Build overall PDF with dynamic blueprint structure"""
    styles = _build_styles()

    story = [
        Spacer(1, 0.15 * inch),
        Paragraph("Executive Summary", styles["SectionTitle"]),
        Spacer(1, 0.1 * inch),
    ]

    # Add agentic content if available
    if agentic_draft:
        story.extend(_render_agentic_draft(agentic_draft, styles, section_overrides))
        story.append(Spacer(1, 0.15 * inch))

    # Add blueprint-specific KPI cards
    if blueprint_structure.kpi_cards:
        kpi_cards = _build_blueprint_kpi_cards(ctx, blueprint_structure, styles)
        story.extend(kpi_cards)
        story.append(Spacer(1, 0.18 * inch))

    # Build PDF
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=0.7 * inch,
        rightMargin=0.7 * inch,
        topMargin=1.4 * inch,
        bottomMargin=0.7 * inch,
        title=f"GKM Executive Summary - {blueprint_structure.title}",
    )
    frame = _build_page_frame(
        f"GKM Executive Summary - {blueprint_structure.title}",
        f"Snapshot {ctx.snapshot.as_of_date} • Source workbook: {ctx.snapshot.source_filename}",
    )
    doc.build(story, onFirstPage=frame, onLaterPages=frame)
    return buf.getvalue()


def _build_dynamic_client_pdf(
    ctx: SnapshotReportContext,
    client: ClientMetrics,
    blueprint_structure: BlueprintStructure,
    agentic_draft: AgenticReportDraft | None,
    section_overrides: dict[str, str]
) -> bytes:
    """Build client PDF with dynamic blueprint structure"""
    styles = _build_styles()

    story = [
        Spacer(1, 0.15 * inch),
        Paragraph("Client Summary", styles["SectionTitle"]),
        Paragraph(
            (
                f"This client report translates dashboard metrics for <b>{client.client_name}</b> "
                f"into an actionable summary. The report structure adapts to the dashboard blueprint "
                f"'{blueprint_structure.title}'."
            ),
            styles["BodyMuted"],
        ),
        Spacer(1, 0.18 * inch),
    ]

    # Add agentic content if available
    if agentic_draft:
        story.extend(_render_agentic_draft(agentic_draft, styles, section_overrides))
        story.append(Spacer(1, 0.15 * inch))

    # Add client-specific metrics
    client_metrics = _build_client_metrics_cards(ctx, client, blueprint_structure, styles)
    story.extend(client_metrics)

    # Build PDF
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=0.7 * inch,
        rightMargin=0.7 * inch,
        topMargin=1.4 * inch,
        bottomMargin=0.7 * inch,
        title=f"{client.client_name} Summary - {blueprint_structure.title}",
    )
    frame = _build_page_frame(
        f"{client.client_name} Summary - {blueprint_structure.title}",
        f"Snapshot {ctx.snapshot.as_of_date} • Client ID: {client.client_external_id} • Source: {ctx.snapshot.source_filename}",
    )
    doc.build(story, onFirstPage=frame, onLaterPages=frame)
    return buf.getvalue()


def _build_blueprint_kpi_cards(ctx: SnapshotReportContext, blueprint_structure: BlueprintStructure, styles) -> list:
    """Build KPI cards based on blueprint configuration"""
    cards = []

    for kpi in blueprint_structure.kpi_cards[:8]:  # Limit to 8 KPIs
        kpi_value = _get_kpi_value(ctx, kpi.key)
        if kpi_value is not None:
            cards.append(_metric_card(kpi.label, kpi_value["value"], kpi_value["meta"], BRAND, styles))

    if cards:
        return [_metric_grid(cards)]
    return []


def _build_client_metrics_cards(ctx: SnapshotReportContext, client: ClientMetrics, blueprint_structure: BlueprintStructure, styles) -> list:
    """Build client-specific metric cards based on blueprint"""
    cards = [
        _metric_card("Contracted", _fmt_num(client.contracted_total), f"{_fmt_num(client.contracted_ind)} individual • {_fmt_num(client.contracted_bus)} business", BRAND, styles),
        _metric_card("Received", _fmt_num(client.received_total), f"{_fmt_pct(client.receipt_rate)} receipt rate", SUCCESS, styles),
        _metric_card("Pending", _fmt_num(client.pending_total), "Returns still pending", WARNING, styles),
    ]

    return [_metric_grid(cards)]


def _get_kpi_value(ctx: SnapshotReportContext, kpi_key: str) -> dict[str, str] | None:
    """Get value and metadata for a specific KPI"""
    total_contracted = sum(item.contracted_total for item in ctx.clients)
    total_received = sum(item.received_total for item in ctx.clients)
    total_pending = sum(item.pending_total for item in ctx.clients)

    kpi_values = {
        "total_contracted": {"value": _fmt_num(total_contracted), "meta": "Total contracted returns in scope"},
        "total_received": {"value": _fmt_num(total_received), "meta": f"{_fmt_pct(total_received/total_contracted if total_contracted else 0)} receipt rate"},
        "total_pending": {"value": _fmt_num(total_pending), "meta": "Returns still pending"},
        "total_received_ind": {"value": _fmt_num(sum(item.received_ind for item in ctx.clients)), "meta": "Individual returns received"},
        "total_received_bus": {"value": _fmt_num(sum(item.received_bus for item in ctx.clients)), "meta": "Business returns received"},
        "zero_received_clients": {"value": _fmt_num(sum(1 for item in ctx.clients if item.contracted_total > 0 and item.received_total == 0)), "meta": "Clients with no receipts yet"},
        "over_delivered_clients": {"value": _fmt_num(sum(1 for item in ctx.clients if item.received_total > item.contracted_total)), "meta": "Clients above contracted scope"},
        "active_clients": {"value": _fmt_num(sum(1 for item in ctx.clients if item.contracted_total > 0 or item.received_total > 0)), "meta": "Clients with active workload"},
    }

    return kpi_values.get(kpi_key)


def _build_variance_overall_summary_pdf(ctx: SnapshotReportContext, gemini: GeminiRequestSettings | None = None) -> bytes:
    styles = _build_styles()
    total_contracted = sum(item.contracted_total for item in ctx.clients)
    total_received = sum(item.received_total for item in ctx.clients)
    total_pending = sum(item.pending_total for item in ctx.clients)
    overall_rate = (total_received / total_contracted) if total_contracted else 0.0
    zero_clients = [client for client in ctx.clients if client.contracted_total > 0 and client.received_total == 0]
    critical_clients = [client for client in ctx.clients if client.receipt_rate is not None and 0 < client.receipt_rate < 0.15]
    at_risk_clients = [client for client in ctx.clients if client.receipt_rate is not None and 0.15 <= client.receipt_rate < 0.35]
    ahead_clients = [client for client in ctx.clients if client.receipt_rate is not None and client.receipt_rate >= 0.6]
    top_pending_clients = sorted(ctx.clients, key=lambda item: item.pending_total, reverse=True)[:8]
    top_staff = ctx.staff[:8]
    predictive = _overall_predictive_summary(ctx)
    actions = _overall_prescriptive_actions(ctx)
    anomaly_rows = _overall_anomaly_rows(ctx)
    quality_suggestions = _overall_quality_suggestions(ctx)
    distribution_rows = _distribution_rows(ctx)
    benchmark_rows = _overall_benchmark_rows(ctx)
    mover_rows = _mover_rows(ctx)
    high_risk_pending = sum(client.pending_total for client in [*zero_clients, *critical_clients, *at_risk_clients])
    bundle = build_summary_reasoning_bundle(ctx, gemini=gemini)
    plan = bundle.plan
    reasoning_packet = bundle.packet
    masked_overall_evidence, overall_lookup = _mask_snapshot_evidence_if_needed(ctx, bundle.evidence)
    agentic_draft = _run_agentic_report_graph(
        mode=plan.mode,
        report_style="executive",
        evidence=masked_overall_evidence,
        required_section_keys=plan.required_section_keys,
        entity_name=plan.entity_name,
        reasoning_packet=reasoning_packet,
        gemini=gemini,
    )
    if agentic_draft:
        agentic_draft = _unmask_agentic_draft(agentic_draft, overall_lookup)
    analytics_plan = _build_analytics_agentic_plan(ctx)
    analytics_draft = _run_agentic_report_graph(
        mode=analytics_plan.mode,
        report_style="analytics",
        evidence=masked_overall_evidence,
        required_section_keys=analytics_plan.required_section_keys,
        entity_name=analytics_plan.entity_name,
        reasoning_packet=reasoning_packet,
        gemini=gemini,
    )
    if analytics_draft:
        analytics_draft = _unmask_agentic_draft(analytics_draft, overall_lookup)

    story = [
        Spacer(1, 0.15 * inch),
        Paragraph("Executive Summary", styles["SectionTitle"]),
        Spacer(1, 0.1 * inch),
    ]

    if agentic_draft:
        story.extend(
            _render_agentic_draft(
                agentic_draft,
                styles,
                plan.title_overrides,
            )
        )
        story.append(Spacer(1, 0.15 * inch))
    else:
        story.extend(_render_reasoning_packet_fallback(reasoning_packet, styles))
        story.append(Spacer(1, 0.08 * inch))

    story.extend(
        _render_agentic_draft(analytics_draft, styles, analytics_plan.title_overrides)
        if analytics_draft
        else _render_data_analysis_summary(reasoning_packet, styles)
    )
    story.append(Spacer(1, 0.12 * inch))

    story.extend([
        _metric_grid(
            [
                _metric_card("Total Contracted", _fmt_num(total_contracted), "Total contracted returns in scope", BRAND, styles),
                _metric_card("Received", _fmt_num(total_received), f"{_fmt_pct(overall_rate)} receipt rate", SUCCESS, styles),
                _metric_card("Outstanding", _fmt_num(total_pending), "Returns still pending", WARNING, styles),
                _metric_card("Active Clients", _fmt_num(sum(1 for item in ctx.clients if item.contracted_total > 0 or item.received_total > 0)), "Clients with active workload", INFO, styles),
                _metric_card("Zero Received", _fmt_num(len(zero_clients)), "Clients with no receipts yet", DANGER, styles),
                _metric_card("Critical + At Risk", _fmt_num(len(critical_clients) + len(at_risk_clients)), f"{_fmt_num(high_risk_pending)} pending returns exposed", DANGER, styles),
                _metric_card("Ahead", _fmt_num(len(ahead_clients)), "Clients already above 60%", SUCCESS, styles),
                _metric_card("Anomalies Flagged", _fmt_num(len(anomaly_rows)), "Operational or data-quality exceptions", WARNING, styles),
            ]
        ),
        _callout_row(
            _callout_box("Executive View", _executive_view_lines(ctx), BRAND, styles),
            _callout_box("Management Priorities", _priority_action_lines(ctx), WARNING, styles),
        ),
        Spacer(1, 0.18 * inch),
        PageBreak(),
        Paragraph("Delivery Outlook", styles["SectionTitle"]),
        Paragraph(
            "Short-term forecasting uses recent movement between snapshots when available. Season-close projection uses a risk-band heuristic so the report still produces foresight even when only one snapshot exists.",
            styles["BodyMuted"],
        ),
        Spacer(1, 0.08 * inch),
        _data_table(
            ["Signal", "Interpretation"],
            [
                ["Current intake pace", predictive["pace_daily"]],
                ["Projected receipts in next 14 days", f'{predictive["projected_14_day_received"]} ({predictive["projected_14_day_rate"]})'],
                ["Estimated time to clear backlog", predictive["backlog_days"]],
                ["Heuristic season-close outcome", f'{predictive["heuristic_final_received"]} received ({predictive["heuristic_final_rate"]})'],
            ],
            [2.5 * inch, 3.8 * inch],
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Recommended Management Actions", styles["SectionTitle"]),
        *_bullet_lines(actions, styles),
        Spacer(1, 0.18 * inch),
        Paragraph("Overview and Portfolio Diagnostics", styles["SectionTitle"]),
        _data_table(
            ["Metric", "Value", "Implication"],
            benchmark_rows,
            [1.75 * inch, 1.25 * inch, 4.0 * inch],
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Risk and Opportunity Highlights", styles["SectionTitle"]),
        _data_table(
            ["Client", "Pending", "Receipt Rate", "Risk", "Recommended Focus"],
            [
                [
                    client.client_name,
                    _fmt_num(client.pending_total),
                    _fmt_pct(client.receipt_rate),
                    _risk_label(client.receipt_rate),
                    "Immediate follow-up" if client in zero_clients or client in critical_clients else "Monitor",
                ]
                for client in top_pending_clients
            ] or [["No client data", "—", "—", "—", "—"]],
            [2.25 * inch, 0.75 * inch, 0.85 * inch, 0.95 * inch, 1.4 * inch],
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Receipt Rate Distribution", styles["SectionTitle"]),
        _data_table(
            ["Band", "Clients", "Contracted", "Received", "Pending", "Rate"],
            distribution_rows,
            [1.35 * inch, 0.7 * inch, 1.0 * inch, 1.0 * inch, 1.0 * inch, 0.85 * inch],
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Staff Load Signals", styles["SectionTitle"]),
        _data_table(
            ["Staff Member", "Type", "Received", "Individual", "Business"],
            [
                [member.name, member.staff_type, _fmt_num(member.received_total), _fmt_num(member.received_ind), _fmt_num(member.received_bus)]
                for member in top_staff
            ] or [["No staff data", "—", "—", "—", "—"]],
            [2.15 * inch, 1.0 * inch, 1.0 * inch, 1.0 * inch, 1.0 * inch],
        ),
        PageBreak(),
        Paragraph("Client Portfolio Review", styles["SectionTitle"]),
        Paragraph(
            "This section mirrors the client table view by identifying the largest movers and the accounts where momentum is strongest or weakest.",
            styles["BodyMuted"],
        ),
        Spacer(1, 0.08 * inch),
        _data_table(
            ["Client", "Contracted", "Received", "Δ Received", "Δ Rate", "Current Risk"],
            mover_rows or [["Need 2 snapshots", "—", "—", "—", "—", "—"]],
            [2.2 * inch, 0.8 * inch, 0.8 * inch, 0.9 * inch, 0.8 * inch, 1.0 * inch],
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Team Capacity Review", styles["SectionTitle"]),
        Paragraph(
            "Workload distribution can signal both capacity risk and hidden dependency on a small number of people.",
            styles["BodyMuted"],
        ),
        Spacer(1, 0.08 * inch),
        _data_table(
            ["Staff", "Type", "Received", "Portfolio Share", "Ind", "Bus", "Relative Load"],
            _staff_rows_with_share(ctx) or [["No staff data", "—", "—", "—", "—", "—", "—"]],
            [1.8 * inch, 0.9 * inch, 0.75 * inch, 1.0 * inch, 0.55 * inch, 0.55 * inch, 1.55 * inch],
        ),
    ])

    story.extend(
        [
            Spacer(1, 0.18 * inch),
            Paragraph("Exceptions and Data Review", styles["SectionTitle"]),
            Paragraph(
                "This is the anomaly layer of the report. It surfaces operational exceptions and source-data concerns that can change interpretation of the dashboard if left unresolved.",
                styles["BodyMuted"],
            ),
            Spacer(1, 0.08 * inch),
            _data_table(
                ["Flag", "Entity", "Impact", "Observation", "Recommended Action"],
                anomaly_rows or [["No anomalies", "Portfolio", "—", "No material anomaly detected in the selected snapshot.", "Continue monitoring."]],
                [1.15 * inch, 1.55 * inch, 0.8 * inch, 1.55 * inch, 2.05 * inch],
            ),
            Spacer(1, 0.18 * inch),
            Paragraph("Data Integrity Notes", styles["SectionTitle"]),
            *_bullet_lines(quality_suggestions, styles),
        ]
    )

    if critical_clients:
        story.extend(
            [
                PageBreak(),
                Paragraph("Critical Client Watchlist", styles["SectionTitle"]),
                Paragraph(
                    "These clients are currently under 15% receipt rate and deserve leadership visibility because they create the most near-term risk to delivery expectations.",
                    styles["BodyMuted"],
                ),
                Spacer(1, 0.08 * inch),
                _data_table(
                    ["Client", "Contracted", "Received", "Pending", "Rate"],
                    [
                        [client.client_name, _fmt_num(client.contracted_total), _fmt_num(client.received_total), _fmt_num(client.pending_total), _fmt_pct(client.receipt_rate)]
                        for client in critical_clients[:15]
                    ],
                    [2.6 * inch, 0.85 * inch, 0.85 * inch, 0.85 * inch, 0.9 * inch],
                ),
            ]
        )

    if agentic_draft:
        story.extend(_render_citation_appendix(bundle.evidence, styles))

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=0.7 * inch,
        rightMargin=0.7 * inch,
        topMargin=1.4 * inch,
        bottomMargin=0.7 * inch,
        title="GKM Executive Summary Report",
    )
    frame = _build_page_frame(
        "GKM Executive Summary",
        f"Snapshot {ctx.snapshot.as_of_date} • Source workbook: {ctx.snapshot.source_filename}",
    )
    doc.build(story, onFirstPage=frame, onLaterPages=frame)
    return buf.getvalue()


def _build_variance_analytics_summary_pdf(ctx: SnapshotReportContext, gemini: GeminiRequestSettings | None = None) -> bytes:
    styles = _build_styles()
    total_contracted = sum(item.contracted_total for item in ctx.clients)
    total_received = sum(item.received_total for item in ctx.clients)
    total_pending = sum(item.pending_total for item in ctx.clients)
    overall_rate = _share(total_received, total_contracted)
    zero_clients = [client for client in ctx.clients if client.contracted_total > 0 and client.received_total == 0]
    critical_clients = [client for client in ctx.clients if client.receipt_rate is not None and 0 < client.receipt_rate < 0.15]
    at_risk_clients = [client for client in ctx.clients if client.receipt_rate is not None and 0.15 <= client.receipt_rate < 0.35]
    top_pending_clients = sorted(ctx.clients, key=lambda item: item.pending_total, reverse=True)[:10]
    bundle = build_summary_reasoning_bundle(ctx, gemini=gemini)
    reasoning_packet = bundle.packet
    analytics_plan = _build_analytics_agentic_plan(ctx)
    masked_evidence, lookup = _mask_snapshot_evidence_if_needed(ctx, bundle.evidence)
    analytics_draft = _run_agentic_report_graph(
        mode=analytics_plan.mode,
        report_style="analytics",
        evidence=masked_evidence,
        required_section_keys=analytics_plan.required_section_keys,
        entity_name=analytics_plan.entity_name,
        reasoning_packet=reasoning_packet,
        gemini=gemini,
    )
    if analytics_draft and lookup:
        analytics_draft = _unmask_agentic_draft(analytics_draft, lookup)

    benchmark_rows = _overall_benchmark_rows(ctx)
    distribution_rows = _distribution_rows(ctx)
    mover_rows = _mover_rows(ctx)
    staff_rows = _staff_rows_with_share(ctx)
    anomaly_rows = _overall_anomaly_rows(ctx)
    predictive = _overall_predictive_summary(ctx)
    eda_scope_rows = _eda_scope_rows(reasoning_packet)
    verified_rows = _verified_finding_rows(reasoning_packet)

    def _rate_progress_note(value: float | None) -> str:
        if value is None:
            return "Uncontracted or no baseline available"
        if value == 0:
            return "No receipts yet"
        if value < 0.15:
            return "Very low progress"
        if value < 0.35:
            return "Below target"
        if value < 0.6:
            return "Moderate progress"
        return "Strong progress"

    backlog_rows = [
        [
            client.client_name,
            _fmt_num(client.pending_total),
            _fmt_pct(client.receipt_rate),
            _rate_progress_note(client.receipt_rate),
        ]
        for client in top_pending_clients
    ] or [["No client rows", "—", "—", "No client progress available"]]
    distribution_visual_rows = [
        [
            row[0],
            row[1],
            row[5],
            _rate_progress_note(None if row[5] == "—" else float(row[5].replace("%", "")) / 100.0),
        ]
        for row in distribution_rows
    ]
    forecast_rows = [
        ["Current intake pace", predictive["pace_daily"]],
        ["Projected receipts in next 14 days", f'{predictive["projected_14_day_received"]} ({predictive["projected_14_day_rate"]})'],
        ["Estimated time to clear backlog", predictive["backlog_days"]],
        ["Heuristic season-close outcome", f'{predictive["heuristic_final_received"]} received ({predictive["heuristic_final_rate"]})'],
    ]

    distribution_chart = _vertical_bar_chart(
        "Distribution Plot",
        "Receipt-rate bands across the client portfolio.",
        [row[0] for row in distribution_rows],
        [float(str(row[1]).replace(",", "")) for row in distribution_rows],
        accent=INFO,
    )
    concentration_chart = _vertical_bar_chart(
        "Top Backlog Clients",
        "Pending workload concentrated in the highest-backlog clients.",
        [row[0] for row in backlog_rows[:8]],
        [float(str(row[1]).replace(",", "")) if str(row[1]).replace(",", "").isdigit() else 0.0 for row in backlog_rows[:8]],
        accent=WARNING,
    )
    anomaly_counts: dict[str, int] = {}
    for row in anomaly_rows:
        anomaly_counts[row[0]] = anomaly_counts.get(row[0], 0) + 1
    anomaly_chart = _vertical_bar_chart(
        "Anomaly Panel",
        "Material operational exceptions and data-quality flags detected in the snapshot.",
        list(anomaly_counts.keys()) or ["No anomalies"],
        [float(value) for value in anomaly_counts.values()] or [0.0],
        accent=DANGER,
    )
    trend_labels, received_series, rate_series = _variance_trend_chart_data(ctx)
    trend_chart = _line_chart(
        "Trend Chart",
        "Observed and projected receipt volume across the available reporting horizon.",
        trend_labels,
        received_series,
        accent=SUCCESS,
    )
    forecast_chart = _vertical_bar_chart(
        "Forecast Chart",
        "Receipt-rate outlook from the deterministic forecast layer.",
        trend_labels,
        rate_series,
        accent=BRAND,
        percent=True,
    )
    staff_chart = _vertical_bar_chart(
        "Staff Load Distribution",
        "Received workload concentration across the most loaded staff members.",
        [row[0] for row in staff_rows[:8]] or ["No staff"],
        [float(str(row[2]).replace(",", "")) if str(row[2]).replace(",", "").isdigit() else 0.0 for row in staff_rows[:8]] or [0.0],
        accent=BRAND,
    )
    chart_specs = _build_variance_chart_specs(
        ctx,
        reasoning_packet,
        bundle.evidence,
        distribution_rows,
        backlog_rows,
        anomaly_rows,
        staff_rows,
        trend_labels,
        received_series,
        rate_series,
    )
    chart_evidence = {
        key: _sanitized_evidence_items([item for item in bundle.evidence if item.id in spec.evidence_ids] or bundle.evidence[:4])
        for key, spec in chart_specs.items()
    }
    chart_interpretations = {
        key: _run_chart_interpretation_graph(spec, chart_evidence[key], gemini=gemini)
        for key, spec in chart_specs.items()
    }
    chart_fallbacks = {
        "distribution": "The chart shows how the portfolio is spread across receipt-rate bands; the heavier bars in low-conversion cohorts indicate where backlog pressure is structurally concentrated.",
        "concentration": "The backlog is not evenly spread. A small client cohort carries a disproportionate share of pending work, so escalation should stay targeted rather than broad-based.",
        "trend": "The trend chart compares current receipt volume with the deterministic planning horizon. With limited historical snapshots, it should be read as directional workload movement rather than a statistical forecast.",
        "forecast": "The forecast chart shows receipt-rate direction from the deterministic forecast layer. It is suitable for planning discussions, but not as a commitment-level prediction.",
        "anomaly": "The anomaly panel highlights exception density, not just workload size. Even low absolute counts matter if the same issue type repeats across critical rows.",
        "staff_load": "The staff-load chart shows whether throughput is balanced or concentrated. If a small number of bars dominate, the operating model carries dependency risk.",
    }

    story = [
        Spacer(1, 0.15 * inch),
        Paragraph("Analytics Summary", styles["SectionTitle"]),
        Paragraph(
            "This report is EDA-first. It leads with charted analytical outputs, promoted findings, and quality checks before any analyst narrative.",
            styles["BodyMuted"],
        ),
        Spacer(1, 0.1 * inch),
        _metric_grid(
            [
                _metric_card("Contracted", _fmt_num(total_contracted), "Contracted workload in the preserved client table", BRAND, styles),
                _metric_card("Received", _fmt_num(total_received), "Delivered workload captured in the same source", SUCCESS, styles),
                _metric_card("Pending", _fmt_num(total_pending), "Backlog still unresolved in the current snapshot", WARNING, styles),
                _metric_card("Receipt Rate", _fmt_pct(overall_rate), "Weighted portfolio conversion rate", INFO, styles),
                _metric_card("Zero Start", _fmt_num(len(zero_clients)), "Clients with contracted work and no receipts", DANGER, styles),
                _metric_card("Critical + At Risk", _fmt_num(len(critical_clients) + len(at_risk_clients)), "Clients below the 35% receipt threshold", DANGER, styles),
            ]
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Analytical Findings", styles["SectionTitle"]),
        _data_table(
            ["Finding", "What it shows", "Why it matters", "Confidence"],
            _user_friendly_finding_rows(reasoning_packet),
            [1.35 * inch, 2.35 * inch, 2.35 * inch, 0.85 * inch],
            styles,
        ),
        PageBreak(),
        Paragraph("Distribution Analysis", styles["SectionTitle"]),
        distribution_chart,
        Spacer(1, 0.08 * inch),
        _render_chart_interpretation_block(chart_interpretations.get("distribution"), chart_fallbacks["distribution"], styles),
        Spacer(1, 0.08 * inch),
        _data_table(["Band", "Clients", "Rate", "Visual"], distribution_visual_rows or [["No bands", "0", "—", "—"]], [2.0 * inch, 1.0 * inch, 1.0 * inch, 2.4 * inch], styles),
        Spacer(1, 0.18 * inch),
        Paragraph("Concentration Analysis", styles["SectionTitle"]),
        concentration_chart,
        Spacer(1, 0.08 * inch),
        _render_chart_interpretation_block(chart_interpretations.get("concentration"), chart_fallbacks["concentration"], styles),
        Spacer(1, 0.08 * inch),
        _data_table(["Client", "Pending Work", "Receipt Rate", "Receipt Progress"], backlog_rows, [2.6 * inch, 1.0 * inch, 1.0 * inch, 1.8 * inch], styles),
        PageBreak(),
        Paragraph("Trend and Forecast Analysis", styles["SectionTitle"]),
        trend_chart,
        Spacer(1, 0.1 * inch),
        _render_chart_interpretation_block(chart_interpretations.get("trend"), chart_fallbacks["trend"], styles),
        Spacer(1, 0.1 * inch),
        forecast_chart,
        Spacer(1, 0.08 * inch),
        _render_chart_interpretation_block(chart_interpretations.get("forecast"), chart_fallbacks["forecast"], styles),
        Spacer(1, 0.08 * inch),
        _data_table(["Signal", "Interpretation"], forecast_rows, [2.5 * inch, 3.8 * inch], styles),
        Spacer(1, 0.18 * inch),
        Paragraph("Movement Since Prior Snapshot", styles["SectionTitle"]),
        _data_table(
            ["Client", "Contracted", "Received", "Delta Received", "Delta Rate", "Risk"],
            mover_rows or [["Need 2 snapshots", "—", "—", "—", "—", "—"]],
            [2.1 * inch, 0.85 * inch, 0.85 * inch, 0.95 * inch, 0.9 * inch, 0.85 * inch],
            styles,
        ),
        PageBreak(),
        Paragraph("Anomaly and Load Panels", styles["SectionTitle"]),
        anomaly_chart,
        Spacer(1, 0.1 * inch),
        _render_chart_interpretation_block(chart_interpretations.get("anomaly"), chart_fallbacks["anomaly"], styles),
        Spacer(1, 0.1 * inch),
        staff_chart,
        Spacer(1, 0.08 * inch),
        _render_chart_interpretation_block(chart_interpretations.get("staff_load"), chart_fallbacks["staff_load"], styles),
        Spacer(1, 0.08 * inch),
        Paragraph("Portfolio Diagnostics", styles["SectionTitle"]),
        _data_table(["Metric", "Value", "Interpretation"], benchmark_rows, [1.8 * inch, 1.2 * inch, 3.95 * inch], styles),
        Spacer(1, 0.18 * inch),
        Paragraph("Staff Load Review", styles["SectionTitle"]),
        _data_table(
            ["Staff", "Type", "Received", "Share", "Mix"],
            [[row[0], row[1], row[2], row[3], row[6]] for row in staff_rows] or [["No staff rows", "—", "—", "—", "—"]],
            [2.0 * inch, 1.0 * inch, 1.0 * inch, 0.9 * inch, 2.0 * inch],
            styles,
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Exceptions and Data Quality", styles["SectionTitle"]),
        _data_table(
            ["Category", "Entity", "Impact", "Observation"],
            [[row[0], row[1], row[2], row[3]] for row in anomaly_rows[:10]] or [["No exception flags", "—", "—", "—"]],
            [1.4 * inch, 2.0 * inch, 1.0 * inch, 2.6 * inch],
            styles,
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Coverage and Limitations", styles["SectionTitle"]),
    ]

    for line in reasoning_packet.limitations[:4]:
        story.append(
            Paragraph(
                f"• {line.text} <font color='#5b5e70'>{_citation_label(line.citations)}</font>",
                styles["BodyMuted"],
            )
        )

    if analytics_draft:
        story.extend(
            [
                Spacer(1, 0.18 * inch),
                Paragraph("Analyst Interpretation", styles["SectionTitle"]),
            ]
        )
        story.extend(_render_agentic_draft(analytics_draft, styles, analytics_plan.title_overrides))

    if analytics_draft:
        story.extend(_render_citation_appendix(bundle.evidence, styles))

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=0.7 * inch,
        rightMargin=0.7 * inch,
        topMargin=1.4 * inch,
        bottomMargin=0.7 * inch,
        title=f"{_dashboard_title(ctx)} Analytics Summary",
    )
    frame = _build_page_frame(
        f"{_dashboard_title(ctx)} Analytics Summary",
        f"Snapshot {ctx.snapshot.as_of_date} • Source workbook: {ctx.snapshot.source_filename}",
    )
    doc.build(story, onFirstPage=frame, onLaterPages=frame)
    return buf.getvalue()


def _build_variance_client_summary_pdf(
    ctx: SnapshotReportContext,
    client_external_id: str,
    gemini: GeminiRequestSettings | None = None,
) -> tuple[bytes, ClientMetrics]:
    client = next((item for item in ctx.clients if item.client_external_id == client_external_id), None)
    if not client:
        raise ValueError("Client not found in snapshot")

    styles = _build_styles()
    predictive = _client_predictive_summary(ctx, client)
    actions = _client_prescriptive_actions(ctx, client)
    quality_suggestions = _client_quality_suggestions(ctx, client)
    risk = _risk_label(client.receipt_rate)
    volume_rank = 1 + sum(1 for item in ctx.clients if item.contracted_total > client.contracted_total)
    rate_rank = 1 + sum(1 for item in ctx.clients if (item.receipt_rate or 0) > (client.receipt_rate or 0))
    total_pending = sum(item.pending_total for item in ctx.clients)
    pending_share = (client.pending_total / total_pending) if total_pending else 0.0
    previous = _client_previous(ctx, client)
    delta_received = _client_delta_received(ctx, client)
    delta_rate = _client_delta_rate(ctx, client)
    portfolio_avg_rate = _avg_rate(ctx.clients)
    on_track_gap = max(round(client.contracted_total * 0.35) - client.received_total, 0) if client.contracted_total > 0 else 0
    ahead_gap = max(round(client.contracted_total * 0.60) - client.received_total, 0) if client.contracted_total > 0 else 0
    client_evidence = _build_client_evidence(ctx, client)
    reasoning_packet = build_client_summary_reasoning_packet(ctx, client)
    masked_client_evidence, client_lookup = _mask_snapshot_evidence_if_needed(ctx, client_evidence)
    agentic_draft = _run_agentic_report_graph(
        mode="client_report",
        report_style="executive",
        evidence=masked_client_evidence,
        required_section_keys=[
            "client_overview",
            "service_mix",
            "comparative_position",
            "predictive_analytics",
            "risk_and_anomaly_signals",
            "quality_assessment",
        ],
        entity_name=f"client-{client.client_external_id}-snapshot-{ctx.snapshot.id}",
        reasoning_packet=reasoning_packet,
        gemini=gemini,
    )
    if agentic_draft:
        agentic_draft = _unmask_agentic_draft(agentic_draft, client_lookup)

    story = [
        Spacer(1, 0.15 * inch),
        Paragraph("Client Summary", styles["SectionTitle"]),
        Paragraph(
            (
                f"This client report translates dashboard metrics for <b>{client.client_name}</b> into an actionable summary. "
                "It covers current performance, forecasted delivery, anomalies, data quality considerations, and recommended next actions."
            ),
            styles["BodyMuted"],
        ),
        Spacer(1, 0.18 * inch),
    ]

    if agentic_draft:
        story.extend(
            _render_agentic_draft(
                agentic_draft,
                styles,
                {
                    "client_overview": "Client Overview",
                    "service_mix": "Service Mix Review",
                    "comparative_position": "Portfolio Position",
                    "predictive_analytics": "Delivery Outlook",
                    "risk_and_anomaly_signals": "Risk and Exceptions Review",
                    "quality_assessment": "Data Integrity Review",
                },
            )
        )
        story.append(Spacer(1, 0.15 * inch))
    else:
        story.extend(_render_reasoning_packet_fallback(reasoning_packet, styles))
        story.append(Spacer(1, 0.08 * inch))

    story.extend([
        _metric_grid(
            [
                _metric_card("Contracted", _fmt_num(client.contracted_total), f"{_fmt_num(client.contracted_ind)} individual • {_fmt_num(client.contracted_bus)} business", BRAND, styles),
                _metric_card("Received", _fmt_num(client.received_total), f"{_fmt_pct(client.receipt_rate)} receipt rate", SUCCESS, styles),
                _metric_card("Pending", _fmt_num(client.pending_total), f"{pending_share * 100:.1f}% of all pending workload", WARNING, styles),
                _metric_card("Current Risk", risk, f"Client ID: {client.client_external_id}", DANGER if risk in {'Critical', 'Not Started'} else INFO, styles),
                _metric_card("Change vs Prior", f"{delta_received:+,}" if delta_received is not None else "—", f"{(delta_rate or 0.0) * 100:+.1f} pts in receipt rate" if delta_rate is not None else "Need 2+ snapshots", BRAND, styles),
                _metric_card("Portfolio Standing", f"#{volume_rank} volume / #{rate_rank} rate", f"Portfolio average rate is {_fmt_pct(portfolio_avg_rate)}", INFO, styles),
            ]
        ),
        Paragraph("Overview and Service Mix", styles["SectionTitle"]),
        _data_table(
            ["Dimension", "Value"],
            [
                ["Client type", client.client_type],
                ["Individual progress", f"{_fmt_num(client.received_ind)} of {_fmt_num(client.contracted_ind)}"],
                ["Business progress", f"{_fmt_num(client.received_bus)} of {_fmt_num(client.contracted_bus)}"],
                ["Pending to reach On Track (35%)", _fmt_num(on_track_gap)],
                ["Pending to reach Ahead (60%)", _fmt_num(ahead_gap)],
                ["Prior snapshot comparison", f"{_fmt_num(previous.received_total)} received previously" if previous else "Not available"],
            ],
            [2.3 * inch, 4.0 * inch],
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Delivery Outlook", styles["SectionTitle"]),
        _data_table(
            ["Signal", "Interpretation"],
            [
                ["Current intake pace", predictive["pace_daily"]],
                ["Projected receipts in next 14 days", f'{predictive["projected_14_day_received"]} ({predictive["projected_14_day_rate"]})'],
                ["Estimated time to clear backlog", predictive["backlog_days"]],
                ["Heuristic season-close outcome", f'{predictive["heuristic_final_received"]} received ({predictive["heuristic_final_rate"]})'],
            ],
            [2.5 * inch, 3.8 * inch],
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Recommended Actions", styles["SectionTitle"]),
        *_bullet_lines(actions, styles),
        Spacer(1, 0.18 * inch),
        Paragraph("Portfolio Position", styles["SectionTitle"]),
        _data_table(
            ["Measure", "Value"],
            [
                ["Volume rank", f"#{volume_rank} by contracted volume"],
                ["Receipt rate rank", f"#{rate_rank} by completion rate"],
                ["Individual progress", f"{_fmt_num(client.received_ind)} of {_fmt_num(client.contracted_ind)}"],
                ["Business progress", f"{_fmt_num(client.received_bus)} of {_fmt_num(client.contracted_bus)}"],
                ["Gap versus portfolio average", f"{((client.receipt_rate or 0.0) - portfolio_avg_rate) * 100:+.1f} pts"],
            ],
            [2.2 * inch, 4.1 * inch],
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Risk and Exceptions Review", styles["SectionTitle"]),
        _data_table(
            ["Signal", "Observation"],
            [
                ["Current risk band", risk],
                ["Row consistency", "Mismatch detected" if _has_total_inconsistency(client) else "Totals reconcile"],
                ["Contract coverage", "Uncontracted receipts present" if client.contracted_total == 0 and client.received_total > 0 else "In scope"],
                ["Backlog movement", "No movement since prior snapshot" if previous and delta_received == 0 and client.pending_total > 0 else "Movement observed or no prior baseline"],
                ["Scope variance", "Received exceeds contracted" if client.received_total > client.contracted_total and client.contracted_total > 0 else "Within contracted range"],
            ],
            [2.2 * inch, 4.1 * inch],
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Data Integrity Notes", styles["SectionTitle"]),
        *_bullet_lines(quality_suggestions, styles),
    ])

    if agentic_draft:
        story.extend(_render_citation_appendix(client_evidence, styles))

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=0.7 * inch,
        rightMargin=0.7 * inch,
        topMargin=1.4 * inch,
        bottomMargin=0.7 * inch,
        title=f"{client.client_name} Summary Report",
    )
    frame = _build_page_frame(
        f"{client.client_name} Summary",
        f"Snapshot {ctx.snapshot.as_of_date} • Client ID: {client.client_external_id} • Source: {ctx.snapshot.source_filename}",
    )
    doc.build(story, onFirstPage=frame, onLaterPages=frame)
    return buf.getvalue(), client


def _dashboard_family(ctx: SnapshotReportContext) -> str:
    if isinstance(ctx.dashboard_config, dict):
        return str(ctx.dashboard_config.get("dashboard_family") or "").strip() or "variance_dashboard"
    if ctx.clients:
        return "variance_dashboard"
    return "generic_review_dashboard"


def _adaptive_generic_dashboard_title(ctx: SnapshotReportContext) -> str:
    runtime = ctx.runtime_payload or {}
    comparison_groups = list(runtime.get("comparison_groups") or [])
    if comparison_groups:
        series_names = sorted(
            {
                str(item.get("series") or "").upper()
                for group in comparison_groups
                for item in (group.get("series_totals") or [])
                if str(item.get("series") or "").strip()
            }
        )
        if {"TC", "BC"}.issubset(set(series_names)):
            return "TC vs BC Portfolio Distribution Overview"
        if series_names:
            return f"{' vs '.join(series_names[:2])} Distribution Overview"
        return "Multi-Period Distribution Overview"
    stem = str(ctx.snapshot.source_filename or "Adaptive Workbook").rsplit(".", 1)[0].replace("_", " ").replace("-", " ").strip()
    return stem.title() or "Adaptive Workbook Summary"


def _dashboard_title(ctx: SnapshotReportContext) -> str:
    if _generic_runtime_ready(ctx):
        return _adaptive_generic_dashboard_title(ctx)
    if isinstance(ctx.dashboard_config, dict):
        title = str(ctx.dashboard_config.get("title") or "").strip()
        if title:
            return title
    return "Adaptive Dashboard Summary"


def _dashboard_subtitle(ctx: SnapshotReportContext) -> str:
    if isinstance(ctx.dashboard_config, dict):
        subtitle = str(ctx.dashboard_config.get("subtitle") or "").strip()
        if subtitle:
            return subtitle
    return "Summary generated from the approved dashboard configuration and source workbook profile."


def _dashboard_tab_labels(ctx: SnapshotReportContext) -> list[str]:
    if not isinstance(ctx.dashboard_config, dict):
        return []
    tabs = ctx.dashboard_config.get("tabs")
    if not isinstance(tabs, list):
        return []
    labels: list[str] = []
    for tab in tabs:
        if isinstance(tab, dict):
            label = str(tab.get("label") or tab.get("key") or "").strip()
            if label:
                labels.append(label)
    return labels


def _status_pipeline_action_lines(runtime: dict[str, Any]) -> list[str]:
    total_returns = int(runtime.get("total_returns") or 0)
    open_returns = int(runtime.get("open_returns") or 0)
    awaiting_answers = int(runtime.get("awaiting_answers") or 0)
    stale_items = runtime.get("stale_items") or []
    note_rows = runtime.get("note_rows") or []
    actions: list[str] = []
    if open_returns:
        actions.append(
            f"Keep the operating focus on the open queue first. {open_returns} of {total_returns or open_returns} returns still require movement through the pipeline."
        )
    if awaiting_answers:
        actions.append(
            f"Escalate the awaiting-answers queue with owners and dates. {awaiting_answers} items are currently blocked on external response."
        )
    if stale_items:
        actions.append(
            f"Run a stale-item sweep on the {len(stale_items)} aged returns before they turn into silent backlog."
        )
    if note_rows:
        actions.append(
            f"Review operational notes systematically. {len(note_rows)} rows contain analyst or CPA notes that may explain pipeline friction."
        )
    if not actions:
        actions.append("Queue health is broadly stable. Maintain daily monitoring on stage movement and completion throughput.")
    return actions[:4]


def _build_status_pipeline_summary_pdf(ctx: SnapshotReportContext, gemini: GeminiRequestSettings | None = None) -> bytes:
    styles = _build_styles()
    runtime = ctx.runtime_payload or {}
    total_returns = int(runtime.get("total_returns") or 0)
    completed_returns = int(runtime.get("completed_returns") or 0)
    open_returns = int(runtime.get("open_returns") or 0)
    awaiting_answers = int(runtime.get("awaiting_answers") or 0)
    under_review = int(runtime.get("under_review") or 0)
    in_process = int(runtime.get("in_process") or 0)
    stale_items = runtime.get("stale_items") or []
    note_rows = runtime.get("note_rows") or []
    status_counts = runtime.get("status_counts") or []
    open_queue = runtime.get("open_queue") or []
    completion_rate = _share(completed_returns, total_returns)
    bundle = build_summary_reasoning_bundle(ctx, gemini=gemini)
    plan = bundle.plan
    reasoning_packet = bundle.packet
    masked_evidence, lookup = _mask_snapshot_evidence_if_needed(ctx, bundle.evidence)
    agentic_draft = _run_agentic_report_graph(
        mode=plan.mode,
        report_style="executive",
        evidence=masked_evidence,
        required_section_keys=plan.required_section_keys,
        entity_name=plan.entity_name,
        reasoning_packet=reasoning_packet,
        gemini=gemini,
    )
    if agentic_draft and lookup:
        agentic_draft = _unmask_agentic_draft(agentic_draft, lookup)
    analytics_plan = _build_analytics_agentic_plan(ctx)
    analytics_draft = _run_agentic_report_graph(
        mode=analytics_plan.mode,
        report_style="analytics",
        evidence=masked_evidence,
        required_section_keys=analytics_plan.required_section_keys,
        entity_name=analytics_plan.entity_name,
        reasoning_packet=reasoning_packet,
        gemini=gemini,
    )
    if analytics_draft and lookup:
        analytics_draft = _unmask_agentic_draft(analytics_draft, lookup)

    story = [
        Spacer(1, 0.15 * inch),
        Paragraph("Executive Summary", styles["SectionTitle"]),
        Spacer(1, 0.1 * inch),
    ]

    if agentic_draft:
        story.extend(_render_agentic_draft(agentic_draft, styles, plan.title_overrides))
        story.append(Spacer(1, 0.15 * inch))
    else:
        story.extend(_render_reasoning_packet_fallback(reasoning_packet, styles))
        story.append(Spacer(1, 0.08 * inch))

    story.extend(
        _render_agentic_draft(analytics_draft, styles, analytics_plan.title_overrides)
        if analytics_draft
        else _render_data_analysis_summary(reasoning_packet, styles)
    )
    story.append(Spacer(1, 0.12 * inch))

    story.extend([
        _metric_grid(
            [
                _metric_card("Total Returns", _fmt_num(total_returns), "Rows currently tracked in the pipeline", BRAND, styles),
                _metric_card("Completed", _fmt_num(completed_returns), f"{_fmt_pct(completion_rate)} completion rate", SUCCESS, styles),
                _metric_card("Open Queue", _fmt_num(open_returns), "Returns still requiring action", WARNING, styles),
                _metric_card("Awaiting Answers", _fmt_num(awaiting_answers), "Externally blocked items", DANGER, styles),
                _metric_card("Under Review", _fmt_num(under_review), "Quality or review-stage workload", INFO, styles),
                _metric_card("In Process", _fmt_num(in_process), "Actively moving items", INFO, styles),
                _metric_card("Stale Items", _fmt_num(len(stale_items)), "Open items that appear aged", WARNING, styles),
                _metric_card("Rows With Notes", _fmt_num(len(note_rows)), "Items carrying analyst or CPA notes", BRAND, styles),
            ]
        ),
        _callout_row(
            _callout_box(
                "Pipeline Story",
                [
                    f"{_fmt_num(completed_returns)} of {_fmt_num(total_returns)} tracked returns are complete, leaving {_fmt_num(open_returns)} still active in the queue.",
                    f"The current completion rate is {_fmt_pct(completion_rate)}, so this dashboard should be read as an operating-control view rather than a portfolio-variance report.",
                    f"{_fmt_num(awaiting_answers)} items are blocked in awaiting answers and {len(stale_items)} appear stale enough to need active follow-up.",
                ],
                BRAND,
                styles,
            ),
            _callout_box("Recommended Management Actions", _status_pipeline_action_lines(runtime), WARNING, styles),
        ),
        Spacer(1, 0.18 * inch),
        PageBreak(),
        Paragraph("Status Distribution", styles["SectionTitle"]),
        _data_table(
            ["Status", "Count"],
            [[str(item.get("label") or "Unknown"), _fmt_num(int(item.get("count") or 0))] for item in status_counts[:12]]
            or [["No status data", "0"]],
            [4.6 * inch, 1.7 * inch],
            styles,
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Open Queue Review", styles["SectionTitle"]),
        _data_table(
            ["Tax Payer", "Return Code", "Status", "Type", "Age (days)", "Notes"],
            [
                [
                    str(row.get("tax_payer_name") or "—"),
                    str(row.get("return_code") or "—"),
                    str(row.get("return_status") or "—"),
                    str(row.get("return_type") or "—"),
                    _fmt_num(int(row.get("age_days") or 0)) if row.get("age_days") is not None else "—",
                    "Yes" if (row.get("cpa_notes") or row.get("gkm_notes")) else "No",
                ]
                for row in open_queue[:14]
            ] or [["No open queue rows", "—", "—", "—", "—", "—"]],
            [1.7 * inch, 1.05 * inch, 1.15 * inch, 0.8 * inch, 0.7 * inch, 0.7 * inch],
            styles,
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Notes and Escalation Signals", styles["SectionTitle"]),
        _data_table(
            ["Tax Payer", "Return Code", "CPA Notes", "GKM Notes"],
            [
                [
                    str(row.get("tax_payer_name") or "—"),
                    str(row.get("return_code") or "—"),
                    str(row.get("cpa_notes") or "—"),
                    str(row.get("gkm_notes") or "—"),
                ]
                for row in note_rows[:12]
            ] or [["No note-driven follow-up items", "—", "—", "—"]],
            [1.7 * inch, 1.0 * inch, 1.8 * inch, 1.8 * inch],
            styles,
        ),
    ])

    if agentic_draft:
        story.extend(_render_citation_appendix(bundle.evidence, styles))

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=0.7 * inch,
        rightMargin=0.7 * inch,
        topMargin=1.4 * inch,
        bottomMargin=0.7 * inch,
        title=f"{_dashboard_title(ctx)} Summary",
    )
    frame = _build_page_frame(
        _dashboard_title(ctx),
        f"Snapshot {ctx.snapshot.as_of_date} • Source workbook: {ctx.snapshot.source_filename}",
    )
    doc.build(story, onFirstPage=frame, onLaterPages=frame)
    return buf.getvalue()


def _product_catalog_action_lines(runtime: dict[str, Any]) -> list[str]:
    quality_gaps = runtime.get("quality_gaps") or []
    category_counts = runtime.get("category_counts") or []
    actions: list[str] = []
    if quality_gaps:
        actions.append(
            f"Clean the {len(quality_gaps)} products with missing key attributes before using this catalog for downstream analytics or operational automation."
        )
    if category_counts:
        top = category_counts[0]
        actions.append(
            f"Review category concentration around {top.get('label') or 'the top category'}, which currently contains {_fmt_num(int(top.get('count') or 0))} products."
        )
    actions.append("Use the dashboard as a master-data control view: completeness, category spread, and attribute consistency matter more than trend forecasting here.")
    return actions[:4]


def _build_product_catalog_summary_pdf(ctx: SnapshotReportContext, gemini: GeminiRequestSettings | None = None) -> bytes:
    styles = _build_styles()
    runtime = ctx.runtime_payload or {}
    total_products = int(runtime.get("total_products") or 0)
    product_type_count = int(runtime.get("product_type_count") or 0)
    uom_count = int(runtime.get("uom_count") or 0)
    category_count = int(runtime.get("category_count") or 0)
    product_type_counts = runtime.get("product_type_counts") or []
    uom_counts = runtime.get("uom_counts") or []
    category_counts = runtime.get("category_counts") or []
    quality_gaps = runtime.get("quality_gaps") or []
    bundle = build_summary_reasoning_bundle(ctx, gemini=gemini)
    plan = bundle.plan
    reasoning_packet = bundle.packet
    masked_evidence, lookup = _mask_snapshot_evidence_if_needed(ctx, bundle.evidence)
    agentic_draft = _run_agentic_report_graph(
        mode=plan.mode,
        report_style="executive",
        evidence=masked_evidence,
        required_section_keys=plan.required_section_keys,
        entity_name=plan.entity_name,
        reasoning_packet=reasoning_packet,
        gemini=gemini,
    )
    if agentic_draft and lookup:
        agentic_draft = _unmask_agentic_draft(agentic_draft, lookup)
    analytics_plan = _build_analytics_agentic_plan(ctx)
    analytics_draft = _run_agentic_report_graph(
        mode=analytics_plan.mode,
        report_style="analytics",
        evidence=masked_evidence,
        required_section_keys=analytics_plan.required_section_keys,
        entity_name=analytics_plan.entity_name,
        reasoning_packet=reasoning_packet,
        gemini=gemini,
    )
    if analytics_draft and lookup:
        analytics_draft = _unmask_agentic_draft(analytics_draft, lookup)

    story = [
        Spacer(1, 0.15 * inch),
        Paragraph("Executive Summary", styles["SectionTitle"]),
        Spacer(1, 0.1 * inch),
    ]

    if agentic_draft:
        story.extend(_render_agentic_draft(agentic_draft, styles, plan.title_overrides))
        story.append(Spacer(1, 0.15 * inch))
    else:
        story.extend(_render_reasoning_packet_fallback(reasoning_packet, styles))
        story.append(Spacer(1, 0.08 * inch))

    story.extend(
        _render_agentic_draft(analytics_draft, styles, analytics_plan.title_overrides)
        if analytics_draft
        else _render_data_analysis_summary(reasoning_packet, styles)
    )
    story.append(Spacer(1, 0.12 * inch))

    story.extend([
        _metric_grid(
            [
                _metric_card("Products", _fmt_num(total_products), "Catalog rows profiled from the source workbook", BRAND, styles),
                _metric_card("Product Types", _fmt_num(product_type_count), "Distinct product-type groupings", INFO, styles),
                _metric_card("Units of Measure", _fmt_num(uom_count), "Distinct base UoM values", INFO, styles),
                _metric_card("Categories", _fmt_num(category_count), "Distinct product categories", SUCCESS, styles),
                _metric_card("Quality Gaps", _fmt_num(len(quality_gaps)), "Rows missing one or more key attributes", WARNING, styles),
            ]
        ),
        _callout_row(
            _callout_box(
                "Catalog Story",
                [
                    f"The workbook carries {_fmt_num(total_products)} products across {_fmt_num(product_type_count)} product types and {_fmt_num(category_count)} categories.",
                    f"The dashboard should be interpreted as a master-data quality and structure view, with {_fmt_num(len(quality_gaps))} rows currently showing missing key attributes.",
                    f"{_fmt_num(uom_count)} distinct units of measure are in use, so normalization and classification consistency matter for downstream reporting.",
                ],
                BRAND,
                styles,
            ),
            _callout_box("Recommended Management Actions", _product_catalog_action_lines(runtime), WARNING, styles),
        ),
        Spacer(1, 0.18 * inch),
        PageBreak(),
        Paragraph("Product Type Mix", styles["SectionTitle"]),
        _data_table(
            ["Product Type", "Count"],
            [[str(item.get("label") or "Unknown"), _fmt_num(int(item.get("count") or 0))] for item in product_type_counts[:12]]
            or [["No product type data", "0"]],
            [4.6 * inch, 1.7 * inch],
            styles,
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Category Concentration", styles["SectionTitle"]),
        _data_table(
            ["Category", "Count"],
            [[str(item.get("label") or "Unknown"), _fmt_num(int(item.get("count") or 0))] for item in category_counts[:12]]
            or [["No category data", "0"]],
            [4.6 * inch, 1.7 * inch],
            styles,
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Unit-of-Measure Review", styles["SectionTitle"]),
        _data_table(
            ["Base UoM", "Count"],
            [[str(item.get("label") or "Unknown"), _fmt_num(int(item.get("count") or 0))] for item in uom_counts[:12]]
            or [["No UoM data", "0"]],
            [4.6 * inch, 1.7 * inch],
            styles,
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Data Quality Exceptions", styles["SectionTitle"]),
        _data_table(
            ["Product ID", "Description", "Missing Fields"],
            [
                [
                    str(row.get("product_id") or "—"),
                    str(row.get("description") or "—"),
                    ", ".join(str(item) for item in (row.get("missing_fields") or [])) or "—",
                ]
                for row in quality_gaps[:15]
            ] or [["No quality gaps detected", "—", "—"]],
            [1.3 * inch, 2.6 * inch, 2.4 * inch],
            styles,
        ),
    ])

    if agentic_draft:
        story.extend(_render_citation_appendix(bundle.evidence, styles))

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=0.7 * inch,
        rightMargin=0.7 * inch,
        topMargin=1.4 * inch,
        bottomMargin=0.7 * inch,
        title=f"{_dashboard_title(ctx)} Summary",
    )
    frame = _build_page_frame(
        _dashboard_title(ctx),
        f"Snapshot {ctx.snapshot.as_of_date} • Source workbook: {ctx.snapshot.source_filename}",
    )
    doc.build(story, onFirstPage=frame, onLaterPages=frame)
    return buf.getvalue()


def _build_generic_review_summary_pdf(ctx: SnapshotReportContext, gemini: GeminiRequestSettings | None = None) -> bytes:
    styles = _build_styles()
    bundle = build_summary_reasoning_bundle(ctx, gemini=gemini)
    plan = bundle.plan
    reasoning_packet = bundle.packet
    masked_evidence, lookup = _mask_snapshot_evidence_if_needed(ctx, bundle.evidence)
    agentic_draft = _run_agentic_report_graph(
        mode=plan.mode,
        report_style="executive",
        evidence=masked_evidence,
        required_section_keys=plan.required_section_keys,
        entity_name=plan.entity_name,
        reasoning_packet=reasoning_packet,
        gemini=gemini,
    )
    if agentic_draft and lookup:
        agentic_draft = _unmask_agentic_draft(agentic_draft, lookup)
    analytics_plan = _build_analytics_agentic_plan(ctx)
    analytics_draft = _run_agentic_report_graph(
        mode=analytics_plan.mode,
        report_style="analytics",
        evidence=masked_evidence,
        required_section_keys=analytics_plan.required_section_keys,
        entity_name=analytics_plan.entity_name,
        reasoning_packet=reasoning_packet,
        gemini=gemini,
    )
    if analytics_draft and lookup:
        analytics_draft = _unmask_agentic_draft(analytics_draft, lookup)
    schema_fields = []
    if isinstance(ctx.dashboard_config, dict):
        schema_fields = ctx.dashboard_config.get("schema_fields") or []
    sheet_rows: list[list[str]] = []
    total_fields = 0
    for sheet in schema_fields:
        sections = sheet.get("sections") or []
        field_count = sum(len(section.get("fields") or []) for section in sections)
        total_fields += field_count
        sheet_rows.append(
            [
                str(sheet.get("sheet_name") or "Sheet"),
                _fmt_num(len(sections)),
                _fmt_num(field_count),
            ]
        )
    runtime = ctx.runtime_payload or {}
    comparison_rows = [
        [
            str(group.get("group_label") or "Period"),
            _fmt_num(int(group.get("matched_pool_count") or 0)),
            _fmt_num(int(group.get("unmatched_tc_pool_count") or 0)),
            _fmt_num(int(group.get("unmatched_bc_pool_count") or 0)),
        ]
        for group in (runtime.get("comparison_groups") or [])[:10]
    ]
    latest_group = None
    comparison_groups = sorted(
        list(runtime.get("comparison_groups") or []),
        key=lambda item: _parse_runtime_period(str(item.get("group_label") or "")) or (9999, 99),
    )
    if comparison_groups:
        latest_group = comparison_groups[-1]
    hotspot_rows = [
        [
            str(item.get("label") or "Pool"),
            f"{float(item.get('ratio') or 0.0):.2%}",
            _fmt_num(int(float(item.get("bad_count") or 0.0))),
            _fmt_num(int(float(item.get("total_count") or 0.0))),
        ]
        for item in ((latest_group or {}).get("highest_rate_segments") or [])[:10]
    ]
    context_lines = (
        [
            bundle.packet.findings[0].insight,
            bundle.packet.findings[1].insight if len(bundle.packet.findings) > 1 else "Pool hotspots were not isolated from the current aligned periods.",
            bundle.packet.findings[2].insight if len(bundle.packet.findings) > 2 else "Forecast readiness is limited by the currently aligned period history.",
        ]
        if _generic_runtime_ready(ctx)
        else [
            "This summary is driven by workbook structure rather than a mature business-runtime model.",
            "Use it to confirm sheet intent, field naming, and the shape of the proposed dashboard before expecting executive insights.",
            "Once the workbook family is standardized, subsequent summaries will become more domain-specific and prescriptive.",
        ]
    )
    action_lines = (
        [action.action for action in bundle.packet.actions[:4]]
        if _generic_runtime_ready(ctx)
        else [
            "Approve or refine the proposed dashboard family and widget layout first.",
            "Standardize header meanings so metrics can be defined deterministically.",
            "Only then promote the workbook into a richer business summary path.",
        ]
    )
    metric_cards = (
        [
            _metric_card("Paired Periods", _fmt_num(int(runtime.get("comparison_group_count") or 0)), "Aligned TC/BC comparison groups detected", BRAND, styles),
            _metric_card("Rows", _fmt_num(int(runtime.get("total_rows") or 0)), "Workbook rows profiled for the adaptive summary", INFO, styles),
            _metric_card("Measures", _fmt_num(int(runtime.get("numeric_measure_count") or 0)), "Numeric or duration measures available for analysis", INFO, styles),
            _metric_card("Reference Sheets", _fmt_num(int(runtime.get("reference_sheet_count") or 0)), "Definition or question sheets used only for context", WARNING, styles),
        ]
        if _generic_runtime_ready(ctx)
        else [
            _metric_card("Sheets", _fmt_num(len(schema_fields)), "Worksheet structures detected in the uploaded file", BRAND, styles),
            _metric_card("Fields", _fmt_num(total_fields), "Header fields available for semantic mapping", INFO, styles),
            _metric_card("Dashboard Family", "Generic Review", "Awaiting stronger semantic standardization", WARNING, styles),
        ]
    )

    story = [
        Spacer(1, 0.15 * inch),
        Paragraph("Executive Summary", styles["SectionTitle"]),
        Spacer(1, 0.1 * inch),
    ]

    if agentic_draft:
        story.extend(_render_agentic_draft(agentic_draft, styles, plan.title_overrides))
        story.append(Spacer(1, 0.15 * inch))
    else:
        story.extend(_render_reasoning_packet_fallback(reasoning_packet, styles))
        story.append(Spacer(1, 0.08 * inch))

    story.extend(
        _render_agentic_draft(analytics_draft, styles, analytics_plan.title_overrides)
        if analytics_draft
        else _render_data_analysis_summary(reasoning_packet, styles)
    )
    story.append(Spacer(1, 0.12 * inch))

    story.extend([
        _metric_grid(metric_cards),
        _callout_row(
            _callout_box(
                "Current Context",
                context_lines,
                BRAND,
                styles,
            ),
            _callout_box(
                "Recommended Next Steps",
                action_lines,
                WARNING,
                styles,
            ),
        ),
        Spacer(1, 0.18 * inch),
        PageBreak(),
        Paragraph("Workbook Structure", styles["SectionTitle"]),
        _data_table(
            ["Sheet", "Sections", "Fields"],
            sheet_rows or [["No schema fields detected", "0", "0"]],
            [3.8 * inch, 1.1 * inch, 1.4 * inch],
            styles,
        ),
    ])

    if _generic_runtime_ready(ctx):
        story.extend(
            [
                Spacer(1, 0.18 * inch),
                Paragraph("Paired Period Coverage", styles["SectionTitle"]),
                _data_table(
                    ["Period", "Matched Pools", "TC-only", "BC-only"],
                    comparison_rows or [["No paired periods detected", "0", "0", "0"]],
                    [2.6 * inch, 1.2 * inch, 1.0 * inch, 1.0 * inch],
                    styles,
                ),
                Spacer(1, 0.18 * inch),
                Paragraph(
                    f"{str((latest_group or {}).get('group_label') or 'Latest')} BC/TC Hotspots",
                    styles["SectionTitle"],
                ),
                _data_table(
                    ["Pool", "BC/TC", "BC", "TC"],
                    hotspot_rows or [["No hotspot pools isolated", "—", "—", "—"]],
                    [2.4 * inch, 1.1 * inch, 1.0 * inch, 1.0 * inch],
                    styles,
                ),
            ]
        )

    if agentic_draft:
        story.extend(_render_citation_appendix(bundle.evidence, styles))

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=0.7 * inch,
        rightMargin=0.7 * inch,
        topMargin=1.4 * inch,
        bottomMargin=0.7 * inch,
        title=f"{_dashboard_title(ctx)} Summary",
    )
    frame = _build_page_frame(
        _dashboard_title(ctx),
        f"Snapshot {ctx.snapshot.as_of_date} • Source workbook: {ctx.snapshot.source_filename}",
    )
    doc.build(story, onFirstPage=frame, onLaterPages=frame)
    return buf.getvalue()


def _adaptive_widget_by_key(runtime: dict[str, Any], key: str) -> dict[str, Any] | None:
    adaptive = dict(runtime.get("adaptive_dashboard") or {})
    widgets = list(adaptive.get("widgets") or [])
    return next((dict(item) for item in widgets if str(item.get("key") or "") == key), None)


def _adaptive_widget_by_keys(runtime: dict[str, Any], *keys: str) -> dict[str, Any] | None:
    for key in keys:
        widget = _adaptive_widget_by_key(runtime, key)
        if widget:
            return widget
    return None


def _widget_rank_rows(widget: dict[str, Any] | None) -> list[dict[str, Any]]:
    return [
        {
            "label": str(item.get("label") or "Unknown"),
            "value": float(item.get("value") or 0.0),
            "share": float(item.get("share") or 0.0),
        }
        for item in list((widget or {}).get("items") or [])
    ]


def _build_generic_analytics_summary_pdf(ctx: SnapshotReportContext, gemini: GeminiRequestSettings | None = None) -> bytes:
    styles = _build_styles()
    bundle = build_summary_reasoning_bundle(ctx, gemini=gemini)
    reasoning_packet = bundle.packet
    analytics_plan = _build_analytics_agentic_plan(ctx)
    masked_evidence, lookup = _mask_snapshot_evidence_if_needed(ctx, bundle.evidence)
    analytics_draft = _run_agentic_report_graph(
        mode=analytics_plan.mode,
        report_style="analytics",
        evidence=masked_evidence,
        required_section_keys=analytics_plan.required_section_keys,
        entity_name=analytics_plan.entity_name,
        reasoning_packet=reasoning_packet,
        gemini=gemini,
    )
    if analytics_draft and lookup:
        analytics_draft = _unmask_agentic_draft(analytics_draft, lookup)

    runtime = ctx.runtime_payload or {}
    adaptive = dict(runtime.get("adaptive_dashboard") or {})
    kpis = list(adaptive.get("kpis") or [])
    top_dues_widget = _adaptive_widget_by_keys(runtime, "top_towers_dues", "exposure_ranking")
    top_penalty_widget = _adaptive_widget_by_keys(runtime, "top_towers_penalty", "measure_mix")
    owner_widget = _adaptive_widget_by_keys(runtime, "owner_exposure", "quality_flags")
    trend_widget = _adaptive_widget_by_keys(runtime, "dues_trend", "sheet_trend", "trend_snapshot")
    quality_widget = _adaptive_widget_by_keys(runtime, "quality_flags")
    forecast_widget = _adaptive_widget_by_keys(runtime, "forecast_outlook")
    forecast = _generic_runtime_forecast(runtime)

    ranked_dues_rows = _widget_rank_rows(top_dues_widget)
    ranked_penalty_rows = _widget_rank_rows(top_penalty_widget)
    trend_items = list((trend_widget or {}).get("items") or [])
    trend_labels = [str(item.get("label") or item.get("period_label") or "Period") for item in trend_items]
    trend_values = [float(item.get("value") or item.get("dues_total") or 0.0) for item in trend_items]

    forecast_rows = []
    if forecast.get("ready"):
        current_value = trend_values[-1] if trend_values else float(forecast.get("next_tc") or 0.0)
        forecast_rows = [
            {"label": "Current", "value": current_value},
            {"label": "Projected Next", "value": float(forecast.get("next_tc") or 0.0)},
        ]
    elif forecast_widget:
        forecast_rows = [
            {"label": str(item.get("label") or "Point"), "value": float(item.get("value") or 0.0)}
            for item in list(forecast_widget.get("items") or [])[:4]
        ]

    distribution_chart = _vertical_bar_chart(
        str((top_dues_widget or {}).get("title") or "Top Exposure Distribution"),
        str((top_dues_widget or {}).get("description") or "Highest-value exposure by the dominant business dimension."),
        [row["label"] for row in ranked_dues_rows[:8]] or ["No values"],
        [row["value"] for row in ranked_dues_rows[:8]] or [0.0],
        accent=INFO,
    )
    concentration_chart = _vertical_bar_chart(
        str((top_penalty_widget or {}).get("title") or "Penalty Concentration"),
        str((top_penalty_widget or {}).get("description") or "Penalty concentration by the dominant business dimension."),
        [row["label"] for row in ranked_penalty_rows[:8]] or ["No values"],
        [row["value"] for row in ranked_penalty_rows[:8]] or [0.0],
        accent=WARNING,
    )
    trend_chart = _line_chart(
        str((trend_widget or {}).get("title") or "Trend Analysis"),
        str((trend_widget or {}).get("description") or "Period-aligned movement across the available workbook history."),
        trend_labels or ["Current"],
        trend_values or [0.0],
        accent=SUCCESS,
    )
    forecast_chart = _vertical_bar_chart(
        "Forecast Outlook",
        "Deterministic forward projection from the currently aligned generic runtime history.",
        [str(item.get("label") or "Point") for item in forecast_rows] or ["Forecast unavailable"],
        [float(item.get("value") or 0.0) for item in forecast_rows] or [0.0],
        accent=BRAND,
    )

    generic_fallback_ids = [item.id for item in bundle.evidence[:4]]
    chart_specs = {
        "distribution": ChartInterpretationSpec(
            key="generic_distribution",
            chart_kind="generic_ranked",
            title=str((top_dues_widget or {}).get("title") or "Top Exposure Distribution"),
            subtitle=str((top_dues_widget or {}).get("description") or ""),
            evidence_ids=_pick_evidence_ids(bundle.evidence, "GEN-", "EDA-", "SEM-", fallback=generic_fallback_ids),
            payload={"rows": ranked_dues_rows[:8]},
        ),
        "concentration": ChartInterpretationSpec(
            key="generic_concentration",
            chart_kind="generic_ranked",
            title=str((top_penalty_widget or {}).get("title") or "Penalty Concentration"),
            subtitle=str((top_penalty_widget or {}).get("description") or ""),
            evidence_ids=_pick_evidence_ids(bundle.evidence, "GEN-", "EDA-", "SEM-", fallback=generic_fallback_ids),
            payload={"rows": ranked_penalty_rows[:8]},
        ),
        "trend": ChartInterpretationSpec(
            key="generic_trend",
            chart_kind="generic_trend",
            title=str((trend_widget or {}).get("title") or "Trend Analysis"),
            subtitle=str((trend_widget or {}).get("description") or ""),
            evidence_ids=_pick_evidence_ids(bundle.evidence, "GEN-", "EDA-", "SEM-", fallback=generic_fallback_ids),
            payload={"labels": trend_labels, "values": trend_values},
        ),
        "forecast": ChartInterpretationSpec(
            key="generic_forecast",
            chart_kind="generic_forecast",
            title="Forecast Outlook",
            subtitle="Deterministic forward projection from the aligned generic runtime history.",
            evidence_ids=_pick_evidence_ids(bundle.evidence, "GEN-", "EDA-", "SEM-", fallback=generic_fallback_ids),
            payload={"rows": forecast_rows},
        ),
    }
    chart_evidence = {
        key: _sanitized_evidence_items([item for item in bundle.evidence if item.id in spec.evidence_ids] or bundle.evidence[:4])
        for key, spec in chart_specs.items()
    }
    chart_interpretations = {
        key: _run_chart_interpretation_graph(spec, chart_evidence[key], gemini=gemini)
        for key, spec in chart_specs.items()
    }

    metric_cards = [
        _metric_card(str(item.get("label") or "Metric"), str(item.get("value") or "—"), str(item.get("meta") or ""), BRAND if idx == 0 else INFO, styles)
        for idx, item in enumerate(kpis[:6])
    ] or [
        _metric_card("Sheets", _fmt_num(int(runtime.get("total_sheets") or 0)), "Worksheets surfaced in the runtime", BRAND, styles),
        _metric_card("Rows", _fmt_num(int(runtime.get("total_rows") or 0)), "Workbook rows available to the adaptive runtime", INFO, styles),
        _metric_card("Measures", _fmt_num(int(runtime.get("numeric_measure_count") or 0)), "Numeric signals available for analysis", INFO, styles),
    ]

    owner_rows = list((owner_widget or {}).get("rows") or [])
    quality_rows = list((quality_widget or {}).get("rows") or [])
    adaptive_highlights = [
        _strip_internal_tooling_text(str(widget.get("insight") or ""))
        for widget in list(adaptive.get("widgets") or [])[:6]
        if str(widget.get("insight") or "").strip()
    ]
    highlight_lines = adaptive_highlights[:4] or [finding.insight for finding in reasoning_packet.findings[:4]] or [
        "The adaptive runtime did not promote any high-confidence analytical findings into the visible summary yet."
    ]
    quality_lines = [
        f"{str(row.get('Check') or 'Check')}: {str(row.get('Result') or '—')}"
        for row in quality_rows[:4]
    ] or [
        "No material quality notes were surfaced by the adaptive runtime.",
    ]
    forecast_summary_rows = (
        [
            ["Forecast readiness", "Ready"],
            ["Direction", str(forecast.get("direction") or "stable").title()],
            ["Projected next ratio", f"{float(forecast.get('next_ratio') or 0.0):.2%}"],
            ["Projected next TC / dues", _fmt_num(int(float(forecast.get("next_tc") or 0.0)))],
        ]
        if forecast.get("ready")
        else [["Forecast readiness", f"Not ready ({_fmt_num(int(forecast.get('periods') or 0))} usable periods)"]]
    )

    story = [
        Spacer(1, 0.15 * inch),
        Paragraph("Analytics Summary", styles["SectionTitle"]),
        Paragraph(
            "This report is EDA-first. It leads with charted analytical outputs, promoted findings, and quality checks from the adaptive runtime before any analyst narrative.",
            styles["BodyMuted"],
        ),
        Spacer(1, 0.1 * inch),
        _metric_grid(metric_cards),
        Spacer(1, 0.12 * inch),
        _callout_row(
            _callout_box("Analytical Highlights", highlight_lines, BRAND, styles),
            _callout_box("Quality and Coverage Checks", quality_lines, WARNING, styles),
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Analytical Findings", styles["SectionTitle"]),
        _data_table(
            ["Finding", "What it shows", "Why it matters", "Confidence"],
            _user_friendly_finding_rows(reasoning_packet),
            [1.35 * inch, 2.35 * inch, 2.35 * inch, 0.85 * inch],
            styles,
        ),
        PageBreak(),
        Paragraph("Distribution Analysis", styles["SectionTitle"]),
        distribution_chart,
        Spacer(1, 0.08 * inch),
        _render_chart_interpretation_block(
            chart_interpretations.get("distribution"),
            str((top_dues_widget or {}).get("insight") or "The highest-ranked categories dominate the visible exposure in this workbook."),
            styles,
        ),
        Spacer(1, 0.08 * inch),
        _data_table(
            ["Category", "Value", "Share"],
            [[row["label"], _fmt_num(int(row["value"])), f"{row['share'] * 100:.1f}%"] for row in ranked_dues_rows[:8]] or [["No ranked values", "—", "—"]],
            [2.8 * inch, 1.6 * inch, 1.3 * inch],
            styles,
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Concentration Analysis", styles["SectionTitle"]),
        concentration_chart,
        Spacer(1, 0.08 * inch),
        _render_chart_interpretation_block(
            chart_interpretations.get("concentration"),
            str((top_penalty_widget or {}).get("insight") or "Penalty concentration remains clustered in a small set of categories."),
            styles,
        ),
        Spacer(1, 0.08 * inch),
        _data_table(
            ["Category", "Penalty", "Share"],
            [[row["label"], _fmt_num(int(row["value"])), f"{row['share'] * 100:.1f}%"] for row in ranked_penalty_rows[:8]] or [["No concentration rows", "—", "—"]],
            [2.8 * inch, 1.6 * inch, 1.3 * inch],
            styles,
        ),
        PageBreak(),
        Paragraph("Trend and Forecast Analysis", styles["SectionTitle"]),
        trend_chart,
        Spacer(1, 0.08 * inch),
        _render_chart_interpretation_block(
            chart_interpretations.get("trend"),
            str((trend_widget or {}).get("insight") or "The period series shows the directional movement across the currently aligned workbook history."),
            styles,
        ),
        Spacer(1, 0.12 * inch),
        forecast_chart,
        Spacer(1, 0.08 * inch),
        _render_chart_interpretation_block(
            chart_interpretations.get("forecast"),
            "The forecast is deterministic and readiness-gated. It should be used for directional planning rather than commitment-level predictions.",
            styles,
        ),
        Spacer(1, 0.08 * inch),
        _data_table(["Signal", "Value"], forecast_summary_rows, [2.3 * inch, 4.0 * inch], styles),
        Spacer(1, 0.18 * inch),
        Paragraph("Owner and Quality Panels", styles["SectionTitle"]),
        _data_table(
            list(owner_rows[0].keys()) if owner_rows else ["Owner", "Dues", "Share"],
            [[str(row.get(column) or "—") for column in (list(owner_rows[0].keys()) if owner_rows else ["Owner", "Dues", "Share"])] for row in owner_rows[:8]] or [["No owner rows", "—", "—"]],
            [2.6 * inch, 1.5 * inch, 1.2 * inch],
            styles,
        ),
        Spacer(1, 0.18 * inch),
        Paragraph("Coverage and Modeling Notes", styles["SectionTitle"]),
        _data_table(
            list(quality_rows[0].keys()) if quality_rows else ["Check", "Result"],
            [[str(row.get(column) or "—") for column in (list(quality_rows[0].keys()) if quality_rows else ["Check", "Result"])] for row in quality_rows[:8]] or [["No quality notes", "—"]],
            [2.6 * inch, 3.7 * inch],
            styles,
        ),
    ]

    if analytics_draft:
        story.extend([Spacer(1, 0.18 * inch), Paragraph("Analyst Interpretation", styles["SectionTitle"])])
        story.extend(_render_agentic_draft(analytics_draft, styles, analytics_plan.title_overrides))
        story.extend(_render_citation_appendix(bundle.evidence, styles))

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=0.7 * inch,
        rightMargin=0.7 * inch,
        topMargin=1.4 * inch,
        bottomMargin=0.7 * inch,
        title=f"{_dashboard_title(ctx)} Analytics Summary",
    )
    frame = _build_page_frame(
        f"{_dashboard_title(ctx)} Analytics Summary",
        f"Snapshot {ctx.snapshot.as_of_date} • Source workbook: {ctx.snapshot.source_filename}",
    )
    doc.build(story, onFirstPage=frame, onLaterPages=frame)
    return buf.getvalue()


def build_analytics_summary_pdf(ctx: SnapshotReportContext, gemini: GeminiRequestSettings | None = None) -> bytes:
    family = _dashboard_family(ctx)
    if family == "variance_dashboard":
        return _build_variance_analytics_summary_pdf(ctx, gemini=gemini)
    if family == "generic_review_dashboard":
        return _build_generic_analytics_summary_pdf(ctx, gemini=gemini)
    styles = _build_styles()
    bundle = build_summary_reasoning_bundle(ctx, gemini=gemini)
    reasoning_packet = bundle.packet
    analytics_plan = _build_analytics_agentic_plan(ctx)
    masked_evidence, lookup = _mask_snapshot_evidence_if_needed(ctx, bundle.evidence)
    analytics_draft = _run_agentic_report_graph(
        mode=analytics_plan.mode,
        report_style="analytics",
        evidence=masked_evidence,
        required_section_keys=analytics_plan.required_section_keys,
        entity_name=analytics_plan.entity_name,
        reasoning_packet=reasoning_packet,
        gemini=gemini,
    )
    if analytics_draft and lookup:
        analytics_draft = _unmask_agentic_draft(analytics_draft, lookup)

    story = [
        Spacer(1, 0.15 * inch),
        Paragraph("Analytics Summary", styles["SectionTitle"]),
        Spacer(1, 0.1 * inch),
    ]

    if analytics_draft:
        story.extend(_render_agentic_draft(analytics_draft, styles, analytics_plan.title_overrides))
    else:
        story.extend(_render_data_analysis_summary(reasoning_packet, styles))

    story.append(Spacer(1, 0.15 * inch))
    if analytics_draft:
        story.extend(_render_citation_appendix(bundle.evidence, styles))

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=LETTER,
        leftMargin=0.7 * inch,
        rightMargin=0.7 * inch,
        topMargin=1.4 * inch,
        bottomMargin=0.7 * inch,
        title=f"{_dashboard_title(ctx)} Analytics Summary",
    )
    frame = _build_page_frame(
        f"{_dashboard_title(ctx)} Analytics Summary",
        f"Snapshot {ctx.snapshot.as_of_date} • Source workbook: {ctx.snapshot.source_filename}",
    )
    doc.build(story, onFirstPage=frame, onLaterPages=frame)
    return buf.getvalue()


def build_overall_summary_pdf(ctx: SnapshotReportContext, gemini: GeminiRequestSettings | None = None) -> bytes:
    family = _dashboard_family(ctx)
    if family == "status_pipeline_dashboard":
        return _build_status_pipeline_summary_pdf(ctx, gemini=gemini)
    if family == "product_catalog_dashboard":
        return _build_product_catalog_summary_pdf(ctx, gemini=gemini)
    if family == "generic_review_dashboard":
        return _build_generic_review_summary_pdf(ctx, gemini=gemini)
    return _build_variance_overall_summary_pdf(ctx, gemini=gemini)


def build_client_summary_pdf(
    ctx: SnapshotReportContext,
    client_external_id: str,
    gemini: GeminiRequestSettings | None = None,
) -> tuple[bytes, ClientMetrics]:
    family = _dashboard_family(ctx)
    if family != "variance_dashboard":
        raise ValueError("Client summary is only available for client-level variance dashboards")
    return _build_variance_client_summary_pdf(ctx, client_external_id, gemini=gemini)
