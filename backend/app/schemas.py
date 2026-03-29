from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, EmailStr


class TokenResponse(BaseModel):
    access_token: str
    token_type: Literal["bearer"] = "bearer"


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class SnapshotSummary(BaseModel):
    id: int
    as_of_date: date
    source_filename: str


class KpiResponse(BaseModel):
    snapshot: SnapshotSummary
    total_contracted: int
    total_received: int
    total_pending: int
    total_contracted_ind: int
    total_contracted_bus: int
    total_received_ind: int
    total_received_bus: int
    overall_receipt_rate: float
    active_clients: int
    zero_received_clients: int
    over_delivered_clients: int
    staff_total_received: int


class ClientRow(BaseModel):
    client_name: str
    client_id: str
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


class StaffRow(BaseModel):
    name: str
    staff_id: str
    staff_type: str
    received_ind: int
    received_bus: int
    received_total: int


class PiiFieldSelection(BaseModel):
    sheet_name: str
    section_key: str
    header_label: str
    pii_type: Literal["name", "email", "phone", "address", "identifier", "custom"]


class WorkbookPreviewField(BaseModel):
    column: str
    header_label: str
    sample_value: str | None = None
    suggested_pii_type: str | None = None


class WorkbookPreviewSection(BaseModel):
    section_key: str
    section_label: str
    header_row: int
    headers: list[WorkbookPreviewField]


class WorkbookPreviewSheet(BaseModel):
    sheet_name: str
    sections: list[WorkbookPreviewSection]


class WorkbookPreviewResponse(BaseModel):
    upload_token: str
    workbook_family: str
    family_label: str
    family_mode: str
    sheets: list[WorkbookPreviewSheet]


class DocumentProcessRequest(BaseModel):
    upload_token: str
    pii_fields: list[PiiFieldSelection] = []
    dashboard_guidance: str | None = None


class DocumentProcessingJobResponse(BaseModel):
    id: int
    upload_token: str
    workbook_family: str
    status: str
    stage: str
    progress_percent: int
    message: str
    error_detail: str | None = None
    snapshot_id: int | None = None


class DashboardSectionSpec(BaseModel):
    key: str
    label: str
    description: str
    renderer: str
    slot: str | None = None
    widget_type: str | None = None
    bindings: dict | None = None


class DashboardTabSpec(BaseModel):
    key: str
    label: str
    description: str
    sections: list[DashboardSectionSpec]


class WorkbookSchemaFieldSpec(BaseModel):
    column: str
    header_label: str
    normalized_header: str
    sample_value: str | None = None
    suggested_pii_type: str | None = None


class WorkbookSchemaSectionSpec(BaseModel):
    section_key: str
    section_label: str
    header_row: int
    fields: list[WorkbookSchemaFieldSpec]


class WorkbookSchemaSheetSpec(BaseModel):
    sheet_name: str
    sections: list[WorkbookSchemaSectionSpec]


class DashboardInvestigationStepResponse(BaseModel):
    key: str
    title: str
    objective: str
    tool: str
    rationale: str
    priority: int | None = None


class DashboardInvestigationEvidenceResponse(BaseModel):
    key: str
    tool: str
    title: str
    detail: str
    confidence_score: float | None = None
    supporting_metrics: list[str] = []


class DashboardLayoutPreferencesResponse(BaseModel):
    hidden_cards: list[str] = []
    card_orders: dict[str, list[str]] = {}


class DashboardBlueprintConfigResponse(BaseModel):
    dashboard_family: str
    layout_template: str
    title: str
    subtitle: str
    tabs: list[DashboardTabSpec]
    kpi_cards: list[dict]
    schema_fields: list[WorkbookSchemaSheetSpec]
    customization_prompts: list[str] = []
    semantic_summary: str | None = None
    semantic_details: dict | None = None
    business_questions: list[str] = []
    ambiguities: list[str] = []
    semantic_confidence: float | None = None
    eda_plan: list[DashboardInvestigationStepResponse] = []
    eda_evidence: list[DashboardInvestigationEvidenceResponse] = []
    eda_workflow: str | None = None
    orchestrator_workflow: str | None = None
    investigation_plan: list[DashboardInvestigationStepResponse] = []
    investigation_evidence: list[DashboardInvestigationEvidenceResponse] = []
    proposal_workflow: str | None = None
    dashboard_preferences: DashboardLayoutPreferencesResponse = DashboardLayoutPreferencesResponse()


class DashboardBlueprintResponse(BaseModel):
    id: int
    blueprint_key: str
    name: str
    description: str
    schema_signature: str
    workbook_type: str
    status: str
    config: DashboardBlueprintConfigResponse


class DashboardRefinementDiffResponse(BaseModel):
    added_tabs: list[str] = []
    added_section_count: int = 0
    added_section_labels: list[str] = []
    accepted_chart_types: list[str] = []
    missing_chart_types: list[str] = []
    changed_title: bool = False
    changed_summary: bool = False


class DashboardRefinementResultResponse(BaseModel):
    status: Literal["not_requested", "fulfilled", "partially_fulfilled", "rejected"]
    message: str
    accepted_requests: list[str] = []
    unsupported_requests: list[str] = []
    warnings: list[str] = []
    diff: DashboardRefinementDiffResponse = DashboardRefinementDiffResponse()


class DashboardProposalResponse(BaseModel):
    id: int
    snapshot_id: int
    status: str
    match_mode: str
    confidence_score: float
    title: str
    summary: str
    rationale: str
    schema_signature: str
    workbook_type: str
    matched_blueprint_id: int | None = None
    approved_blueprint_id: int | None = None
    proposal: DashboardBlueprintConfigResponse
    refinement_result: DashboardRefinementResultResponse | None = None


class DashboardProposalRequest(BaseModel):
    user_guidance: str | None = None


class DashboardLayoutPreferencesRequest(BaseModel):
    hidden_cards: list[str] = []
    card_orders: dict[str, list[str]] = {}


class AssistantCardResponse(BaseModel):
    title: str
    value: str
    meta: str | None = None


class DashboardChatRequest(BaseModel):
    snapshot_id: int | None = None
    question: str


class DashboardChatResponse(BaseModel):
    title: str
    summary: str | None = None
    cards: list[AssistantCardResponse] = []
    bullets: list[str] = []


class AppLogResponse(BaseModel):
    id: int
    run_key: str | None = None
    level: str
    state: str | None = None
    category: str
    event: str
    agent_name: str | None = None
    workflow: str | None = None
    tool_name: str | None = None
    model_name: str | None = None
    message: str
    detail: str | None = None
    payload_json: dict | None = None
    request_path: str | None = None
    snapshot_id: int | None = None
    proposal_id: int | None = None
    blueprint_id: int | None = None
    user_id: int | None = None
    created_at: str


class AnalysisSetMemberResponse(BaseModel):
    snapshot_id: int
    source_filename: str
    as_of_date: date | None = None
    workbook_type: str | None = None
    member_order: int
    role_label: str | None = None


class AnalysisSetProposalRequest(BaseModel):
    snapshot_ids: list[int]
    intent: str | None = None
    title: str | None = None


class AnalysisSetConfirmRequest(BaseModel):
    title: str | None = None
    intent: str | None = None
    relationship_type: str | None = None
    join_keys: list[str] = []
    member_labels: dict[int, str] = {}


class AnalysisSetProposalResponse(BaseModel):
    id: int
    status: str
    name: str
    summary: str
    intent: str | None = None
    relationship_type: str
    confidence_score: float
    comparability: str
    rationale: str
    suggested_join_keys: list[str] = []
    suggested_period_order: list[AnalysisSetMemberResponse] = []
    conflicts: list[str] = []
    dashboard_hypothesis: list[str] = []
    members: list[AnalysisSetMemberResponse] = []
