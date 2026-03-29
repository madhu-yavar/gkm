from __future__ import annotations

import json
import logging
import re
from typing import Any, Literal

from pydantic import BaseModel, Field

from app.app_logs import log_app_event
from app.field_roles import classify_table_fields, dimension_headers, measure_headers
from app.gemini_reasoning import GeminiReasoningError, GeminiRequestSettings, gemini_generate_json, gemini_generate_structured
from app.raw_data_store import RawTable
from app.schemas import DashboardTabSpec


logger = logging.getLogger(__name__)

SUPPORTED_DASHBOARD_FAMILIES = {
    "variance_dashboard",
    "status_pipeline_dashboard",
    "product_catalog_dashboard",
    "generic_review_dashboard",
}


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(text or "").strip().lower()).strip("_")


def _extract_chart_preferences(user_guidance: str | None) -> list[str]:
    text = str(user_guidance or "").lower()
    found: list[str] = []
    aliases = [
        ("pie", ["pie", "pie chart", "piechart", "donut", "donut chart"]),
        ("gantt", ["gantt", "timeline"]),
        ("scatter", ["scatter", "scatter plot", "scatterplot", "dot plot"]),
        ("line", ["line", "trend line", "line chart"]),
        ("bar", ["bar", "bar chart", "ranked bar"]),
        ("table", ["table", "tabular"]),
    ]
    for key, terms in aliases:
        if any(term in text for term in terms) and key not in found:
            found.append(key)
    return found


def _normalize_ambiguities(value: Any) -> list[str]:
    items = value if isinstance(value, list) else []
    ambiguities: list[str] = []
    for item in items[:6]:
        if isinstance(item, str):
            text = item.strip()
        elif isinstance(item, dict):
            text = str(
                item.get("ambiguity_description")
                or item.get("description")
                or item.get("reason")
                or item.get("column_name")
                or item.get("field_name")
                or item.get("title")
                or ""
            ).strip()
        else:
            text = str(item or "").strip()
        if text:
            ambiguities.append(text)
    return ambiguities


def _normalize_questions(value: Any) -> list[str]:
    items = value if isinstance(value, list) else []
    questions: list[str] = []
    for item in items[:6]:
        if isinstance(item, str):
            text = item.strip()
        elif isinstance(item, dict):
            text = str(item.get("question") or item.get("text") or item.get("title") or "").strip()
        else:
            text = str(item or "").strip()
        if text:
            questions.append(text)
    return questions


def _semantic_name(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        for key in (
            "measure_name",
            "dimension_name",
            "entity_name",
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


def _normalize_sheet_summary(item: Any) -> SemanticSheetSummary | None:
    if not isinstance(item, dict):
        return None
    sheet_name = str(item.get("sheet_name") or item.get("name") or item.get("title") or "").strip()
    if not sheet_name:
        return None
    likely_role = str(item.get("likely_role") or item.get("role") or item.get("sheet_role") or "worksheet").strip()
    entities = item.get("primary_entities") or item.get("entities") or item.get("entity_types") or []
    dimensions = item.get("candidate_dimensions") or item.get("dimensions") or item.get("grouping_dimensions") or []
    measures = item.get("candidate_measures") or item.get("measures") or item.get("metrics") or []
    confidence = item.get("confidence_score") or item.get("confidence") or 0.72
    try:
        confidence_value = float(confidence)
    except Exception:
        confidence_value = 0.72
    return SemanticSheetSummary(
        sheet_name=sheet_name,
        likely_role=likely_role,
        primary_entities=[_semantic_name(value) for value in list(entities)[:5] if _semantic_name(value)],
        candidate_dimensions=[_semantic_name(value) for value in list(dimensions)[:8] if _semantic_name(value)],
        candidate_measures=[_semantic_name(value) for value in list(measures)[:8] if _semantic_name(value)],
        confidence_score=max(0.0, min(1.0, confidence_value)),
    )


def _normalize_semantic_payload(payload: Any) -> SemanticWorkbookInterpretation | None:
    if not isinstance(payload, dict):
        return None
    sheets_payload = payload.get("sheets") or payload.get("sheet_summaries") or payload.get("worksheets") or []
    sheets = [item for item in (_normalize_sheet_summary(entry) for entry in list(sheets_payload)[:8]) if item is not None]
    semantic_summary = str(
        payload.get("semantic_summary")
        or payload.get("workbook_business_meaning")
        or payload.get("workbook_meaning")
        or payload.get("summary")
        or payload.get("description")
        or ""
    ).strip()
    workbook_story = str(
        payload.get("workbook_story")
        or payload.get("story")
        or payload.get("dashboard_story")
        or semantic_summary
    ).strip()
    dominant_domain = str(
        payload.get("dominant_domain")
        or payload.get("domain")
        or payload.get("business_domain")
        or "unknown"
    ).strip()
    confidence = payload.get("confidence_score") or payload.get("confidence") or 0.7
    try:
        confidence_value = float(confidence)
    except Exception:
        confidence_value = 0.7
    if not semantic_summary:
        return None
    return SemanticWorkbookInterpretation(
        semantic_summary=semantic_summary,
        workbook_story=workbook_story or semantic_summary,
        dominant_domain=dominant_domain or "unknown",
        sheets=sheets[:8],
        ambiguities=_normalize_ambiguities(payload.get("ambiguities")),
        business_questions=_normalize_questions(payload.get("business_questions") or payload.get("follow_up_questions")),
        confidence_score=max(0.0, min(1.0, confidence_value)),
    )


class SemanticSheetSummary(BaseModel):
    sheet_name: str
    likely_role: str
    primary_entities: list[str] = Field(default_factory=list, max_length=5)
    candidate_dimensions: list[str] = Field(default_factory=list, max_length=8)
    candidate_measures: list[str] = Field(default_factory=list, max_length=8)
    confidence_score: float = Field(ge=0.0, le=1.0)


class SemanticWorkbookInterpretation(BaseModel):
    semantic_summary: str
    workbook_story: str
    dominant_domain: str
    sheets: list[SemanticSheetSummary] = Field(default_factory=list, max_length=8)
    ambiguities: list[str] = Field(default_factory=list, max_length=6)
    business_questions: list[str] = Field(default_factory=list, max_length=6)
    confidence_score: float = Field(ge=0.0, le=1.0)


class PlannedDashboardConfig(BaseModel):
    title: str
    subtitle: str
    tabs: list[DashboardTabSpec] = Field(default_factory=list)
    kpi_cards: list[dict[str, Any]] = Field(default_factory=list)
    customization_prompts: list[str] = Field(default_factory=list, max_length=5)


class DashboardHypothesisPlan(BaseModel):
    title: str
    summary: str
    rationale: str
    confidence_score: float = Field(ge=0.3, le=1.0)
    config: PlannedDashboardConfig


class InvestigationPromptStep(BaseModel):
    key: str
    title: str
    objective: str
    tool: str
    rationale: str
    priority: int | None = None


class InvestigationPromptEvidence(BaseModel):
    key: str
    tool: str
    title: str
    detail: str
    confidence_score: float | None = None
    supporting_metrics: list[str] = Field(default_factory=list)


def _table_group(name: str) -> tuple[str | None, str | None]:
    match = re.match(r"^([A-Za-z]+)[ _-]+(.+)$", str(name or "").strip())
    if not match:
        return None, None
    return match.group(1), match.group(2)


def _sheet_summaries(profile_json: dict[str, Any]) -> list[dict[str, Any]]:
    sheets = []
    for sheet in profile_json.get("sheets", []):
        section_summaries = []
        for section in sheet.get("sections", []):
            fields = section.get("fields", [])
            section_summaries.append(
                {
                    "section_key": section.get("section_key"),
                    "section_label": section.get("section_label"),
                    "headers": [
                        {
                            "header_label": field.get("header_label"),
                            "sample_value": field.get("sample_value"),
                        }
                        for field in fields[:12]
                    ],
                }
            )
        sheets.append({"sheet_name": sheet.get("sheet_name"), "sections": section_summaries[:6]})
    return sheets[:8]


def _headers_for_sheet(sheet: dict[str, Any]) -> list[str]:
    return [
        str(header.get("header_label") or "").strip()
        for section in sheet.get("sections", [])
        for header in section.get("headers", [])
        if str(header.get("header_label") or "").strip()
    ]


def _reference_sheet_notes(raw_tables: list[RawTable]) -> list[str]:
    notes: list[str] = []
    for table in raw_tables:
        if str(table.name or "").strip().lower() not in {"definitions", "questions"}:
            continue
        for row in table.rows[:5]:
            parts = []
            for header in table.headers[:4]:
                value = str(row.get(header) or "").strip()
                if value:
                    parts.append(f"{header}: {value}")
            if parts:
                notes.append(" | ".join(parts[:3]))
    return notes[:8]


def _semantic_raw_table_inventory(raw_tables: list[RawTable]) -> list[dict[str, Any]]:
    inventory: list[dict[str, Any]] = []
    for table in raw_tables[:8]:
        if not table.headers:
            continue
        profiles = classify_table_fields(table)
        inventory.append(
            {
                "name": table.name,
                "row_count": len(table.rows),
                "headers": table.headers[:12],
                "dimensions": dimension_headers(table, profiles)[:5],
                "measures": measure_headers(table, profiles)[:6],
                "sample_rows": [
                    {
                        header: row.get(header)
                        for header in (dimension_headers(table, profiles)[:2] + measure_headers(table, profiles)[:4])[:6]
                        if row.get(header) not in (None, "")
                    }
                    for row in table.rows[:2]
                ],
            }
        )
    return inventory


def _semantic_pairing_evidence(raw_tables: list[RawTable], runtime_summary: dict[str, Any] | None) -> dict[str, Any]:
    groups: dict[str, dict[str, Any]] = {}
    repeated_prefixes = 0
    pool_sheet_count = 0
    bucket_headers: list[str] = []
    for table in raw_tables:
        prefix, period = _table_group(table.name)
        if prefix and period:
            repeated_prefixes += 1
            group = groups.setdefault(period, {"period": period, "series": [], "row_counts": {}, "headers": []})
            group["series"].append(prefix.upper())
            group["row_counts"][prefix.upper()] = len(table.rows)
            if not group["headers"]:
                group["headers"] = table.headers[:12]
        lowered_headers = {str(header or "").strip().lower() for header in table.headers}
        if "pools" in lowered_headers or "pool" in lowered_headers:
            pool_sheet_count += 1
            if not bucket_headers:
                bucket_headers = [
                    header
                    for header in table.headers
                    if str(header or "").strip().lower() not in {"pools", "pool"}
                ][:8]
    paired_groups = [
        {
            "period": period,
            "series": sorted(set(group["series"])),
            "row_counts": group["row_counts"],
            "headers": group["headers"],
        }
        for period, group in groups.items()
        if len(set(group["series"])) >= 2
    ]
    paired_groups.sort(key=lambda item: str(item["period"]))
    return {
        "repeated_prefix_count": repeated_prefixes,
        "pool_sheet_count": pool_sheet_count,
        "paired_group_count": len(paired_groups),
        "paired_groups": paired_groups[:6],
        "bucket_headers": bucket_headers[:8],
        "comparison_group_count": int((runtime_summary or {}).get("comparison_group_count") or 0),
    }


def _build_semantic_evidence(
    *,
    profile,
    runtime_summary: dict[str, Any] | None,
    raw_tables: list[RawTable],
    user_guidance: str | None,
) -> dict[str, Any]:
    return {
        "source_filename": getattr(profile, "source_filename", None),
        "workbook_type_hint": getattr(profile, "workbook_type", None),
        "runtime_summary": runtime_summary or {},
        "raw_table_inventory": _semantic_raw_table_inventory(raw_tables),
        "reference_notes": _reference_sheet_notes(raw_tables),
        "pairing_evidence": _semantic_pairing_evidence(raw_tables, runtime_summary),
        "user_guidance": user_guidance or "",
    }


def _infer_matrix_style_generic_workbook(
    profile_json: dict[str, Any],
    runtime_summary: dict[str, Any] | None = None,
    raw_tables: list[RawTable] | None = None,
    user_guidance: str | None = None,
) -> SemanticWorkbookInterpretation | None:
    sheets = _sheet_summaries(profile_json)
    if not sheets:
        return None
    raw_tables = raw_tables or []
    raw_sheets = profile_json.get("sheets", [])
    sheet_names = [str(sheet.get("sheet_name") or "") for sheet in raw_sheets]
    data_sheets = [
        sheet for sheet in raw_sheets
        if str(sheet.get("sheet_name") or "").lower() not in {"definitions", "questions"}
    ]
    repeated_prefixes = sum(
        1
        for name in sheet_names
        if name.lower().startswith(("tc_", "bc_", "tc ", "bc "))
    )
    common_headers = None
    for sheet in data_sheets[:4]:
        headers = {header.lower() for header in _headers_for_sheet(sheet)}
        common_headers = headers if common_headers is None else common_headers & headers
    common_headers = common_headers or set()
    pairing_evidence = _semantic_pairing_evidence(raw_tables, runtime_summary)
    raw_bucket_headers = pairing_evidence.get("bucket_headers") or []
    guidance_text = str(user_guidance or "").lower()
    guidance_supports_matrix = any(
        phrase in guidance_text
        for phrase in ("total count", "bad count", "pool", "quarter", "month", "tc", "bc", "distribution")
    )
    has_pool_headers = ("pools" in common_headers) or ("pools" in {header.lower() for sheet in data_sheets[:4] for header in _headers_for_sheet(sheet)})
    has_pairing_support = int(pairing_evidence.get("paired_group_count") or 0) >= 2 or int(pairing_evidence.get("comparison_group_count") or 0) >= 2
    if len(data_sheets) < 4 or repeated_prefixes < 4 or not (has_pool_headers or has_pairing_support or guidance_supports_matrix):
        return None

    candidate_measures = raw_bucket_headers or [header for header in _headers_for_sheet(data_sheets[0]) if header.lower() != "pools"][:8]
    candidate_questions = [
        "How do TC and BC distributions move across the available periods?",
        "Which pools contribute the highest concentration or deterioration over time?",
        "Which rating buckets dominate volume in each period?",
    ]
    if any(str(name).lower() == "questions" for name in sheet_names):
        candidate_questions.append("Should the dashboard explicitly answer the business prompts captured in the Questions sheet?")
    return SemanticWorkbookInterpretation(
        semantic_summary="The workbook looks like a repeated portfolio matrix across time periods, with pool-level distributions split across multiple rating or outcome buckets.",
        workbook_story="This is a multi-period portfolio monitoring workbook where the main need is comparing distributions across sheet pairs, surfacing the most material pools, and answering business questions from the reference tabs.",
        dominant_domain="portfolio analytics",
        sheets=[
            SemanticSheetSummary(
                sheet_name=str(sheet.get("sheet_name") or f"Sheet {idx + 1}"),
                likely_role="portfolio distribution matrix" if idx < 6 else "reference worksheet",
                primary_entities=["pool"],
                candidate_dimensions=["pool", "period", "series"],
                candidate_measures=candidate_measures,
                confidence_score=0.78 if idx < 6 else 0.68,
            )
            for idx, sheet in enumerate(sheets[:4])
        ],
        ambiguities=[
            "The workbook exposes repeated distribution matrices, but the exact business meaning of TC versus BC still needs confirmation." if not guidance_supports_matrix else "The workbook likely compares total-count and bad-count distributions, but the preferred executive cuts still need confirmation.",
            "The rating buckets are visible, but the preferred executive cuts and comparisons still need SME direction.",
        ],
        business_questions=candidate_questions[:6],
        confidence_score=0.84 if has_pairing_support or guidance_supports_matrix else 0.76,
    )


def _infer_work_hours_generic_workbook(
    profile_json: dict[str, Any],
    raw_tables: list[RawTable] | None = None,
) -> SemanticWorkbookInterpretation | None:
    sheets = profile_json.get("sheets", [])
    raw_tables = raw_tables or []
    if len(sheets) != 1 and len(raw_tables) != 1:
        return None
    source_name = str((raw_tables[0].name if raw_tables else sheets[0].get("sheet_name")) or "Sheet1")
    headers = {
        str(header).lower()
        for header in ((raw_tables[0].headers if raw_tables else _headers_for_sheet(sheets[0])))
    }
    has_hours_signal = any("hour" in header or "time" in header for header in headers)
    has_total_signal = any("total" in header for header in headers)
    has_people_signal = any(term in headers for term in {"employee", "preparer", "reviewer", "cursory reviewer"})
    has_client_signal = any(term in headers for term in {"client name", "tax payer", "status", "return type"})
    if not has_hours_signal or not has_total_signal or not (has_people_signal or has_client_signal):
        return None
    if not any(term in header for header in headers for term in ("entries", "time", "hour")):
        return None
    return SemanticWorkbookInterpretation(
        semantic_summary="The workbook appears to be an operational time-log and productivity report showing client, preparer, and reviewer workload across return-processing tasks and logged time.",
        workbook_story="This looks like a workflow productivity workbook where the main need is time distribution, workload concentration by client and team member, and the relationship between actual versus total logged effort.",
        dominant_domain="operations productivity",
        sheets=[
            SemanticSheetSummary(
                sheet_name=source_name,
                likely_role="time-log productivity report",
                primary_entities=["client", "preparer", "reviewer"],
                candidate_dimensions=["client name", "preparer", "reviewer", "status", "return type"],
                candidate_measures=["preparer actual time", "reviewer actual time", "preparer total time", "reviewer total time"],
                confidence_score=0.82,
            )
        ],
        ambiguities=[],
        business_questions=[
            "Which clients or team members consume disproportionate logged time?",
            "Should the dashboard emphasize client-level workload concentration, reviewer throughput, or actual-versus-total time leakage?",
        ],
        confidence_score=0.82,
    )


def _infer_process_quality_generic_workbook(
    profile_json: dict[str, Any],
    raw_tables: list[RawTable] | None = None,
) -> SemanticWorkbookInterpretation | None:
    sheets = profile_json.get("sheets", [])
    raw_tables = raw_tables or []
    if len(sheets) != 1 and len(raw_tables) != 1:
        return None
    source_name = str((raw_tables[0].name if raw_tables else sheets[0].get("sheet_name")) or "Sheet1")
    headers = {
        str(header).lower()
        for header in ((raw_tables[0].headers if raw_tables else _headers_for_sheet(sheets[0])))
    }
    required = {"machine", "charge_no", "total_qty", "process"}
    if not required <= headers:
        return None
    return SemanticWorkbookInterpretation(
        semantic_summary="The workbook appears to be a process-quality or manufacturing operations log centered on machine, charge, process step, timestamps, and produced quantity.",
        workbook_story="This looks like a heat-treatment operations workbook where the main need is process distribution, machine workload, quantity outliers, and operational traceability.",
        dominant_domain="manufacturing operations",
        sheets=[
            SemanticSheetSummary(
                sheet_name=source_name,
                likely_role="process operations log",
                primary_entities=["machine", "charge"],
                candidate_dimensions=["machine", "process", "charge_no"],
                candidate_measures=["total_qty"],
                confidence_score=0.84,
            )
        ],
        ambiguities=[],
        business_questions=[
            "Which machines or processes account for the largest quantity throughput?",
            "Should the dashboard emphasize process stability, machine utilization, or quality-risk outliers?",
        ],
        confidence_score=0.84,
    )


def _infer_collections_dues_generic_workbook(
    profile_json: dict[str, Any],
    raw_tables: list[RawTable] | None = None,
    user_guidance: str | None = None,
) -> SemanticWorkbookInterpretation | None:
    raw_tables = raw_tables or []
    data_tables = [table for table in raw_tables if str(table.name or "").strip().lower() not in {"definitions", "questions"}]
    if len(data_tables) < 2:
        return None
    lowered_headers = {
        str(header or "").strip().lower()
        for table in data_tables[:6]
        for header in table.headers
    }
    guidance_text = str(user_guidance or "").lower()
    has_dues = any(term in lowered_headers for term in {"total dues", "dues", "amount due", "outstanding"}) or "dues" in guidance_text
    has_penalty = any(term in lowered_headers for term in {"accumulated penalty", "penalty", "late fee"}) or "penalty" in guidance_text
    has_unit = any(term in lowered_headers for term in {"unit", "house", "number", "tower"}) or any(term in guidance_text for term in ("tower", "unit", "house"))
    quarter_named = sum(1 for table in data_tables if re.search(r"\bq[1-4]\b", str(table.name).lower())) >= 2
    if not ((has_dues or has_penalty) and has_unit):
        return None
    dimension_names = ["tower", "unit", "owner"]
    measure_names = ["total dues", "accumulated penalty", "count"]
    questions = [
        "Which towers carry the highest outstanding dues and accumulated penalty?",
        "How do dues and penalty shift across the available quarter sheets?",
        "Which owners or units dominate the highest-risk dues exposure?",
    ]
    if "pie" in guidance_text:
        questions.append("Should the tower dues share be emphasized with a pie-style composition view?")
    if "gantt" in guidance_text:
        questions.append("Is there a date-aligned collections timeline available to justify a gantt-style follow-up view?")
    return SemanticWorkbookInterpretation(
        semantic_summary="The workbook appears to be a collections or CAM dues workbook with unit-level balances across multiple quarter sheets, including total dues and accumulated penalty.",
        workbook_story="This looks like a multi-period dues analysis workbook where the dashboard should rank towers, compare outstanding amount and penalty across quarters, and surface the owners or units driving the largest exposure.",
        dominant_domain="collections analytics",
        sheets=[
            SemanticSheetSummary(
                sheet_name=table.name,
                likely_role="collections period ledger" if quarter_named else "dues ledger",
                primary_entities=["tower", "unit", "owner"],
                candidate_dimensions=dimension_names,
                candidate_measures=measure_names,
                confidence_score=0.84,
            )
            for table in data_tables[:6]
        ],
        ambiguities=[],
        business_questions=questions[:6],
        confidence_score=0.86 if quarter_named else 0.8,
    )


def _fallback_interpretation(
    profile,
    runtime_summary: dict[str, Any] | None,
    raw_tables: list[RawTable] | None = None,
    user_guidance: str | None = None,
) -> SemanticWorkbookInterpretation:
    workbook_type = str(profile.workbook_type or "generic_workbook_v1")
    profile_json = profile.profile_json or {}
    sheets = _sheet_summaries(profile_json)
    raw_tables = raw_tables or []
    if workbook_type == "contracted_actual_v1":
        return SemanticWorkbookInterpretation(
            semantic_summary="The workbook appears to track contracted, received, and pending tax-return workload by client and staff.",
            workbook_story="This looks like an operational delivery workbook where the main question is execution pace versus contracted volume.",
            dominant_domain="tax operations",
            sheets=[
                SemanticSheetSummary(
                    sheet_name="Clients and Staff",
                    likely_role="delivery performance tracking",
                    primary_entities=["client", "staff"],
                    candidate_dimensions=["client type", "staff type", "risk band"],
                    candidate_measures=["contracted total", "received total", "pending total", "receipt rate"],
                    confidence_score=0.82,
                )
            ],
            ambiguities=[],
            business_questions=[
                "Should the dashboard optimize for client-level backlog recovery or staff throughput review first?",
                "Do business and individual returns need separate executive treatment?",
            ],
            confidence_score=0.82,
        )
    if workbook_type == "product_master_v1":
        return SemanticWorkbookInterpretation(
            semantic_summary="The workbook appears to be a product master catalog focused on type, category, unit-of-measure, and attribute consistency.",
            workbook_story="This looks like a catalog-governance workbook where the main question is structural quality and concentration, not transaction performance.",
            dominant_domain="product master data",
            sheets=[
                SemanticSheetSummary(
                    sheet_name=str((sheets[0] or {}).get("sheet_name") if sheets else "Catalog"),
                    likely_role="master-data catalog",
                    primary_entities=["product"],
                    candidate_dimensions=["product type", "product category", "base uom", "hsn code"],
                    candidate_measures=["product count"],
                    confidence_score=0.84,
                )
            ],
            ambiguities=[],
            business_questions=[
                "Is the main dashboard goal catalog governance, reporting readiness, or data-cleansing prioritization?",
                "Should category concentration and data quality be shown together or as separate tabs?",
            ],
            confidence_score=0.84,
        )
    if workbook_type == "client_status_report_v1":
        return SemanticWorkbookInterpretation(
            semantic_summary="The workbook appears to track operational return-status progression, queue health, and note-driven exceptions.",
            workbook_story="This looks like a pipeline-control workbook where status bottlenecks and stale work are more important than contracted-versus-actual variance.",
            dominant_domain="workflow operations",
            sheets=[
                SemanticSheetSummary(
                    sheet_name=str((sheets[0] or {}).get("sheet_name") if sheets else "Pipeline"),
                    likely_role="status pipeline",
                    primary_entities=["return", "queue item"],
                    candidate_dimensions=["status", "return type", "owner"],
                    candidate_measures=["open count", "completed count", "aged items"],
                    confidence_score=0.8,
                )
            ],
            ambiguities=[],
            business_questions=[
                "Should the dashboard emphasize queue control, SLA risk, or analyst workload?",
                "Are notes meant to be treated as blockers, escalations, or commentary only?",
            ],
            confidence_score=0.8,
        )
    inferred = _infer_matrix_style_generic_workbook(
        profile_json,
        runtime_summary=runtime_summary,
        raw_tables=raw_tables,
        user_guidance=user_guidance,
    )
    if inferred is not None:
        return inferred
    inferred = _infer_work_hours_generic_workbook(profile_json, raw_tables=raw_tables)
    if inferred is not None:
        return inferred
    inferred = _infer_process_quality_generic_workbook(profile_json, raw_tables=raw_tables)
    if inferred is not None:
        return inferred
    inferred = _infer_collections_dues_generic_workbook(profile_json, raw_tables=raw_tables, user_guidance=user_guidance)
    if inferred is not None:
        return inferred
    return SemanticWorkbookInterpretation(
        semantic_summary="The workbook structure is visible, but the business meaning of the headers is still only partially grounded.",
        workbook_story="This looks like a non-standard workbook that needs semantic interpretation before a reliable executive dashboard can be finalized.",
        dominant_domain="unknown",
        sheets=[
            SemanticSheetSummary(
                sheet_name=str(sheet.get("sheet_name") or f"Sheet {idx + 1}"),
                likely_role="unresolved worksheet role",
                primary_entities=[],
                candidate_dimensions=[header.get("header_label") for section in sheet.get("sections", []) for header in section.get("headers", [])[:3]][:6],
                candidate_measures=[],
                confidence_score=0.45,
            )
            for idx, sheet in enumerate(sheets[:3])
        ],
        ambiguities=[
            "The workbook does not clearly map to a stable business family from header structure alone.",
            "Several columns may be descriptive fields rather than KPI-ready dimensions or measures.",
        ],
        business_questions=[
            "What business decision should this dashboard support first?",
            "Which columns represent core outcome measures versus descriptive attributes?",
        ],
        confidence_score=0.45,
    )


def interpret_workbook_semantics(
    *,
    profile,
    runtime_summary: dict[str, Any] | None,
    raw_tables: list[RawTable] | None,
    gemini: GeminiRequestSettings | None,
    user_guidance: str | None = None,
) -> SemanticWorkbookInterpretation:
    available_raw_tables = raw_tables or []
    log_app_event(
        level="info",
        state="started",
        category="semantic_interpreter",
        event="semantic_interpretation_started",
        agent_name="semantic_interpreter",
        workflow="semantic_interpretation_v1",
        model_name=getattr(gemini, "model", None) if gemini is not None else None,
        message="Semantic interpretation started",
        payload={"has_gemini": gemini is not None, "raw_table_count": len(available_raw_tables)},
        snapshot_id=getattr(profile, "snapshot_id", None),
    )
    semantic_evidence = _build_semantic_evidence(
        profile=profile,
        runtime_summary=runtime_summary,
        raw_tables=available_raw_tables,
        user_guidance=user_guidance,
    )
    if gemini is None:
        logger.info("dashboard semantics: Gemini unavailable, using deterministic interpretation fallback for snapshot profile %s", getattr(profile, "id", "unknown"))
        log_app_event(
            level="warning",
            state="fallback",
            category="semantic_interpreter",
            event="semantic_interpretation_fallback",
            agent_name="semantic_interpreter",
            workflow="semantic_interpretation_v1:fallback",
            message="Gemini unavailable, using deterministic semantic interpretation fallback",
            snapshot_id=getattr(profile, "snapshot_id", None),
        )
        return _fallback_interpretation(
            profile,
            runtime_summary,
            raw_tables=available_raw_tables,
            user_guidance=user_guidance,
        )

    prompt = json.dumps(
        {
            "role": "You are the workbook semantic interpreter for adaptive business dashboards.",
            "task": [
                "Infer the likely business meaning of each sheet from headers, section labels, sample values, runtime clues, and raw-table evidence.",
                "Identify likely business entities, candidate dimensions, candidate measures, and the dominant business domain.",
                "Use pairing evidence, reference-sheet notes, and sample rows to resolve workbook-level semantics before asking generic questions.",
                "List ambiguity explicitly when the workbook meaning is unclear.",
                "Ask follow-up business questions only when those answers would materially improve dashboard quality.",
                "Do not invent columns, metrics, or relationships not supported by the workbook profile.",
            ],
            "workbook_type_hint": getattr(profile, "workbook_type", None),
            "workbook_profile": {
                "source_filename": getattr(profile, "source_filename", None),
                "sheets": _sheet_summaries(profile.profile_json or {}),
            },
            "semantic_evidence": semantic_evidence,
            "output_rules": [
                "Return JSON only.",
                "Be analytical and explicit about uncertainty.",
                "Prefer business semantics over technical wording.",
                "Avoid generic questions when the workbook meaning is already strongly supported by the evidence.",
            ],
        },
        ensure_ascii=False,
    )
    try:
        interpretation = gemini_generate_structured(
            prompt=prompt,
            schema=SemanticWorkbookInterpretation,
            settings=gemini,
        )
        logger.info(
            "dashboard semantics: Gemini interpretation succeeded for snapshot profile %s with confidence %.2f",
            getattr(profile, "id", "unknown"),
            interpretation.confidence_score,
        )
        log_app_event(
            level="info",
            state="completed",
            category="semantic_interpreter",
            event="semantic_interpretation_completed",
            agent_name="semantic_interpreter",
            workflow="semantic_interpretation_v1",
            model_name=getattr(gemini, "model", None) if gemini is not None else None,
            message="Gemini semantic interpretation succeeded",
            payload={"confidence_score": interpretation.confidence_score, "dominant_domain": interpretation.dominant_domain},
            snapshot_id=getattr(profile, "snapshot_id", None),
        )
        return interpretation
    except GeminiReasoningError as exc:
        try:
            repaired = _normalize_semantic_payload(gemini_generate_json(prompt=prompt, settings=gemini))
            if repaired is not None:
                logger.info(
                    "dashboard semantics: Gemini interpretation repaired for snapshot profile %s with confidence %.2f",
                    getattr(profile, "id", "unknown"),
                    repaired.confidence_score,
                )
                log_app_event(
                    level="warning",
                    state="repaired",
                    category="semantic_interpreter",
                    event="semantic_interpretation_repaired",
                    agent_name="semantic_interpreter",
                    workflow="semantic_interpretation_v1",
                    model_name=getattr(gemini, "model", None) if gemini is not None else None,
                    message="Gemini semantic interpretation repaired from raw JSON output",
                    payload={"confidence_score": repaired.confidence_score, "dominant_domain": repaired.dominant_domain},
                    snapshot_id=getattr(profile, "snapshot_id", None),
                )
                return repaired
        except GeminiReasoningError:
            pass
        logger.warning(
            "dashboard semantics: Gemini interpretation failed for snapshot profile %s, falling back to deterministic interpretation: %s",
            getattr(profile, "id", "unknown"),
            exc,
        )
        log_app_event(
            level="warning",
            state="fallback",
            category="semantic_interpreter",
            event="semantic_interpretation_failed_fallback",
            agent_name="semantic_interpreter",
            workflow="semantic_interpretation_v1:fallback",
            model_name=getattr(gemini, "model", None) if gemini is not None else None,
            message="Gemini semantic interpretation failed; deterministic fallback used",
            detail=str(exc),
            snapshot_id=getattr(profile, "snapshot_id", None),
        )
        return _fallback_interpretation(
            profile,
            runtime_summary,
            raw_tables=available_raw_tables,
            user_guidance=user_guidance,
        )


def _sanitize_tabs(base_tabs: list[dict[str, Any]], planned_tabs: list[DashboardTabSpec]) -> list[dict[str, Any]]:
    planned_by_key = {tab.key: tab for tab in planned_tabs}
    sanitized: list[dict[str, Any]] = []
    for base_tab in base_tabs:
        planned = planned_by_key.get(str(base_tab.get("key") or ""))
        if planned is None:
            sanitized.append(base_tab)
            continue
        sanitized.append(
            {
                **base_tab,
                "label": planned.label,
                "description": planned.description,
                "sections": [section.model_dump() for section in planned.sections] or base_tab.get("sections", []),
            }
        )
    return sanitized


def _sanitize_kpi_cards(base_cards: list[dict[str, Any]], planned_cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not planned_cards:
        return base_cards
    planned_by_key = {str(item.get("key") or ""): item for item in planned_cards}
    sanitized: list[dict[str, Any]] = []
    for base_card in base_cards:
        key = str(base_card.get("key") or "")
        planned = planned_by_key.get(key)
        if planned is None:
            sanitized.append(base_card)
            continue
        sanitized.append({"key": key, "label": str(planned.get("label") or base_card.get("label") or key)})
    return sanitized


def _adaptive_generic_tabs(
    interpretation: SemanticWorkbookInterpretation,
    chart_preferences: list[str],
    user_guidance: str | None,
) -> list[dict[str, Any]]:
    guidance_text = str(user_guidance or "").lower()
    dominant_entity = next((entity for sheet in interpretation.sheets for entity in sheet.primary_entities if entity), "entity")
    dominant_measure = next((measure for sheet in interpretation.sheets for measure in sheet.candidate_measures if measure), "value")
    summary_text = f"{interpretation.semantic_summary} {interpretation.workbook_story}".lower()
    has_time_signal = any(token in summary_text or token in guidance_text for token in ("trend", "forecast", "period", "quarter", "month", "year", "timeline"))
    has_comparison_signal = any(token in guidance_text for token in ("compare", "comparison", "versus", "vs", "benchmark", "side by side"))
    has_drilldown_signal = any(token in guidance_text for token in ("particular item", "specific item", "drilldown", "drill-down", "detail table", "detail view", "comparison table"))
    requests_odr_top5_by_month = (
        any(token in guidance_text for token in ("odr", "default rate"))
        and "month" in guidance_text
        and any(token in guidance_text for token in ("top 5", "top five", "top-5"))
    )
    needs_quality = bool(interpretation.ambiguities) or any(token in guidance_text for token in ("quality", "missing", "exception", "outlier", "coverage"))
    primary_chart = "pie" if "pie" in chart_preferences else "bar"
    trend_chart = "scatter" if "scatter" in chart_preferences else ("gantt" if "gantt" in chart_preferences else "line")
    tabs = [
        {
            "key": "overview",
            "label": "Overview",
            "description": "Entity-level exposure, concentration, and headline metrics.",
            "sections": [
                {"key": "overview_metrics", "label": "Exposure Metrics", "description": "Primary KPI cards for the workbook.", "renderer": "adaptive_widget", "slot": "full_width", "widget_type": "metric_grid"},
                {"key": "overview_ranked", "label": "Top Exposure", "description": "Top entities by the strongest inferred measure.", "renderer": "adaptive_widget", "slot": "full_width", "widget_type": primary_chart},
            ],
        },
        {
            "key": "analysis",
            "label": f"{dominant_entity.title()} Analysis" if dominant_entity not in {"", "entity"} else "Analysis",
            "description": "Concentration and composition views based on the inferred business entities and measures.",
            "sections": [
                {"key": "analysis_secondary", "label": "Secondary Measure", "description": "Secondary measure by primary entity.", "renderer": "adaptive_widget", "slot": "full_width", "widget_type": primary_chart},
                {"key": "analysis_mix", "label": "Composition", "description": "Share of major contributors in the portfolio.", "renderer": "adaptive_widget", "slot": "main_right", "widget_type": "pie" if "pie" in chart_preferences else "bar"},
            ],
        },
    ]
    if has_comparison_signal or len({measure for sheet in interpretation.sheets for measure in sheet.candidate_measures if measure}) > 1:
        tabs.append(
            {
                "key": "comparison",
                "label": "Comparison",
                "description": f"Side-by-side views for {dominant_entity} and {dominant_measure}.",
                "sections": [
                    {"key": "comparison_table", "label": "Comparison Table", "description": "Ranked comparison table for the leading entities and measures.", "renderer": "adaptive_widget", "slot": "full_width", "widget_type": "table"},
                    {"key": "comparison_chart", "label": "Comparison Chart", "description": "Direct comparison chart for the selected measures.", "renderer": "adaptive_widget", "slot": "full_width", "widget_type": primary_chart},
                ],
            }
        )
    if has_time_signal:
        trend_sections = [
            {"key": "trend_primary", "label": "Period Trend", "description": "Primary measure over time.", "renderer": "adaptive_widget", "slot": "full_width", "widget_type": trend_chart},
            {"key": "trend_forecast", "label": "Forecast Outlook", "description": "Deterministic next-period direction when enough history exists.", "renderer": "adaptive_widget", "slot": "main_right", "widget_type": "bar"},
        ]
        if requests_odr_top5_by_month:
            trend_sections.insert(
                0,
                {
                    "key": "odr_top5_month",
                    "label": "Top 5 ODR by Month",
                    "description": "Select a month and compare the top five pools by ODR for that period.",
                    "renderer": "adaptive_widget",
                    "slot": "full_width",
                    "widget_type": "bar",
                },
            )
        tabs.append(
            {
                "key": "trends",
                "label": "Trends",
                "description": "Period movement and requested timeline-oriented analysis where data supports it.",
                "sections": trend_sections,
            }
        )
    if has_drilldown_signal:
        tabs.append(
            {
                "key": "drilldown",
                "label": "Detail View",
                "description": f"Focused detail tables and item-level analysis for {dominant_entity}.",
                "sections": [
                    {"key": "detail_table", "label": "Detail Table", "description": "Drill into a particular item or subset requested by the SME.", "renderer": "adaptive_widget", "slot": "full_width", "widget_type": "table"},
                ],
            }
        )
    if needs_quality:
        tabs.append(
            {
                "key": "quality",
                "label": "Quality",
                "description": "Exceptions, data quality signals, and unresolved analytical limits.",
                "sections": [
                    {"key": "quality_flags", "label": "Quality Flags", "description": "Key data-quality or modeling risks.", "renderer": "adaptive_widget", "slot": "full_width", "widget_type": "table"},
                ],
            }
        )
    return tabs


def _adaptive_generic_kpis(interpretation: SemanticWorkbookInterpretation) -> list[dict[str, Any]]:
    measures = [measure for sheet in interpretation.sheets for measure in sheet.candidate_measures][:3]
    cards = [{"key": "entity_count", "label": "Entities"}]
    for measure in measures:
        cards.append({"key": _slug(measure), "label": measure.title()})
    cards.append({"key": "period_count", "label": "Periods"})
    return cards[:5]


def _build_adaptive_generic_hypothesis(
    *,
    interpretation: SemanticWorkbookInterpretation,
    base_title: str,
    base_summary: str,
    base_rationale: str,
    base_confidence: float,
    next_config: dict[str, Any],
    investigation_evidence: list[dict[str, Any]],
    user_guidance: str | None,
) -> tuple[str, str, dict[str, Any], str, float]:
    chart_preferences = _extract_chart_preferences(user_guidance)
    dominant_measure = next((measure for sheet in interpretation.sheets for measure in sheet.candidate_measures if measure), "value")
    primary_entity = next((entity for sheet in interpretation.sheets for entity in sheet.primary_entities if entity), "entity")
    dominant_domain = interpretation.dominant_domain
    summary_text = f"{interpretation.semantic_summary} {interpretation.workbook_story}".lower()
    if dominant_domain in {"unknown", ""} and any(token in summary_text for token in ("cam", "dues", "penalty", "tower", "property", "maintenance")):
        dominant_domain = "collections analytics"
    if primary_entity in {"entity", "", "property unit"} and "tower" in summary_text:
        primary_entity = "tower"
    if dominant_measure in {"value", "", "measure"}:
        if "dues" in summary_text:
            dominant_measure = "total dues"
        elif "penalty" in summary_text:
            dominant_measure = "accumulated penalty"
    if any(token in str(user_guidance or "").lower() for token in ("odr", "default rate")) and any(token in summary_text for token in ("portfolio", "bad count", "total count")):
        dominant_measure = "ODR"
    story = interpretation.workbook_story or base_summary or interpretation.semantic_summary
    title = base_title
    if dominant_domain == "collections analytics":
        title = "Collections Exposure Dashboard"
    elif dominant_domain not in {"unknown", ""}:
        title = f"{dominant_domain.title()} Dashboard"
    summary = story
    next_config["layout_template"] = "adaptive_semantic"
    next_config["title"] = title
    next_config["subtitle"] = f"Adaptive semantic dashboard centered on {primary_entity} and {dominant_measure}."
    next_config["tabs"] = _adaptive_generic_tabs(interpretation, chart_preferences, user_guidance)
    next_config["kpi_cards"] = _adaptive_generic_kpis(interpretation)
    next_config["customization_prompts"] = interpretation.business_questions[:5]
    next_config["chart_preferences"] = chart_preferences
    next_config["adaptive_dashboard_enabled"] = True
    next_config["adaptive_dashboard_spec"] = {
        "domain": dominant_domain,
        "primary_entity": primary_entity,
        "primary_measure": dominant_measure,
        "secondary_measures": [measure for sheet in interpretation.sheets for measure in sheet.candidate_measures if measure and measure != dominant_measure][:3],
        "chart_preferences": chart_preferences,
        "requested_features": (
            ["odr_top5_by_month"]
            if any(token in str(user_guidance or "").lower() for token in ("odr", "default rate"))
            and "month" in str(user_guidance or "").lower()
            and any(token in str(user_guidance or "").lower() for token in ("top 5", "top five", "top-5"))
            else []
        ),
        "controls": (
            ["month_select"]
            if any(token in str(user_guidance or "").lower() for token in ("odr", "default rate"))
            and "month" in str(user_guidance or "").lower()
            else []
        ),
        "questions": interpretation.business_questions[:4],
        "evidence_titles": [str(item.get("title") or "") for item in investigation_evidence[:6] if str(item.get("title") or "").strip()],
    }
    rationale = (
        f"{base_rationale} The generic fallback was upgraded into an adaptive semantic dashboard so unknown workbooks can still produce business-oriented charts and rankings. "
        f"The design is driven by the inferred domain '{dominant_domain}', the primary entity '{primary_entity}', and the strongest measure '{dominant_measure}'."
    )
    confidence = min(0.97, max(base_confidence, interpretation.confidence_score))
    return title, summary, next_config, rationale, confidence


def plan_dashboard_hypothesis(
    *,
    profile,
    runtime_summary: dict[str, Any] | None,
    base_title: str,
    base_summary: str,
    base_config: dict[str, Any],
    base_rationale: str,
    base_confidence: float,
    interpretation: SemanticWorkbookInterpretation,
    gemini: GeminiRequestSettings | None,
    investigation_plan: Any | None = None,
    investigation_evidence: list[Any] | None = None,
    user_guidance: str | None = None,
) -> tuple[str, str, dict[str, Any], str, float]:
    next_config = dict(base_config)
    next_config["semantic_summary"] = interpretation.semantic_summary
    next_config["business_questions"] = interpretation.business_questions
    next_config["ambiguities"] = interpretation.ambiguities
    next_config["semantic_confidence"] = interpretation.confidence_score
    plan_steps = [
        InvestigationPromptStep.model_validate(step.model_dump() if hasattr(step, "model_dump") else step).model_dump()
        for step in getattr(investigation_plan, "steps", [])[:6]
    ]
    evidence_items = [
        InvestigationPromptEvidence.model_validate(item.model_dump() if hasattr(item, "model_dump") else item).model_dump()
        for item in (investigation_evidence or [])[:8]
    ]

    if str(base_config.get("dashboard_family") or "") == "generic_review_dashboard":
        return _build_adaptive_generic_hypothesis(
            interpretation=interpretation,
            base_title=base_title,
            base_summary=base_summary,
            base_rationale=base_rationale,
            base_confidence=base_confidence,
            next_config=next_config,
            investigation_evidence=evidence_items,
            user_guidance=user_guidance,
        )

    if gemini is None:
        next_config["customization_prompts"] = interpretation.business_questions[:5]
        log_app_event(
            level="warning",
            state="fallback",
            category="dashboard_hypothesis",
            event="hypothesis_fallback",
            agent_name="dashboard_hypothesis_planner",
            workflow="dashboard_hypothesis_v1:fallback",
            message="Gemini unavailable; deterministic dashboard hypothesis used",
            snapshot_id=getattr(profile, "snapshot_id", None),
        )
        return (
            base_title,
            interpretation.workbook_story,
            next_config,
            f"{base_rationale} Added deterministic semantic interpretation, an explicit investigation plan, and evidence-backed ambiguity tracking before final dashboard approval.",
            min(0.95, max(base_confidence, interpretation.confidence_score)),
        )

    prompt = json.dumps(
        {
            "role": "You are the dashboard hypothesis planner for business users.",
            "task": [
                "Use the semantic interpretation to produce a stronger dashboard proposal than the baseline.",
                "Keep the same dashboard_family, layout_template, and top-level tab keys/order as the baseline config.",
                "Improve the title, summary, rationale, KPI labels, tab descriptions, and customization prompts.",
                "Reflect ambiguity openly rather than pretending certainty.",
                "Use the investigation evidence to justify what the dashboard should emphasize.",
                "Make the proposal sound like an analyst who understands the workbook's business meaning.",
            ],
            "semantic_interpretation": interpretation.model_dump(),
            "user_guidance": user_guidance or "",
            "investigation_plan": {
                "steps": plan_steps,
                "unresolved_questions": list(getattr(investigation_plan, "unresolved_questions", [])[:6]),
            },
            "investigation_evidence": evidence_items,
            "runtime_summary": runtime_summary or {},
            "baseline_proposal": {
                "title": base_title,
                "summary": base_summary,
                "rationale": base_rationale,
                "confidence_score": base_confidence,
                "config": {
                    "dashboard_family": base_config.get("dashboard_family"),
                    "layout_template": base_config.get("layout_template"),
                    "title": base_config.get("title"),
                    "subtitle": base_config.get("subtitle"),
                    "tabs": base_config.get("tabs", []),
                    "kpi_cards": base_config.get("kpi_cards", []),
                },
            },
            "output_rules": [
                "Return JSON only.",
                "Do not change dashboard_family or layout_template.",
                "Do not change top-level tab keys or order.",
                "Do not invent unsupported widgets or metrics.",
            ],
        },
        ensure_ascii=False,
    )
    try:
        planned = gemini_generate_structured(prompt=prompt, schema=DashboardHypothesisPlan, settings=gemini)
    except GeminiReasoningError as exc:
        logger.warning(
            "dashboard semantics: Gemini hypothesis planning failed for snapshot profile %s, using deterministic hypothesis fallback: %s",
            getattr(profile, "id", "unknown"),
            exc,
        )
        next_config["customization_prompts"] = interpretation.business_questions[:5]
        log_app_event(
            level="warning",
            state="fallback",
            category="dashboard_hypothesis",
            event="hypothesis_failed_fallback",
            agent_name="dashboard_hypothesis_planner",
            workflow="dashboard_hypothesis_v1:fallback",
            model_name=getattr(gemini, "model", None) if gemini is not None else None,
            message="Gemini dashboard hypothesis failed; deterministic fallback used",
            detail=str(exc),
            snapshot_id=getattr(profile, "snapshot_id", None),
        )
        return (
            base_title,
            interpretation.workbook_story,
            next_config,
            f"{base_rationale} Gemini hypothesis planning failed, so the deterministic semantic interpretation and investigation evidence were retained instead.",
            min(0.95, max(base_confidence, interpretation.confidence_score)),
        )

    next_config["title"] = planned.config.title
    next_config["subtitle"] = planned.config.subtitle
    next_config["tabs"] = _sanitize_tabs(base_config.get("tabs", []), planned.config.tabs)
    next_config["kpi_cards"] = _sanitize_kpi_cards(base_config.get("kpi_cards", []), planned.config.kpi_cards)
    next_config["customization_prompts"] = planned.config.customization_prompts or interpretation.business_questions[:5]
    logger.info(
        "dashboard semantics: Gemini hypothesis planning succeeded for snapshot profile %s with confidence %.2f",
        getattr(profile, "id", "unknown"),
        planned.confidence_score,
    )
    log_app_event(
        level="info",
        state="completed",
        category="dashboard_hypothesis",
        event="hypothesis_completed",
        agent_name="dashboard_hypothesis_planner",
        workflow="dashboard_hypothesis_v1",
        model_name=getattr(gemini, "model", None) if gemini is not None else None,
        message="Gemini dashboard hypothesis planning succeeded",
        payload={"confidence_score": planned.confidence_score, "title": planned.title},
        snapshot_id=getattr(profile, "snapshot_id", None),
    )
    return planned.title, planned.summary, next_config, planned.rationale, planned.confidence_score
