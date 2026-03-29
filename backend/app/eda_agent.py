from __future__ import annotations

import json
import logging
import math
import re
from typing import Any, Literal, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field

from app.app_logs import log_app_event
from app.dashboard_duckdb_tools import run_dashboard_duckdb_tool
from app.dashboard_semantics import SemanticWorkbookInterpretation
from app.field_roles import classify_table_fields, coerce_measure_value, measure_headers
from app.gemini_reasoning import GeminiReasoningError, GeminiRequestSettings, gemini_generate_json, gemini_generate_structured
from app.raw_data_store import RawTable


logger = logging.getLogger(__name__)

EdaToolName = Literal[
    "schema_inventory_scan",
    "semantic_alignment_scan",
    "runtime_signal_scan",
    "concentration_signal_scan",
    "ambiguity_review",
    "dashboard_coverage_check",
    "distribution_sql_scan",
    "top_dimension_sql_scan",
    "measure_by_dimension_sql_scan",
    "quality_gap_sql_scan",
    "cross_dimension_sql_scan",
    "duplicate_scan",
    "outlier_scan",
    "period_alignment_scan",
    "forecast_signal_scan",
]

ALLOWED_EDA_TOOLS: dict[str, str] = {
    "schema_inventory_scan": "Summarize workbook structure, sheet roles, and header density.",
    "semantic_alignment_scan": "Check whether inferred dimensions and measures are supported by visible headers.",
    "runtime_signal_scan": "Surface the strongest currently available operational or catalog signals from runtime data.",
    "concentration_signal_scan": "Check whether key runtime distributions are concentrated or fragmented.",
    "ambiguity_review": "List unresolved semantic questions that still block confident analytical design.",
    "dashboard_coverage_check": "Check whether the interpreted business questions are covered by the current analysis scope.",
    "distribution_sql_scan": "Run a DuckDB distribution scan over preserved raw tables to identify dominant business segments.",
    "top_dimension_sql_scan": "Run a DuckDB top-segment scan over the highest-signal categorical field.",
    "measure_by_dimension_sql_scan": "Run a DuckDB grouped-measure scan to find where the main numeric workload or volume sits.",
    "quality_gap_sql_scan": "Run a DuckDB null-hotspot scan on core business fields.",
    "cross_dimension_sql_scan": "Run a DuckDB cross-segment scan to surface dominant combinations across two business dimensions.",
    "duplicate_scan": "Detect exact duplicate rows or repeated analytical records in the primary raw table.",
    "outlier_scan": "Detect numeric outlier concentrations that could distort charts or mislead stakeholders.",
    "period_alignment_scan": "Check whether multi-period sheets align cleanly into comparable monthly or quarterly groups.",
    "forecast_signal_scan": "Assess trend direction and produce a deterministic next-period projection when enough aligned history exists.",
}


class EDAPlanStep(BaseModel):
    key: str
    title: str
    objective: str
    tool: EdaToolName
    rationale: str
    priority: int = Field(ge=1, le=5)


class EDAPlan(BaseModel):
    summary: str
    steps: list[EDAPlanStep] = Field(default_factory=list, max_length=12)
    unresolved_questions: list[str] = Field(default_factory=list, max_length=8)


class EDAEvidence(BaseModel):
    key: str
    tool: EdaToolName
    title: str
    detail: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    supporting_metrics: list[str] = Field(default_factory=list, max_length=8)


class EDAAgentResult(BaseModel):
    plan: EDAPlan
    evidence: list[EDAEvidence] = Field(default_factory=list)
    workflow_mode: str


class EDAAgentState(TypedDict, total=False):
    plan: EDAPlan
    evidence: list[EDAEvidence]


def _normalize_eda_tool(value: Any) -> EdaToolName | None:
    text = str(value or "").strip()
    if text in ALLOWED_EDA_TOOLS:
        return text  # type: ignore[return-value]
    lowered = text.lower().replace(" ", "_").replace("-", "_")
    alias_map = {
        "distribution_scan": "distribution_sql_scan",
        "distribution": "distribution_sql_scan",
        "top_dimension_scan": "top_dimension_sql_scan",
        "top_segment_scan": "top_dimension_sql_scan",
        "measure_scan": "measure_by_dimension_sql_scan",
        "grouped_measure_scan": "measure_by_dimension_sql_scan",
        "quality_scan": "quality_gap_sql_scan",
        "quality_gap_scan": "quality_gap_sql_scan",
        "cross_dimension_scan": "cross_dimension_sql_scan",
        "duplicates": "duplicate_scan",
        "duplicate_check": "duplicate_scan",
        "outliers": "outlier_scan",
        "period_alignment": "period_alignment_scan",
        "forecast": "forecast_signal_scan",
        "forecast_readiness": "forecast_signal_scan",
    }
    mapped = alias_map.get(lowered)
    if mapped in ALLOWED_EDA_TOOLS:
        return mapped  # type: ignore[return-value]
    return None


def _normalize_eda_plan_payload(payload: Any, fallback: EDAPlan) -> EDAPlan | None:
    if isinstance(payload, list):
        raw_steps = payload
        summary = fallback.summary
        unresolved = fallback.unresolved_questions
    elif isinstance(payload, dict):
        raw_steps = payload.get("steps") or payload.get("plan") or payload.get("tool_calls") or []
        summary = str(payload.get("summary") or payload.get("plan_summary") or fallback.summary).strip()
        unresolved = list(payload.get("unresolved_questions") or payload.get("questions") or fallback.unresolved_questions)
    else:
        return None

    steps: list[EDAPlanStep] = []
    for index, item in enumerate(list(raw_steps)[:12], start=1):
        if isinstance(item, str):
            tool_name = _normalize_eda_tool(item)
            if tool_name is None:
                continue
            steps.append(
                EDAPlanStep(
                    key=f"eda_step_{index}",
                    title=ALLOWED_EDA_TOOLS[tool_name].split(".")[0],
                    objective=ALLOWED_EDA_TOOLS[tool_name],
                    tool=tool_name,
                    rationale="Recovered from Gemini plan repair output.",
                    priority=min(5, index),
                )
            )
            continue
        if not isinstance(item, dict):
            continue
        tool_name = _normalize_eda_tool(item.get("tool") or item.get("tool_name") or item.get("name"))
        if tool_name is None:
            continue
        steps.append(
            EDAPlanStep(
                key=str(item.get("key") or item.get("id") or f"eda_step_{index}").strip() or f"eda_step_{index}",
                title=str(item.get("title") or item.get("name") or ALLOWED_EDA_TOOLS[tool_name].split(".")[0]).strip() or f"EDA Step {index}",
                objective=str(item.get("objective") or item.get("purpose") or item.get("goal") or ALLOWED_EDA_TOOLS[tool_name]).strip(),
                tool=tool_name,
                rationale=str(item.get("rationale") or item.get("reason") or "Recovered from Gemini plan repair output.").strip(),
                priority=max(1, min(5, int(item.get("priority") or min(index, 5)))),
            )
        )
    if not steps:
        return None
    return EDAPlan(
        summary=summary or fallback.summary,
        steps=steps[:12],
        unresolved_questions=[str(item).strip() for item in unresolved[:8] if str(item).strip()],
    )


def _sheet_field_inventory(profile_json: dict[str, Any]) -> tuple[int, int, list[str]]:
    sections = 0
    headers: list[str] = []
    for sheet in profile_json.get("sheets", []):
        for section in sheet.get("sections", []):
            sections += 1
            for field in section.get("fields", []):
                label = str(field.get("header_label") or "").strip()
                if label:
                    headers.append(label)
    return sections, len(headers), headers[:12]


def _summarize_runtime_for_llm(runtime_payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(runtime_payload, dict):
        return {}
    summary: dict[str, Any] = {}
    for key in (
        "total_returns",
        "completed_returns",
        "open_returns",
        "awaiting_answers",
        "under_review",
        "in_process",
        "ready_for_preparation",
        "total_products",
        "product_type_count",
        "uom_count",
        "category_count",
        "total_sheets",
        "tabular_sheet_count",
        "reference_sheet_count",
        "total_rows",
        "numeric_measure_count",
        "comparison_group_count",
    ):
        if key in runtime_payload:
            summary[key] = runtime_payload.get(key)
    for key in (
        "status_counts",
        "return_type_counts",
        "category_counts",
        "product_type_counts",
        "uom_counts",
        "comparison_groups",
        "sheet_summaries",
    ):
        if isinstance(runtime_payload.get(key), list):
            summary[key] = runtime_payload[key][:6]
    return summary


def _top_count_payload(items: list[dict[str, Any]]) -> tuple[str | None, int | None, int]:
    if not items:
        return None, None, 0
    top = items[0]
    total = sum(int(item.get("count") or 0) for item in items)
    return str(top.get("label") or "Unknown"), int(top.get("count") or 0), total


def _pick_primary_table(raw_tables: list[RawTable]) -> RawTable | None:
    return max(raw_tables, key=lambda item: len(item.rows), default=None)


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text)
    except ValueError:
        return None


def _numeric_columns(table: RawTable) -> list[str]:
    profiles = classify_table_fields(table)
    numeric_headers: list[str] = []
    for header in measure_headers(table, profiles):
        values = [coerce_measure_value(row.get(header), profiles[header]) for row in table.rows]
        usable = [value for value in values if value is not None]
        if usable and len(usable) >= max(5, len(table.rows) // 3 if table.rows else 5):
            numeric_headers.append(header)
    return numeric_headers


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    pos = (len(ordered) - 1) * q
    lower = math.floor(pos)
    upper = math.ceil(pos)
    if lower == upper:
        return ordered[lower]
    weight = pos - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _parse_period_label(label: str) -> tuple[int, int] | None:
    text = str(label or "").strip()
    if not text:
        return None
    month_map = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    match = re.match(r"^([A-Za-z]{3,9})[-_ ]?(\d{2,4})$", text)
    if not match:
        return None
    month_key = match.group(1)[:3].lower()
    month = month_map.get(month_key)
    if month is None:
        return None
    year = int(match.group(2))
    if year < 100:
        year += 2000 if year < 70 else 1900
    return year, month


def _linear_projection(values: list[float]) -> tuple[float, float]:
    n = len(values)
    if n == 1:
        return values[0], 0.0
    x_values = list(range(n))
    x_mean = sum(x_values) / n
    y_mean = sum(values) / n
    denominator = sum((x - x_mean) ** 2 for x in x_values)
    if denominator == 0:
        return values[-1], 0.0
    slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, values)) / denominator
    intercept = y_mean - slope * x_mean
    return intercept + slope * n, slope


def _fallback_eda_plan(
    *,
    profile,
    interpretation: SemanticWorkbookInterpretation,
    runtime_payload: dict[str, Any] | None,
    raw_tables: list[RawTable],
) -> EDAPlan:
    steps = [
        EDAPlanStep(
            key="eda_structure_scan",
            title="Inspect workbook structure",
            objective="Confirm the sheet and section layout before any analytical interpretation is trusted.",
            tool="schema_inventory_scan",
            rationale="Non-standard workbooks need a structure-first read before deeper analysis can be scoped correctly.",
            priority=1,
        ),
        EDAPlanStep(
            key="eda_semantic_alignment",
            title="Check semantic alignment",
            objective="Verify that inferred entities, dimensions, and measures are actually visible in the workbook headers.",
            tool="semantic_alignment_scan",
            rationale="Semantic interpretation should be grounded in visible workbook structure rather than guessed from titles alone.",
            priority=2,
        ),
    ]
    if runtime_payload:
        steps.extend(
            [
                EDAPlanStep(
                    key="eda_runtime_signals",
                    title="Read runtime signals",
                    objective="Use available runtime metrics to identify the strongest currently computable business signals.",
                    tool="runtime_signal_scan",
                    rationale="EDA should start from what the system can already compute reliably before branching into raw-table scans.",
                    priority=3,
                ),
                EDAPlanStep(
                    key="eda_concentration_scan",
                    title="Check concentration patterns",
                    objective="Determine whether key distributions are concentrated enough to justify ranked lists, exceptions, or focused drilldowns.",
                    tool="concentration_signal_scan",
                    rationale="Concentration is one of the fastest ways to identify dashboard-worthy business patterns.",
                    priority=4,
                ),
            ]
        )
    if raw_tables:
        steps.extend(
            [
                EDAPlanStep(
                    key="eda_distribution_sql",
                    title="Slice dominant business segments",
                    objective="Use DuckDB to identify the most dominant business segment in the preserved raw tables.",
                    tool="distribution_sql_scan",
                    rationale="Dashboard design should follow the dominant raw-data segment, not just a schema summary.",
                    priority=3,
                ),
                EDAPlanStep(
                    key="eda_measure_sql",
                    title="Locate numeric concentration",
                    objective="Use DuckDB to determine where the main visible numeric workload or volume sits.",
                    tool="measure_by_dimension_sql_scan",
                    rationale="High-volume measures often determine the most important charts and exception panels.",
                    priority=4,
                ),
            ]
        )
    comparison_groups = list((runtime_payload or {}).get("comparison_groups") or [])
    if comparison_groups:
        steps.append(
            EDAPlanStep(
                key="eda_period_alignment",
                title="Check period alignment",
                objective="Verify that the workbook exposes stable paired periods that can support trend analysis.",
                tool="period_alignment_scan",
                rationale="Trend and comparison analytics are only reliable when the workbook periods align cleanly.",
                priority=4,
            )
        )
    if len(comparison_groups) >= 3:
        steps.append(
            EDAPlanStep(
                key="eda_forecast_signal",
                title="Assess forecast readiness",
                objective="Estimate whether the aligned period history is sufficient for a deterministic next-period projection.",
                tool="forecast_signal_scan",
                rationale="Forecasts should only appear when there is enough consistent history to support them.",
                priority=5,
            )
        )
    if raw_tables:
        steps.extend(
            [
                EDAPlanStep(
                    key="eda_quality_sql",
                    title="Check raw-field quality hotspots",
                    objective="Use DuckDB to find missing-data hotspots in the key business fields.",
                    tool="quality_gap_sql_scan",
                    rationale="EDA should surface material quality risks before the dashboard overstates data confidence.",
                    priority=5,
                ),
                EDAPlanStep(
                    key="eda_duplicates",
                    title="Check duplicate records",
                    objective="Detect exact duplicate rows or suspicious repeated records in the primary analytical table.",
                    tool="duplicate_scan",
                    rationale="Repeated records can materially distort distributions, rankings, and portfolio totals.",
                    priority=5,
                ),
                EDAPlanStep(
                    key="eda_outliers",
                    title="Check numeric outliers",
                    objective="Detect whether any numeric measure contains extreme values that could distort charts or totals.",
                    tool="outlier_scan",
                    rationale="Outliers can make an otherwise reasonable dashboard look misleading or unstable.",
                    priority=5,
                ),
            ]
        )
    if interpretation.ambiguities:
        steps.append(
            EDAPlanStep(
                key="eda_ambiguity_review",
                title="Review unresolved ambiguity",
                objective="Surface where workbook meaning is still uncertain and what user input would remove that uncertainty.",
                tool="ambiguity_review",
                rationale="The agent should ask for clarification when meaning is weak instead of pretending certainty.",
                priority=5,
            )
        )
    return EDAPlan(
        summary="Deterministic EDA plan built from workbook semantics, runtime availability, and preserved raw-table coverage.",
        steps=steps[:12],
        unresolved_questions=interpretation.business_questions[:8],
    )


def build_eda_plan(
    *,
    profile,
    interpretation: SemanticWorkbookInterpretation,
    runtime_payload: dict[str, Any] | None,
    raw_tables: list[RawTable],
    gemini: GeminiRequestSettings | None,
    user_guidance: str | None,
) -> EDAPlan:
    fallback = _fallback_eda_plan(
        profile=profile,
        interpretation=interpretation,
        runtime_payload=runtime_payload,
        raw_tables=raw_tables,
    )
    if gemini is None:
        logger.info(
            "eda agent: Gemini unavailable, using deterministic EDA plan for snapshot profile %s",
            getattr(profile, "id", "unknown"),
        )
        return fallback

    prompt = json.dumps(
        {
            "role": "You are the EDA planning agent for adaptive business dashboards.",
            "task": [
                "Choose a concise set of allowed deterministic EDA tools to analyze the workbook before dashboard design.",
                "Prioritize tools that reduce semantic ambiguity, surface high-signal distributions, and establish data coverage limits.",
                "Do not invent new tools or free-form analyses.",
                "Do not design charts or tabs. Focus only on analysis planning.",
            ],
            "allowed_tools": ALLOWED_EDA_TOOLS,
            "semantic_interpretation": interpretation.model_dump(),
            "runtime_summary": _summarize_runtime_for_llm(runtime_payload),
            "user_guidance": user_guidance or "",
            "raw_table_inventory": [
                {
                    "name": table.name,
                    "row_count": len(table.rows),
                    "headers": table.headers[:12],
                }
                for table in raw_tables[:5]
            ],
            "output_rules": [
                "Return JSON only.",
                "Each step must use one of the allowed tool names exactly.",
                "Use at most 12 steps.",
                "Plan only EDA, not dashboard layout.",
            ],
        },
        ensure_ascii=False,
    )
    try:
        plan = gemini_generate_structured(prompt=prompt, schema=EDAPlan, settings=gemini)
        plan.steps = [step for step in plan.steps if step.tool in ALLOWED_EDA_TOOLS][:12]
        if not plan.steps:
            logger.warning(
                "eda agent: Gemini returned no usable EDA steps for snapshot profile %s; using fallback",
                getattr(profile, "id", "unknown"),
            )
            return fallback
        logger.info(
            "eda agent: Gemini EDA plan succeeded for snapshot profile %s with %s steps",
            getattr(profile, "id", "unknown"),
            len(plan.steps),
        )
        return plan
    except GeminiReasoningError as exc:
        try:
            repaired = _normalize_eda_plan_payload(gemini_generate_json(prompt=prompt, settings=gemini), fallback)
            if repaired is not None:
                logger.info(
                    "eda agent: Gemini EDA plan repaired for snapshot profile %s with %s steps",
                    getattr(profile, "id", "unknown"),
                    len(repaired.steps),
                )
                return repaired
        except GeminiReasoningError:
            pass
        logger.warning(
            "eda agent: Gemini EDA planning failed for snapshot profile %s, using fallback: %s",
            getattr(profile, "id", "unknown"),
            exc,
        )
        return fallback


def _run_schema_inventory_scan(profile) -> EDAEvidence:
    profile_json = profile.profile_json or {}
    sheet_count = len(profile_json.get("sheets", []))
    section_count, field_count, header_samples = _sheet_field_inventory(profile_json)
    detail = (
        f"The workbook exposes {sheet_count} sheets, {section_count} detected sections, and {field_count} visible fields. "
        f"Representative headers include {', '.join(header_samples[:8]) or 'no readable headers'}."
    )
    return EDAEvidence(
        key="schema_inventory_scan",
        tool="schema_inventory_scan",
        title="Workbook structure inventory",
        detail=detail,
        confidence_score=0.95,
        supporting_metrics=[f"{sheet_count} sheets", f"{section_count} sections", f"{field_count} fields"],
    )


def _run_semantic_alignment_scan(profile, interpretation: SemanticWorkbookInterpretation) -> EDAEvidence:
    _, _, headers = _sheet_field_inventory(profile.profile_json or {})
    normalized_headers = {header.lower() for header in headers}
    candidates = interpretation.sheets[:4]
    aligned: list[str] = []
    missing: list[str] = []
    for sheet in candidates:
        for label in [*sheet.candidate_dimensions, *sheet.candidate_measures]:
            if not label:
                continue
            target = label.lower()
            if any(target in header or header in target for header in normalized_headers):
                aligned.append(label)
            else:
                missing.append(label)
    detail = (
        f"The semantic interpretation aligns with workbook-visible fields for {len(aligned)} inferred dimensions or measures. "
        f"{'Potentially unresolved items include ' + ', '.join(missing[:5]) + '.' if missing else 'No major semantic gaps were detected from the visible headers.'}"
    )
    return EDAEvidence(
        key="semantic_alignment_scan",
        tool="semantic_alignment_scan",
        title="Semantic alignment review",
        detail=detail,
        confidence_score=0.82 if not missing else 0.68,
        supporting_metrics=[f"{len(aligned)} aligned", f"{len(missing)} unresolved"],
    )


def _run_runtime_signal_scan(runtime_payload: dict[str, Any] | None) -> EDAEvidence:
    if not runtime_payload:
        return EDAEvidence(
            key="runtime_signal_scan",
            tool="runtime_signal_scan",
            title="Runtime signals unavailable",
            detail="No runtime payload is currently available for this workbook family, so EDA has to rely on structure and semantics rather than computed metrics.",
            confidence_score=0.55,
            supporting_metrics=[],
        )
    summary = _summarize_runtime_for_llm(runtime_payload)
    signal_lines: list[str] = []
    metrics: list[str] = []
    for key in (
        "total_returns",
        "open_returns",
        "completed_returns",
        "total_products",
        "product_type_count",
        "uom_count",
        "category_count",
        "total_sheets",
        "total_rows",
        "comparison_group_count",
    ):
        if key in summary:
            signal_lines.append(f"{key.replace('_', ' ')}={summary[key]}")
            metrics.append(f"{key.replace('_', ' ')} {summary[key]}")
    for dist_key in ("status_counts", "product_type_counts", "category_counts", "uom_counts", "return_type_counts"):
        label, count, total = _top_count_payload(summary.get(dist_key, []))
        if label is not None and count is not None:
            share = (count / total) if total else 0.0
            signal_lines.append(f"top {dist_key.replace('_', ' ')} bucket is {label} at {count} rows ({share:.1%})")
            metrics.append(f"{label} {share:.1%}")
    return EDAEvidence(
        key="runtime_signal_scan",
        tool="runtime_signal_scan",
        title="Runtime signal scan",
        detail="Available runtime metrics show " + "; ".join(signal_lines[:6]) + ".",
        confidence_score=0.9,
        supporting_metrics=metrics[:8],
    )


def _run_concentration_signal_scan(runtime_payload: dict[str, Any] | None) -> EDAEvidence:
    if not runtime_payload:
        return EDAEvidence(
            key="concentration_signal_scan",
            tool="concentration_signal_scan",
            title="Distribution concentration scan",
            detail="Concentration analysis could not run because the workbook family does not expose runtime distributions yet.",
            confidence_score=0.45,
            supporting_metrics=[],
        )
    observations: list[str] = []
    metrics: list[str] = []
    for dist_key in ("status_counts", "product_type_counts", "category_counts", "uom_counts", "return_type_counts"):
        items = runtime_payload.get(dist_key, [])
        label, count, total = _top_count_payload(items if isinstance(items, list) else [])
        if label is None or count is None or total <= 0:
            continue
        share = count / total
        tone = "highly concentrated" if share >= 0.5 else "moderately concentrated" if share >= 0.25 else "distributed"
        observations.append(f"{dist_key.replace('_', ' ')} is {tone}; top bucket {label} holds {share:.1%} of visible rows")
        metrics.append(f"{label} {share:.1%}")
    detail = (
        "Distribution evidence suggests " + "; ".join(observations[:4]) + "."
        if observations
        else "No meaningful runtime distributions were available for concentration analysis."
    )
    return EDAEvidence(
        key="concentration_signal_scan",
        tool="concentration_signal_scan",
        title="Concentration scan",
        detail=detail,
        confidence_score=0.83 if observations else 0.5,
        supporting_metrics=metrics[:6],
    )


def _run_ambiguity_review(interpretation: SemanticWorkbookInterpretation) -> EDAEvidence:
    detail = (
        "Open analytical ambiguity remains around " + ", ".join(interpretation.ambiguities[:4])
        if interpretation.ambiguities
        else "The semantic interpretation does not currently expose major unresolved ambiguity that would block EDA."
    )
    return EDAEvidence(
        key="ambiguity_review",
        tool="ambiguity_review",
        title="Ambiguity review",
        detail=detail,
        confidence_score=0.75 if interpretation.ambiguities else 0.88,
        supporting_metrics=[f"{len(interpretation.ambiguities)} ambiguities", f"{len(interpretation.business_questions)} follow-up questions"],
    )


def _run_coverage_check(interpretation: SemanticWorkbookInterpretation, runtime_payload: dict[str, Any] | None) -> EDAEvidence:
    runtime_keys = sorted(_summarize_runtime_for_llm(runtime_payload).keys())
    detail = (
        f"Interpreted business questions currently focus on {', '.join(interpretation.business_questions[:3]) or 'general workbook review'}. "
        f"{'Computed runtime metrics are available for ' + ', '.join(runtime_keys[:6]) + '.' if runtime_keys else 'No computed runtime metrics are available yet, so the first analytical pass depends more on schema and semantics.'}"
    )
    return EDAEvidence(
        key="dashboard_coverage_check",
        tool="dashboard_coverage_check",
        title="Analytical coverage check",
        detail=detail,
        confidence_score=0.8,
        supporting_metrics=[f"{len(interpretation.business_questions)} business questions", f"{len(runtime_keys)} runtime signal groups"],
    )


def _run_duplicate_scan(raw_tables: list[RawTable]) -> EDAEvidence:
    table = _pick_primary_table(raw_tables)
    if table is None or not table.rows:
        return EDAEvidence(
            key="duplicate_scan",
            tool="duplicate_scan",
            title="Duplicate scan unavailable",
            detail="No preserved raw table was available for duplicate detection.",
            confidence_score=0.4,
            supporting_metrics=[],
        )
    fingerprints: dict[tuple[tuple[str, Any], ...], int] = {}
    for row in table.rows:
        fingerprint = tuple((header, row.get(header)) for header in table.headers)
        fingerprints[fingerprint] = fingerprints.get(fingerprint, 0) + 1
    duplicate_groups = [count for count in fingerprints.values() if count > 1]
    duplicate_rows = sum(count - 1 for count in duplicate_groups)
    total_rows = len(table.rows)
    if duplicate_rows <= 0:
        return EDAEvidence(
            key="duplicate_scan",
            tool="duplicate_scan",
            title="Duplicate scan",
            detail=f"No exact duplicate rows were detected in the primary analytical table '{table.name}'.",
            confidence_score=0.86,
            supporting_metrics=[f"{total_rows} rows checked", "0 duplicate rows"],
        )
    share = duplicate_rows / total_rows if total_rows else 0.0
    return EDAEvidence(
        key="duplicate_scan",
        tool="duplicate_scan",
        title="Duplicate scan",
        detail=f"Exact duplicate detection found {duplicate_rows} repeated rows across {len(duplicate_groups)} duplicate groups in '{table.name}' ({share:.1%} of checked rows).",
        confidence_score=0.89,
        supporting_metrics=[f"{duplicate_rows} duplicate rows", f"{len(duplicate_groups)} duplicate groups", f"{share:.1%} duplicate share"],
    )


def _run_outlier_scan(raw_tables: list[RawTable]) -> EDAEvidence:
    table = _pick_primary_table(raw_tables)
    if table is None or not table.rows:
        return EDAEvidence(
            key="outlier_scan",
            tool="outlier_scan",
            title="Outlier scan unavailable",
            detail="No preserved raw table was available for numeric outlier analysis.",
            confidence_score=0.4,
            supporting_metrics=[],
        )
    strongest: tuple[str, int, float, float, float] | None = None
    for header in _numeric_columns(table):
        values = [_coerce_float(row.get(header)) for row in table.rows]
        usable = [value for value in values if value is not None]
        if len(usable) < 8:
            continue
        q1 = _quantile(usable, 0.25)
        q3 = _quantile(usable, 0.75)
        iqr = q3 - q1
        if iqr <= 0:
            continue
        lower = q1 - (1.5 * iqr)
        upper = q3 + (1.5 * iqr)
        outlier_count = sum(1 for value in usable if value < lower or value > upper)
        if outlier_count <= 0:
            continue
        share = outlier_count / len(usable)
        candidate = (header, outlier_count, share, lower, upper)
        if strongest is None or share > strongest[2]:
            strongest = candidate
    if strongest is None:
        return EDAEvidence(
            key="outlier_scan",
            tool="outlier_scan",
            title="Outlier scan",
            detail=f"No major numeric outlier concentration was detected in the primary analytical table '{table.name}'.",
            confidence_score=0.8,
            supporting_metrics=[f"{len(_numeric_columns(table))} numeric fields checked"],
        )
    header, outlier_count, share, lower, upper = strongest
    return EDAEvidence(
        key="outlier_scan",
        tool="outlier_scan",
        title="Outlier scan",
        detail=f"The numeric field '{header}' contains {outlier_count} IQR outliers ({share:.1%} of usable rows), outside the expected range {lower:.2f} to {upper:.2f}.",
        confidence_score=0.84,
        supporting_metrics=[f"{header} {share:.1%} outliers", f"{outlier_count} outlier rows", f"IQR range {lower:.2f}..{upper:.2f}"],
    )


def _run_period_alignment_scan(runtime_payload: dict[str, Any] | None) -> EDAEvidence:
    comparison_groups = list((runtime_payload or {}).get("comparison_groups") or [])
    sheet_summaries = list((runtime_payload or {}).get("sheet_summaries") or [])
    if not comparison_groups:
        return EDAEvidence(
            key="period_alignment_scan",
            tool="period_alignment_scan",
            title="Period alignment scan",
            detail="No stable multi-period comparison groups were detected, so trend analysis is not yet aligned.",
            confidence_score=0.56,
            supporting_metrics=["0 aligned period groups"],
        )
    ordered_groups = sorted(
        comparison_groups,
        key=lambda item: _parse_period_label(str(item.get("group_label") or "")) or (9999, 99),
    )
    aligned = sum(1 for group in ordered_groups if (group.get("matched_pool_count") or 0) > 0)
    tc_only = sum(int(group.get("unmatched_tc_pool_count") or 0) for group in ordered_groups)
    bc_only = sum(int(group.get("unmatched_bc_pool_count") or 0) for group in ordered_groups)
    first_label = ordered_groups[0].get("group_label") or "first period"
    last_label = ordered_groups[-1].get("group_label") or "latest period"
    distribution_sheets = sum(1 for item in sheet_summaries if item.get("sheet_kind") == "distribution")
    return EDAEvidence(
        key="period_alignment_scan",
        tool="period_alignment_scan",
        title="Period alignment scan",
        detail=f"The workbook aligns {aligned} paired period groups from {first_label} through {last_label}. Distribution sheets={distribution_sheets}, TC-only unmatched pools={tc_only}, BC-only unmatched pools={bc_only}.",
        confidence_score=0.88 if aligned >= 3 else 0.72,
        supporting_metrics=[f"{aligned} aligned periods", f"{tc_only} TC-only pools", f"{bc_only} BC-only pools"],
    )


def _run_forecast_signal_scan(runtime_payload: dict[str, Any] | None) -> EDAEvidence:
    comparison_groups = list((runtime_payload or {}).get("comparison_groups") or [])
    series_points: dict[str, list[tuple[tuple[int, int], float]]] = {}
    ratio_points: list[tuple[tuple[int, int], float]] = []
    for group in comparison_groups:
        period_key = _parse_period_label(str(group.get("group_label") or ""))
        if period_key is None:
            continue
        totals = group.get("series_totals") or []
        by_series = {str(item.get("series") or ""): float(item.get("grand_total") or 0.0) for item in totals}
        for series, value in by_series.items():
            series_points.setdefault(series, []).append((period_key, value))
        tc_total = by_series.get("TC")
        bc_total = by_series.get("BC")
        if tc_total and tc_total > 0 and bc_total is not None:
            ratio_points.append((period_key, bc_total / tc_total))
    ratio_points.sort(key=lambda item: item[0])
    if len(ratio_points) < 3:
        return EDAEvidence(
            key="forecast_signal_scan",
            tool="forecast_signal_scan",
            title="Forecast readiness scan",
            detail="A deterministic forecast is not yet reliable because fewer than three aligned paired periods are available for trend projection.",
            confidence_score=0.5,
            supporting_metrics=[f"{len(ratio_points)} aligned ratio periods"],
        )
    ordered_ratios = [value for _, value in ratio_points]
    projected_ratio, ratio_slope = _linear_projection(ordered_ratios)
    projected_ratio = max(0.0, projected_ratio)
    direction = "improving" if ratio_slope < 0 else "deteriorating" if ratio_slope > 0 else "stable"
    details = [f"Observed BC/TC trend is {direction}; projected next-period BC/TC ratio is {projected_ratio:.2%}."]
    metrics = [f"{len(ratio_points)} ratio periods", f"next BC/TC {projected_ratio:.2%}"]
    for series in ("TC", "BC"):
        ordered_series = sorted(series_points.get(series, []), key=lambda item: item[0])
        if len(ordered_series) < 3:
            continue
        next_value, slope = _linear_projection([value for _, value in ordered_series])
        trend = "up" if slope > 0 else "down" if slope < 0 else "flat"
        details.append(f"{series} volume trends {trend} with next-period projection {max(0.0, next_value):.0f}.")
        metrics.append(f"{series} next {max(0.0, next_value):.0f}")
    return EDAEvidence(
        key="forecast_signal_scan",
        tool="forecast_signal_scan",
        title="Forecast signal scan",
        detail=" ".join(details),
        confidence_score=0.76,
        supporting_metrics=metrics[:6],
    )


def _wrap_duckdb_signal(
    tool_name: EdaToolName,
    raw_tables: list[RawTable],
    interpretation: SemanticWorkbookInterpretation,
    family: str,
) -> EDAEvidence | None:
    signal = run_dashboard_duckdb_tool(
        tool_name=tool_name,
        tables=raw_tables,
        interpretation=interpretation,
        family=family,
    )
    if signal is None:
        return None
    return EDAEvidence(
        key=tool_name,
        tool=tool_name,
        title=signal.title,
        detail=signal.detail,
        confidence_score=signal.confidence_score,
        supporting_metrics=signal.supporting_metrics,
    )


def run_eda_tools(
    *,
    profile,
    interpretation: SemanticWorkbookInterpretation,
    plan: EDAPlan,
    runtime_payload: dict[str, Any] | None,
    raw_tables: list[RawTable],
) -> list[EDAEvidence]:
    tool_map = {
        "schema_inventory_scan": lambda: _run_schema_inventory_scan(profile),
        "semantic_alignment_scan": lambda: _run_semantic_alignment_scan(profile, interpretation),
        "runtime_signal_scan": lambda: _run_runtime_signal_scan(runtime_payload),
        "concentration_signal_scan": lambda: _run_concentration_signal_scan(runtime_payload),
        "ambiguity_review": lambda: _run_ambiguity_review(interpretation),
        "dashboard_coverage_check": lambda: _run_coverage_check(interpretation, runtime_payload),
        "distribution_sql_scan": lambda: _wrap_duckdb_signal("distribution_sql_scan", raw_tables, interpretation, getattr(profile, "workbook_type", "generic_workbook_v1")),
        "top_dimension_sql_scan": lambda: _wrap_duckdb_signal("top_dimension_sql_scan", raw_tables, interpretation, getattr(profile, "workbook_type", "generic_workbook_v1")),
        "measure_by_dimension_sql_scan": lambda: _wrap_duckdb_signal("measure_by_dimension_sql_scan", raw_tables, interpretation, getattr(profile, "workbook_type", "generic_workbook_v1")),
        "quality_gap_sql_scan": lambda: _wrap_duckdb_signal("quality_gap_sql_scan", raw_tables, interpretation, getattr(profile, "workbook_type", "generic_workbook_v1")),
        "cross_dimension_sql_scan": lambda: _wrap_duckdb_signal("cross_dimension_sql_scan", raw_tables, interpretation, getattr(profile, "workbook_type", "generic_workbook_v1")),
        "duplicate_scan": lambda: _run_duplicate_scan(raw_tables),
        "outlier_scan": lambda: _run_outlier_scan(raw_tables),
        "period_alignment_scan": lambda: _run_period_alignment_scan(runtime_payload),
        "forecast_signal_scan": lambda: _run_forecast_signal_scan(runtime_payload),
    }
    evidence: list[EDAEvidence] = []
    for step in plan.steps:
        tool = tool_map.get(step.tool)
        if tool is None:
            logger.warning(
                "eda agent: skipped unsupported tool %s for snapshot profile %s",
                step.tool,
                getattr(profile, "id", "unknown"),
            )
            continue
        log_app_event(
            level="info",
            state="started",
            category="eda_tool",
            event="tool_started",
            agent_name="eda_agent",
            workflow="eda_langgraph_v1",
            tool_name=step.tool,
            message=f"EDA tool started: {step.tool}",
            payload={"step_key": step.key, "title": step.title},
            snapshot_id=getattr(profile, "snapshot_id", None),
        )
        item = tool()
        if item is not None:
            evidence.append(item)
            log_app_event(
                level="info",
                state="completed",
                category="eda_tool",
                event="tool_completed",
                agent_name="eda_agent",
                workflow="eda_langgraph_v1",
                tool_name=step.tool,
                message=f"EDA tool completed: {step.tool}",
                payload={"evidence_key": item.key, "confidence_score": item.confidence_score},
                snapshot_id=getattr(profile, "snapshot_id", None),
            )
    return evidence


def run_eda_agent(
    *,
    profile,
    interpretation: SemanticWorkbookInterpretation,
    runtime_payload: dict[str, Any] | None,
    raw_tables: list[RawTable] | None,
    gemini: GeminiRequestSettings | None,
    user_guidance: str | None,
) -> EDAAgentResult:
    available_raw_tables = raw_tables or []
    log_app_event(
        level="info",
        state="started",
        category="eda_agent",
        event="eda_started",
        agent_name="eda_agent",
        workflow="eda_langgraph_v1" if gemini is not None else "eda_langgraph_v1:fallback",
        message="EDA agent started",
        payload={"has_gemini": gemini is not None, "raw_table_count": len(available_raw_tables)},
        snapshot_id=getattr(profile, "snapshot_id", None),
    )

    def plan_node(_: EDAAgentState) -> EDAAgentState:
        return {
            "plan": build_eda_plan(
                profile=profile,
                interpretation=interpretation,
                runtime_payload=runtime_payload,
                raw_tables=available_raw_tables,
                gemini=gemini,
                user_guidance=user_guidance,
            )
        }

    def run_tools_node(state: EDAAgentState) -> EDAAgentState:
        return {
            "evidence": run_eda_tools(
                profile=profile,
                interpretation=interpretation,
                plan=state["plan"],
                runtime_payload=runtime_payload,
                raw_tables=available_raw_tables,
            )
        }

    graph = StateGraph(EDAAgentState)
    graph.add_node("plan_eda", plan_node)
    graph.add_node("run_eda_tools", run_tools_node)
    graph.add_edge(START, "plan_eda")
    graph.add_edge("plan_eda", "run_eda_tools")
    graph.add_edge("run_eda_tools", END)
    compiled = graph.compile(checkpointer=InMemorySaver())
    result = compiled.invoke({}, config={"configurable": {"thread_id": f"eda:{getattr(profile, 'snapshot_id', getattr(profile, 'id', 'unknown'))}"}})
    workflow_mode = "eda_langgraph_v1" if gemini is not None else "eda_langgraph_v1:fallback"
    log_app_event(
        level="info",
        state="completed",
        category="eda_agent",
        event="eda_completed",
        agent_name="eda_agent",
        workflow=workflow_mode,
        message="EDA agent completed",
        payload={"step_count": len(result["plan"].steps), "evidence_count": len(result.get("evidence", []))},
        snapshot_id=getattr(profile, "snapshot_id", None),
    )
    return EDAAgentResult(
        plan=result["plan"],
        evidence=result.get("evidence", []),
        workflow_mode=workflow_mode,
    )
