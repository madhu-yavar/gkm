from __future__ import annotations

import json
import logging
from typing import Any, Literal, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field

from app.dashboard_duckdb_tools import run_dashboard_duckdb_tool
from app.dashboard_semantics import (
    SemanticWorkbookInterpretation,
    interpret_workbook_semantics,
    plan_dashboard_hypothesis,
)
from app.gemini_reasoning import GeminiReasoningError, GeminiRequestSettings, gemini_generate_structured
from app.raw_data_store import RawTable


logger = logging.getLogger(__name__)

DashboardToolName = Literal[
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
]

ALLOWED_DASHBOARD_TOOLS: dict[str, str] = {
    "schema_inventory_scan": "Summarize workbook structure, sheet roles, and header density.",
    "semantic_alignment_scan": "Check whether inferred dimensions and measures are supported by visible headers.",
    "runtime_signal_scan": "Surface the strongest currently available operational or catalog signals from runtime data.",
    "concentration_signal_scan": "Check whether key runtime distributions are concentrated or fragmented.",
    "ambiguity_review": "List unresolved semantic questions that block confident dashboard design.",
    "dashboard_coverage_check": "Check whether the proposed tabs and KPI cards cover the interpreted business questions.",
    "distribution_sql_scan": "Run a DuckDB distribution scan over preserved raw tables to identify dominant business segments.",
    "top_dimension_sql_scan": "Run a DuckDB top-segment scan over the highest-signal categorical field.",
    "measure_by_dimension_sql_scan": "Run a DuckDB grouped-measure scan to find where the main numeric workload or volume sits.",
    "quality_gap_sql_scan": "Run a DuckDB null-hotspot scan on core business fields.",
    "cross_dimension_sql_scan": "Run a DuckDB cross-segment scan to surface dominant combinations across two business dimensions.",
}


class DashboardInvestigationStep(BaseModel):
    key: str
    title: str
    objective: str
    tool: DashboardToolName
    rationale: str
    priority: int = Field(ge=1, le=5)


class DashboardInvestigationPlan(BaseModel):
    summary: str
    steps: list[DashboardInvestigationStep] = Field(default_factory=list, max_length=6)
    unresolved_questions: list[str] = Field(default_factory=list, max_length=6)


class DashboardInvestigationEvidence(BaseModel):
    key: str
    tool: DashboardToolName
    title: str
    detail: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    supporting_metrics: list[str] = Field(default_factory=list, max_length=6)


class DashboardProposalAgentResult(BaseModel):
    title: str
    summary: str
    config: dict[str, Any]
    rationale: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    interpretation: SemanticWorkbookInterpretation
    investigation_plan: DashboardInvestigationPlan
    investigation_evidence: list[DashboardInvestigationEvidence] = Field(default_factory=list)
    workflow_mode: str


class DashboardProposalAgentState(TypedDict, total=False):
    interpretation: SemanticWorkbookInterpretation
    investigation_plan: DashboardInvestigationPlan
    investigation_evidence: list[DashboardInvestigationEvidence]
    title: str
    summary: str
    config: dict[str, Any]
    rationale: str
    confidence_score: float
    validation_errors: list[str]


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
    ):
        if key in runtime_payload:
            summary[key] = runtime_payload.get(key)
    for key in ("status_counts", "return_type_counts", "category_counts", "product_type_counts", "uom_counts"):
        if isinstance(runtime_payload.get(key), list):
            summary[key] = runtime_payload[key][:6]
    return summary


def _top_count_payload(items: list[dict[str, Any]]) -> tuple[str | None, int | None, int]:
    if not items:
        return None, None, 0
    top = items[0]
    total = sum(int(item.get("count") or 0) for item in items)
    return str(top.get("label") or "Unknown"), int(top.get("count") or 0), total


def _fallback_investigation_plan(
    interpretation: SemanticWorkbookInterpretation,
    runtime_payload: dict[str, Any] | None,
    raw_tables: list[RawTable],
) -> DashboardInvestigationPlan:
    steps = [
        DashboardInvestigationStep(
            key="structure_scan",
            title="Inspect workbook structure",
            objective="Confirm the sheet and section layout before proposing dashboard coverage.",
            tool="schema_inventory_scan",
            rationale="Non-standard workbooks need a structure-first read before the dashboard can be trusted.",
            priority=1,
        ),
        DashboardInvestigationStep(
            key="semantic_alignment",
            title="Check semantic alignment",
            objective="Verify that inferred entities, dimensions, and measures are actually visible in the workbook headers.",
            tool="semantic_alignment_scan",
            rationale="Semantic interpretation should be grounded in the visible headers rather than guessed from titles alone.",
            priority=2,
        ),
    ]
    if runtime_payload:
        steps.append(
            DashboardInvestigationStep(
                key="runtime_signals",
                title="Read runtime signals",
                objective="Use available runtime metrics to identify the strongest operational or catalog signals.",
                tool="runtime_signal_scan",
                rationale="Dashboard choices should reflect the strongest currently computable signals, not just schema shape.",
                priority=3,
            )
        )
        steps.append(
            DashboardInvestigationStep(
                key="concentration_scan",
                title="Check distribution concentration",
                objective="See whether key distributions are concentrated enough to deserve dedicated dashboard attention.",
                tool="concentration_signal_scan",
                rationale="Highly concentrated distributions often justify ranked lists, exception panels, or focused category views.",
                priority=4,
            )
        )
    if raw_tables:
        steps.append(
            DashboardInvestigationStep(
                key="distribution_sql",
                title="Slice dominant business segments",
                objective="Use DuckDB to identify the most concentrated business segment in the preserved raw tables.",
                tool="distribution_sql_scan",
                rationale="Dashboard emphasis should follow the dominant raw-data segment, not just the runtime summary.",
                priority=3,
            )
        )
        steps.append(
            DashboardInvestigationStep(
                key="quality_sql",
                title="Check raw-field quality hotspots",
                objective="Use DuckDB to find missing-data hotspots in the key business fields.",
                tool="quality_gap_sql_scan",
                rationale="Data quality gaps often change what should appear as KPI cards versus exception panels.",
                priority=4,
            )
        )
    if interpretation.ambiguities:
        steps.append(
            DashboardInvestigationStep(
                key="ambiguity_review",
                title="Review unresolved ambiguity",
                objective="Surface where the workbook meaning is still uncertain and what user input would remove that uncertainty.",
                tool="ambiguity_review",
                rationale="The dashboard should ask for clarification when meaning is weak instead of pretending certainty.",
                priority=4,
            )
        )
    steps.append(
        DashboardInvestigationStep(
            key="coverage_check",
            title="Check dashboard coverage",
            objective="Assess whether the tabs and KPI cards cover the interpreted business questions.",
            tool="dashboard_coverage_check",
            rationale="A dashboard should only be approved once its layout matches the interpreted business purpose.",
            priority=5,
        )
    )
    return DashboardInvestigationPlan(
        summary="Deterministic investigation plan built from workbook semantics, runtime availability, and dashboard-coverage needs.",
        steps=steps[:6],
        unresolved_questions=interpretation.business_questions[:6],
    )


def build_dashboard_investigation_plan(
    *,
    profile,
    interpretation: SemanticWorkbookInterpretation,
    runtime_payload: dict[str, Any] | None,
    raw_tables: list[RawTable],
    base_config: dict[str, Any],
    gemini: GeminiRequestSettings | None,
    user_guidance: str | None,
) -> DashboardInvestigationPlan:
    fallback = _fallback_investigation_plan(interpretation, runtime_payload, raw_tables)
    if gemini is None:
        logger.info(
            "dashboard agent: Gemini unavailable, using deterministic investigation plan for snapshot profile %s",
            getattr(profile, "id", "unknown"),
        )
        return fallback

    prompt = json.dumps(
        {
            "role": "You are the dashboard investigation planner for adaptive business dashboards.",
            "task": [
                "Choose a small set of allowed deterministic tools to investigate the workbook before finalizing the dashboard proposal.",
                "Prioritize tools that reduce semantic ambiguity, surface high-signal metrics, and test whether the baseline layout covers the business questions.",
                "Do not invent new tools or free-form analyses.",
                "Keep the plan concise and decision-oriented.",
            ],
            "allowed_tools": ALLOWED_DASHBOARD_TOOLS,
            "semantic_interpretation": interpretation.model_dump(),
            "runtime_summary": _summarize_runtime_for_llm(runtime_payload),
            "user_guidance": user_guidance or "",
            "raw_table_inventory": [
                {
                    "name": table.name,
                    "row_count": len(table.rows),
                    "headers": table.headers[:12],
                }
                for table in raw_tables[:4]
            ],
            "baseline_dashboard": {
                "dashboard_family": base_config.get("dashboard_family"),
                "layout_template": base_config.get("layout_template"),
                "title": base_config.get("title"),
                "tabs": [
                    {
                        "key": tab.get("key"),
                        "label": tab.get("label"),
                        "description": tab.get("description"),
                    }
                    for tab in base_config.get("tabs", [])
                ],
                "kpi_cards": base_config.get("kpi_cards", []),
            },
            "output_rules": [
                "Return JSON only.",
                "Each step must use one of the allowed tool names exactly.",
                "Use at most 6 steps.",
                "Prefer explicit ambiguity and evidence-building over generic wording.",
            ],
        },
        ensure_ascii=False,
    )
    try:
        plan = gemini_generate_structured(prompt=prompt, schema=DashboardInvestigationPlan, settings=gemini)
        plan.steps = [step for step in plan.steps if step.tool in ALLOWED_DASHBOARD_TOOLS][:6]
        if not plan.steps:
            logger.warning(
                "dashboard agent: Gemini investigation plan returned no usable steps for snapshot profile %s; using deterministic plan",
                getattr(profile, "id", "unknown"),
            )
            return fallback
        logger.info(
            "dashboard agent: Gemini investigation plan succeeded for snapshot profile %s with %s steps",
            getattr(profile, "id", "unknown"),
            len(plan.steps),
        )
        return plan
    except GeminiReasoningError as exc:
        logger.warning(
            "dashboard agent: Gemini investigation planning failed for snapshot profile %s, using deterministic plan: %s",
            getattr(profile, "id", "unknown"),
            exc,
        )
        return fallback


def _run_schema_inventory_scan(profile) -> DashboardInvestigationEvidence:
    profile_json = profile.profile_json or {}
    sheet_count = len(profile_json.get("sheets", []))
    section_count, field_count, header_samples = _sheet_field_inventory(profile_json)
    detail = (
        f"The workbook exposes {sheet_count} sheets, {section_count} detected sections, and {field_count} visible fields. "
        f"Representative headers include {', '.join(header_samples[:8]) or 'no readable headers'}."
    )
    return DashboardInvestigationEvidence(
        key="schema_inventory_scan",
        tool="schema_inventory_scan",
        title="Workbook structure inventory",
        detail=detail,
        confidence_score=0.95,
        supporting_metrics=[
            f"{sheet_count} sheets",
            f"{section_count} sections",
            f"{field_count} fields",
        ],
    )


def _run_semantic_alignment_scan(profile, interpretation: SemanticWorkbookInterpretation) -> DashboardInvestigationEvidence:
    _, _, headers = _sheet_field_inventory(profile.profile_json or {})
    normalized_headers = {header.lower() for header in headers}
    candidates = interpretation.sheets[:3]
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
    metrics = [f"{len(aligned)} aligned", f"{len(missing)} unresolved"]
    return DashboardInvestigationEvidence(
        key="semantic_alignment_scan",
        tool="semantic_alignment_scan",
        title="Semantic alignment review",
        detail=detail,
        confidence_score=0.82 if not missing else 0.68,
        supporting_metrics=metrics,
    )


def _run_runtime_signal_scan(runtime_payload: dict[str, Any] | None) -> DashboardInvestigationEvidence:
    if not runtime_payload:
        return DashboardInvestigationEvidence(
            key="runtime_signal_scan",
            tool="runtime_signal_scan",
            title="Runtime signals unavailable",
            detail="No runtime payload is currently available for this workbook family, so the dashboard proposal has to rely on structure and semantics rather than computed metrics.",
            confidence_score=0.55,
            supporting_metrics=[],
        )
    summary = _summarize_runtime_for_llm(runtime_payload)
    signal_lines: list[str] = []
    metrics: list[str] = []
    for key in ("total_returns", "open_returns", "completed_returns", "total_products", "product_type_count", "uom_count", "category_count"):
        if key in summary:
            signal_lines.append(f"{key.replace('_', ' ')}={summary[key]}")
            metrics.append(f"{key.replace('_', ' ')} {summary[key]}")
    for dist_key in ("status_counts", "product_type_counts", "category_counts", "uom_counts", "return_type_counts"):
        label, count, total = _top_count_payload(summary.get(dist_key, []))
        if label is not None and count is not None:
            share = (count / total) if total else 0.0
            signal_lines.append(f"top {dist_key.replace('_', ' ')} bucket is {label} at {count} rows ({share:.1%})")
            metrics.append(f"{label} {share:.1%}")
    return DashboardInvestigationEvidence(
        key="runtime_signal_scan",
        tool="runtime_signal_scan",
        title="Runtime signal scan",
        detail="Available runtime metrics show " + "; ".join(signal_lines[:5]) + ".",
        confidence_score=0.9,
        supporting_metrics=metrics[:6],
    )


def _run_concentration_signal_scan(runtime_payload: dict[str, Any] | None) -> DashboardInvestigationEvidence:
    if not runtime_payload:
        return DashboardInvestigationEvidence(
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
    return DashboardInvestigationEvidence(
        key="concentration_signal_scan",
        tool="concentration_signal_scan",
        title="Concentration scan",
        detail=detail,
        confidence_score=0.83 if observations else 0.5,
        supporting_metrics=metrics[:6],
    )


def _run_ambiguity_review(interpretation: SemanticWorkbookInterpretation) -> DashboardInvestigationEvidence:
    detail = (
        "Open dashboard ambiguity remains around "
        + ", ".join(interpretation.ambiguities[:4])
        if interpretation.ambiguities
        else "The semantic interpretation does not currently expose major unresolved ambiguity that would block dashboard design."
    )
    return DashboardInvestigationEvidence(
        key="ambiguity_review",
        tool="ambiguity_review",
        title="Ambiguity review",
        detail=detail,
        confidence_score=0.75 if interpretation.ambiguities else 0.88,
        supporting_metrics=[f"{len(interpretation.ambiguities)} ambiguities", f"{len(interpretation.business_questions)} follow-up questions"],
    )


def _run_dashboard_coverage_check(
    base_config: dict[str, Any],
    interpretation: SemanticWorkbookInterpretation,
    runtime_payload: dict[str, Any] | None,
) -> DashboardInvestigationEvidence:
    tab_labels = [str(tab.get("label") or tab.get("key") or "") for tab in base_config.get("tabs", [])]
    card_labels = [str(card.get("label") or card.get("key") or "") for card in base_config.get("kpi_cards", [])]
    runtime_keys = sorted(_summarize_runtime_for_llm(runtime_payload).keys())
    detail = (
        f"The baseline dashboard currently covers tabs for {', '.join(tab_labels[:4]) or 'no tabs'} and KPI cards for "
        f"{', '.join(card_labels[:4]) or 'no cards'}. "
        f"Interpreted business questions focus on {', '.join(interpretation.business_questions[:3]) or 'general workbook review'}. "
        f"{'Computed runtime metrics are available for ' + ', '.join(runtime_keys[:5]) + '.' if runtime_keys else 'No computed runtime metrics are available yet, so semantic coverage matters more than KPI completeness.'}"
    )
    return DashboardInvestigationEvidence(
        key="dashboard_coverage_check",
        tool="dashboard_coverage_check",
        title="Dashboard coverage check",
        detail=detail,
        confidence_score=0.8,
        supporting_metrics=[f"{len(tab_labels)} tabs", f"{len(card_labels)} cards", f"{len(interpretation.business_questions)} business questions"],
    )


def run_dashboard_investigation(
    *,
    profile,
    interpretation: SemanticWorkbookInterpretation,
    investigation_plan: DashboardInvestigationPlan,
    runtime_payload: dict[str, Any] | None,
    raw_tables: list[RawTable],
    base_config: dict[str, Any],
) -> list[DashboardInvestigationEvidence]:
    tool_map = {
        "schema_inventory_scan": lambda: _run_schema_inventory_scan(profile),
        "semantic_alignment_scan": lambda: _run_semantic_alignment_scan(profile, interpretation),
        "runtime_signal_scan": lambda: _run_runtime_signal_scan(runtime_payload),
        "concentration_signal_scan": lambda: _run_concentration_signal_scan(runtime_payload),
        "ambiguity_review": lambda: _run_ambiguity_review(interpretation),
        "dashboard_coverage_check": lambda: _run_dashboard_coverage_check(base_config, interpretation, runtime_payload),
        "distribution_sql_scan": lambda: _wrap_duckdb_signal("distribution_sql_scan", raw_tables, interpretation, getattr(profile, "workbook_type", "generic_workbook_v1")),
        "top_dimension_sql_scan": lambda: _wrap_duckdb_signal("top_dimension_sql_scan", raw_tables, interpretation, getattr(profile, "workbook_type", "generic_workbook_v1")),
        "measure_by_dimension_sql_scan": lambda: _wrap_duckdb_signal("measure_by_dimension_sql_scan", raw_tables, interpretation, getattr(profile, "workbook_type", "generic_workbook_v1")),
        "quality_gap_sql_scan": lambda: _wrap_duckdb_signal("quality_gap_sql_scan", raw_tables, interpretation, getattr(profile, "workbook_type", "generic_workbook_v1")),
        "cross_dimension_sql_scan": lambda: _wrap_duckdb_signal("cross_dimension_sql_scan", raw_tables, interpretation, getattr(profile, "workbook_type", "generic_workbook_v1")),
    }
    evidence: list[DashboardInvestigationEvidence] = []
    for step in investigation_plan.steps:
        tool = tool_map.get(step.tool)
        if tool is None:
            logger.warning("dashboard agent: skipped unsupported tool %s for snapshot profile %s", step.tool, getattr(profile, "id", "unknown"))
            continue
        item = tool()
        if item is not None:
            evidence.append(item)
    return evidence


def _wrap_duckdb_signal(
    tool_name: str,
    raw_tables: list[RawTable],
    interpretation: SemanticWorkbookInterpretation,
    family: str,
) -> DashboardInvestigationEvidence | None:
    signal = run_dashboard_duckdb_tool(
        tool_name=tool_name,
        tables=raw_tables,
        interpretation=interpretation,
        family=family,
    )
    if signal is None:
        return None
    return DashboardInvestigationEvidence(
        key=tool_name,
        tool=tool_name,  # type: ignore[arg-type]
        title=signal.title,
        detail=signal.detail,
        confidence_score=signal.confidence_score,
        supporting_metrics=signal.supporting_metrics,
    )


def _attach_workflow_metadata(
    config: dict[str, Any],
    interpretation: SemanticWorkbookInterpretation,
    investigation_plan: DashboardInvestigationPlan,
    investigation_evidence: list[DashboardInvestigationEvidence],
) -> dict[str, Any]:
    next_config = dict(config)
    next_config["semantic_summary"] = interpretation.semantic_summary
    next_config["business_questions"] = interpretation.business_questions
    next_config["ambiguities"] = interpretation.ambiguities
    next_config["semantic_confidence"] = interpretation.confidence_score
    next_config["investigation_plan"] = [step.model_dump() for step in investigation_plan.steps]
    next_config["investigation_evidence"] = [item.model_dump() for item in investigation_evidence]
    next_config["proposal_workflow"] = "dashboard_langgraph_v1"
    return next_config


def _validate_proposal_config(base_config: dict[str, Any], next_config: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if next_config.get("dashboard_family") != base_config.get("dashboard_family"):
        errors.append("dashboard_family changed during dashboard agent refinement")
    if next_config.get("layout_template") != base_config.get("layout_template"):
        errors.append("layout_template changed during dashboard agent refinement")
    base_tabs = [str(tab.get("key") or "") for tab in base_config.get("tabs", [])]
    next_tabs = [str(tab.get("key") or "") for tab in next_config.get("tabs", [])]
    if base_tabs != next_tabs:
        errors.append("top-level tab keys changed during dashboard agent refinement")
    if not next_config.get("investigation_plan"):
        errors.append("investigation_plan metadata is missing from dashboard proposal config")
    if "proposal_workflow" not in next_config:
        errors.append("proposal_workflow metadata is missing from dashboard proposal config")
    return errors


def run_dashboard_proposal_agent(
    *,
    profile,
    runtime_payload: dict[str, Any] | None,
    raw_tables: list[RawTable] | None,
    user_guidance: str | None,
    base_title: str,
    base_summary: str,
    base_config: dict[str, Any],
    base_rationale: str,
    base_confidence: float,
    gemini: GeminiRequestSettings | None,
) -> DashboardProposalAgentResult:
    llm_runtime_summary = _summarize_runtime_for_llm(runtime_payload)
    available_raw_tables = raw_tables or []

    def interpret_node(_: DashboardProposalAgentState) -> DashboardProposalAgentState:
        interpretation = interpret_workbook_semantics(
            profile=profile,
            runtime_summary=llm_runtime_summary,
            raw_tables=available_raw_tables,
            gemini=gemini,
            user_guidance=user_guidance,
        )
        return {"interpretation": interpretation}

    def plan_node(state: DashboardProposalAgentState) -> DashboardProposalAgentState:
        plan = build_dashboard_investigation_plan(
            profile=profile,
            interpretation=state["interpretation"],
            runtime_payload=runtime_payload,
            raw_tables=available_raw_tables,
            base_config=base_config,
            gemini=gemini,
            user_guidance=user_guidance,
        )
        return {"investigation_plan": plan}

    def run_tools_node(state: DashboardProposalAgentState) -> DashboardProposalAgentState:
        evidence = run_dashboard_investigation(
            profile=profile,
            interpretation=state["interpretation"],
            investigation_plan=state["investigation_plan"],
            runtime_payload=runtime_payload,
            raw_tables=available_raw_tables,
            base_config=base_config,
        )
        return {"investigation_evidence": evidence}

    def refine_node(state: DashboardProposalAgentState) -> DashboardProposalAgentState:
        title, summary, config, rationale, confidence = plan_dashboard_hypothesis(
            profile=profile,
            runtime_summary=llm_runtime_summary,
            base_title=base_title,
            base_summary=base_summary,
            base_config=base_config,
            base_rationale=base_rationale,
            base_confidence=base_confidence,
            interpretation=state["interpretation"],
            gemini=gemini,
            investigation_plan=state["investigation_plan"],
            investigation_evidence=state["investigation_evidence"],
            user_guidance=user_guidance,
        )
        config = _attach_workflow_metadata(
            config,
            state["interpretation"],
            state["investigation_plan"],
            state["investigation_evidence"],
        )
        return {
            "title": title,
            "summary": summary,
            "config": config,
            "rationale": rationale,
            "confidence_score": confidence,
        }

    def validate_node(state: DashboardProposalAgentState) -> DashboardProposalAgentState:
        return {"validation_errors": _validate_proposal_config(base_config, state["config"])}

    graph = StateGraph(DashboardProposalAgentState)
    graph.add_node("interpret", interpret_node)
    graph.add_node("plan_investigation", plan_node)
    graph.add_node("run_tools", run_tools_node)
    graph.add_node("refine_proposal", refine_node)
    graph.add_node("validate", validate_node)
    graph.add_edge(START, "interpret")
    graph.add_edge("interpret", "plan_investigation")
    graph.add_edge("plan_investigation", "run_tools")
    graph.add_edge("run_tools", "refine_proposal")
    graph.add_edge("refine_proposal", "validate")
    graph.add_edge("validate", END)
    compiled = graph.compile(checkpointer=InMemorySaver())

    result = compiled.invoke({}, config={"configurable": {"thread_id": f"dashboard:{getattr(profile, 'snapshot_id', getattr(profile, 'id', 'unknown'))}"}})
    if result.get("validation_errors"):
        logger.warning(
            "dashboard agent: validation failed for snapshot profile %s, falling back to deterministic semantic workflow: %s",
            getattr(profile, "id", "unknown"),
            "; ".join(result["validation_errors"]),
        )
        interpretation = result["interpretation"]
        plan = result["investigation_plan"]
        evidence = result.get("investigation_evidence", [])
        title, summary, config, rationale, confidence = plan_dashboard_hypothesis(
            profile=profile,
            runtime_summary=llm_runtime_summary,
            base_title=base_title,
            base_summary=base_summary,
            base_config=base_config,
            base_rationale=base_rationale,
            base_confidence=base_confidence,
            interpretation=interpretation,
            gemini=None,
            investigation_plan=plan,
            investigation_evidence=evidence,
            user_guidance=user_guidance,
        )
        config = _attach_workflow_metadata(config, interpretation, plan, evidence)
        return DashboardProposalAgentResult(
            title=title,
            summary=summary,
            config=config,
            rationale=rationale,
            confidence_score=confidence,
            interpretation=interpretation,
            investigation_plan=plan,
            investigation_evidence=evidence,
            workflow_mode="dashboard_langgraph_v1:fallback",
        )

    return DashboardProposalAgentResult(
        title=result["title"],
        summary=result["summary"],
        config=result["config"],
        rationale=result["rationale"],
        confidence_score=result["confidence_score"],
        interpretation=result["interpretation"],
        investigation_plan=result["investigation_plan"],
        investigation_evidence=result.get("investigation_evidence", []),
        workflow_mode="dashboard_langgraph_v1",
    )
