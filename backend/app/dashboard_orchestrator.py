from __future__ import annotations

from typing import Any, TypedDict

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel

from app.app_logs import log_app_event
from app.dashboard_semantics import SemanticWorkbookInterpretation, interpret_workbook_semantics, plan_dashboard_hypothesis
from app.eda_agent import EDAAgentResult, run_eda_agent
from app.gemini_reasoning import GeminiRequestSettings
from app.raw_data_store import RawTable


class DashboardOrchestratorResult(BaseModel):
    title: str
    summary: str
    config: dict[str, Any]
    rationale: str
    confidence_score: float
    interpretation: SemanticWorkbookInterpretation
    eda_result: EDAAgentResult
    workflow_mode: str


class DashboardOrchestratorState(TypedDict, total=False):
    interpretation: SemanticWorkbookInterpretation
    eda_result: EDAAgentResult
    title: str
    summary: str
    config: dict[str, Any]
    rationale: str
    confidence_score: float
    validation_errors: list[str]


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
    return summary


def _attach_workflow_metadata(
    config: dict[str, Any],
    interpretation: SemanticWorkbookInterpretation,
    eda_result: EDAAgentResult,
) -> dict[str, Any]:
    next_config = dict(config)
    next_config["semantic_summary"] = interpretation.semantic_summary
    next_config["business_questions"] = interpretation.business_questions
    next_config["ambiguities"] = interpretation.ambiguities
    next_config["semantic_confidence"] = interpretation.confidence_score
    next_config["eda_plan"] = [step.model_dump() for step in eda_result.plan.steps]
    next_config["eda_evidence"] = [item.model_dump() for item in eda_result.evidence]
    next_config["eda_workflow"] = eda_result.workflow_mode
    next_config["orchestrator_workflow"] = "dashboard_orchestrator_v1"
    next_config["proposal_workflow"] = "dashboard_orchestrator_v1"
    next_config["investigation_plan"] = next_config["eda_plan"]
    next_config["investigation_evidence"] = next_config["eda_evidence"]
    return next_config


def _validate_proposal_config(base_config: dict[str, Any], next_config: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if next_config.get("dashboard_family") != base_config.get("dashboard_family"):
        errors.append("dashboard_family changed during dashboard design refinement")
    base_tabs = [str(tab.get("key") or "") for tab in base_config.get("tabs", [])]
    next_tabs = [str(tab.get("key") or "") for tab in next_config.get("tabs", [])]
    adaptive_generic = (
        str(base_config.get("dashboard_family") or "") == "generic_review_dashboard"
        and bool(next_config.get("adaptive_dashboard_enabled"))
    )
    if not adaptive_generic and next_config.get("layout_template") != base_config.get("layout_template"):
        errors.append("layout_template changed during dashboard design refinement")
    if not adaptive_generic and base_tabs != next_tabs:
        errors.append("top-level tab keys changed during dashboard design refinement")
    if not next_config.get("eda_plan"):
        errors.append("eda_plan metadata is missing from dashboard proposal config")
    if "proposal_workflow" not in next_config:
        errors.append("proposal_workflow metadata is missing from dashboard proposal config")
    return errors


def run_dashboard_orchestrator(
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
) -> DashboardOrchestratorResult:
    llm_runtime_summary = _summarize_runtime_for_llm(runtime_payload)
    available_raw_tables = raw_tables or []
    log_app_event(
        level="info",
        state="started",
        category="dashboard_orchestrator",
        event="orchestrator_started",
        agent_name="dashboard_orchestrator",
        workflow="dashboard_orchestrator_v1",
        message="Dashboard orchestrator started",
        payload={
            "snapshot_id": getattr(profile, "snapshot_id", None),
            "workbook_type": getattr(profile, "workbook_type", None),
            "has_gemini": gemini is not None,
        },
        snapshot_id=getattr(profile, "snapshot_id", None),
    )

    def interpret_node(_: DashboardOrchestratorState) -> DashboardOrchestratorState:
        interpretation = interpret_workbook_semantics(
            profile=profile,
            runtime_summary=llm_runtime_summary,
            raw_tables=available_raw_tables,
            gemini=gemini,
            user_guidance=user_guidance,
        )
        log_app_event(
            level="info",
            state="completed",
            category="dashboard_orchestrator",
            event="semantic_interpretation_completed",
            agent_name="semantic_interpreter",
            workflow="semantic_interpretation_v1",
            message="Semantic interpretation completed",
            payload={
                "confidence_score": interpretation.confidence_score,
                "dominant_domain": interpretation.dominant_domain,
            },
            snapshot_id=getattr(profile, "snapshot_id", None),
        )
        return {"interpretation": interpretation}

    def eda_node(state: DashboardOrchestratorState) -> DashboardOrchestratorState:
        eda_result = run_eda_agent(
            profile=profile,
            interpretation=state["interpretation"],
            runtime_payload=runtime_payload,
            raw_tables=available_raw_tables,
            gemini=gemini,
            user_guidance=user_guidance,
        )
        log_app_event(
            level="info",
            state="completed",
            category="dashboard_orchestrator",
            event="eda_completed",
            agent_name="eda_agent",
            workflow=eda_result.workflow_mode,
            message="EDA agent completed",
            payload={
                "step_count": len(eda_result.plan.steps),
                "evidence_count": len(eda_result.evidence),
            },
            snapshot_id=getattr(profile, "snapshot_id", None),
        )
        return {"eda_result": eda_result}

    def design_node(state: DashboardOrchestratorState) -> DashboardOrchestratorState:
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
            investigation_plan=state["eda_result"].plan,
            investigation_evidence=state["eda_result"].evidence,
            user_guidance=user_guidance,
        )
        config = _attach_workflow_metadata(config, state["interpretation"], state["eda_result"])
        log_app_event(
            level="info",
            state="completed",
            category="dashboard_orchestrator",
            event="design_completed",
            agent_name="dashboard_hypothesis_planner",
            workflow="dashboard_design_v1",
            message="Dashboard design hypothesis completed",
            payload={"confidence_score": confidence, "title": title},
            snapshot_id=getattr(profile, "snapshot_id", None),
        )
        return {
            "title": title,
            "summary": summary,
            "config": config,
            "rationale": rationale,
            "confidence_score": confidence,
        }

    def validate_node(state: DashboardOrchestratorState) -> DashboardOrchestratorState:
        return {"validation_errors": _validate_proposal_config(base_config, state["config"])}

    graph = StateGraph(DashboardOrchestratorState)
    graph.add_node("interpret", interpret_node)
    graph.add_node("eda", eda_node)
    graph.add_node("design", design_node)
    graph.add_node("validate", validate_node)
    graph.add_edge(START, "interpret")
    graph.add_edge("interpret", "eda")
    graph.add_edge("eda", "design")
    graph.add_edge("design", "validate")
    graph.add_edge("validate", END)
    compiled = graph.compile(checkpointer=InMemorySaver())
    result = compiled.invoke({}, config={"configurable": {"thread_id": f"dashboard-orchestrator:{getattr(profile, 'snapshot_id', getattr(profile, 'id', 'unknown'))}"}})

    if result.get("validation_errors"):
        interpretation = result["interpretation"]
        eda_result = result["eda_result"]
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
            investigation_plan=eda_result.plan,
            investigation_evidence=eda_result.evidence,
            user_guidance=user_guidance,
        )
        config = _attach_workflow_metadata(config, interpretation, eda_result)
        log_app_event(
            level="warning",
            state="fallback",
            category="dashboard_orchestrator",
            event="orchestrator_fallback",
            agent_name="dashboard_orchestrator",
            workflow="dashboard_orchestrator_v1:fallback",
            message="Dashboard orchestrator fell back after validation errors",
            payload={"validation_errors": result.get("validation_errors", [])},
            snapshot_id=getattr(profile, "snapshot_id", None),
        )
        return DashboardOrchestratorResult(
            title=title,
            summary=summary,
            config=config,
            rationale=rationale,
            confidence_score=confidence,
            interpretation=interpretation,
            eda_result=eda_result,
            workflow_mode="dashboard_orchestrator_v1:fallback",
        )

    log_app_event(
        level="info",
        state="completed",
        category="dashboard_orchestrator",
        event="orchestrator_completed",
        agent_name="dashboard_orchestrator",
        workflow="dashboard_orchestrator_v1",
        message="Dashboard orchestrator completed",
        payload={"title": result["title"], "confidence_score": result["confidence_score"]},
        snapshot_id=getattr(profile, "snapshot_id", None),
    )
    return DashboardOrchestratorResult(
        title=result["title"],
        summary=result["summary"],
        config=result["config"],
        rationale=result["rationale"],
        confidence_score=result["confidence_score"],
        interpretation=result["interpretation"],
        eda_result=result["eda_result"],
        workflow_mode="dashboard_orchestrator_v1",
    )
