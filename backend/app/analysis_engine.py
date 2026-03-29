from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from statistics import median
from typing import Any, Literal

from app.duckdb_analysis import run_duckdb_analysis
from app.pandas_sandbox import run_pandas_sandbox_analysis
from app.raw_data_store import RawTable
from app.workbook_families import normalize_header


@dataclass(frozen=True)
class FieldProfile:
    name: str
    normalized_name: str
    data_type: Literal["numeric", "categorical", "text", "date", "unknown"]
    non_null_count: int
    distinct_count: int
    missing_rate: float
    sample_values: list[str]


@dataclass(frozen=True)
class TableProfile:
    name: str
    row_count: int
    fields: list[FieldProfile]


@dataclass(frozen=True)
class AnalysisPlanStep:
    key: str
    table_name: str
    operator: str
    rationale: str
    priority: int
    dimension: str | None = None
    measure: str | None = None


@dataclass(frozen=True)
class AnalysisEvidence:
    id: str
    table_name: str
    title: str
    detail: str


@dataclass(frozen=True)
class AnalysisFinding:
    key: str
    title: str
    insight: str
    implication: str
    priority: Literal["critical", "high", "medium", "low"]
    confidence: Literal["high", "medium", "low"]
    materiality: float
    actionability: float
    score: float
    evidence_ids: list[str]


@dataclass(frozen=True)
class ReasonedAction:
    action: str
    rationale: str
    confidence: Literal["high", "medium", "low"]
    evidence_ids: list[str]


@dataclass(frozen=True)
class CoverageNote:
    text: str
    evidence_ids: list[str]


@dataclass(frozen=True)
class AnalysisReport:
    family: str
    plan_summary: str
    profiles: list[TableProfile]
    steps: list[AnalysisPlanStep]
    evidence: list[AnalysisEvidence]
    findings: list[AnalysisFinding]
    actions: list[ReasonedAction]
    limitations: list[CoverageNote]


def _is_numeric(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    if value in (None, ""):
        return False
    text = str(value).strip().replace(",", "")
    if not text:
        return False
    try:
        float(text)
        return True
    except Exception:
        return False


def _to_float(value: Any) -> float | None:
    if _is_numeric(value):
        try:
            return float(str(value).strip().replace(",", ""))
        except Exception:
            return None
    return None


def _sample_values(values: list[Any], limit: int = 4) -> list[str]:
    seen: list[str] = []
    for value in values:
        if value in (None, ""):
            continue
        text = str(value)
        if text not in seen:
            seen.append(text)
        if len(seen) >= limit:
            break
    return seen


def _infer_field_type(values: list[Any]) -> Literal["numeric", "categorical", "text", "date", "unknown"]:
    non_null = [value for value in values if value not in (None, "")]
    if not non_null:
        return "unknown"
    numeric = sum(1 for value in non_null if _is_numeric(value))
    if numeric == len(non_null):
        return "numeric"
    distinct = len({str(value) for value in non_null})
    if distinct <= 25:
        return "categorical"
    return "text"


def profile_tables(tables: list[RawTable]) -> list[TableProfile]:
    profiles: list[TableProfile] = []
    for table in tables:
        fields: list[FieldProfile] = []
        for header in table.headers:
            values = [row.get(header) for row in table.rows]
            non_null = [value for value in values if value not in (None, "")]
            distinct = len({str(value) for value in non_null})
            fields.append(
                FieldProfile(
                    name=header,
                    normalized_name=normalize_header(header),
                    data_type=_infer_field_type(values),
                    non_null_count=len(non_null),
                    distinct_count=distinct,
                    missing_rate=1 - (len(non_null) / len(table.rows) if table.rows else 0.0),
                    sample_values=_sample_values(values),
                )
            )
        profiles.append(TableProfile(name=table.name, row_count=len(table.rows), fields=fields))
    return profiles


def _rank_dimension(field: FieldProfile, row_count: int, family: str) -> float:
    if field.data_type not in {"categorical", "text"}:
        return -1.0
    if field.distinct_count < 2:
        return -1.0
    if row_count and field.distinct_count > max(25, row_count // 2):
        return -1.0
    score = 0.2
    name = field.normalized_name
    if any(token in name for token in ("status", "type", "category", "uom", "client", "staff", "return")):
        score += 0.5
    if "id" in name:
        score -= 0.35
    if "name" in name:
        score -= 0.45
    if row_count <= 10 and any(token in name for token in ("name", "id")):
        score -= 0.40
    if family == "product_master_v1" and any(token in name for token in ("category", "type", "uom")):
        score += 0.4
    if family == "client_status_report_v1" and any(token in name for token in ("status", "type", "client type")):
        score += 0.4
    if family == "contracted_actual_v1" and any(token in name for token in ("client type", "staff type")):
        score += 0.4
    score += max(0.0, 0.2 - abs(field.distinct_count - 8) * 0.01)
    return score


def _rank_measure(field: FieldProfile, family: str) -> float:
    if field.data_type != "numeric":
        return -1.0
    score = 0.2
    name = field.normalized_name
    if any(token in name for token in ("contracted", "received", "pending", "age", "days")):
        score += 0.6
    if family == "contracted_actual_v1" and any(token in name for token in ("contracted", "received", "pending", "rate")):
        score += 0.4
    return score


def _table_lookup(tables: list[RawTable]) -> dict[str, RawTable]:
    return {table.name: table for table in tables}


def _profile_lookup(profiles: list[TableProfile]) -> dict[str, TableProfile]:
    return {profile.name: profile for profile in profiles}


def _largest_table(profiles: list[TableProfile]) -> TableProfile | None:
    return max(profiles, key=lambda item: item.row_count, default=None)


def plan_analysis(tables: list[RawTable], profiles: list[TableProfile], family: str) -> tuple[str, list[AnalysisPlanStep]]:
    steps: list[AnalysisPlanStep] = []
    largest = _largest_table(profiles)
    if largest is None:
        return ("No tables were available for analysis.", [])
    profile_map = _profile_lookup(profiles)
    key_idx = 1

    def add_step(table_name: str, operator: str, rationale: str, priority: int, dimension: str | None = None, measure: str | None = None):
        nonlocal key_idx
        steps.append(
            AnalysisPlanStep(
                key=f"STEP-{key_idx:03d}",
                table_name=table_name,
                operator=operator,
                rationale=rationale,
                priority=priority,
                dimension=dimension,
                measure=measure,
            )
        )
        key_idx += 1

    for profile in profiles:
        add_step(profile.name, "completeness_scan", "Check completeness and structural quality before trusting deeper findings.", 1)

    for profile in profiles:
        dimensions = sorted(
            [field for field in profile.fields],
            key=lambda field: _rank_dimension(field, profile.row_count, family),
            reverse=True,
        )
        measures = sorted([field for field in profile.fields], key=lambda field: _rank_measure(field, family), reverse=True)
        top_dimensions = [field for field in dimensions if _rank_dimension(field, profile.row_count, family) > 0][:3]
        top_measures = [field for field in measures if _rank_measure(field, family) > 0][:3]

        for field in top_dimensions:
            add_step(profile.name, "segment_concentration", "Measure whether a few segments dominate the population or workload.", 2, dimension=field.name)

        if family == "contracted_actual_v1":
            if profile.name == "clients":
                add_step(profile.name, "top_entities_by_measure", "Find the clients carrying the largest backlog exposure.", 1, dimension="client_name", measure="pending_total")
                add_step(profile.name, "top_entities_by_measure", "Find the clients driving the largest contracted workload.", 2, dimension="client_name", measure="contracted_total")
            relevant_measures = [field for field in top_measures if field.normalized_name in {"contracted total", "received total", "pending total", "receipt rate"}]
            for field in relevant_measures[:3]:
                for dimension in top_dimensions[:2]:
                    add_step(profile.name, "measure_by_dimension", "Compare execution and backlog signals across the strongest business dimensions.", 3, dimension=dimension.name, measure=field.name)
        elif family == "client_status_report_v1":
            if any(field.normalized_name == "age days" for field in profile.fields):
                add_step(profile.name, "numeric_outliers", "Look for aging exceptions that signal hidden queue risk.", 3, measure="Age (days)")
        elif family == "product_master_v1":
            add_step(profile.name, "field_cardinality", "Assess standardization complexity across classification fields.", 3)
        else:
            add_step(profile.name, "field_cardinality", "Review semantic readiness by checking categorical spread and structural variety.", 3)

    steps.sort(key=lambda item: (item.priority, item.table_name, item.operator))
    summary = (
        "The planner profiles the preserved raw tables, prioritizes the most informative dimensions and measures, "
        "then schedules completeness, concentration, segmentation, and exception scans before report writing."
    )
    return summary, steps[:14]


def _make_evidence(evidence: list[AnalysisEvidence], table_name: str, title: str, detail: str) -> str:
    evidence_id = f"ANL-{len(evidence)+1:03d}"
    evidence.append(AnalysisEvidence(id=evidence_id, table_name=table_name, title=title, detail=detail))
    return evidence_id


def _priority_from_score(score: float) -> Literal["critical", "high", "medium", "low"]:
    if score >= 0.78:
        return "critical"
    if score >= 0.58:
        return "high"
    if score >= 0.36:
        return "medium"
    return "low"


def _confidence_from_support(row_count: int, support_count: int) -> Literal["high", "medium", "low"]:
    if support_count >= max(10, row_count * 0.2):
        return "high"
    if support_count >= max(3, row_count * 0.05):
        return "medium"
    return "low"


def _count_by(rows: list[dict[str, Any]], field: str) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        value = row.get(field)
        label = str(value).strip() if value not in (None, "") else "Unknown"
        counts[label] += 1
    return counts


def _sum_by(rows: list[dict[str, Any]], dimension: str, measure: str) -> dict[str, float]:
    sums: dict[str, float] = defaultdict(float)
    for row in rows:
        dimension_value = str(row.get(dimension)).strip() if row.get(dimension) not in (None, "") else "Unknown"
        measure_value = _to_float(row.get(measure))
        if measure_value is not None:
            sums[dimension_value] += measure_value
    return dict(sums)


def _run_completeness_step(step: AnalysisPlanStep, table: RawTable, profile: TableProfile, evidence: list[AnalysisEvidence]) -> list[AnalysisFinding]:
    findings: list[AnalysisFinding] = []
    for field in sorted(profile.fields, key=lambda item: item.missing_rate, reverse=True)[:4]:
        if field.missing_rate < 0.05:
            continue
        evidence_id = _make_evidence(
            evidence,
            table.name,
            f"Completeness for {field.name}",
            f"Field={field.name}, missing_rate={field.missing_rate:.1%}, non_null={field.non_null_count}, distinct={field.distinct_count}.",
        )
        materiality = min(1.0, field.missing_rate * 1.4)
        actionability = 0.8 if any(token in field.normalized_name for token in ("id", "description", "status", "type", "category")) else 0.55
        score = materiality * 0.55 + actionability * 0.45
        findings.append(
            AnalysisFinding(
                key=f"{step.key}-{normalize_header(field.name)[:16]}",
                title=f"Missing Data in {field.name}",
                insight=f"{field.name} is missing in {field.missing_rate:.1%} of rows in {table.name}.",
                implication="Incomplete high-use fields reduce confidence in downstream segmentation, control views, and recommendations.",
                priority=_priority_from_score(score),
                confidence=_confidence_from_support(profile.row_count, int(profile.row_count * (1 - field.missing_rate))),
                materiality=materiality,
                actionability=actionability,
                score=score,
                evidence_ids=[evidence_id],
            )
        )
    return findings


def _run_concentration_step(step: AnalysisPlanStep, table: RawTable, evidence: list[AnalysisEvidence]) -> list[AnalysisFinding]:
    if not step.dimension:
        return []
    counts = _count_by(table.rows, step.dimension)
    total = sum(counts.values())
    if total == 0 or len(counts) < 2:
        return []
    ordered = counts.most_common(5)
    top_label, top_count = ordered[0]
    top_share = top_count / total
    top5_share = sum(count for _, count in ordered) / total
    if len(counts) <= 5 and top_share <= 0.4:
        return []
    evidence_id = _make_evidence(
        evidence,
        table.name,
        f"Concentration by {step.dimension}",
        f"Dimension={step.dimension}, top_segment={top_label}, top_share={top_share:.1%}, top_5_share={top5_share:.1%}, distinct_segments={len(counts)}.",
    )
    materiality = max(top_share, top5_share * 0.75)
    actionability = 0.8 if any(token in normalize_header(step.dimension) for token in ("status", "type", "category", "uom")) else 0.6
    score = materiality * 0.6 + actionability * 0.4
    if top_share < 0.18 and top5_share < 0.45:
        return []
    return [
        AnalysisFinding(
            key=f"{step.key}-{normalize_header(step.dimension)[:16]}",
            title=f"Concentration in {step.dimension}",
            insight=f"{top_label} is the largest segment in {step.dimension} at {top_share:.1%} of {table.name}, while the top five segments account for {top5_share:.1%}.",
            implication="A small set of segments is shaping the overall story, so leadership focus should stay on the dominant groups rather than the full population.",
            priority=_priority_from_score(score),
            confidence=_confidence_from_support(total, top_count),
            materiality=materiality,
            actionability=actionability,
            score=score,
            evidence_ids=[evidence_id],
        )
    ]


def _run_top_entities_step(step: AnalysisPlanStep, table: RawTable, evidence: list[AnalysisEvidence]) -> list[AnalysisFinding]:
    if not step.dimension or not step.measure:
        return []
    grouped = _sum_by(table.rows, step.dimension, step.measure)
    total = sum(grouped.values())
    if total <= 0 or len(grouped) < 2:
        return []
    ordered = sorted(grouped.items(), key=lambda item: item[1], reverse=True)[:5]
    top_label, top_value = ordered[0]
    top_share = top_value / total
    top5_share = sum(value for _, value in ordered) / total
    evidence_id = _make_evidence(
        evidence,
        table.name,
        f"Top entities by {step.measure}",
        f"Dimension={step.dimension}, measure={step.measure}, top_entity={top_label}, top_value={top_value:.2f}, top_share={top_share:.1%}, top_5_share={top5_share:.1%}.",
    )
    materiality = max(top_share, top5_share * 0.75)
    actionability = 0.9
    score = materiality * 0.7 + actionability * 0.3
    return [
        AnalysisFinding(
            key=f"{step.key}-{normalize_header(step.measure)[:12]}",
            title=f"Top Entities by {step.measure}",
            insight=f"The largest {step.dimension} for {step.measure} is {top_label}, contributing {top_share:.1%} of the total; the top five entities contribute {top5_share:.1%}.",
            implication="Workload and risk are concentrated in a small named entity set, so escalation and follow-up should be targeted there first.",
            priority=_priority_from_score(score),
            confidence=_confidence_from_support(len(table.rows), len(ordered)),
            materiality=materiality,
            actionability=actionability,
            score=score,
            evidence_ids=[evidence_id],
        )
    ]


def _run_measure_by_dimension_step(step: AnalysisPlanStep, table: RawTable, evidence: list[AnalysisEvidence]) -> list[AnalysisFinding]:
    if not step.dimension or not step.measure:
        return []
    grouped = _sum_by(table.rows, step.dimension, step.measure)
    total = sum(grouped.values())
    if total <= 0 or len(grouped) < 2:
        return []
    ordered = sorted(grouped.items(), key=lambda item: item[1], reverse=True)[:5]
    top_label, top_value = ordered[0]
    top_share = top_value / total
    evidence_id = _make_evidence(
        evidence,
        table.name,
        f"{step.measure} by {step.dimension}",
        f"Dimension={step.dimension}, measure={step.measure}, top_segment={top_label}, top_value={top_value:.2f}, top_share={top_share:.1%}, total={total:.2f}.",
    )
    actionability = 0.85 if any(token in normalize_header(step.measure) for token in ("pending", "received", "contracted")) else 0.6
    materiality = top_share
    score = materiality * 0.65 + actionability * 0.35
    if top_share < 0.2:
        return []
    implication = (
        "Performance is not evenly distributed, so operational action should be prioritized at the segment level."
        if any(token in normalize_header(step.measure) for token in ("pending", "received", "contracted"))
        else "A small segment set is dominating this measure and should be reviewed before treating portfolio averages as representative."
    )
    return [
        AnalysisFinding(
            key=f"{step.key}-{normalize_header(step.dimension)[:10]}-{normalize_header(step.measure)[:10]}",
            title=f"{step.measure} is concentrated by {step.dimension}",
            insight=f"The largest {step.dimension} segment is {top_label}, representing {top_share:.1%} of total {step.measure}.",
            implication=implication,
            priority=_priority_from_score(score),
            confidence=_confidence_from_support(len(table.rows), max(1, int(top_share * len(table.rows)))),
            materiality=materiality,
            actionability=actionability,
            score=score,
            evidence_ids=[evidence_id],
        )
    ]


def _run_numeric_outlier_step(step: AnalysisPlanStep, table: RawTable, evidence: list[AnalysisEvidence]) -> list[AnalysisFinding]:
    if not step.measure:
        return []
    values = [_to_float(row.get(step.measure)) for row in table.rows]
    series = [value for value in values if value is not None]
    if len(series) < 6:
        return []
    ordered = sorted(series)
    q1 = ordered[len(ordered) // 4]
    q3 = ordered[(len(ordered) * 3) // 4]
    iqr = q3 - q1
    threshold = q3 + (1.5 * iqr)
    outliers = [value for value in series if value > threshold]
    if not outliers:
        return []
    evidence_id = _make_evidence(
        evidence,
        table.name,
        f"Outliers in {step.measure}",
        f"Measure={step.measure}, q1={q1:.2f}, q3={q3:.2f}, iqr={iqr:.2f}, outlier_threshold={threshold:.2f}, outlier_count={len(outliers)}.",
    )
    materiality = min(1.0, len(outliers) / len(series) + (max(outliers) / max(q3, 1)) * 0.1)
    actionability = 0.75
    score = materiality * 0.55 + actionability * 0.45
    return [
        AnalysisFinding(
            key=f"{step.key}-{normalize_header(step.measure)[:16]}",
            title=f"Outliers in {step.measure}",
            insight=f"{len(outliers)} rows in {table.name} exceed the high-side outlier threshold for {step.measure}.",
            implication="Extreme values may represent genuine escalation risk or data exceptions and should be reviewed before using averages as the operating signal.",
            priority=_priority_from_score(score),
            confidence=_confidence_from_support(len(series), len(outliers)),
            materiality=materiality,
            actionability=actionability,
            score=score,
            evidence_ids=[evidence_id],
        )
    ]


def _run_field_cardinality_step(step: AnalysisPlanStep, table: RawTable, profile: TableProfile, evidence: list[AnalysisEvidence]) -> list[AnalysisFinding]:
    findings: list[AnalysisFinding] = []
    for field in sorted(profile.fields, key=lambda item: item.distinct_count, reverse=True)[:4]:
        if field.data_type not in {"categorical", "text"} or field.distinct_count < 10:
            continue
        if any(token in field.normalized_name for token in ("id", "description", "name")):
            continue
        if profile.row_count and (field.distinct_count / profile.row_count) >= 0.95:
            continue
        evidence_id = _make_evidence(
            evidence,
            table.name,
            f"Cardinality in {field.name}",
            f"Field={field.name}, distinct_values={field.distinct_count}, row_count={profile.row_count}, missing_rate={field.missing_rate:.1%}.",
        )
        materiality = min(1.0, field.distinct_count / max(profile.row_count, 1))
        actionability = 0.75 if any(token in field.normalized_name for token in ("category", "uom", "type")) else 0.55
        score = materiality * 0.45 + actionability * 0.55
        findings.append(
            AnalysisFinding(
                key=f"{step.key}-{normalize_header(field.name)[:16]}",
                title=f"High Cardinality in {field.name}",
                insight=f"{field.name} contains {field.distinct_count} distinct values across {profile.row_count} rows in {table.name}.",
                implication="High categorical spread increases governance and standardization effort and can complicate reporting consistency.",
                priority=_priority_from_score(score),
                confidence=_confidence_from_support(profile.row_count, field.distinct_count),
                materiality=materiality,
                actionability=actionability,
                score=score,
                evidence_ids=[evidence_id],
            )
        )
    return findings


def execute_plan(tables: list[RawTable], profiles: list[TableProfile], steps: list[AnalysisPlanStep]) -> tuple[list[AnalysisEvidence], list[AnalysisFinding]]:
    evidence: list[AnalysisEvidence] = []
    findings: list[AnalysisFinding] = []
    table_map = _table_lookup(tables)
    profile_map = _profile_lookup(profiles)
    for step in steps:
        table = table_map.get(step.table_name)
        profile = profile_map.get(step.table_name)
        if table is None or profile is None:
            continue
        if step.operator == "completeness_scan":
            findings.extend(_run_completeness_step(step, table, profile, evidence))
        elif step.operator == "segment_concentration":
            findings.extend(_run_concentration_step(step, table, evidence))
        elif step.operator == "measure_by_dimension":
            findings.extend(_run_measure_by_dimension_step(step, table, evidence))
        elif step.operator == "top_entities_by_measure":
            findings.extend(_run_top_entities_step(step, table, evidence))
        elif step.operator == "numeric_outliers":
            findings.extend(_run_numeric_outlier_step(step, table, evidence))
        elif step.operator == "field_cardinality":
            findings.extend(_run_field_cardinality_step(step, table, profile, evidence))
    return evidence, findings


def evaluate_findings(findings: list[AnalysisFinding]) -> list[AnalysisFinding]:
    ranked = sorted(findings, key=lambda item: item.score, reverse=True)
    selected: list[AnalysisFinding] = []
    seen_titles: set[str] = set()
    for finding in ranked:
        normalized_title = normalize_header(finding.title)
        if any(token in normalized_title for token in ("staff id", "client id", "employee id")) and (
            finding.actionability < 0.8 or finding.confidence == "low" or finding.score < 0.72
        ):
            continue
        if "concentration in" in normalized_title and any(token in normalized_title for token in ("staff name", "client name")) and (
            finding.actionability < 0.8 or finding.confidence == "low" or finding.score < 0.75
        ):
            continue
        title_key = normalize_header(finding.title)
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)
        selected.append(finding)
        if len(selected) >= 6:
            break
    return selected


def derive_actions(family: str, findings: list[AnalysisFinding]) -> list[ReasonedAction]:
    actions: list[ReasonedAction] = []
    for finding in findings:
        title = normalize_header(finding.title)
        if "concentration" in title:
            actions.append(
                ReasonedAction(
                    action=f"Focus review on the dominant segment highlighted by {finding.title}.",
                    rationale=finding.implication,
                    confidence=finding.confidence,
                    evidence_ids=finding.evidence_ids,
                )
            )
        elif "missing data" in title:
            actions.append(
                ReasonedAction(
                    action=f"Remediate completeness issues tied to {finding.title}.",
                    rationale=finding.implication,
                    confidence=finding.confidence,
                    evidence_ids=finding.evidence_ids,
                )
            )
        elif "outliers" in title:
            actions.append(
                ReasonedAction(
                    action=f"Review the extreme rows behind {finding.title} before relying on summary averages.",
                    rationale=finding.implication,
                    confidence=finding.confidence,
                    evidence_ids=finding.evidence_ids,
                )
            )
        elif "cardinality" in title:
            actions.append(
                ReasonedAction(
                    action=f"Standardize the field behind {finding.title} before scaling reporting or automation.",
                    rationale=finding.implication,
                    confidence=finding.confidence,
                    evidence_ids=finding.evidence_ids,
                )
            )
    if not actions and findings:
        top = findings[0]
        actions.append(
            ReasonedAction(
                action="Prioritize the highest-ranked finding in the next operating review.",
                rationale=top.implication,
                confidence=top.confidence,
                evidence_ids=top.evidence_ids,
            )
        )
    return actions[:4]


def derive_limitations(family: str, tables: list[RawTable], evidence: list[AnalysisEvidence]) -> list[CoverageNote]:
    limitations: list[CoverageNote] = []
    if family == "contracted_actual_v1":
        limitations.append(
            CoverageNote(
                text="The workbook is already aggregated at the client and staff level, so deeper transaction-level slicing is not available from this source.",
                evidence_ids=[evidence[0].id] if evidence else [],
            )
        )
    elif family == "product_master_v1":
        limitations.append(
            CoverageNote(
                text="The product dataset supports structural and quality analysis, but not demand or transaction-behavior analysis because those measures are absent.",
                evidence_ids=[evidence[0].id] if evidence else [],
            )
        )
    elif family == "generic_workbook_v1":
        limitations.append(
            CoverageNote(
                text="Generic workbooks need semantic mapping before operator coverage can reach the same depth as mature business families.",
                evidence_ids=[evidence[0].id] if evidence else [],
            )
        )
    else:
        limitations.append(
            CoverageNote(
                text="The current analysis is grounded in the fields preserved from the workbook and does not infer causes beyond those recorded signals.",
                evidence_ids=[evidence[0].id] if evidence else [],
            )
        )
    return limitations[:2]


def _coerce_priority(value: str, score: float) -> Literal["critical", "high", "medium", "low"]:
    normalized = normalize_header(value)
    if normalized in {"critical", "high", "medium", "low"}:
        return normalized  # type: ignore[return-value]
    return _priority_from_score(score)


def _coerce_confidence(value: str) -> Literal["high", "medium", "low"]:
    normalized = normalize_header(value)
    if normalized in {"high", "medium", "low"}:
        return normalized  # type: ignore[return-value]
    return "medium"


def _materialize_external_findings(
    payload: dict[str, Any],
    *,
    prefix: str,
    source_label: str,
    evidence: list[AnalysisEvidence],
) -> tuple[list[AnalysisEvidence], list[AnalysisFinding], list[CoverageNote], str]:
    key_map: dict[str, str] = {}
    extra_evidence: list[AnalysisEvidence] = []
    for item in payload.get("evidence", []):
        evidence_id = f"{prefix}-{len(evidence) + len(extra_evidence) + 1:03d}"
        key_map[str(item.get("key") or evidence_id)] = evidence_id
        extra_evidence.append(
            AnalysisEvidence(
                id=evidence_id,
                table_name=str(item.get("table_name") or source_label),
                title=str(item.get("title") or f"{source_label} evidence"),
                detail=str(item.get("detail") or ""),
            )
        )

    extra_findings: list[AnalysisFinding] = []
    for item in payload.get("findings", []):
        score = float(item.get("score") or 0.0)
        extra_findings.append(
            AnalysisFinding(
                key=str(item.get("key") or f"{prefix}-F{len(extra_findings) + 1:03d}"),
                title=str(item.get("title") or f"{source_label} finding"),
                insight=str(item.get("insight") or ""),
                implication=str(item.get("implication") or ""),
                priority=_coerce_priority(str(item.get("priority") or ""), score),
                confidence=_coerce_confidence(str(item.get("confidence") or "")),
                materiality=float(item.get("materiality") or 0.0),
                actionability=float(item.get("actionability") or 0.0),
                score=score,
                evidence_ids=[key_map[key] for key in item.get("evidence_keys", []) if key in key_map],
            )
        )

    extra_limitations = [
        CoverageNote(
            text=str(item.get("text") or ""),
            evidence_ids=[key_map[key] for key in item.get("evidence_keys", []) if key in key_map],
        )
        for item in payload.get("limitations", [])
        if str(item.get("text") or "").strip()
    ]
    return extra_evidence, extra_findings, extra_limitations, str(payload.get("plan_summary") or "").strip()


def _augment_with_query_engine(
    family: str,
    tables: list[RawTable],
    plan_summary: str,
    evidence: list[AnalysisEvidence],
    findings: list[AnalysisFinding],
    limitations: list[CoverageNote],
    *,
    preferred_engine: Literal["auto", "duckdb", "pandas"] = "auto",
) -> tuple[str, list[AnalysisEvidence], list[AnalysisFinding], list[CoverageNote]]:
    duckdb_payload = {"error": "duckdb skipped"}
    if preferred_engine in {"auto", "duckdb"}:
        duckdb_payload = run_duckdb_analysis(tables, family)
        if not duckdb_payload.get("error"):
            duckdb_evidence, duckdb_findings, duckdb_limitations, duckdb_summary = _materialize_external_findings(
                duckdb_payload,
                prefix="DDB",
                source_label="duckdb",
                evidence=evidence,
            )
            merged_summary = f"{plan_summary} {duckdb_summary}".strip() if duckdb_summary else plan_summary
            return (
                merged_summary,
                [*evidence, *duckdb_evidence],
                [*findings, *duckdb_findings],
                [*limitations, *duckdb_limitations][:3],
            )
        if preferred_engine == "duckdb":
            limitations = [*limitations, CoverageNote(text=str(duckdb_payload["error"]), evidence_ids=[])]
            return plan_summary, evidence, findings, limitations[:3]

    pandas_payload = run_pandas_sandbox_analysis(tables, family)
    if pandas_payload.get("error"):
        limitations = [*limitations]
        if preferred_engine in {"auto", "duckdb"} and duckdb_payload.get("error") and duckdb_payload["error"] != "duckdb skipped":
            limitations.append(CoverageNote(text=str(duckdb_payload["error"]), evidence_ids=[]))
        limitations.append(CoverageNote(text=str(pandas_payload["error"]), evidence_ids=[]))
        return plan_summary, evidence, findings, limitations[:4]

    pandas_evidence, pandas_findings, pandas_limitations, pandas_summary = _materialize_external_findings(
        pandas_payload,
        prefix="PDX",
        source_label="pandas",
        evidence=evidence,
    )
    merged_summary = f"{plan_summary} {pandas_summary}".strip() if pandas_summary else plan_summary
    limitations = [*limitations]
    if preferred_engine == "auto" and duckdb_payload.get("error") and duckdb_payload["error"] != "duckdb skipped":
        limitations.append(CoverageNote(text=f"DuckDB unavailable, fell back to pandas sandbox: {duckdb_payload['error']}", evidence_ids=[]))
    limitations.extend(pandas_limitations)
    return merged_summary, [*evidence, *pandas_evidence], [*findings, *pandas_findings], limitations[:4]


def build_analysis_report(
    tables: list[RawTable],
    family: str,
    include_query_engine: bool = True,
    preferred_engine: Literal["auto", "duckdb", "pandas"] = "auto",
) -> AnalysisReport:
    profiles = profile_tables(tables)
    plan_summary, steps = plan_analysis(tables, profiles, family)
    evidence, candidate_findings = execute_plan(tables, profiles, steps)
    limitations = derive_limitations(family, tables, evidence)
    if include_query_engine:
        plan_summary, evidence, candidate_findings, limitations = _augment_with_query_engine(
            family,
            tables,
            plan_summary,
            evidence,
            candidate_findings,
            limitations,
            preferred_engine=preferred_engine,
        )
    findings = evaluate_findings(candidate_findings)
    return AnalysisReport(
        family=family,
        plan_summary=plan_summary,
        profiles=profiles,
        steps=steps,
        evidence=evidence,
        findings=findings,
        actions=derive_actions(family, findings),
        limitations=limitations,
    )
