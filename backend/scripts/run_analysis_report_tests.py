from __future__ import annotations

from pathlib import Path
import sys

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.analysis_engine import build_analysis_report
from app.raw_data_store import load_tables_from_path


ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
OUT = ROOT / "backend" / "storage" / "test_reports"
OUT.mkdir(parents=True, exist_ok=True)


def render_report(
    title: str,
    workbook_type: str,
    source_path: Path,
    *,
    include_query_engine: bool,
    preferred_engine: str = "auto",
) -> str:
    tables = load_tables_from_path(source_path, workbook_type)
    report = build_analysis_report(
        tables,
        workbook_type,
        include_query_engine=include_query_engine,
        preferred_engine=preferred_engine,  # type: ignore[arg-type]
    )

    lines: list[str] = [
        f"# {title}",
        "",
        f"- Source: `{source_path.name}`",
        f"- Workbook type: `{workbook_type}`",
        f"- Tables preserved: {len(tables)}",
        f"- Findings selected: {len(report.findings)}",
        "",
        "## Profiles",
    ]
    for profile in report.profiles:
        lines.append(f"- `{profile.name}`: {profile.row_count} rows, {len(profile.fields)} fields")

    lines.extend(["", "## Planned Steps"])
    for step in report.steps:
        target = f"{step.dimension} / {step.measure}" if step.dimension and step.measure else step.dimension or step.measure or "table-wide"
        lines.append(f"- `{step.key}` `{step.operator}` on `{step.table_name}` targeting `{target}`")
        lines.append(f"  rationale: {step.rationale}")

    lines.extend(["", "## Findings"])
    for finding in report.findings:
        lines.append(f"- `{finding.title}` [{finding.priority}/{finding.confidence}] score={finding.score:.2f}")
        lines.append(f"  insight: {finding.insight}")
        lines.append(f"  implication: {finding.implication}")
        lines.append(f"  evidence: {', '.join(finding.evidence_ids)}")

    lines.extend(["", "## Actions"])
    for action in report.actions:
        lines.append(f"- {action.action} [{action.confidence}]")
        lines.append(f"  rationale: {action.rationale}")
        lines.append(f"  evidence: {', '.join(action.evidence_ids)}")

    lines.extend(["", "## Limitations"])
    for note in report.limitations:
        lines.append(f"- {note.text}")
        if note.evidence_ids:
            lines.append(f"  evidence: {', '.join(note.evidence_ids)}")

    lines.extend(["", "## Evidence"])
    for item in report.evidence:
        lines.append(f"- `{item.id}` `{item.table_name}` {item.title}: {item.detail}")

    return "\n".join(lines) + "\n"


def render_comparison(
    title: str,
    workbook_type: str,
    source_path: Path,
    baseline_name: str,
    augmented_name: str,
) -> str:
    tables = load_tables_from_path(source_path, workbook_type)
    baseline = build_analysis_report(tables, workbook_type, include_query_engine=False)
    augmented = build_analysis_report(tables, workbook_type, include_query_engine=True)

    baseline_titles = {item.title for item in baseline.findings}
    augmented_titles = {item.title for item in augmented.findings}
    new_titles = [item.title for item in augmented.findings if item.title not in baseline_titles]
    dropped_titles = [item.title for item in baseline.findings if item.title not in augmented_titles]

    lines = [
        f"# {title}",
        "",
        f"- Source: `{source_path.name}`",
        f"- Workbook type: `{workbook_type}`",
        f"- Baseline findings: {len(baseline.findings)}",
        f"- Augmented findings: {len(augmented.findings)}",
        "",
        "## New Findings From DuckDB Engine",
    ]
    if new_titles:
        lines.extend(f"- {title}" for title in new_titles)
    else:
        lines.append("- None")

    lines.extend(["", "## Findings Dropped After Reranking"])
    if dropped_titles:
        lines.extend(f"- {title}" for title in dropped_titles)
    else:
        lines.append("- None")

    lines.extend(["", "## Baseline Report", ""])
    lines.append(render_report(baseline_name, workbook_type, source_path, include_query_engine=False))
    lines.extend(["", "## Augmented Report", ""])
    lines.append(render_report(augmented_name, workbook_type, source_path, include_query_engine=True))
    return "\n".join(lines) + "\n"


def render_engine_comparison(title: str, workbook_type: str, source_path: Path) -> str:
    tables = load_tables_from_path(source_path, workbook_type)
    pandas_report = build_analysis_report(
        tables,
        workbook_type,
        include_query_engine=True,
        preferred_engine="pandas",
    )
    duckdb_report = build_analysis_report(
        tables,
        workbook_type,
        include_query_engine=True,
        preferred_engine="duckdb",
    )
    pandas_titles = {item.title for item in pandas_report.findings}
    duckdb_titles = {item.title for item in duckdb_report.findings}
    duckdb_only = [item.title for item in duckdb_report.findings if item.title not in pandas_titles]
    pandas_only = [item.title for item in pandas_report.findings if item.title not in duckdb_titles]

    lines = [
        f"# {title}",
        "",
        f"- Source: `{source_path.name}`",
        f"- Workbook type: `{workbook_type}`",
        f"- Pandas fallback findings: {len(pandas_report.findings)}",
        f"- DuckDB findings: {len(duckdb_report.findings)}",
        "",
        "## DuckDB-Only Findings",
    ]
    lines.extend(f"- {item}" for item in duckdb_only) if duckdb_only else lines.append("- None")
    lines.extend(["", "## Pandas-Only Findings"])
    lines.extend(f"- {item}" for item in pandas_only) if pandas_only else lines.append("- None")
    lines.extend(["", "## Pandas Fallback Report", ""])
    lines.append(
        render_report(
            f"{title} Pandas Fallback",
            workbook_type,
            source_path,
            include_query_engine=True,
            preferred_engine="pandas",
        )
    )
    lines.extend(["", "## DuckDB Report", ""])
    lines.append(
        render_report(
            f"{title} DuckDB",
            workbook_type,
            source_path,
            include_query_engine=True,
            preferred_engine="duckdb",
        )
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    cases = [
        (
            "Contracted vs Actual Analysis Test",
            "contracted_actual_v1",
            DATA / "Contracted vs Actual -2026.xlsx",
            OUT / "contracted-vs-actual-analysis.md",
        ),
        (
            "SAP Product Analysis Test",
            "product_master_v1",
            DATA / "SAP Product details - 16June25.xlsx",
            OUT / "sap-product-analysis.md",
        ),
    ]

    for title, workbook_type, source_path, out_path in cases:
        report_text = render_report(title, workbook_type, source_path, include_query_engine=True)
        out_path.write_text(report_text, encoding="utf-8")
        print(f"Wrote {out_path}")
        print(report_text)

    comparisons = [
        (
            "Contracted vs Actual Comparison",
            "contracted_actual_v1",
            DATA / "Contracted vs Actual -2026.xlsx",
            OUT / "contracted-vs-actual-comparison.md",
            "Contracted vs Actual Baseline",
            "Contracted vs Actual Augmented",
        ),
        (
            "SAP Product Comparison",
            "product_master_v1",
            DATA / "SAP Product details - 16June25.xlsx",
            OUT / "sap-product-comparison.md",
            "SAP Product Baseline",
            "SAP Product Augmented",
        ),
    ]
    for title, workbook_type, source_path, out_path, baseline_name, augmented_name in comparisons:
        report_text = render_comparison(title, workbook_type, source_path, baseline_name, augmented_name)
        out_path.write_text(report_text, encoding="utf-8")
        print(f"Wrote {out_path}")
        print(report_text)

    engine_comparisons = [
        (
            "Contracted vs Actual Engine Comparison",
            "contracted_actual_v1",
            DATA / "Contracted vs Actual -2026.xlsx",
            OUT / "contracted-vs-actual-engine-comparison.md",
        ),
        (
            "SAP Product Engine Comparison",
            "product_master_v1",
            DATA / "SAP Product details - 16June25.xlsx",
            OUT / "sap-product-engine-comparison.md",
        ),
    ]
    for title, workbook_type, source_path, out_path in engine_comparisons:
        report_text = render_engine_comparison(title, workbook_type, source_path)
        out_path.write_text(report_text, encoding="utf-8")
        print(f"Wrote {out_path}")
        print(report_text)


if __name__ == "__main__":
    main()
