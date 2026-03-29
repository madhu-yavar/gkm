from __future__ import annotations

from typing import Any

import duckdb
import pandas as pd

from app.raw_data_store import RawTable


def _register_tables(conn: duckdb.DuckDBPyConnection, tables: list[RawTable]) -> None:
    for table in tables:
        conn.register(table.name, pd.DataFrame(table.rows))


def _pct(value: float, total: float) -> float:
    return (value / total) if total else 0.0


def _quote_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def run_duckdb_analysis(tables: list[RawTable], family: str) -> dict[str, Any]:
    conn = duckdb.connect(database=":memory:")
    try:
        _register_tables(conn, tables)
        if family == "contracted_actual_v1":
            return _contracted_actual_analysis(conn)
        if family == "product_master_v1":
            return _product_master_analysis(conn)
        return {"plan_summary": "", "evidence": [], "findings": [], "limitations": []}
    finally:
        conn.close()


def _contracted_actual_analysis(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    limitations: list[dict[str, Any]] = []

    def push_evidence(title: str, detail: str) -> str:
        key = f"DDB-{len(evidence) + 1:03d}"
        evidence.append({"key": key, "table_name": "clients", "title": title, "detail": detail})
        return key

    cohort_row = conn.execute(
        """
        with client_bands as (
            select
                client_name,
                contracted_total,
                received_total,
                pending_total,
                receipt_rate,
                case
                    when contracted_total = 0 then 'Uncontracted'
                    when coalesce(receipt_rate, 0) = 0 then 'Not Started'
                    when receipt_rate < 0.15 then 'Critical'
                    when receipt_rate < 0.35 then 'At Risk'
                    when receipt_rate < 0.60 then 'On Track'
                    else 'Ahead'
                end as receipt_band
            from clients
        )
        select
            sum(case when receipt_band in ('Not Started', 'Critical', 'At Risk') then pending_total else 0 end) as low_conv_pending,
            sum(pending_total) as total_pending,
            sum(case when receipt_band in ('Not Started', 'Critical', 'At Risk') then 1 else 0 end) as low_conv_clients
        from client_bands
        """
    ).fetchone()
    low_conv_pending = float(cohort_row[0] or 0.0)
    total_pending = float(cohort_row[1] or 0.0)
    low_conv_clients = int(cohort_row[2] or 0)
    low_conv_share = _pct(low_conv_pending, total_pending)
    if total_pending > 0 and low_conv_share >= 0.6:
        evidence_key = push_evidence(
            "Pending backlog by receipt band",
            f"Low-conversion bands carry pending_total={low_conv_pending:.2f} of {total_pending:.2f} ({low_conv_share:.1%}) across {low_conv_clients} clients.",
        )
        findings.append(
            {
                "key": "DDB-F001",
                "title": "Backlog is concentrated in low-conversion cohorts",
                "insight": f"Not Started, Critical, and At Risk cohorts hold {low_conv_share:.1%} of pending backlog, which indicates the backlog is sitting with clients already behind plan.",
                "implication": "Delivery recovery should focus on stalled and low-conversion cohorts first, because generic portfolio-level actions will dilute the highest-risk backlog.",
                "priority": "high",
                "confidence": "high",
                "materiality": min(1.0, low_conv_share),
                "actionability": 0.92,
                "score": 0.84,
                "evidence_keys": [evidence_key],
            }
        )

    top_row = conn.execute(
        """
        with ranked_clients as (
            select
                client_name,
                pending_total,
                receipt_rate
            from clients
            order by pending_total desc
            limit 5
        )
        select
            sum(pending_total) as top5_pending,
            sum(case when coalesce(receipt_rate, 0) < 0.35 then 1 else 0 end) as weak_top_count
        from ranked_clients
        """
    ).fetchone()
    top5_pending = float(top_row[0] or 0.0)
    weak_top = int(top_row[1] or 0)
    top5_share = _pct(top5_pending, total_pending)
    if total_pending > 0 and top5_share >= 0.3:
        evidence_key = push_evidence(
            "Pending concentration in top clients",
            f"Top five clients contribute pending_total={top5_pending:.2f} of {total_pending:.2f} ({top5_share:.1%}); {weak_top} of those top five clients are below a 35% receipt rate.",
        )
        findings.append(
            {
                "key": "DDB-F002",
                "title": "Backlog exposure is concentrated in a small client cohort",
                "insight": f"The top five clients account for {top5_share:.1%} of backlog, and {weak_top} of them are still below a 35% receipt rate.",
                "implication": "Escalation should be client-specific, because a small cohort is carrying a disproportionate share of unresolved work.",
                "priority": "high",
                "confidence": "medium",
                "materiality": min(1.0, top5_share),
                "actionability": 0.90,
                "score": 0.78,
                "evidence_keys": [evidence_key],
            }
        )

    portfolio_row = conn.execute(
        """
        select
            sum(received_total) as received_total,
            sum(contracted_total) as contracted_total
        from clients
        """
    ).fetchone()
    overall_received = float(portfolio_row[0] or 0.0)
    overall_contracted = float(portfolio_row[1] or 0.0)
    portfolio_rate = _pct(overall_received, overall_contracted)
    if overall_contracted > 0 and portfolio_rate < 0.3:
        evidence_key = push_evidence(
            "Portfolio receipt rate",
            f"Portfolio totals show received_total={overall_received:.2f} against contracted_total={overall_contracted:.2f}, for a receipt rate of {portfolio_rate:.1%}.",
        )
        findings.append(
            {
                "key": "DDB-F003",
                "title": "Portfolio conversion remains materially below plan",
                "insight": f"The portfolio has converted only {portfolio_rate:.1%} of contracted work into received work.",
                "implication": "Leadership reporting should treat the backlog as an execution problem, not just a sizing issue, because the conversion baseline itself is weak.",
                "priority": "medium",
                "confidence": "high",
                "materiality": min(1.0, 1 - portfolio_rate),
                "actionability": 0.82,
                "score": 0.73,
                "evidence_keys": [evidence_key],
            }
        )

    return {
        "plan_summary": "DuckDB executed cohort, concentration, and portfolio-rate SQL slices over the preserved client tables.",
        "evidence": evidence,
        "findings": findings,
        "limitations": limitations,
    }


def _product_master_analysis(conn: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    limitations: list[dict[str, Any]] = []
    table_name = conn.execute("select table_name from information_schema.tables where table_schema = 'main' limit 1").fetchone()
    resolved_name = str(table_name[0]) if table_name else "product_table"
    quoted_name = _quote_identifier(resolved_name)

    def push_evidence(title: str, detail: str) -> str:
        key = f"DDB-{len(evidence) + 1:03d}"
        evidence.append({"key": key, "table_name": resolved_name, "title": title, "detail": detail})
        return key

    category_row = conn.execute(
        f"""
        with category_counts as (
            select coalesce(cast("Product Category" as varchar), 'Unknown') as category_name, count(*) as row_count
            from {quoted_name}
            group by 1
        ),
        ranked as (
            select
                category_name,
                row_count,
                row_number() over (order by row_count desc, category_name) as rn
            from category_counts
        )
        select
            sum(case when rn <= 10 then row_count else 0 end) as top10_rows,
            count(*) as category_count,
            sum(case when row_count = 1 then 1 else 0 end) as singleton_categories,
            (select count(*) from {quoted_name}) as total_rows
        from ranked
        """
    ).fetchone()
    top10_rows = float(category_row[0] or 0.0)
    category_count = int(category_row[1] or 0)
    singleton_count = int(category_row[2] or 0)
    total_rows = int(category_row[3] or 0)
    top10_share = _pct(top10_rows, float(total_rows))
    if total_rows > 0 and top10_share >= 0.45 and category_count >= 50:
        evidence_key = push_evidence(
            "Category mix concentration and long tail",
            f"Top ten categories account for {top10_share:.1%} of rows; {category_count} categories exist in total, including {singleton_count} single-category entries.",
        )
        findings.append(
            {
                "key": "DDB-F101",
                "title": "Category taxonomy is concentrated at the top and fragmented in the tail",
                "insight": f"The top ten categories cover {top10_share:.1%} of the catalog, while the taxonomy still spans {category_count} categories with {singleton_count} singleton categories.",
                "implication": "Catalog governance has to balance focus on the dominant categories with cleanup of the long tail, because both concentration and fragmentation are present at the same time.",
                "priority": "high",
                "confidence": "high",
                "materiality": min(1.0, top10_share + 0.15),
                "actionability": 0.90,
                "score": 0.86,
                "evidence_keys": [evidence_key],
            }
        )

    uom_row = conn.execute(
        f"""
        with uom_counts as (
            select coalesce(cast("Base UoM" as varchar), 'Unknown') as base_uom, count(*) as row_count
            from {quoted_name}
            group by 1
        ),
        ranked as (
            select
                base_uom,
                row_count,
                row_number() over (order by row_count desc, base_uom) as rn
            from uom_counts
        )
        select
            max(case when rn = 1 then base_uom end) as top_uom,
            max(case when rn = 1 then row_count end) as top_uom_rows,
            sum(case when rn <= 5 then row_count else 0 end) as top5_rows,
            count(*) as distinct_uoms,
            (select count(*) from {quoted_name}) as total_rows
        from ranked
        """
    ).fetchone()
    top_uom = str(uom_row[0] or "Unknown")
    top_uom_rows = float(uom_row[1] or 0.0)
    top5_rows = float(uom_row[2] or 0.0)
    distinct_uoms = int(uom_row[3] or 0)
    total_rows = int(uom_row[4] or 0)
    top_uom_share = _pct(top_uom_rows, float(total_rows))
    top5_uom_share = _pct(top5_rows, float(total_rows))
    if total_rows > 0 and top_uom_share >= 0.65:
        evidence_key = push_evidence(
            "Base UoM standardization profile",
            f"Top Base UoM={top_uom} covers {top_uom_share:.1%} of rows; top five UoMs cover {top5_uom_share:.1%} across {distinct_uoms} distinct UoMs.",
        )
        findings.append(
            {
                "key": "DDB-F102",
                "title": "Operational catalog is standardized around a narrow UoM set",
                "insight": f"{top_uom} alone represents {top_uom_share:.1%} of the catalog, and the top five UoMs cover {top5_uom_share:.1%}.",
                "implication": "Process design can optimize around a small operational UoM core, while exceptions should be governed as edge cases rather than treated as the norm.",
                "priority": "high",
                "confidence": "high",
                "materiality": min(1.0, top_uom_share),
                "actionability": 0.82,
                "score": 0.81,
                "evidence_keys": [evidence_key],
            }
        )

    quality_row = conn.execute(
        f"""
        select
            sum(case when lower(trim(coalesce(cast("Product Description" as varchar), ''))) = 'unknown' then 1 else 0 end) as unknown_description_count,
            count(*) as total_rows
        from {quoted_name}
        """
    ).fetchone()
    unknown_descriptions = int(quality_row[0] or 0)
    total_rows = int(quality_row[1] or 0)
    if unknown_descriptions > 0:
        evidence_key = push_evidence(
            "Unknown product descriptions",
            f"Rows with Product Description='Unknown' total {unknown_descriptions} out of {total_rows}.",
        )
        findings.append(
            {
                "key": "DDB-F103",
                "title": "Description quality exceptions are limited but real",
                "insight": f"{unknown_descriptions} products still carry an 'Unknown' description placeholder.",
                "implication": "The issue is small enough for targeted remediation, so unresolved description placeholders should be cleared now before they propagate into downstream reporting or catalog operations.",
                "priority": "medium",
                "confidence": "high",
                "materiality": min(1.0, unknown_descriptions / max(total_rows, 1) + 0.12),
                "actionability": 0.88,
                "score": 0.58,
                "evidence_keys": [evidence_key],
            }
        )

    return {
        "plan_summary": "DuckDB executed category, UoM, and description-quality SQL slices over the preserved product catalog.",
        "evidence": evidence,
        "findings": findings,
        "limitations": limitations,
    }
