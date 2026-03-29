from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import duckdb
import pandas as pd

from app.field_roles import classify_table_fields, dimension_headers, measure_headers
from app.raw_data_store import RawTable
from app.workbook_families import normalize_header


@dataclass(frozen=True)
class DashboardDuckDbSignal:
    title: str
    detail: str
    confidence_score: float
    supporting_metrics: list[str]


def _register_tables(conn: duckdb.DuckDBPyConnection, tables: list[RawTable]) -> None:
    for table in tables:
        conn.register(table.name, pd.DataFrame(table.rows))


def _quote_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _is_numeric_series(values: list[Any]) -> bool:
    usable = [value for value in values if value not in (None, "")]
    if not usable:
        return False
    count = 0
    for value in usable:
        text = str(value).strip().replace(",", "")
        try:
            float(text)
            count += 1
        except Exception:
            return False
    return count == len(usable)


def _pick_primary_table(tables: list[RawTable]) -> RawTable | None:
    return max(tables, key=lambda item: len(item.rows), default=None)


def _pick_dimension(table: RawTable, preferred_terms: list[str]) -> str | None:
    profiles = classify_table_fields(table)
    typed_dimensions = dimension_headers(table, profiles)
    if typed_dimensions:
        ranked: list[tuple[float, str]] = []
        for header in typed_dimensions:
            values = [row.get(header) for row in table.rows]
            distinct = len({str(value) for value in values if value not in (None, "")})
            normalized = normalize_header(header)
            score = 0.4
            if any(term in normalized for term in preferred_terms):
                score += 0.8
            if profiles[header].role == "identifier":
                score -= 0.5
            elif profiles[header].role == "code":
                score -= 0.15
            score += max(0.0, 0.2 - abs(distinct - 8) * 0.01)
            ranked.append((score, header))
        ranked.sort(reverse=True)
        if ranked and ranked[0][0] > 0:
            return ranked[0][1]
    ranked: list[tuple[float, str]] = []
    for header in table.headers:
        values = [row.get(header) for row in table.rows]
        distinct = len({str(value) for value in values if value not in (None, "")})
        if distinct < 2 or distinct > max(30, len(table.rows) // 2 if table.rows else 30):
            continue
        if _is_numeric_series(values):
            continue
        normalized = normalize_header(header)
        score = 0.2
        if any(term in normalized for term in preferred_terms):
            score += 0.8
        if "id" in normalized:
            score -= 0.4
        if "name" in normalized:
            score -= 0.2
        score += max(0.0, 0.2 - abs(distinct - 8) * 0.01)
        ranked.append((score, header))
    ranked.sort(reverse=True)
    return ranked[0][1] if ranked and ranked[0][0] > 0 else None


def _pick_measure(table: RawTable, preferred_terms: list[str]) -> str | None:
    profiles = classify_table_fields(table)
    typed_measures = measure_headers(table, profiles)
    if typed_measures:
        ranked: list[tuple[float, str]] = []
        for header in typed_measures:
            normalized = normalize_header(header)
            score = 0.4
            if any(term in normalized for term in preferred_terms):
                score += 0.8
            if profiles[header].role == "duration":
                score += 0.1
            ranked.append((score, header))
        ranked.sort(reverse=True)
        return ranked[0][1]
    ranked: list[tuple[float, str]] = []
    for header in table.headers:
        values = [row.get(header) for row in table.rows]
        if not _is_numeric_series(values):
            continue
        normalized = normalize_header(header)
        score = 0.2
        if any(term in normalized for term in preferred_terms):
            score += 0.8
        ranked.append((score, header))
    ranked.sort(reverse=True)
    return ranked[0][1] if ranked else None


def _top_bucket_signal(conn: duckdb.DuckDBPyConnection, table: RawTable, dimension: str) -> DashboardDuckDbSignal | None:
    quoted_table = _quote_identifier(table.name)
    quoted_dimension = _quote_identifier(dimension)
    row = conn.execute(
        f"""
        with counts as (
            select
                coalesce(cast({quoted_dimension} as varchar), 'Unknown') as bucket,
                count(*) as row_count
            from {quoted_table}
            group by 1
        ),
        ranked as (
            select
                bucket,
                row_count,
                row_number() over (order by row_count desc, bucket) as rn
            from counts
        )
        select
            max(case when rn = 1 then bucket end) as top_bucket,
            max(case when rn = 1 then row_count end) as top_bucket_rows,
            count(*) as distinct_buckets,
            sum(row_count) as total_rows
        from ranked
        """
    ).fetchone()
    if not row:
        return None
    top_bucket = str(row[0] or "Unknown")
    top_rows = float(row[1] or 0.0)
    distinct_buckets = int(row[2] or 0)
    total_rows = float(row[3] or 0.0)
    if total_rows <= 0:
        return None
    share = top_rows / total_rows
    tone = "highly concentrated" if share >= 0.5 else "moderately concentrated" if share >= 0.25 else "distributed"
    return DashboardDuckDbSignal(
        title="DuckDB distribution scan",
        detail=f"{dimension} is {tone}; the top bucket '{top_bucket}' holds {share:.1%} of {int(total_rows)} rows across {distinct_buckets} visible buckets.",
        confidence_score=0.9,
        supporting_metrics=[f"{top_bucket} {share:.1%}", f"{distinct_buckets} buckets", f"{int(total_rows)} rows"],
    )


def _measure_by_dimension_signal(
    conn: duckdb.DuckDBPyConnection,
    table: RawTable,
    dimension: str,
    measure: str,
) -> DashboardDuckDbSignal | None:
    quoted_table = _quote_identifier(table.name)
    quoted_dimension = _quote_identifier(dimension)
    quoted_measure = _quote_identifier(measure)
    row = conn.execute(
        f"""
        with grouped as (
            select
                coalesce(cast({quoted_dimension} as varchar), 'Unknown') as bucket,
                sum(try_cast({quoted_measure} as double)) as measure_total
            from {quoted_table}
            group by 1
        ),
        ranked as (
            select
                bucket,
                measure_total,
                row_number() over (order by measure_total desc, bucket) as rn
            from grouped
        )
        select
            max(case when rn = 1 then bucket end) as top_bucket,
            max(case when rn = 1 then measure_total end) as top_total,
            sum(measure_total) as overall_total
        from ranked
        """
    ).fetchone()
    if not row:
        return None
    top_bucket = str(row[0] or "Unknown")
    top_total = float(row[1] or 0.0)
    overall_total = float(row[2] or 0.0)
    if overall_total <= 0:
        return None
    share = top_total / overall_total
    return DashboardDuckDbSignal(
        title="DuckDB measure-by-dimension scan",
        detail=f"{measure} is concentrated in '{top_bucket}', which contributes {share:.1%} of total visible {measure}.",
        confidence_score=0.88,
        supporting_metrics=[f"{top_bucket} {share:.1%}", f"{measure} total {overall_total:.0f}"],
    )


def _quality_gap_signal(conn: duckdb.DuckDBPyConnection, table: RawTable, fields: list[str]) -> DashboardDuckDbSignal | None:
    if not fields:
        return None
    clauses = ", ".join(
        [
            f"sum(case when nullif(trim(cast({_quote_identifier(field)} as varchar)), '') is null then 1 else 0 end) as {_quote_identifier(normalize_header(field).replace(' ', '_') + '_missing')}"
            for field in fields
        ]
    )
    row = conn.execute(f"select {clauses} from {_quote_identifier(table.name)}").fetchone()
    if not row:
        return None
    metrics: list[str] = []
    detail_parts: list[str] = []
    total_rows = len(table.rows)
    for idx, field in enumerate(fields):
        missing = int(row[idx] or 0)
        if missing <= 0:
            continue
        share = missing / total_rows if total_rows else 0.0
        detail_parts.append(f"{field} is blank in {missing} rows ({share:.1%})")
        metrics.append(f"{field} {share:.1%} missing")
    if not detail_parts:
        return DashboardDuckDbSignal(
            title="DuckDB quality-gap scan",
            detail="No major null hotspots were detected in the main business fields selected for dashboard design.",
            confidence_score=0.84,
            supporting_metrics=["0 significant null hotspots"],
        )
    return DashboardDuckDbSignal(
        title="DuckDB quality-gap scan",
        detail="Null-hotspot analysis shows " + "; ".join(detail_parts[:4]) + ".",
        confidence_score=0.86,
        supporting_metrics=metrics[:4],
    )


def _cross_dimension_signal(conn: duckdb.DuckDBPyConnection, table: RawTable, left_dimension: str, right_dimension: str) -> DashboardDuckDbSignal | None:
    quoted_table = _quote_identifier(table.name)
    left = _quote_identifier(left_dimension)
    right = _quote_identifier(right_dimension)
    row = conn.execute(
        f"""
        with grouped as (
            select
                coalesce(cast({left} as varchar), 'Unknown') as left_bucket,
                coalesce(cast({right} as varchar), 'Unknown') as right_bucket,
                count(*) as row_count
            from {quoted_table}
            group by 1, 2
        ),
        ranked as (
            select
                left_bucket,
                right_bucket,
                row_count,
                row_number() over (order by row_count desc, left_bucket, right_bucket) as rn
            from grouped
        )
        select
            max(case when rn = 1 then left_bucket end) as top_left,
            max(case when rn = 1 then right_bucket end) as top_right,
            max(case when rn = 1 then row_count end) as top_rows,
            (select count(*) from {quoted_table}) as total_rows
        from ranked
        """
    ).fetchone()
    if not row:
        return None
    top_left = str(row[0] or "Unknown")
    top_right = str(row[1] or "Unknown")
    top_rows = float(row[2] or 0.0)
    total_rows = float(row[3] or 0.0)
    if total_rows <= 0:
        return None
    share = top_rows / total_rows
    return DashboardDuckDbSignal(
        title="DuckDB cross-dimension scan",
        detail=f"The strongest visible segment combination is '{top_left}' x '{top_right}', which represents {share:.1%} of rows.",
        confidence_score=0.82,
        supporting_metrics=[f"{top_left} x {top_right}", f"{share:.1%} of rows"],
    )


def _domain_terms(interpretation, family: str) -> tuple[list[str], list[str]]:
    dimension_terms: list[str] = []
    measure_terms: list[str] = []
    for sheet in getattr(interpretation, "sheets", [])[:4]:
        dimension_terms.extend(sheet.candidate_dimensions)
        measure_terms.extend(sheet.candidate_measures)
    if family == "contracted_actual_v1":
        dimension_terms.extend(["client", "staff", "type", "risk"])
        measure_terms.extend(["contracted", "received", "pending", "rate"])
    elif family == "product_master_v1":
        dimension_terms.extend(["product type", "product category", "base uom", "hsn"])
        measure_terms.extend(["count"])
    elif family == "client_status_report_v1":
        dimension_terms.extend(["status", "return type", "client type", "owner"])
        measure_terms.extend(["count", "age", "days"])
    return [normalize_header(term) for term in dimension_terms if term], [normalize_header(term) for term in measure_terms if term]


def run_dashboard_duckdb_tool(
    *,
    tool_name: str,
    tables: list[RawTable],
    interpretation,
    family: str,
) -> DashboardDuckDbSignal | None:
    if not tables:
        return None
    primary = _pick_primary_table(tables)
    if primary is None or not primary.rows:
        return None
    dimension_terms, measure_terms = _domain_terms(interpretation, family)
    conn = duckdb.connect(database=":memory:")
    try:
        _register_tables(conn, tables)
        if tool_name == "distribution_sql_scan":
            dimension = _pick_dimension(primary, dimension_terms)
            return _top_bucket_signal(conn, primary, dimension) if dimension else None
        if tool_name == "top_dimension_sql_scan":
            dimension = _pick_dimension(primary, dimension_terms)
            return _top_bucket_signal(conn, primary, dimension) if dimension else None
        if tool_name == "measure_by_dimension_sql_scan":
            dimension = _pick_dimension(primary, dimension_terms)
            measure = _pick_measure(primary, measure_terms)
            return _measure_by_dimension_signal(conn, primary, dimension, measure) if dimension and measure else None
        if tool_name == "quality_gap_sql_scan":
            profiles = classify_table_fields(primary)
            candidate_fields = []
            for header in primary.headers:
                normalized = normalize_header(header)
                if profiles[header].role in {"dimension", "measure", "duration"} and any(term in normalized for term in dimension_terms[:6]):
                    candidate_fields.append(header)
            return _quality_gap_signal(conn, primary, candidate_fields[:4] or primary.headers[:4])
        if tool_name == "cross_dimension_sql_scan":
            left_dimension = _pick_dimension(primary, dimension_terms)
            remaining_terms = [term for term in dimension_terms if left_dimension is None or term not in normalize_header(left_dimension)]
            right_dimension = _pick_dimension(primary, remaining_terms)
            if left_dimension and right_dimension and left_dimension != right_dimension:
                return _cross_dimension_signal(conn, primary, left_dimension, right_dimension)
        return None
    finally:
        conn.close()
