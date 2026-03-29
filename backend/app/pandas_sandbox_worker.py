from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


def _to_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


def _push_evidence(store: list[dict[str, Any]], table_name: str, title: str, detail: str) -> str:
    key = f"SBX-{len(store) + 1:03d}"
    store.append({"key": key, "table_name": table_name, "title": title, "detail": detail})
    return key


def _add_finding(
    findings: list[dict[str, Any]],
    *,
    key: str,
    title: str,
    insight: str,
    implication: str,
    priority: str,
    confidence: str,
    materiality: float,
    actionability: float,
    score: float,
    evidence_keys: list[str],
) -> None:
    findings.append(
        {
            "key": key,
            "title": title,
            "insight": insight,
            "implication": implication,
            "priority": priority,
            "confidence": confidence,
            "materiality": materiality,
            "actionability": actionability,
            "score": score,
            "evidence_keys": evidence_keys,
        }
    )


def _contracted_actual_analysis(tables: dict[str, pd.DataFrame]) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    limitations: list[dict[str, Any]] = []
    clients = tables.get("clients")
    if clients is None or clients.empty:
        return {"plan_summary": "", "evidence": evidence, "findings": findings, "limitations": limitations}

    clients = _to_numeric(
        clients.copy(),
        ["contracted_total", "received_total", "pending_total", "receipt_rate"],
    )
    if "receipt_rate" not in clients.columns:
        clients["receipt_rate"] = clients["received_total"] / clients["contracted_total"].replace({0: pd.NA})

    bins = [-2, -0.1, 0, 0.15, 0.35, 0.6, 10]
    labels = ["Uncontracted", "Not Started", "Critical", "At Risk", "On Track", "Ahead"]
    clients["receipt_band"] = pd.cut(
        clients["receipt_rate"].fillna(-1),
        bins=bins,
        labels=labels,
        include_lowest=True,
    )

    band_rollup = (
        clients.groupby("receipt_band", observed=False)
        .agg(
            clients=("client_name", "count"),
            pending_total=("pending_total", "sum"),
            contracted_total=("contracted_total", "sum"),
            received_total=("received_total", "sum"),
        )
        .reset_index()
    )
    total_pending = float(band_rollup["pending_total"].sum() or 0.0)
    low_conv = band_rollup[band_rollup["receipt_band"].isin(["Not Started", "Critical", "At Risk"])]
    low_conv_pending = float(low_conv["pending_total"].sum() or 0.0)
    low_conv_clients = int(low_conv["clients"].sum() or 0)
    low_conv_share = (low_conv_pending / total_pending) if total_pending > 0 else 0.0
    if total_pending > 0 and low_conv_share >= 0.6:
        evidence_key = _push_evidence(
            evidence,
            "clients",
            "Pending backlog by receipt band",
            (
                f"Low-conversion bands carry pending_total={low_conv_pending:.2f} of {total_pending:.2f} "
                f"({low_conv_share:.1%}) across {low_conv_clients} clients."
            ),
        )
        _add_finding(
            findings,
            key="SBX-F001",
            title="Backlog is concentrated in low-conversion cohorts",
            insight=(
                f"Not Started, Critical, and At Risk cohorts hold {low_conv_share:.1%} of pending backlog, "
                f"which indicates the backlog is sitting with clients already behind plan."
            ),
            implication="Delivery recovery should focus on stalled and low-conversion cohorts first, because generic portfolio-level actions will dilute the highest-risk backlog.",
            priority="high",
            confidence="high",
            materiality=min(1.0, low_conv_share),
            actionability=0.92,
            score=0.84,
            evidence_keys=[evidence_key],
        )

    top_pending = clients[["client_name", "pending_total", "receipt_rate"]].copy()
    top_pending = top_pending.sort_values("pending_total", ascending=False).head(10)
    top5_pending = float(top_pending.head(5)["pending_total"].sum() or 0.0)
    top5_share = (top5_pending / total_pending) if total_pending > 0 else 0.0
    weak_top = int((top_pending.head(5)["receipt_rate"].fillna(0) < 0.35).sum())
    if total_pending > 0 and top5_share >= 0.3:
        evidence_key = _push_evidence(
            evidence,
            "clients",
            "Pending concentration in top clients",
            (
                f"Top five clients contribute pending_total={top5_pending:.2f} of {total_pending:.2f} "
                f"({top5_share:.1%}); {weak_top} of those top five clients are below a 35% receipt rate."
            ),
        )
        _add_finding(
            findings,
            key="SBX-F002",
            title="Backlog exposure is concentrated in a small client cohort",
            insight=(
                f"The top five clients account for {top5_share:.1%} of backlog, and {weak_top} of them are still "
                f"below a 35% receipt rate."
            ),
            implication="Escalation should be client-specific, because a small cohort is carrying a disproportionate share of unresolved work.",
            priority="high",
            confidence="medium",
            materiality=min(1.0, top5_share),
            actionability=0.9,
            score=0.78,
            evidence_keys=[evidence_key],
        )

    overall_contracted = float(clients["contracted_total"].sum() or 0.0)
    overall_received = float(clients["received_total"].sum() or 0.0)
    portfolio_rate = (overall_received / overall_contracted) if overall_contracted > 0 else 0.0
    if portfolio_rate < 0.3:
        evidence_key = _push_evidence(
            evidence,
            "clients",
            "Portfolio receipt rate",
            (
                f"Portfolio totals show received_total={overall_received:.2f} against contracted_total={overall_contracted:.2f}, "
                f"for a receipt rate of {portfolio_rate:.1%}."
            ),
        )
        _add_finding(
            findings,
            key="SBX-F003",
            title="Portfolio conversion remains materially below plan",
            insight=f"The portfolio has converted only {portfolio_rate:.1%} of contracted work into received work.",
            implication="Leadership reporting should treat the backlog as an execution problem, not just a sizing issue, because the conversion baseline itself is weak.",
            priority="medium",
            confidence="high",
            materiality=min(1.0, 1 - portfolio_rate),
            actionability=0.82,
            score=0.73,
            evidence_keys=[evidence_key],
        )

    return {
        "plan_summary": "A bounded pandas sandbox computed cohort, concentration, and portfolio-rate slices over the preserved client tables.",
        "evidence": evidence,
        "findings": findings,
        "limitations": limitations,
    }


def _product_master_analysis(tables: dict[str, pd.DataFrame]) -> dict[str, Any]:
    evidence: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    limitations: list[dict[str, Any]] = []
    table_name, df = next(iter(tables.items()), (None, None))
    if table_name is None or df is None or df.empty:
        return {"plan_summary": "", "evidence": evidence, "findings": findings, "limitations": limitations}

    df = df.copy()
    row_count = len(df)

    if "Product Category" in df.columns:
        category_counts = df["Product Category"].fillna("Unknown").astype(str).value_counts()
        top10_share = float(category_counts.head(10).sum() / row_count) if row_count else 0.0
        singleton_count = int((category_counts == 1).sum())
        if top10_share >= 0.45 and category_counts.size >= 50:
            evidence_key = _push_evidence(
                evidence,
                table_name,
                "Category mix concentration and long tail",
                (
                    f"Top ten categories account for {top10_share:.1%} of rows; "
                    f"{category_counts.size} categories exist in total, including {singleton_count} single-category entries."
                ),
            )
            _add_finding(
                findings,
                key="SBX-F101",
                title="Category taxonomy is concentrated at the top and fragmented in the tail",
                insight=(
                    f"The top ten categories cover {top10_share:.1%} of the catalog, while the taxonomy still spans "
                    f"{category_counts.size} categories with {singleton_count} singleton categories."
                ),
                implication="Catalog governance has to balance focus on the dominant categories with cleanup of the long tail, because both concentration and fragmentation are present at the same time.",
                priority="high",
                confidence="high",
                materiality=min(1.0, top10_share + 0.15),
                actionability=0.9,
                score=0.86,
                evidence_keys=[evidence_key],
            )

    if "Base UoM" in df.columns:
        uom_counts = df["Base UoM"].fillna("Unknown").astype(str).value_counts()
        top_uom = str(uom_counts.index[0])
        top_uom_share = float(uom_counts.iloc[0] / row_count) if row_count else 0.0
        top5_uom_share = float(uom_counts.head(5).sum() / row_count) if row_count else 0.0
        if top_uom_share >= 0.65:
            evidence_key = _push_evidence(
                evidence,
                table_name,
                "Base UoM standardization profile",
                (
                    f"Top Base UoM={top_uom} covers {top_uom_share:.1%} of rows; "
                    f"top five UoMs cover {top5_uom_share:.1%} across {uom_counts.size} distinct UoMs."
                ),
            )
            _add_finding(
                findings,
                key="SBX-F102",
                title="Operational catalog is standardized around a narrow UoM set",
                insight=(
                    f"{top_uom} alone represents {top_uom_share:.1%} of the catalog, and the top five UoMs cover "
                    f"{top5_uom_share:.1%}."
                ),
                implication="Process design can optimize around a small operational UoM core, while exceptions should be governed as edge cases rather than treated as the norm.",
                priority="high",
                confidence="high",
                materiality=min(1.0, top_uom_share),
                actionability=0.82,
                score=0.81,
                evidence_keys=[evidence_key],
            )

    if "Product Description" in df.columns:
        unknown_descriptions = int(
            (df["Product Description"].fillna("").astype(str).str.strip().str.lower() == "unknown").sum()
        )
        if unknown_descriptions > 0:
            evidence_key = _push_evidence(
                evidence,
                table_name,
                "Unknown product descriptions",
                f"Rows with Product Description='Unknown' total {unknown_descriptions} out of {row_count}.",
            )
            _add_finding(
                findings,
                key="SBX-F103",
                title="Description quality exceptions are limited but real",
                insight=f"{unknown_descriptions} products still carry an 'Unknown' description placeholder.",
                implication="The issue is small enough for targeted remediation, so unresolved description placeholders should be cleared now before they propagate into downstream reporting or catalog operations.",
                priority="medium",
                confidence="high",
                materiality=min(1.0, unknown_descriptions / max(row_count, 1) + 0.12),
                actionability=0.88,
                score=0.58,
                evidence_keys=[evidence_key],
            )

    return {
        "plan_summary": "A bounded pandas sandbox profiled category concentration, UoM standardization, and description-quality exceptions in the preserved product table.",
        "evidence": evidence,
        "findings": findings,
        "limitations": limitations,
    }


def run_worker(payload: dict[str, Any]) -> dict[str, Any]:
    family = str(payload.get("family") or "").strip()
    tables = {
        str(item["name"]): pd.DataFrame(item.get("rows") or [])
        for item in payload.get("tables", [])
        if item.get("name")
    }
    if family == "contracted_actual_v1":
        return _contracted_actual_analysis(tables)
    if family == "product_master_v1":
        return _product_master_analysis(tables)
    return {
        "plan_summary": "",
        "evidence": [],
        "findings": [],
        "limitations": [],
    }


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        raise SystemExit("Usage: python -m app.pandas_sandbox_worker <input.json> <output.json>")
    input_path = Path(argv[1])
    output_path = Path(argv[2])
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    result = run_worker(payload)
    output_path.write_text(json.dumps(result, ensure_ascii=True), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
