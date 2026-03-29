from __future__ import annotations

from pathlib import Path
import sys

BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app import models
from app.analytics_bundle import generate_snapshot_analytics_bundle, get_or_generate_snapshot_analytics_bundle
from app.dashboard_blueprints import ensure_schema_profile, get_effective_blueprint
from app.db import session_scope
from app.reporting import (
    build_agentic_chat_context,
    build_analytics_summary_pdf,
    build_overall_summary_pdf,
    build_summary_reasoning_bundle,
    load_snapshot_report_context,
)


ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "backend" / "storage" / "test_reports"
OUT.mkdir(parents=True, exist_ok=True)


CASES = [
    ("tower-wise-cam", "Tower wise Cam Report"),
    ("time-log", "Time Log Report"),
    ("status", "Return Status Update"),
    ("variance", "Contracted vs Actual"),
]


def _first_titles(items: list[dict], limit: int = 4) -> list[str]:
    titles: list[str] = []
    for item in items[:limit]:
        title = str(item.get("title") or item.get("label") or "").strip()
        if title:
            titles.append(title)
    return titles


def _find_snapshot(db, pattern: str) -> models.Snapshot | None:
    return (
        db.query(models.Snapshot)
        .filter(models.Snapshot.source_filename.ilike(f"%{pattern}%"))
        .order_by(models.Snapshot.id.desc())
        .first()
    )


def render_case(key: str, pattern: str) -> str:
    with session_scope() as db:
        snapshot = _find_snapshot(db, pattern)
        if snapshot is None:
            return f"# {key}\n\n- Status: snapshot not found for pattern `{pattern}`\n"

        profile = ensure_schema_profile(db, snapshot=snapshot)
        blueprint = get_effective_blueprint(db, snapshot=snapshot)
        proposal = snapshot.dashboard_proposal
        bundle = get_or_generate_snapshot_analytics_bundle(
            db,
            snapshot=snapshot,
            blueprint=blueprint,
            proposal=proposal,
        )
        refreshed = generate_snapshot_analytics_bundle(
            db,
            snapshot=snapshot,
            blueprint=blueprint,
            proposal=proposal,
            force=True,
        )
        ctx = load_snapshot_report_context(db, snapshot.id)
        reasoning = build_summary_reasoning_bundle(ctx)
        chat_context = build_agentic_chat_context(ctx)
        executive_pdf = build_overall_summary_pdf(ctx)
        analytics_pdf = build_analytics_summary_pdf(ctx)
        payload = dict(bundle.payload_json or {})
        dashboard_payload = dict(payload.get("dashboard") or {})
        runtime_payload = dict(dashboard_payload.get("runtime_payload") or {})
        surface_payloads = dict(dashboard_payload.get("surface_payloads") or {})
        adaptive = dict(runtime_payload.get("adaptive_dashboard") or {})
        widgets = list(adaptive.get("widgets") or [])
        kpis = list(adaptive.get("kpis") or [])
        lines = [
            f"# {key}",
            "",
            f"- Snapshot: `{snapshot.id}` `{snapshot.source_filename}`",
            f"- Workbook type: `{profile.workbook_type}`",
            f"- Blueprint: `{blueprint.id if blueprint else 'none'}`",
            f"- Proposal: `{proposal.id if proposal else 'none'}`",
            f"- Bundle id: `{bundle.id}`",
            f"- Bundle version: `{bundle.bundle_version}`",
            f"- Generation mode: `{bundle.generation_mode}`",
            f"- Stale: `{bundle.stale}`",
            f"- Rebuild replaced existing bundle in-place: `{bundle.id == refreshed.id}`",
            f"- Runtime from bundle: `{bool(dashboard_payload.get('runtime_payload') is not None)}`",
            f"- Surface payloads from bundle: `{sorted(surface_payloads.keys())}`",
            f"- Reasoning findings: `{len(reasoning.packet.findings)}`",
            f"- Chat evidence: `{len(chat_context.evidence)}`",
            f"- Executive PDF bytes: `{len(executive_pdf)}`",
            f"- Analytics PDF bytes: `{len(analytics_pdf)}`",
            "",
            "## Dashboard Signals",
        ]
        if kpis:
            lines.extend(f"- KPI: {item.get('label')} = {item.get('value')}" for item in kpis[:4])
        else:
            lines.append("- No adaptive KPIs were stored in the runtime payload")
        if widgets:
            lines.extend(f"- Widget: {title}" for title in _first_titles(widgets, limit=6))
        else:
            lines.append("- No adaptive widgets were stored in the runtime payload")
        lines.extend(
            [
                "",
                "## Reasoning Signals",
            ]
        )
        if reasoning.packet.findings:
            lines.extend(f"- Finding: {item.title}" for item in reasoning.packet.findings[:6])
        else:
            lines.append("- No promoted findings were stored in the reasoning bundle")
        lines.extend(
            [
                "",
                "## Chat Signals",
            ]
        )
        if chat_context.evidence:
            lines.extend(f"- Evidence: {item.title}" for item in chat_context.evidence[:6])
        else:
            lines.append("- No chat evidence was stored in the canonical bundle")
        return "\n".join(lines) + "\n"


def main() -> None:
    for key, pattern in CASES:
        report = render_case(key, pattern)
        out_path = OUT / f"{key}-bundle-consistency.md"
        out_path.write_text(report, encoding="utf-8")
        print(f"Wrote {out_path}")
        print(report)


if __name__ == "__main__":
    main()
