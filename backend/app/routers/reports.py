from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Query
from fastapi.responses import Response
from sqlalchemy.orm import Session

from app import models
from app.db import get_db
from app.deps import get_current_user
from app.gemini_reasoning import gemini_settings_from_headers
from app.reporting import build_analytics_summary_pdf, build_client_summary_pdf, build_overall_summary_pdf, load_snapshot_report_context


router = APIRouter(prefix="/reports", tags=["reports"])


@router.get("/summary.pdf")
def download_overall_summary_pdf(
    snapshot_id: int | None = Query(default=None),
    x_gemini_api_key: str | None = Header(default=None),
    x_gemini_model: str | None = Header(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    try:
        ctx = load_snapshot_report_context(db, snapshot_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    pdf = build_overall_summary_pdf(ctx, gemini=gemini_settings_from_headers(x_gemini_api_key, x_gemini_model))
    filename = f"gkm-summary-{ctx.snapshot.as_of_date.isoformat()}.pdf"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=pdf, media_type="application/pdf", headers=headers)


@router.get("/analytics-summary.pdf")
def download_analytics_summary_pdf(
    snapshot_id: int | None = Query(default=None),
    x_gemini_api_key: str | None = Header(default=None),
    x_gemini_model: str | None = Header(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    try:
        ctx = load_snapshot_report_context(db, snapshot_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    pdf = build_analytics_summary_pdf(ctx, gemini=gemini_settings_from_headers(x_gemini_api_key, x_gemini_model))
    filename = f"gkm-analytics-summary-{ctx.snapshot.as_of_date.isoformat()}.pdf"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=pdf, media_type="application/pdf", headers=headers)


@router.get("/clients/{client_external_id}/summary.pdf")
def download_client_summary_pdf(
    client_external_id: str,
    snapshot_id: int | None = Query(default=None),
    x_gemini_api_key: str | None = Header(default=None),
    x_gemini_model: str | None = Header(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    try:
        ctx = load_snapshot_report_context(db, snapshot_id)
        pdf, client = build_client_summary_pdf(
            ctx,
            client_external_id,
            gemini=gemini_settings_from_headers(x_gemini_api_key, x_gemini_model),
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    filename = f"gkm-{client.client_external_id.lower()}-{ctx.snapshot.as_of_date.isoformat()}-summary.pdf"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=pdf, media_type="application/pdf", headers=headers)
