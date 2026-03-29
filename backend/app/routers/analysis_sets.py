from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException
from fastapi.responses import Response
from sqlalchemy.orm import Session

from app import models
from app.analysis_sets import (
    build_analysis_set_analytics_pdf,
    build_analysis_set_dashboard_view,
    build_analysis_set_executive_pdf,
    confirm_analysis_set,
    generate_analysis_set_bundle,
    list_analysis_sets,
    propose_analysis_set,
    serialize_analysis_set,
)
from app.db import get_db
from app.deps import get_current_user
from app.gemini_reasoning import gemini_settings_from_headers
from app.schemas import AnalysisSetConfirmRequest, AnalysisSetProposalRequest, AnalysisSetProposalResponse


router = APIRouter(prefix="/analysis-sets", tags=["analysis-sets"])


def _get_analysis_set(db: Session, analysis_set_id: int) -> models.AnalysisSet:
    analysis_set = db.query(models.AnalysisSet).filter(models.AnalysisSet.id == analysis_set_id).first()
    if not analysis_set:
        raise HTTPException(status_code=404, detail="Analysis set not found")
    return analysis_set


@router.get("", response_model=list[AnalysisSetProposalResponse])
def get_analysis_sets(
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    return [serialize_analysis_set(item) for item in list_analysis_sets(db)]


@router.get("/{analysis_set_id}", response_model=AnalysisSetProposalResponse)
def get_analysis_set(
    analysis_set_id: int,
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    return serialize_analysis_set(_get_analysis_set(db, analysis_set_id))


@router.post("/proposals", response_model=AnalysisSetProposalResponse)
def create_analysis_set_proposal(
    payload: AnalysisSetProposalRequest,
    x_gemini_api_key: str | None = Header(default=None),
    x_gemini_model: str | None = Header(default=None),
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    if len(payload.snapshot_ids) < 2:
        raise HTTPException(status_code=400, detail="Select at least two processed documents.")
    try:
        analysis_set = propose_analysis_set(
            db,
            snapshot_ids=payload.snapshot_ids,
            actor_user_id=user.id,
            intent=(payload.intent or "").strip() or None,
            title=(payload.title or "").strip() or None,
            gemini=gemini_settings_from_headers(x_gemini_api_key, x_gemini_model),
        )
        return serialize_analysis_set(analysis_set)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{analysis_set_id}/confirm", response_model=AnalysisSetProposalResponse)
def confirm_analysis_set_proposal(
    analysis_set_id: int,
    payload: AnalysisSetConfirmRequest,
    db: Session = Depends(get_db),
    user: models.User = Depends(get_current_user),
):
    analysis_set = _get_analysis_set(db, analysis_set_id)
    confirmed = confirm_analysis_set(
        db,
        analysis_set=analysis_set,
        actor_user_id=user.id,
        title=(payload.title or "").strip() or None,
        intent=(payload.intent or "").strip() or None,
        relationship_type=(payload.relationship_type or "").strip() or None,
        join_keys=payload.join_keys,
        member_labels=payload.member_labels,
    )
    generate_analysis_set_bundle(db, analysis_set=confirmed, force=True)
    return serialize_analysis_set(confirmed)


@router.get("/{analysis_set_id}/dashboard-view")
def get_analysis_set_dashboard_view(
    analysis_set_id: int,
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    analysis_set = _get_analysis_set(db, analysis_set_id)
    if analysis_set.status != "confirmed":
        raise HTTPException(status_code=409, detail="Confirm the combined analysis before opening the merged dashboard")
    return build_analysis_set_dashboard_view(db, analysis_set)


@router.get("/{analysis_set_id}/summary.pdf")
def download_analysis_set_executive_pdf(
    analysis_set_id: int,
    x_gemini_api_key: str | None = Header(default=None),
    x_gemini_model: str | None = Header(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    analysis_set = _get_analysis_set(db, analysis_set_id)
    if analysis_set.status != "confirmed":
        raise HTTPException(status_code=409, detail="Confirm the combined analysis before downloading the executive summary")
    pdf = build_analysis_set_executive_pdf(
        db,
        analysis_set=analysis_set,
        gemini=gemini_settings_from_headers(x_gemini_api_key, x_gemini_model),
    )
    filename = f"gkm-combined-summary-{analysis_set.id}.pdf"
    return Response(content=pdf, media_type="application/pdf", headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@router.get("/{analysis_set_id}/analytics-summary.pdf")
def download_analysis_set_analytics_pdf(
    analysis_set_id: int,
    x_gemini_api_key: str | None = Header(default=None),
    x_gemini_model: str | None = Header(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    analysis_set = _get_analysis_set(db, analysis_set_id)
    if analysis_set.status != "confirmed":
        raise HTTPException(status_code=409, detail="Confirm the combined analysis before downloading the analytics summary")
    pdf = build_analysis_set_analytics_pdf(
        db,
        analysis_set=analysis_set,
        gemini=gemini_settings_from_headers(x_gemini_api_key, x_gemini_model),
    )
    filename = f"gkm-combined-analytics-{analysis_set.id}.pdf"
    return Response(content=pdf, media_type="application/pdf", headers={"Content-Disposition": f'attachment; filename="{filename}"'})
