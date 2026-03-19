from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app import models
from app.db import get_db
from app.deps import get_current_user, require_role
from app.schemas import SnapshotSummary


router = APIRouter(prefix="/snapshots", tags=["snapshots"])


@router.get("", response_model=list[SnapshotSummary])
def list_snapshots(db: Session = Depends(get_db), _=Depends(get_current_user)):
    rows = db.query(models.Snapshot).order_by(models.Snapshot.as_of_date.desc(), models.Snapshot.id.desc()).all()
    return [SnapshotSummary(id=r.id, as_of_date=r.as_of_date, source_filename=r.source_filename) for r in rows]


@router.delete("/{snapshot_id}", status_code=204)
def delete_snapshot(
    snapshot_id: int,
    db: Session = Depends(get_db),
    _=Depends(require_role(models.UserRole.admin, models.UserRole.analyst)),
):
    snap = db.query(models.Snapshot).filter(models.Snapshot.id == snapshot_id).first()
    if not snap:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Snapshot not found")
    db.delete(snap)
    return None

