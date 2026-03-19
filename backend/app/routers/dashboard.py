from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app import models
from app.db import get_db
from app.deps import get_current_user
from app.schemas import ClientRow, KpiResponse, SnapshotSummary, StaffRow


router = APIRouter(prefix="/dashboard", tags=["dashboard"])


def _get_snapshot(db: Session, snapshot_id: int | None) -> models.Snapshot:
    if snapshot_id is None:
        snap = db.query(models.Snapshot).order_by(models.Snapshot.as_of_date.desc(), models.Snapshot.id.desc()).first()
    else:
        snap = db.query(models.Snapshot).filter(models.Snapshot.id == snapshot_id).first()
    if not snap:
        raise HTTPException(status_code=404, detail="Snapshot not found")
    return snap


@router.get("/kpis", response_model=KpiResponse)
def get_kpis(
    snapshot_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    snap = _get_snapshot(db, snapshot_id)
    rows = (
        db.query(models.ClientSnapshot, models.Client)
        .join(models.Client, models.Client.id == models.ClientSnapshot.client_id)
        .filter(models.ClientSnapshot.snapshot_id == snap.id)
        .all()
    )

    tot_con_ind = sum(r.ClientSnapshot.contracted_ind for r in rows)
    tot_con_bus = sum(r.ClientSnapshot.contracted_bus for r in rows)
    tot_con = tot_con_ind + tot_con_bus
    tot_rec_ind = sum(r.ClientSnapshot.received_ind for r in rows)
    tot_rec_bus = sum(r.ClientSnapshot.received_bus for r in rows)
    tot_rec = tot_rec_ind + tot_rec_bus
    tot_pend = tot_con - tot_rec
    overall_rate = (tot_rec / tot_con) if tot_con > 0 else 0.0

    active_clients = sum(1 for r in rows if (r.ClientSnapshot.contracted_total > 0 or r.ClientSnapshot.received_total > 0))
    zero_clients = sum(1 for r in rows if (r.ClientSnapshot.contracted_total > 0 and r.ClientSnapshot.received_total == 0))
    over_clients = sum(1 for r in rows if (r.ClientSnapshot.received_total > r.ClientSnapshot.contracted_total))

    staff_total = (
        db.query(models.StaffSnapshot)
        .filter(models.StaffSnapshot.snapshot_id == snap.id)
        .all()
    )
    staff_tot_received = sum(s.received_total for s in staff_total)

    return KpiResponse(
        snapshot=SnapshotSummary(id=snap.id, as_of_date=snap.as_of_date, source_filename=snap.source_filename),
        total_contracted=tot_con,
        total_received=tot_rec,
        total_pending=tot_pend,
        total_contracted_ind=tot_con_ind,
        total_contracted_bus=tot_con_bus,
        total_received_ind=tot_rec_ind,
        total_received_bus=tot_rec_bus,
        overall_receipt_rate=overall_rate,
        active_clients=active_clients,
        zero_received_clients=zero_clients,
        over_delivered_clients=over_clients,
        staff_total_received=staff_tot_received,
    )


@router.get("/clients", response_model=list[ClientRow])
def list_clients(
    snapshot_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    snap = _get_snapshot(db, snapshot_id)
    rows = (
        db.query(models.ClientSnapshot, models.Client)
        .join(models.Client, models.Client.id == models.ClientSnapshot.client_id)
        .filter(models.ClientSnapshot.snapshot_id == snap.id)
        .all()
    )

    out: list[ClientRow] = []
    for cs, c in rows:
        rate = (cs.received_total / cs.contracted_total) if cs.contracted_total > 0 else (None if cs.received_total > 0 else 0.0)
        out.append(
            ClientRow(
                client_name=c.name,
                client_id=c.external_id,
                client_type=c.client_type,
                contracted_ind=cs.contracted_ind,
                contracted_bus=cs.contracted_bus,
                contracted_total=cs.contracted_total,
                received_ind=cs.received_ind,
                received_bus=cs.received_bus,
                received_total=cs.received_total,
                pending_ind=cs.pending_ind,
                pending_bus=cs.pending_bus,
                pending_total=cs.pending_total,
                receipt_rate=rate,
            )
        )
    return out


@router.get("/staff", response_model=list[StaffRow])
def list_staff(
    snapshot_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
    _=Depends(get_current_user),
):
    snap = _get_snapshot(db, snapshot_id)
    rows = (
        db.query(models.StaffSnapshot)
        .filter(models.StaffSnapshot.snapshot_id == snap.id)
        .order_by(models.StaffSnapshot.received_total.desc())
        .all()
    )
    return [
        StaffRow(
            name=r.name,
            staff_id=r.staff_external_id,
            staff_type=r.staff_type,
            received_ind=r.received_ind,
            received_bus=r.received_bus,
            received_total=r.received_total,
        )
        for r in rows
    ]

