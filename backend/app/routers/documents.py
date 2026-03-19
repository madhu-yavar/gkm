from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, UploadFile, status
from sqlalchemy.orm import Session

from app import models
from app.db import get_db
from app.deps import require_role
from app.gemini_client import GeminiError, gemini_validate_or_correct
from app.ingest_excel import parse_contracted_vs_actual_xlsx
from app.settings import settings


router = APIRouter(prefix="/documents", tags=["documents"])


def _ensure_storage_dir() -> Path:
    p = Path(settings.storage_dir).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


@router.post("/excel/contracted-vs-actual", status_code=201)
def upload_contracted_vs_actual_excel(
    use_gemini: bool = Query(default=False),
    file: UploadFile = File(...),
    x_gemini_api_key: str | None = Header(default=None),
    x_gemini_model: str | None = Header(default=None),
    db: Session = Depends(get_db),
    _=Depends(require_role(models.UserRole.admin, models.UserRole.analyst)),
):
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only .xlsx supported for this endpoint")

    storage = _ensure_storage_dir()
    tmp_path = storage / f"upload_{file.filename}"
    with tmp_path.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    parsed = parse_contracted_vs_actual_xlsx(tmp_path)

    gemini_used = False
    if use_gemini:
        if not x_gemini_api_key:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Gemini enabled but X-Gemini-Api-Key missing")
        model = (x_gemini_model or "gemini-2.5-flash").strip()
        try:
            parsed, _meta = gemini_validate_or_correct(api_key=x_gemini_api_key, model=model, parsed=parsed)
            gemini_used = True
        except GeminiError as e:
            # Fallback to deterministic parse for resiliency.
            gemini_used = False

    snapshot = models.Snapshot(source_filename=file.filename, as_of_date=parsed.as_of_date)
    db.add(snapshot)
    db.flush()

    # upsert clients + add snapshot rows
    for row in parsed.clients:
        if not row.external_id:
            continue
        client = db.query(models.Client).filter(models.Client.external_id == row.external_id).first()
        if not client:
            client = models.Client(name=row.name, external_id=row.external_id, client_type=row.client_type)
            db.add(client)
            db.flush()
        else:
            # keep latest name/type
            client.name = row.name
            client.client_type = row.client_type

        db.add(
            models.ClientSnapshot(
                snapshot_id=snapshot.id,
                client_id=client.id,
                contracted_ind=row.contracted_ind,
                contracted_bus=row.contracted_bus,
                contracted_total=row.contracted_total,
                received_ind=row.received_ind,
                received_bus=row.received_bus,
                received_total=row.received_total,
                pending_ind=row.pending_ind,
                pending_bus=row.pending_bus,
                pending_total=row.pending_total,
            )
        )

    for row in parsed.staff:
        if not row.external_id:
            continue
        db.add(
            models.StaffSnapshot(
                snapshot_id=snapshot.id,
                name=row.name,
                staff_external_id=row.external_id,
                staff_type=row.staff_type,
                received_ind=row.received_ind,
                received_bus=row.received_bus,
                received_total=row.received_total,
            )
        )

    return {"snapshot_id": snapshot.id, "as_of_date": str(snapshot.as_of_date), "gemini_used": gemini_used}

