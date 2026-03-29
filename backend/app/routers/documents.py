from __future__ import annotations

import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, Header, HTTPException, Query, UploadFile, status
from sqlalchemy.orm import Session

from app import models
from app.dashboard_blueprints import build_or_refresh_proposal, ensure_schema_profile, ensure_schema_profile_from_profile_json
from app.db import SessionLocal, get_db
from app.deps import require_role
from app.gemini_client import GeminiError, gemini_validate_or_correct
from app.ingest_excel import parse_contracted_vs_actual_xlsx, preview_contracted_vs_actual_xlsx
from app.pii import mask_parsed_workbook, persist_snapshot_pii_token_mappings, replace_snapshot_pii_fields, unmask_parsed_workbook
from app.raw_data_store import extract_raw_tables_from_workbook, persist_snapshot_raw_tables
from app.schemas import DocumentProcessRequest, DocumentProcessingJobResponse, WorkbookPreviewField, WorkbookPreviewResponse, WorkbookPreviewSection, WorkbookPreviewSheet
from app.settings import settings
from app.workbook_families import detect_workbook_family_from_profile, normalize_header, workbook_family_label, workbook_family_mode


router = APIRouter(prefix="/documents", tags=["documents"])


def _ensure_storage_dir() -> Path:
    p = Path(settings.storage_dir).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


def _parse_pii_config(raw: str | None) -> list[dict]:
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except Exception:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid pii_config JSON")
    return value if isinstance(value, list) else []


def _serialize_job(job: models.DocumentProcessingJob, upload: models.UploadedWorkbook | None = None) -> DocumentProcessingJobResponse:
    up = upload or job.upload
    return DocumentProcessingJobResponse(
        id=job.id,
        upload_token=up.upload_token,
        workbook_family=job.workbook_family,
        status=job.status,
        stage=job.stage,
        progress_percent=job.progress_percent,
        message=job.message,
        error_detail=job.error_detail,
        snapshot_id=job.snapshot_id,
    )


def _job_update(job_id: int, **fields) -> None:
    db = SessionLocal()
    try:
        job = db.query(models.DocumentProcessingJob).filter(models.DocumentProcessingJob.id == job_id).first()
        if not job:
            return
        for key, value in fields.items():
            setattr(job, key, value)
        if "status" in fields and job.upload:
            if fields["status"] == "running":
                job.upload.status = "processing"
            elif fields["status"] == "succeeded":
                job.upload.status = "consumed"
            elif fields["status"] == "failed":
                job.upload.status = "failed"
        db.commit()
    finally:
        db.close()


def _profile_json_from_preview_response(preview: WorkbookPreviewResponse, source_filename: str) -> dict:
    return {
        "source_filename": source_filename,
        "sheets": [
            {
                "sheet_name": sheet.sheet_name,
                "sections": [
                    {
                        "section_key": section.section_key,
                        "section_label": section.section_label,
                        "header_row": section.header_row,
                        "fields": [
                            {
                                "column": field.column,
                                "header_label": field.header_label,
                                "normalized_header": normalize_header(field.header_label),
                                "sample_value": field.sample_value,
                                "suggested_pii_type": field.suggested_pii_type,
                            }
                            for field in section.headers
                        ],
                    }
                    for section in sheet.sections
                ],
            }
            for sheet in preview.sheets
        ],
    }


def _upsert_contracted_snapshot_rows(db: Session, snapshot: models.Snapshot, parsed) -> None:
    for row in parsed.clients:
        if not row.external_id:
            continue
        client = db.query(models.Client).filter(models.Client.external_id == row.external_id).first()
        if not client:
            client = models.Client(name=row.name, external_id=row.external_id, client_type=row.client_type)
            db.add(client)
            db.flush()
        else:
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


def _process_uploaded_workbook(job_id: int, dashboard_guidance: str | None = None) -> None:
    _job_update(
        job_id,
        status="running",
        stage="initializing",
        progress_percent=10,
        message="Preparing workbook session",
        started_at=datetime.now(timezone.utc),
    )
    db = SessionLocal()
    try:
        job = db.query(models.DocumentProcessingJob).filter(models.DocumentProcessingJob.id == job_id).first()
        if not job:
            return
        upload = db.query(models.UploadedWorkbook).filter(models.UploadedWorkbook.id == job.upload_id).first()
        if not upload:
            job.status = "failed"
            job.stage = "failed"
            job.progress_percent = 100
            job.message = "Upload session not found"
            job.error_detail = "Upload session not found"
            job.finished_at = datetime.now(timezone.utc)
            db.commit()
            return

        path = Path(upload.stored_path)
        if not path.exists():
            job.status = "failed"
            job.stage = "failed"
            job.progress_percent = 100
            job.message = "Uploaded workbook is missing from storage"
            job.error_detail = "Uploaded workbook is missing from storage"
            job.finished_at = datetime.now(timezone.utc)
            upload.status = "failed"
            db.commit()
            return

        pii_fields = job.pii_config_json or []
        _job_update(job_id, stage="family_detected", progress_percent=20, message=f"Detected {upload.family_label} workbook")

        snapshot_as_of = date.today()
        parsed = None
        lookup = None
        if upload.workbook_family == "contracted_actual_v1":
            _job_update(job_id, stage="parsing", progress_percent=45, message="Parsing contracted vs actual workbook")
            parsed = parse_contracted_vs_actual_xlsx(path)
            snapshot_as_of = parsed.as_of_date
            _masked, lookup = mask_parsed_workbook(parsed, pii_fields)

        _job_update(job_id, stage="persisting_snapshot", progress_percent=70, message="Persisting snapshot and schema profile")
        snapshot = models.Snapshot(source_filename=upload.source_filename, as_of_date=snapshot_as_of)
        db.add(snapshot)
        db.flush()

        ensure_schema_profile_from_profile_json(db, snapshot=snapshot, profile_json=upload.preview_json)
        raw_tables = extract_raw_tables_from_workbook(path, upload.workbook_family)
        if raw_tables:
            persist_snapshot_raw_tables(snapshot.id, upload.workbook_family, upload.source_filename, raw_tables)
        policies_by_key = replace_snapshot_pii_fields(
            db,
            snapshot_id=snapshot.id,
            selections=pii_fields,
            actor_user_id=job.created_by_user_id,
        )
        if lookup is not None:
            persist_snapshot_pii_token_mappings(
                db,
                snapshot_id=snapshot.id,
                lookup=lookup,
                policies_by_key=policies_by_key,
                actor_user_id=job.created_by_user_id,
            )
        if parsed is not None:
            _upsert_contracted_snapshot_rows(db, snapshot, parsed)

        _job_update(job_id, stage="building_proposal", progress_percent=85, message="Generating dashboard proposal")
        build_or_refresh_proposal(
            db,
            snapshot=snapshot,
            actor_user_id=job.created_by_user_id,
            user_guidance=(dashboard_guidance or "").strip() or None,
        )

        job.snapshot_id = snapshot.id
        job.status = "succeeded"
        job.stage = "completed"
        job.progress_percent = 100
        job.message = "Processing completed"
        job.finished_at = datetime.now(timezone.utc)
        upload.status = "consumed"
        upload.consumed_at = datetime.now(timezone.utc)
        db.commit()
    except Exception as exc:
        db.rollback()
        _job_update(
            job_id,
            status="failed",
            stage="failed",
            progress_percent=100,
            message="Processing failed",
            error_detail=str(exc),
            finished_at=datetime.now(timezone.utc),
        )
    finally:
        db.close()


@router.post("/excel/contracted-vs-actual/preview", response_model=WorkbookPreviewResponse)
def preview_contracted_vs_actual_excel(
    file: UploadFile = File(...),
    user: models.User = Depends(require_role(models.UserRole.admin, models.UserRole.analyst)),
):
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only .xlsx supported for this endpoint")

    storage = _ensure_storage_dir()
    upload_token = uuid4().hex
    safe_name = file.filename.replace("/", "_")
    tmp_path = storage / f"upload_{upload_token}_{safe_name}"
    with tmp_path.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    sheets = preview_contracted_vs_actual_xlsx(tmp_path)
    preview = WorkbookPreviewResponse(
        upload_token=upload_token,
        workbook_family="generic_workbook_v1",
        family_label="Generic Workbook",
        family_mode="metadata_snapshot",
        sheets=[
            WorkbookPreviewSheet(
                sheet_name=sheet.sheet_name,
                sections=[
                    WorkbookPreviewSection(
                        section_key=section.section_key,
                        section_label=section.section_label,
                        header_row=section.header_row,
                        headers=[
                            WorkbookPreviewField(
                                column=field.column,
                                header_label=field.header_label,
                                sample_value=field.sample_value,
                                suggested_pii_type=field.suggested_pii_type,
                            )
                            for field in section.headers
                        ],
                    )
                    for section in sheet.sections
                ],
            )
            for sheet in sheets
        ]
    )
    profile_json = _profile_json_from_preview_response(preview, file.filename)
    family = detect_workbook_family_from_profile(profile_json)
    upload = models.UploadedWorkbook(
        upload_token=upload_token,
        source_filename=file.filename,
        stored_path=str(tmp_path),
        workbook_family=family,
        family_label=workbook_family_label(family),
        family_mode=workbook_family_mode(family),
        preview_json=profile_json,
        created_by_user_id=user.id,
    )
    preview.workbook_family = family
    preview.family_label = upload.family_label
    preview.family_mode = upload.family_mode
    return_data = preview
    db = SessionLocal()
    try:
        db.add(upload)
        db.commit()
    finally:
        db.close()
    return return_data


@router.post("/process", response_model=DocumentProcessingJobResponse, status_code=202)
def process_uploaded_workbook(
    payload: DocumentProcessRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    user: models.User = Depends(require_role(models.UserRole.admin, models.UserRole.analyst)),
):
    upload = (
        db.query(models.UploadedWorkbook)
        .filter(models.UploadedWorkbook.upload_token == payload.upload_token)
        .first()
    )
    if not upload:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Upload session not found")
    if upload.status == "consumed":
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Upload session was already processed")
    if upload.status in {"queued", "processing"}:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Upload session is already being processed")

    existing_job = (
        db.query(models.DocumentProcessingJob)
        .filter(
            models.DocumentProcessingJob.upload_id == upload.id,
            models.DocumentProcessingJob.status.in_(("queued", "running")),
        )
        .order_by(models.DocumentProcessingJob.id.desc())
        .first()
    )
    if existing_job:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="A processing job is already active for this upload")

    job = models.DocumentProcessingJob(
        upload_id=upload.id,
        workbook_family=upload.workbook_family,
        status="queued",
        stage="queued",
        progress_percent=0,
        message="Queued for processing",
        pii_config_json=[item.model_dump() if hasattr(item, "model_dump") else item for item in payload.pii_fields],
        created_by_user_id=user.id,
    )
    db.add(job)
    upload.status = "queued"
    db.flush()
    background_tasks.add_task(_process_uploaded_workbook, job.id, payload.dashboard_guidance)
    return _serialize_job(job, upload)


@router.get("/jobs/{job_id}", response_model=DocumentProcessingJobResponse)
def get_processing_job(
    job_id: int,
    db: Session = Depends(get_db),
    _=Depends(require_role(models.UserRole.admin, models.UserRole.analyst)),
):
    job = db.query(models.DocumentProcessingJob).filter(models.DocumentProcessingJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Processing job not found")
    return _serialize_job(job)


@router.post("/excel/contracted-vs-actual", status_code=201)
def upload_contracted_vs_actual_excel(
    use_gemini: bool = Query(default=False),
    pii_config: str | None = Form(default=None),
    file: UploadFile = File(...),
    x_gemini_api_key: str | None = Header(default=None),
    x_gemini_model: str | None = Header(default=None),
    db: Session = Depends(get_db),
    user: models.User = Depends(require_role(models.UserRole.admin, models.UserRole.analyst)),
):
    if not file.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Only .xlsx supported for this endpoint")

    storage = _ensure_storage_dir()
    tmp_path = storage / f"upload_{file.filename}"
    with tmp_path.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    preview_sheets = preview_contracted_vs_actual_xlsx(tmp_path)
    try:
        parsed = parse_contracted_vs_actual_xlsx(tmp_path)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    pii_fields = _parse_pii_config(pii_config)
    masked_parsed, lookup = mask_parsed_workbook(parsed, pii_fields)

    snapshot = models.Snapshot(source_filename=file.filename, as_of_date=parsed.as_of_date)
    db.add(snapshot)
    db.flush()
    ensure_schema_profile(db, snapshot=snapshot, preview_sheets=preview_sheets)
    raw_tables = extract_raw_tables_from_workbook(tmp_path, "contracted_actual_v1")
    if raw_tables:
        persist_snapshot_raw_tables(snapshot.id, "contracted_actual_v1", file.filename, raw_tables)
    policies_by_key = replace_snapshot_pii_fields(
        db,
        snapshot_id=snapshot.id,
        selections=pii_fields,
        actor_user_id=user.id,
    )
    persist_snapshot_pii_token_mappings(
        db,
        snapshot_id=snapshot.id,
        lookup=lookup,
        policies_by_key=policies_by_key,
        actor_user_id=user.id,
    )

    gemini_used = False
    if use_gemini:
        if not x_gemini_api_key:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Gemini enabled but X-Gemini-Api-Key missing")
        model = (x_gemini_model or "gemini-2.5-flash").strip()
        try:
            parsed, _meta = gemini_validate_or_correct(api_key=x_gemini_api_key, model=model, parsed=masked_parsed)
            parsed = unmask_parsed_workbook(parsed, lookup)
            snapshot.as_of_date = parsed.as_of_date
            gemini_used = True
        except GeminiError:
            gemini_used = False

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
