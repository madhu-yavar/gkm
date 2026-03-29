from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass

from sqlalchemy.orm import Session

from app import models
from app.ingest_excel import ParsedClientRow, ParsedStaffRow, ParsedWorkbook


PII_TYPES = {"name", "email", "phone", "address", "identifier", "custom"}


@dataclass
class PiiTokenRecord:
    pii_type: str
    sheet_name: str | None
    section_key: str
    header_label: str
    original_value: str
    masked_token: str


@dataclass
class PiiMaskLookup:
    original_to_token: dict[str, str]
    token_to_original: dict[str, str]
    token_records: list[PiiTokenRecord]


def normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def suggest_pii_type(header_label: str) -> str | None:
    normalized = normalize_header(header_label)
    if "email" in normalized:
        return "email"
    if "phone" in normalized or "mobile" in normalized or "fax" in normalized:
        return "phone"
    if "address" in normalized or "street" in normalized or "city" in normalized or "zip" in normalized:
        return "address"
    if normalized in {"client id", "staff id", "employee id", "tax id", "ssn", "ein", "id"} or normalized.endswith(" id"):
        return "identifier"
    if "name" in normalized:
        return "name"
    return None


def _token_prefix(pii_type: str) -> str:
    return {
        "name": "NAME",
        "email": "EMAIL",
        "phone": "PHONE",
        "address": "ADDRESS",
        "identifier": "ID",
        "custom": "PII",
    }.get(pii_type, "PII")


def _selection_details(selections: list[dict]) -> dict[tuple[str, str], dict]:
    mapped: dict[tuple[str, str], dict] = {}
    for item in selections:
        pii_type = str(item.get("pii_type") or "").strip().lower()
        if pii_type not in PII_TYPES:
            continue
        sheet_name = str(item.get("sheet_name") or "").strip() or None
        section_key = str(item.get("section_key") or "").strip().lower()
        header_label = str(item.get("header_label") or "").strip()
        normalized_header = normalize_header(header_label)
        if section_key and normalized_header:
            mapped[(section_key, normalized_header)] = {
                "sheet_name": sheet_name,
                "section_key": section_key,
                "header_label": header_label,
                "normalized_header": normalized_header,
                "pii_type": pii_type,
            }
    return mapped


def _hash_value(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _mask_value(
    value: str,
    selection: dict | None,
    counters: dict[str, int],
    original_to_token: dict[str, str],
    token_to_original: dict[str, str],
    token_records: list[PiiTokenRecord],
) -> str:
    clean = (value or "").strip()
    if not clean or not selection:
        return clean
    if clean in original_to_token:
        return original_to_token[clean]
    prefix = _token_prefix(selection["pii_type"])
    counters[prefix] += 1
    token = f"[{prefix}_{counters[prefix]:03d}]"
    original_to_token[clean] = token
    token_to_original[token] = clean
    token_records.append(
        PiiTokenRecord(
            pii_type=selection["pii_type"],
            sheet_name=selection.get("sheet_name"),
            section_key=selection["section_key"],
            header_label=selection["header_label"],
            original_value=clean,
            masked_token=token,
        )
    )
    return token


def mask_parsed_workbook(parsed: ParsedWorkbook, selections: list[dict]) -> tuple[ParsedWorkbook, PiiMaskLookup]:
    details = _selection_details(selections)
    counters: dict[str, int] = defaultdict(int)
    original_to_token: dict[str, str] = {}
    token_to_original: dict[str, str] = {}
    token_records: list[PiiTokenRecord] = []

    client_name_selection = details.get(("clients", normalize_header("Client Name")))
    client_id_selection = details.get(("clients", normalize_header("Client ID"))) or details.get(("clients", normalize_header("ID")))
    staff_name_selection = details.get(("staff", normalize_header("Staff Name"))) or details.get(("staff", normalize_header("Client Name")))
    staff_id_selection = details.get(("staff", normalize_header("Staff ID"))) or details.get(("staff", normalize_header("Client ID"))) or details.get(("staff", normalize_header("ID")))

    masked_clients: list[ParsedClientRow] = []
    for row in parsed.clients:
        masked_clients.append(
            ParsedClientRow(
                name=_mask_value(row.name, client_name_selection, counters, original_to_token, token_to_original, token_records),
                external_id=_mask_value(row.external_id, client_id_selection, counters, original_to_token, token_to_original, token_records),
                client_type=row.client_type,
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

    masked_staff: list[ParsedStaffRow] = []
    for row in parsed.staff:
        masked_staff.append(
            ParsedStaffRow(
                name=_mask_value(row.name, staff_name_selection, counters, original_to_token, token_to_original, token_records),
                external_id=_mask_value(row.external_id, staff_id_selection, counters, original_to_token, token_to_original, token_records),
                staff_type=row.staff_type,
                received_ind=row.received_ind,
                received_bus=row.received_bus,
                received_total=row.received_total,
            )
        )

    return (
        ParsedWorkbook(as_of_date=parsed.as_of_date, clients=masked_clients, staff=masked_staff),
        PiiMaskLookup(
            original_to_token=original_to_token,
            token_to_original=token_to_original,
            token_records=token_records,
        ),
    )


def unmask_parsed_workbook(parsed: ParsedWorkbook, lookup: PiiMaskLookup) -> ParsedWorkbook:
    def restore(value: str) -> str:
        return lookup.token_to_original.get(value, value)

    return ParsedWorkbook(
        as_of_date=parsed.as_of_date,
        clients=[
            ParsedClientRow(
                name=restore(row.name),
                external_id=restore(row.external_id),
                client_type=row.client_type,
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
            for row in parsed.clients
        ],
        staff=[
            ParsedStaffRow(
                name=restore(row.name),
                external_id=restore(row.external_id),
                staff_type=row.staff_type,
                received_ind=row.received_ind,
                received_bus=row.received_bus,
                received_total=row.received_total,
            )
            for row in parsed.staff
        ],
    )


def mask_text(text: str, lookup: PiiMaskLookup) -> str:
    if not text:
        return text
    out = text
    for original in sorted(lookup.original_to_token.keys(), key=len, reverse=True):
        out = out.replace(original, lookup.original_to_token[original])
    return out


def unmask_text(text: str, lookup: PiiMaskLookup) -> str:
    if not text:
        return text
    out = text
    for token in sorted(lookup.token_to_original.keys(), key=len, reverse=True):
        out = out.replace(token, lookup.token_to_original[token])
    return out


def replace_snapshot_pii_fields(
    db: Session,
    *,
    snapshot_id: int,
    selections: list[dict],
    actor_user_id: int | None,
) -> dict[tuple[str, str], models.SnapshotPiiField]:
    db.query(models.SnapshotPiiTokenMapping).filter(models.SnapshotPiiTokenMapping.snapshot_id == snapshot_id).delete()
    db.query(models.SnapshotPiiField).filter(models.SnapshotPiiField.snapshot_id == snapshot_id).delete()
    db.flush()

    policies_by_key: dict[tuple[str, str], models.SnapshotPiiField] = {}
    for detail in _selection_details(selections).values():
        policy = models.SnapshotPiiField(
            snapshot_id=snapshot_id,
            sheet_name=detail["sheet_name"] or "",
            section_key=detail["section_key"],
            header_label=detail["header_label"],
            normalized_header=detail["normalized_header"],
            pii_type=detail["pii_type"],
            masking_strategy="tokenize",
            selection_source="user_selected",
            is_active=True,
            created_by_user_id=actor_user_id,
            updated_by_user_id=actor_user_id,
        )
        db.add(policy)
        db.flush()
        policies_by_key[(detail["section_key"], detail["normalized_header"])] = policy
    return policies_by_key


def persist_snapshot_pii_token_mappings(
    db: Session,
    *,
    snapshot_id: int,
    lookup: PiiMaskLookup,
    policies_by_key: dict[tuple[str, str], models.SnapshotPiiField],
    actor_user_id: int | None,
) -> None:
    db.query(models.SnapshotPiiTokenMapping).filter(models.SnapshotPiiTokenMapping.snapshot_id == snapshot_id).delete()
    db.flush()

    for record in lookup.token_records:
        policy = policies_by_key.get((record.section_key, normalize_header(record.header_label)))
        db.add(
            models.SnapshotPiiTokenMapping(
                snapshot_id=snapshot_id,
                pii_field_id=policy.id if policy else None,
                pii_type=record.pii_type,
                masking_strategy="tokenize",
                source_sheet_name=record.sheet_name,
                source_section_key=record.section_key,
                source_header_label=record.header_label,
                original_value=record.original_value,
                original_value_hash=_hash_value(record.original_value),
                masked_token=record.masked_token,
                created_by_user_id=actor_user_id,
            )
        )


def load_snapshot_pii_field_selections(db: Session, snapshot_id: int) -> list[dict]:
    rows = (
        db.query(models.SnapshotPiiField)
        .filter(models.SnapshotPiiField.snapshot_id == snapshot_id, models.SnapshotPiiField.is_active.is_(True))
        .order_by(models.SnapshotPiiField.id.asc())
        .all()
    )
    return [
        {
            "sheet_name": row.sheet_name,
            "section_key": row.section_key,
            "header_label": row.header_label,
            "pii_type": row.pii_type,
        }
        for row in rows
    ]


def load_snapshot_pii_lookup(db: Session, snapshot_id: int) -> PiiMaskLookup | None:
    rows = (
        db.query(models.SnapshotPiiTokenMapping)
        .filter(models.SnapshotPiiTokenMapping.snapshot_id == snapshot_id)
        .order_by(models.SnapshotPiiTokenMapping.id.asc())
        .all()
    )
    if not rows:
        return None

    original_to_token: dict[str, str] = {}
    token_to_original: dict[str, str] = {}
    token_records: list[PiiTokenRecord] = []
    for row in rows:
        original_to_token[row.original_value] = row.masked_token
        token_to_original[row.masked_token] = row.original_value
        token_records.append(
            PiiTokenRecord(
                pii_type=row.pii_type,
                sheet_name=row.source_sheet_name,
                section_key=row.source_section_key or "",
                header_label=row.source_header_label or "",
                original_value=row.original_value,
                masked_token=row.masked_token,
            )
        )
    return PiiMaskLookup(original_to_token=original_to_token, token_to_original=token_to_original, token_records=token_records)
