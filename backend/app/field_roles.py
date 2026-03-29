from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any

from app.workbook_families import normalize_header


@dataclass(frozen=True)
class FieldRoleProfile:
    header: str
    normalized_header: str
    role: str
    data_kind: str
    confidence: float


_GENERIC_HEADERS = {"hour", "hours", "time", "value", "amount", "count", "qty", "quantity", "total"}
_ID_TERMS = (" id", "id ", "_id", "code", " no", "no ", "number", "batch", "order", "charge")
_DURATION_TERMS = ("hour", "hours", "duration", "std hour", "standard hour")
_DATETIME_TERMS = ("date", "time", "timestamp", "updated", "created", "start", "end")
_MEASURE_TERMS = (
    "qty",
    "quantity",
    "count",
    "total",
    "amount",
    "volume",
    "entries",
    "received",
    "contracted",
    "pending",
    "rate",
    "score",
    "qat",
    "credit",
    "disallowance",
)
_DIMENSION_TERMS = (
    "employee",
    "client",
    "staff",
    "machine",
    "process",
    "pool",
    "category",
    "type",
    "status",
    "uom",
    "product",
    "operator",
)
_CODE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9/_-]{1,24}$")
_DURATION_PATTERN = re.compile(r"^\d{1,4}:\d{2}(?::\d{2})?$")


def unique_headers(header_values: list[str]) -> tuple[list[int], list[str]]:
    included_columns = [idx for idx, value in enumerate(header_values) if str(value or "").strip()]
    headers: list[str] = []
    seen: dict[str, int] = {}
    previous_context = ""
    for idx in included_columns:
        raw = str(header_values[idx] or "").strip() or f"Column {idx + 1}"
        base = raw
        normalized = normalize_header(raw)
        if normalized in _GENERIC_HEADERS and previous_context:
            base = f"{previous_context} {raw}"
        occurrence = seen.get(base.lower(), 0) + 1
        seen[base.lower()] = occurrence
        if occurrence > 1:
            base = f"{base} ({occurrence})"
        headers.append(base)
        if normalized and normalized not in _GENERIC_HEADERS and normalized not in {"updated at", "start time", "end time"}:
            previous_context = raw
    return included_columns, headers


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y", "%d-%m-%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text)
    except ValueError:
        return None


def _coerce_duration_hours(value: Any) -> float | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if not _DURATION_PATTERN.match(text):
        return None
    parts = [int(part) for part in text.split(":")]
    if len(parts) == 2:
        hours, minutes = parts
        seconds = 0
    else:
        hours, minutes, seconds = parts
    return hours + (minutes / 60.0) + (seconds / 3600.0)


def classify_table_fields(table: Any) -> dict[str, FieldRoleProfile]:
    profiles: dict[str, FieldRoleProfile] = {}
    row_count = len(table.rows)
    for header in table.headers:
        normalized = normalize_header(header)
        values = [row.get(header) for row in table.rows]
        usable = [value for value in values if value not in (None, "")]
        distinct = len({str(value).strip() for value in usable if str(value).strip()})
        unique_ratio = (distinct / len(usable)) if usable else 0.0
        numeric_values = [_coerce_float(value) for value in usable]
        numeric_ratio = (sum(1 for value in numeric_values if value is not None) / len(usable)) if usable else 0.0
        duration_values = [_coerce_duration_hours(value) for value in usable]
        duration_ratio = (sum(1 for value in duration_values if value is not None) / len(usable)) if usable else 0.0
        datetime_ratio = (sum(1 for value in usable if _parse_datetime(value) is not None) / len(usable)) if usable else 0.0
        code_ratio = (
            sum(1 for value in usable if _CODE_PATTERN.match(str(value).strip() or "") is not None) / len(usable)
            if usable else 0.0
        )

        role = "dimension"
        data_kind = "text"
        confidence = 0.55

        if duration_ratio >= 0.8 or any(term in normalized for term in _DURATION_TERMS):
            role = "duration"
            data_kind = "duration"
            confidence = 0.9 if duration_ratio >= 0.8 else 0.72
        elif datetime_ratio >= 0.8 or any(term in normalized for term in _DATETIME_TERMS):
            role = "datetime"
            data_kind = "datetime"
            confidence = 0.88 if datetime_ratio >= 0.8 else 0.7
        elif any(term in normalized for term in _ID_TERMS):
            role = "identifier"
            data_kind = "code" if code_ratio >= 0.5 or numeric_ratio >= 0.8 else "text"
            confidence = 0.9
        elif "operator_id" in normalized or "hsn" in normalized or "work order" in normalized:
            role = "identifier"
            data_kind = "code"
            confidence = 0.9
        elif any(term in normalized for term in _MEASURE_TERMS) and (numeric_ratio >= 0.5 or duration_ratio >= 0.5):
            role = "measure"
            data_kind = "numeric"
            confidence = 0.82
        elif numeric_ratio >= 0.85 and unique_ratio > 0.92 and not any(term in normalized for term in _MEASURE_TERMS):
            role = "identifier"
            data_kind = "numeric"
            confidence = 0.76
        elif numeric_ratio >= 0.85:
            role = "measure"
            data_kind = "numeric"
            confidence = 0.74
        elif code_ratio >= 0.8 and unique_ratio > 0.7:
            role = "code"
            data_kind = "code"
            confidence = 0.72
        elif any(term in normalized for term in _DIMENSION_TERMS):
            role = "dimension"
            data_kind = "text"
            confidence = 0.78
        elif usable and max(len(str(value)) for value in usable) > 32:
            role = "free_text"
            data_kind = "text"
            confidence = 0.68
        elif usable and distinct <= max(40, row_count // 2 if row_count else 40):
            role = "dimension"
            data_kind = "text"
            confidence = 0.64

        profiles[header] = FieldRoleProfile(
            header=header,
            normalized_header=normalized,
            role=role,
            data_kind=data_kind,
            confidence=confidence,
        )
    return profiles


def measure_headers(table: Any, profiles: dict[str, FieldRoleProfile] | None = None) -> list[str]:
    profiles = profiles or classify_table_fields(table)
    return [header for header in table.headers if profiles[header].role in {"measure", "duration"}]


def dimension_headers(table: Any, profiles: dict[str, FieldRoleProfile] | None = None) -> list[str]:
    profiles = profiles or classify_table_fields(table)
    ordered_roles = ("dimension", "code", "identifier")
    headers: list[str] = []
    for role in ordered_roles:
        headers.extend([header for header in table.headers if profiles[header].role == role])
    return headers


def coerce_measure_value(value: Any, profile: FieldRoleProfile) -> float | None:
    if profile.role == "duration" or profile.data_kind == "duration":
        return _coerce_duration_hours(value)
    return _coerce_float(value)
