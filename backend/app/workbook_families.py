from __future__ import annotations

import re
from typing import Any


def normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def detect_workbook_family_from_profile(profile: dict[str, Any]) -> str:
    normalized_headers = {
        field["normalized_header"]
        for sheet in profile.get("sheets", [])
        for section in sheet.get("sections", [])
        for field in section.get("fields", [])
    }
    sheet_names = {str(sheet.get("sheet_name") or "").lower() for sheet in profile.get("sheets", [])}

    if (
        "client name" in normalized_headers
        and "client id" in normalized_headers
        and any(header.startswith("contracted") for header in normalized_headers)
        and any(header.startswith("received as of") or header.startswith("received till date") for header in normalized_headers)
    ):
        return "contracted_actual_v1"

    if {"tax payer name", "return code", "return status"} <= normalized_headers:
        return "client_status_report_v1"

    if {"product id", "product description", "product type", "base uom"} <= normalized_headers:
        return "product_master_v1"

    if any("delivery" in name or "awaiting" in name or "weekly" in name for name in sheet_names) and "return code" in normalized_headers:
        return "client_status_report_v1"

    return "generic_workbook_v1"


def workbook_family_label(family: str) -> str:
    return {
        "contracted_actual_v1": "Contracted vs Actual",
        "client_status_report_v1": "Client Status Report",
        "product_master_v1": "Product Master",
        "generic_workbook_v1": "Generic Workbook",
    }.get(family, "Generic Workbook")


def workbook_family_mode(family: str) -> str:
    return {
        "contracted_actual_v1": "structured_snapshot",
        "client_status_report_v1": "metadata_snapshot",
        "product_master_v1": "metadata_snapshot",
        "generic_workbook_v1": "metadata_snapshot",
    }.get(family, "metadata_snapshot")
