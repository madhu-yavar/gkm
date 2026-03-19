from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

import httpx

from app.ingest_excel import ParsedClientRow, ParsedStaffRow, ParsedWorkbook


class GeminiError(RuntimeError):
    pass


def _gemini_url(model: str, api_key: str) -> str:
    # REST API (Generative Language API)
    return f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"


def _to_prompt(parsed: ParsedWorkbook) -> str:
    # Keep this minimal and structured.
    sample_clients = [asdict(r) for r in parsed.clients[:10]]
    sample_staff = [asdict(r) for r in parsed.staff[:10]]
    payload = {
        "as_of_date": str(parsed.as_of_date),
        "clients_sample": sample_clients,
        "staff_sample": sample_staff,
        "counts": {"clients": len(parsed.clients), "staff": len(parsed.staff)},
        "task": "Validate the extracted snapshot. If any row looks wrong, return a corrected full dataset.",
        "output_schema": {
            "as_of_date": "YYYY-MM-DD",
            "clients": "list of client rows with fields: name, external_id, client_type, contracted_ind, contracted_bus, contracted_total, received_ind, received_bus, received_total, pending_ind, pending_bus, pending_total",
            "staff": "list of staff rows with fields: name, external_id, staff_type, received_ind, received_bus, received_total",
        },
        "rules": [
            "Return JSON only.",
            "Do not include markdown.",
            "If you cannot improve anything, return the same values and set unchanged=true.",
        ],
    }
    return json.dumps(payload, ensure_ascii=False)


def gemini_validate_or_correct(
    *,
    api_key: str,
    model: str,
    parsed: ParsedWorkbook,
    timeout_s: float = 45.0,
) -> tuple[ParsedWorkbook, dict[str, Any]]:
    """
    Optional Gemini step:
    - takes deterministic parse output
    - asks Gemini to validate/correct
    - if response is invalid, falls back to deterministic parse
    """
    url = _gemini_url(model=model, api_key=api_key)
    body = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": _to_prompt(parsed)},
                ],
            }
        ],
        "generationConfig": {
            "temperature": 0.0,
            "responseMimeType": "application/json",
        },
    }

    with httpx.Client(timeout=timeout_s) as client:
        r = client.post(url, json=body)
        if r.status_code >= 400:
            raise GeminiError(f"Gemini HTTP {r.status_code}: {r.text[:300]}")
        data = r.json()

    try:
        text = data["candidates"][0]["content"]["parts"][0]["text"]
        obj = json.loads(text)
    except Exception as e:
        raise GeminiError(f"Gemini parse error: {e}")

    meta = {"raw": obj}

    try:
        as_of_date = obj.get("as_of_date") or str(parsed.as_of_date)
        unchanged = bool(obj.get("unchanged"))
        if unchanged:
            return parsed, {"unchanged": True}

        clients = []
        for row in obj.get("clients", []):
            clients.append(
                ParsedClientRow(
                    name=str(row.get("name", "")).strip(),
                    external_id=str(row.get("external_id", "")).strip(),
                    client_type=str(row.get("client_type", "CPA")).strip(),
                    contracted_ind=int(row.get("contracted_ind", 0) or 0),
                    contracted_bus=int(row.get("contracted_bus", 0) or 0),
                    contracted_total=int(row.get("contracted_total", 0) or 0),
                    received_ind=int(row.get("received_ind", 0) or 0),
                    received_bus=int(row.get("received_bus", 0) or 0),
                    received_total=int(row.get("received_total", 0) or 0),
                    pending_ind=int(row.get("pending_ind", 0) or 0),
                    pending_bus=int(row.get("pending_bus", 0) or 0),
                    pending_total=int(row.get("pending_total", 0) or 0),
                )
            )

        staff = []
        for row in obj.get("staff", []):
            staff.append(
                ParsedStaffRow(
                    name=str(row.get("name", "")).strip(),
                    external_id=str(row.get("external_id", "")).strip(),
                    staff_type=str(row.get("staff_type", "")).strip(),
                    received_ind=int(row.get("received_ind", 0) or 0),
                    received_bus=int(row.get("received_bus", 0) or 0),
                    received_total=int(row.get("received_total", 0) or 0),
                )
            )

        from datetime import date as _date

        y, m, d = [int(x) for x in as_of_date.split("-")]
        wb = ParsedWorkbook(as_of_date=_date(y, m, d), clients=clients or parsed.clients, staff=staff or parsed.staff)
        return wb, meta
    except Exception as e:
        raise GeminiError(f"Gemini schema error: {e}")

