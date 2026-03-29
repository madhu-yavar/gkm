from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TypeVar

import httpx
from pydantic import BaseModel, ValidationError


T = TypeVar("T", bound=BaseModel)


class GeminiReasoningError(RuntimeError):
    pass


@dataclass(frozen=True)
class GeminiRequestSettings:
    api_key: str
    model: str


def gemini_settings_from_headers(api_key: str | None, model: str | None) -> GeminiRequestSettings | None:
    key = (api_key or "").strip()
    if not key:
        return None
    resolved_model = (model or "gemini-2.5-flash").strip() or "gemini-2.5-flash"
    return GeminiRequestSettings(api_key=key, model=resolved_model)


def _gemini_url(model: str, api_key: str) -> str:
    return f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"


def _extract_text(data: dict) -> str:
    try:
        parts = data["candidates"][0]["content"]["parts"]
    except Exception as exc:  # pragma: no cover - defensive parsing
        raise GeminiReasoningError(f"Gemini response shape error: {exc}") from exc

    text = "".join(str(part.get("text") or "") for part in parts if isinstance(part, dict))
    text = text.strip()
    if not text:
        raise GeminiReasoningError("Gemini returned an empty response")
    return text


def _clean_json_text(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def gemini_generate_structured(
    *,
    prompt: str,
    schema: type[T],
    settings: GeminiRequestSettings,
    timeout_s: float = 60.0,
) -> T:
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
        },
    }

    with httpx.Client(timeout=timeout_s) as client:
        response = client.post(_gemini_url(settings.model, settings.api_key), json=body)
        if response.status_code >= 400:
            raise GeminiReasoningError(f"Gemini HTTP {response.status_code}: {response.text[:300]}")
        raw = response.json()

    text = _clean_json_text(_extract_text(raw))
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise GeminiReasoningError(f"Gemini JSON parse error: {exc}") from exc
    try:
        return schema.model_validate(payload)
    except ValidationError as exc:
        raise GeminiReasoningError(f"Gemini schema validation error: {exc}") from exc


def gemini_generate_json(
    *,
    prompt: str,
    settings: GeminiRequestSettings,
    timeout_s: float = 60.0,
) -> dict | list:
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
        },
    }

    with httpx.Client(timeout=timeout_s) as client:
        response = client.post(_gemini_url(settings.model, settings.api_key), json=body)
        if response.status_code >= 400:
            raise GeminiReasoningError(f"Gemini HTTP {response.status_code}: {response.text[:300]}")
        raw = response.json()

    text = _clean_json_text(_extract_text(raw))
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise GeminiReasoningError(f"Gemini JSON parse error: {exc}") from exc
    if not isinstance(payload, (dict, list)):
        raise GeminiReasoningError("Gemini JSON payload must be an object or array")
    return payload
