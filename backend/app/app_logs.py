from __future__ import annotations

from contextvars import ContextVar
import json
import traceback
from typing import Any

from app import models
from app.db import SessionLocal


_log_context: ContextVar[dict[str, Any]] = ContextVar("app_log_context", default={})


def _stringify_detail(detail: Any) -> str | None:
    if detail is None:
        return None
    if isinstance(detail, str):
        return detail[:20000]
    try:
        return json.dumps(detail, default=str)[:20000]
    except Exception:
        return str(detail)[:20000]


def _json_payload(payload: Any) -> dict | None:
    if payload is None:
        return None
    if isinstance(payload, dict):
        return payload
    try:
        dumped = json.loads(json.dumps(payload, default=str))
        return dumped if isinstance(dumped, dict) else {"value": dumped}
    except Exception:
        return {"value": str(payload)[:20000]}


def set_log_context(**kwargs: Any):
    merged = {**_log_context.get({}), **{key: value for key, value in kwargs.items() if value is not None}}
    return _log_context.set(merged)


def reset_log_context(token) -> None:
    _log_context.reset(token)


def log_app_event(
    *,
    level: str,
    state: str | None = None,
    category: str,
    event: str,
    message: str,
    agent_name: str | None = None,
    workflow: str | None = None,
    tool_name: str | None = None,
    model_name: str | None = None,
    payload: Any = None,
    detail: Any = None,
    request_path: str | None = None,
    run_key: str | None = None,
    snapshot_id: int | None = None,
    proposal_id: int | None = None,
    blueprint_id: int | None = None,
    user_id: int | None = None,
) -> None:
    context = _log_context.get({})
    session = SessionLocal()
    try:
        session.add(
            models.AppLog(
                run_key=(str(run_key or context.get("run_key") or "")[:64] or None),
                level=str(level or "info")[:16],
                state=(str(state or context.get("state") or "")[:32] or None),
                category=str(category or "general")[:64],
                event=str(event or "event")[:128],
                agent_name=(str(agent_name or context.get("agent_name") or "")[:64] or None),
                workflow=(str(workflow or context.get("workflow") or "")[:128] or None),
                tool_name=(str(tool_name or context.get("tool_name") or "")[:128] or None),
                model_name=(str(model_name or context.get("model_name") or "")[:128] or None),
                message=str(message or "")[:512],
                detail=_stringify_detail(detail),
                payload_json=_json_payload(payload),
                request_path=(str(request_path or context.get("request_path") or "")[:255] or None),
                snapshot_id=snapshot_id if snapshot_id is not None else context.get("snapshot_id"),
                proposal_id=proposal_id if proposal_id is not None else context.get("proposal_id"),
                blueprint_id=blueprint_id if blueprint_id is not None else context.get("blueprint_id"),
                user_id=user_id if user_id is not None else context.get("user_id"),
            )
        )
        session.commit()
    except Exception:
        session.rollback()
    finally:
        session.close()


def exception_detail(exc: Exception) -> dict[str, str]:
    return {
        "type": exc.__class__.__name__,
        "message": str(exc),
        "traceback": traceback.format_exc(),
    }
