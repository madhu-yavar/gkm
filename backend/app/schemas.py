from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, EmailStr


class TokenResponse(BaseModel):
    access_token: str
    token_type: Literal["bearer"] = "bearer"


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class SnapshotSummary(BaseModel):
    id: int
    as_of_date: date
    source_filename: str


class KpiResponse(BaseModel):
    snapshot: SnapshotSummary
    total_contracted: int
    total_received: int
    total_pending: int
    total_contracted_ind: int
    total_contracted_bus: int
    total_received_ind: int
    total_received_bus: int
    overall_receipt_rate: float
    active_clients: int
    zero_received_clients: int
    over_delivered_clients: int
    staff_total_received: int


class ClientRow(BaseModel):
    client_name: str
    client_id: str
    client_type: str
    contracted_ind: int
    contracted_bus: int
    contracted_total: int
    received_ind: int
    received_bus: int
    received_total: int
    pending_ind: int
    pending_bus: int
    pending_total: int
    receipt_rate: float | None


class StaffRow(BaseModel):
    name: str
    staff_id: str
    staff_type: str
    received_ind: int
    received_bus: int
    received_total: int

