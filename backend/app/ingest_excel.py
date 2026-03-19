from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from openpyxl import load_workbook


DATE_RE = re.compile(r"received\s+as\s+of\s+(\d{1,2})/(\d{1,2})", re.IGNORECASE)


def _safe_int(v) -> int:
    if v is None or v == "":
        return 0
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return 0


@dataclass(frozen=True)
class ParsedClientRow:
    name: str
    external_id: str
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


@dataclass(frozen=True)
class ParsedStaffRow:
    name: str
    external_id: str
    staff_type: str
    received_ind: int
    received_bus: int
    received_total: int


@dataclass(frozen=True)
class ParsedWorkbook:
    as_of_date: date
    clients: list[ParsedClientRow]
    staff: list[ParsedStaffRow]


def infer_as_of_date(ws) -> date:
    """
    The sample sheet has header: 'Received as of 03/09' (month/day). We interpret it
    as current year by default; if the workbook has a year indicator we can refine later.
    """
    # scan first row for 'Received as of'
    for c in range(1, 20):
        v = ws.cell(1, c).value
        if isinstance(v, str):
            m = DATE_RE.search(v)
            if m:
                month = int(m.group(1))
                day = int(m.group(2))
                year = date.today().year
                return date(year, month, day)
    return date.today()


def parse_contracted_vs_actual_xlsx(path: str | Path) -> ParsedWorkbook:
    wb = load_workbook(filename=str(path), data_only=True, read_only=True)
    if "Est vs Actual-2026" in wb.sheetnames:
        ws = wb["Est vs Actual-2026"]
    else:
        ws = wb[wb.sheetnames[0]]

    as_of = infer_as_of_date(ws)

    clients: list[ParsedClientRow] = []
    staff: list[ParsedStaffRow] = []

    # client rows appear from row 3 until the first "Total" row at column A.
    r = 3
    while r <= ws.max_row:
        first = ws.cell(r, 1).value
        if first is None:
            r += 1
            continue
        if isinstance(first, str) and first.strip().lower() == "total":
            break
        # A-L: name, id, type, contracted ind/bus/tot, received ind/bus/tot, pending ind/bus/tot
        name = str(ws.cell(r, 1).value).strip()
        ext_id = str(ws.cell(r, 2).value or "").strip()
        ctype = str(ws.cell(r, 3).value or "CPA").strip()
        con_ind = _safe_int(ws.cell(r, 4).value)
        con_bus = _safe_int(ws.cell(r, 5).value)
        con_tot = _safe_int(ws.cell(r, 6).value) or (con_ind + con_bus)
        rec_ind = _safe_int(ws.cell(r, 7).value)
        rec_bus = _safe_int(ws.cell(r, 8).value)
        rec_tot = _safe_int(ws.cell(r, 9).value) or (rec_ind + rec_bus)
        pend_ind = _safe_int(ws.cell(r, 10).value)
        pend_bus = _safe_int(ws.cell(r, 11).value)
        pend_tot = _safe_int(ws.cell(r, 12).value) or (con_tot - rec_tot)

        clients.append(
            ParsedClientRow(
                name=name,
                external_id=ext_id,
                client_type=ctype,
                contracted_ind=con_ind,
                contracted_bus=con_bus,
                contracted_total=con_tot,
                received_ind=rec_ind,
                received_bus=rec_bus,
                received_total=rec_tot,
                pending_ind=pend_ind,
                pending_bus=pend_bus,
                pending_total=pend_tot,
            )
        )
        r += 1

    # find staff section start (row where A == 'Client Name' and D == 'Received Till date')
    staff_start = None
    for rr in range(1, ws.max_row + 1):
        a = ws.cell(rr, 1).value
        d = ws.cell(rr, 4).value
        if isinstance(a, str) and isinstance(d, str):
            if a.strip().lower() == "client name" and d.strip().lower().startswith("received"):
                staff_start = rr + 2  # header + subheader
                break

    if staff_start:
        rr = staff_start
        while rr <= ws.max_row:
            a = ws.cell(rr, 1).value
            if a is None:
                rr += 1
                continue
            if isinstance(a, str) and a.strip().lower() == "total":
                break
            name = str(ws.cell(rr, 1).value).strip()
            ext_id = str(ws.cell(rr, 2).value or "").strip()
            stype = str(ws.cell(rr, 3).value or "").strip()
            rec_ind = _safe_int(ws.cell(rr, 4).value)
            rec_bus = _safe_int(ws.cell(rr, 5).value)
            rec_tot = _safe_int(ws.cell(rr, 6).value) or (rec_ind + rec_bus)
            staff.append(
                ParsedStaffRow(
                    name=name,
                    external_id=ext_id,
                    staff_type=stype,
                    received_ind=rec_ind,
                    received_bus=rec_bus,
                    received_total=rec_tot,
                )
            )
            rr += 1

    return ParsedWorkbook(as_of_date=as_of, clients=clients, staff=staff)

