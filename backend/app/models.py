from __future__ import annotations

import enum
from datetime import datetime, date

from sqlalchemy import Boolean, Date, DateTime, Enum, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class UserRole(str, enum.Enum):
    admin = "admin"
    analyst = "analyst"
    client_viewer = "client_viewer"


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    email: Mapped[str] = mapped_column(String(320), unique=True, index=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[UserRole] = mapped_column(Enum(UserRole), index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)


class Client(Base):
    __tablename__ = "clients"
    __table_args__ = (UniqueConstraint("external_id", name="uq_clients_external_id"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    external_id: Mapped[str] = mapped_column(String(64), index=True)  # e.g. "CH"
    client_type: Mapped[str] = mapped_column(String(64), default="CPA")

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    snapshots: Mapped[list["ClientSnapshot"]] = relationship(back_populates="client", cascade="all, delete-orphan")


class Snapshot(Base):
    __tablename__ = "snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_filename: Mapped[str] = mapped_column(String(512))
    as_of_date: Mapped[date] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    client_rows: Mapped[list["ClientSnapshot"]] = relationship(back_populates="snapshot", cascade="all, delete-orphan")
    staff_rows: Mapped[list["StaffSnapshot"]] = relationship(back_populates="snapshot", cascade="all, delete-orphan")


class ClientSnapshot(Base):
    __tablename__ = "client_snapshots"
    __table_args__ = (UniqueConstraint("snapshot_id", "client_id", name="uq_client_snapshot"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("snapshots.id", ondelete="CASCADE"), index=True)
    client_id: Mapped[int] = mapped_column(ForeignKey("clients.id", ondelete="CASCADE"), index=True)

    contracted_ind: Mapped[int] = mapped_column(Integer, default=0)
    contracted_bus: Mapped[int] = mapped_column(Integer, default=0)
    contracted_total: Mapped[int] = mapped_column(Integer, default=0)

    received_ind: Mapped[int] = mapped_column(Integer, default=0)
    received_bus: Mapped[int] = mapped_column(Integer, default=0)
    received_total: Mapped[int] = mapped_column(Integer, default=0)

    pending_ind: Mapped[int] = mapped_column(Integer, default=0)
    pending_bus: Mapped[int] = mapped_column(Integer, default=0)
    pending_total: Mapped[int] = mapped_column(Integer, default=0)

    snapshot: Mapped["Snapshot"] = relationship(back_populates="client_rows")
    client: Mapped["Client"] = relationship(back_populates="snapshots")


class StaffSnapshot(Base):
    __tablename__ = "staff_snapshots"
    __table_args__ = (UniqueConstraint("snapshot_id", "staff_external_id", name="uq_staff_snapshot"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    snapshot_id: Mapped[int] = mapped_column(ForeignKey("snapshots.id", ondelete="CASCADE"), index=True)

    name: Mapped[str] = mapped_column(String(255))
    staff_external_id: Mapped[str] = mapped_column(String(64))  # e.g. "TM"
    staff_type: Mapped[str] = mapped_column(String(64))  # e.g. "2 FTE"

    received_ind: Mapped[int] = mapped_column(Integer, default=0)
    received_bus: Mapped[int] = mapped_column(Integer, default=0)
    received_total: Mapped[int] = mapped_column(Integer, default=0)

    snapshot: Mapped["Snapshot"] = relationship(back_populates="staff_rows")
