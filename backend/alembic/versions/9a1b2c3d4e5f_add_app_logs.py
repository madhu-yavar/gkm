"""add app logs

Revision ID: 9a1b2c3d4e5f
Revises: 4b7f21c0de91
Create Date: 2026-03-27 11:20:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "9a1b2c3d4e5f"
down_revision = "4b7f21c0de91"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "app_logs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("level", sa.String(length=16), nullable=False),
        sa.Column("category", sa.String(length=64), nullable=False),
        sa.Column("event", sa.String(length=128), nullable=False),
        sa.Column("message", sa.String(length=512), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("request_path", sa.String(length=255), nullable=True),
        sa.Column("snapshot_id", sa.Integer(), sa.ForeignKey("snapshots.id", ondelete="SET NULL"), nullable=True),
        sa.Column("proposal_id", sa.Integer(), sa.ForeignKey("dashboard_proposals.id", ondelete="SET NULL"), nullable=True),
        sa.Column("blueprint_id", sa.Integer(), sa.ForeignKey("dashboard_blueprints.id", ondelete="SET NULL"), nullable=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_app_logs_level", "app_logs", ["level"], unique=False)
    op.create_index("ix_app_logs_category", "app_logs", ["category"], unique=False)
    op.create_index("ix_app_logs_event", "app_logs", ["event"], unique=False)
    op.create_index("ix_app_logs_snapshot_id", "app_logs", ["snapshot_id"], unique=False)
    op.create_index("ix_app_logs_proposal_id", "app_logs", ["proposal_id"], unique=False)
    op.create_index("ix_app_logs_blueprint_id", "app_logs", ["blueprint_id"], unique=False)
    op.create_index("ix_app_logs_user_id", "app_logs", ["user_id"], unique=False)
    op.create_index("ix_app_logs_created_at", "app_logs", ["created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_app_logs_created_at", table_name="app_logs")
    op.drop_index("ix_app_logs_user_id", table_name="app_logs")
    op.drop_index("ix_app_logs_blueprint_id", table_name="app_logs")
    op.drop_index("ix_app_logs_proposal_id", table_name="app_logs")
    op.drop_index("ix_app_logs_snapshot_id", table_name="app_logs")
    op.drop_index("ix_app_logs_event", table_name="app_logs")
    op.drop_index("ix_app_logs_category", table_name="app_logs")
    op.drop_index("ix_app_logs_level", table_name="app_logs")
    op.drop_table("app_logs")
