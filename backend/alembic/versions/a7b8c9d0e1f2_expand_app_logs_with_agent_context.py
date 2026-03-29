"""expand app logs with agent context

Revision ID: a7b8c9d0e1f2
Revises: 9a1b2c3d4e5f
Create Date: 2026-03-27 12:05:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "a7b8c9d0e1f2"
down_revision = "9a1b2c3d4e5f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("app_logs", sa.Column("run_key", sa.String(length=64), nullable=True))
    op.add_column("app_logs", sa.Column("state", sa.String(length=32), nullable=True))
    op.add_column("app_logs", sa.Column("agent_name", sa.String(length=64), nullable=True))
    op.add_column("app_logs", sa.Column("workflow", sa.String(length=128), nullable=True))
    op.add_column("app_logs", sa.Column("tool_name", sa.String(length=128), nullable=True))
    op.add_column("app_logs", sa.Column("model_name", sa.String(length=128), nullable=True))
    op.add_column("app_logs", sa.Column("payload_json", sa.JSON(), nullable=True))
    op.create_index("ix_app_logs_run_key", "app_logs", ["run_key"], unique=False)
    op.create_index("ix_app_logs_state", "app_logs", ["state"], unique=False)
    op.create_index("ix_app_logs_agent_name", "app_logs", ["agent_name"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_app_logs_agent_name", table_name="app_logs")
    op.drop_index("ix_app_logs_state", table_name="app_logs")
    op.drop_index("ix_app_logs_run_key", table_name="app_logs")
    op.drop_column("app_logs", "payload_json")
    op.drop_column("app_logs", "model_name")
    op.drop_column("app_logs", "tool_name")
    op.drop_column("app_logs", "workflow")
    op.drop_column("app_logs", "agent_name")
    op.drop_column("app_logs", "state")
    op.drop_column("app_logs", "run_key")
