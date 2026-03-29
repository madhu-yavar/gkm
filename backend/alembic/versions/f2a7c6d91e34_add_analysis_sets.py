"""add analysis sets

Revision ID: f2a7c6d91e34
Revises: e6d4f9a8c1b2
Create Date: 2026-03-26 18:05:00.000000

"""

from alembic import op
import sqlalchemy as sa


revision = "f2a7c6d91e34"
down_revision = "e6d4f9a8c1b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "analysis_sets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("intent", sa.Text(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="draft"),
        sa.Column("relationship_type", sa.String(length=64), nullable=False, server_default="comparison"),
        sa.Column("confidence_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("proposal_json", sa.JSON(), nullable=False),
        sa.Column("confirmed_json", sa.JSON(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("confirmed_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_analysis_sets_status", "analysis_sets", ["status"], unique=False)
    op.create_index("ix_analysis_sets_relationship_type", "analysis_sets", ["relationship_type"], unique=False)
    op.create_index("ix_analysis_sets_created_by_user_id", "analysis_sets", ["created_by_user_id"], unique=False)
    op.create_index("ix_analysis_sets_confirmed_by_user_id", "analysis_sets", ["confirmed_by_user_id"], unique=False)

    op.create_table(
        "analysis_set_members",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("analysis_set_id", sa.Integer(), sa.ForeignKey("analysis_sets.id", ondelete="CASCADE"), nullable=False),
        sa.Column("snapshot_id", sa.Integer(), sa.ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False),
        sa.Column("member_order", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("role_label", sa.String(length=128), nullable=True),
        sa.Column("source_filename", sa.String(length=512), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=True),
        sa.Column("workbook_type", sa.String(length=64), nullable=True),
        sa.UniqueConstraint("analysis_set_id", "snapshot_id", name="uq_analysis_set_member"),
    )
    op.create_index("ix_analysis_set_members_analysis_set_id", "analysis_set_members", ["analysis_set_id"], unique=False)
    op.create_index("ix_analysis_set_members_snapshot_id", "analysis_set_members", ["snapshot_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_analysis_set_members_snapshot_id", table_name="analysis_set_members")
    op.drop_index("ix_analysis_set_members_analysis_set_id", table_name="analysis_set_members")
    op.drop_table("analysis_set_members")

    op.drop_index("ix_analysis_sets_confirmed_by_user_id", table_name="analysis_sets")
    op.drop_index("ix_analysis_sets_created_by_user_id", table_name="analysis_sets")
    op.drop_index("ix_analysis_sets_relationship_type", table_name="analysis_sets")
    op.drop_index("ix_analysis_sets_status", table_name="analysis_sets")
    op.drop_table("analysis_sets")
