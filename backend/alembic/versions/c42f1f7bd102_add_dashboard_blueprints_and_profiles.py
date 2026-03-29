"""add dashboard blueprints and profiles

Revision ID: c42f1f7bd102
Revises: b8f6c9a12d41
Create Date: 2026-03-20 14:05:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c42f1f7bd102"
down_revision = "b8f6c9a12d41"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workbook_schema_profiles",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("snapshot_id", sa.Integer(), sa.ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False),
        sa.Column("schema_signature", sa.String(length=64), nullable=False),
        sa.Column("workbook_type", sa.String(length=64), nullable=False),
        sa.Column("profile_json", sa.JSON(), nullable=False),
        sa.Column("source_filename", sa.String(length=512), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_workbook_schema_profiles_snapshot_id", "workbook_schema_profiles", ["snapshot_id"], unique=True)
    op.create_index("ix_workbook_schema_profiles_schema_signature", "workbook_schema_profiles", ["schema_signature"], unique=False)
    op.create_index("ix_workbook_schema_profiles_workbook_type", "workbook_schema_profiles", ["workbook_type"], unique=False)

    op.create_table(
        "dashboard_blueprints",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("blueprint_key", sa.String(length=128), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("schema_signature", sa.String(length=64), nullable=False),
        sa.Column("workbook_type", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="approved"),
        sa.Column("config_json", sa.JSON(), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("approved_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("schema_signature", name="uq_dashboard_blueprints_schema_signature"),
    )
    op.create_index("ix_dashboard_blueprints_blueprint_key", "dashboard_blueprints", ["blueprint_key"], unique=True)
    op.create_index("ix_dashboard_blueprints_schema_signature", "dashboard_blueprints", ["schema_signature"], unique=False)
    op.create_index("ix_dashboard_blueprints_workbook_type", "dashboard_blueprints", ["workbook_type"], unique=False)
    op.create_index("ix_dashboard_blueprints_status", "dashboard_blueprints", ["status"], unique=False)
    op.create_index("ix_dashboard_blueprints_created_by_user_id", "dashboard_blueprints", ["created_by_user_id"], unique=False)
    op.create_index("ix_dashboard_blueprints_approved_by_user_id", "dashboard_blueprints", ["approved_by_user_id"], unique=False)

    op.create_table(
        "dashboard_proposals",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("snapshot_id", sa.Integer(), sa.ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False),
        sa.Column("schema_profile_id", sa.Integer(), sa.ForeignKey("workbook_schema_profiles.id", ondelete="CASCADE"), nullable=False),
        sa.Column("matched_blueprint_id", sa.Integer(), sa.ForeignKey("dashboard_blueprints.id", ondelete="SET NULL"), nullable=True),
        sa.Column("approved_blueprint_id", sa.Integer(), sa.ForeignKey("dashboard_blueprints.id", ondelete="SET NULL"), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("match_mode", sa.String(length=32), nullable=False, server_default="inferred"),
        sa.Column("confidence_score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("rationale", sa.Text(), nullable=False),
        sa.Column("proposal_json", sa.JSON(), nullable=False),
        sa.Column("created_by_system", sa.String(length=64), nullable=False, server_default="schema_profiler"),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("approved_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_dashboard_proposals_snapshot_id", "dashboard_proposals", ["snapshot_id"], unique=True)
    op.create_index("ix_dashboard_proposals_schema_profile_id", "dashboard_proposals", ["schema_profile_id"], unique=False)
    op.create_index("ix_dashboard_proposals_matched_blueprint_id", "dashboard_proposals", ["matched_blueprint_id"], unique=False)
    op.create_index("ix_dashboard_proposals_approved_blueprint_id", "dashboard_proposals", ["approved_blueprint_id"], unique=False)
    op.create_index("ix_dashboard_proposals_status", "dashboard_proposals", ["status"], unique=False)
    op.create_index("ix_dashboard_proposals_created_by_user_id", "dashboard_proposals", ["created_by_user_id"], unique=False)
    op.create_index("ix_dashboard_proposals_approved_by_user_id", "dashboard_proposals", ["approved_by_user_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_dashboard_proposals_approved_by_user_id", table_name="dashboard_proposals")
    op.drop_index("ix_dashboard_proposals_created_by_user_id", table_name="dashboard_proposals")
    op.drop_index("ix_dashboard_proposals_status", table_name="dashboard_proposals")
    op.drop_index("ix_dashboard_proposals_approved_blueprint_id", table_name="dashboard_proposals")
    op.drop_index("ix_dashboard_proposals_matched_blueprint_id", table_name="dashboard_proposals")
    op.drop_index("ix_dashboard_proposals_schema_profile_id", table_name="dashboard_proposals")
    op.drop_index("ix_dashboard_proposals_snapshot_id", table_name="dashboard_proposals")
    op.drop_table("dashboard_proposals")

    op.drop_index("ix_dashboard_blueprints_approved_by_user_id", table_name="dashboard_blueprints")
    op.drop_index("ix_dashboard_blueprints_created_by_user_id", table_name="dashboard_blueprints")
    op.drop_index("ix_dashboard_blueprints_status", table_name="dashboard_blueprints")
    op.drop_index("ix_dashboard_blueprints_workbook_type", table_name="dashboard_blueprints")
    op.drop_index("ix_dashboard_blueprints_schema_signature", table_name="dashboard_blueprints")
    op.drop_index("ix_dashboard_blueprints_blueprint_key", table_name="dashboard_blueprints")
    op.drop_table("dashboard_blueprints")

    op.drop_index("ix_workbook_schema_profiles_workbook_type", table_name="workbook_schema_profiles")
    op.drop_index("ix_workbook_schema_profiles_schema_signature", table_name="workbook_schema_profiles")
    op.drop_index("ix_workbook_schema_profiles_snapshot_id", table_name="workbook_schema_profiles")
    op.drop_table("workbook_schema_profiles")
