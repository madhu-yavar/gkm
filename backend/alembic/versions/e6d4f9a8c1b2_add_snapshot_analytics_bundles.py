"""add snapshot analytics bundles

Revision ID: e6d4f9a8c1b2
Revises: d9f02eea1b44
Create Date: 2026-03-26 14:20:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e6d4f9a8c1b2"
down_revision = "d9f02eea1b44"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "snapshot_analytics_bundles",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("snapshot_id", sa.Integer(), sa.ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False),
        sa.Column("blueprint_id", sa.Integer(), sa.ForeignKey("dashboard_blueprints.id", ondelete="SET NULL"), nullable=True),
        sa.Column("proposal_id", sa.Integer(), sa.ForeignKey("dashboard_proposals.id", ondelete="SET NULL"), nullable=True),
        sa.Column("bundle_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="ready"),
        sa.Column("generation_mode", sa.String(length=32), nullable=False, server_default="fallback"),
        sa.Column("stale", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("payload_json", sa.JSON(), nullable=False),
        sa.Column("diagnostics_json", sa.JSON(), nullable=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("snapshot_id", name="uq_snapshot_analytics_bundles_snapshot_id"),
    )
    op.create_index("ix_snapshot_analytics_bundles_snapshot_id", "snapshot_analytics_bundles", ["snapshot_id"], unique=True)
    op.create_index("ix_snapshot_analytics_bundles_blueprint_id", "snapshot_analytics_bundles", ["blueprint_id"], unique=False)
    op.create_index("ix_snapshot_analytics_bundles_proposal_id", "snapshot_analytics_bundles", ["proposal_id"], unique=False)
    op.create_index("ix_snapshot_analytics_bundles_status", "snapshot_analytics_bundles", ["status"], unique=False)
    op.create_index("ix_snapshot_analytics_bundles_stale", "snapshot_analytics_bundles", ["stale"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_snapshot_analytics_bundles_stale", table_name="snapshot_analytics_bundles")
    op.drop_index("ix_snapshot_analytics_bundles_status", table_name="snapshot_analytics_bundles")
    op.drop_index("ix_snapshot_analytics_bundles_proposal_id", table_name="snapshot_analytics_bundles")
    op.drop_index("ix_snapshot_analytics_bundles_blueprint_id", table_name="snapshot_analytics_bundles")
    op.drop_index("ix_snapshot_analytics_bundles_snapshot_id", table_name="snapshot_analytics_bundles")
    op.drop_table("snapshot_analytics_bundles")
