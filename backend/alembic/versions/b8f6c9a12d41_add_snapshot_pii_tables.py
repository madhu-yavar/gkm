"""add snapshot pii tables

Revision ID: b8f6c9a12d41
Revises: 6382daab13d5
Create Date: 2026-03-20 12:10:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "b8f6c9a12d41"
down_revision = "6382daab13d5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "snapshot_pii_fields",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("snapshot_id", sa.Integer(), sa.ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False),
        sa.Column("sheet_name", sa.String(length=255), nullable=False),
        sa.Column("section_key", sa.String(length=64), nullable=False),
        sa.Column("header_label", sa.String(length=255), nullable=False),
        sa.Column("normalized_header", sa.String(length=255), nullable=False),
        sa.Column("pii_type", sa.String(length=32), nullable=False),
        sa.Column("masking_strategy", sa.String(length=32), nullable=False, server_default="tokenize"),
        sa.Column("selection_source", sa.String(length=32), nullable=False, server_default="user_selected"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("updated_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("snapshot_id", "sheet_name", "section_key", "header_label", name="uq_snapshot_pii_field"),
    )
    op.create_index("ix_snapshot_pii_fields_snapshot_id", "snapshot_pii_fields", ["snapshot_id"], unique=False)
    op.create_index("ix_snapshot_pii_fields_section_key", "snapshot_pii_fields", ["section_key"], unique=False)
    op.create_index("ix_snapshot_pii_fields_normalized_header", "snapshot_pii_fields", ["normalized_header"], unique=False)
    op.create_index("ix_snapshot_pii_fields_created_by_user_id", "snapshot_pii_fields", ["created_by_user_id"], unique=False)
    op.create_index("ix_snapshot_pii_fields_updated_by_user_id", "snapshot_pii_fields", ["updated_by_user_id"], unique=False)

    op.create_table(
        "snapshot_pii_token_mappings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("snapshot_id", sa.Integer(), sa.ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False),
        sa.Column("pii_field_id", sa.Integer(), sa.ForeignKey("snapshot_pii_fields.id", ondelete="SET NULL"), nullable=True),
        sa.Column("pii_type", sa.String(length=32), nullable=False),
        sa.Column("masking_strategy", sa.String(length=32), nullable=False, server_default="tokenize"),
        sa.Column("source_sheet_name", sa.String(length=255), nullable=True),
        sa.Column("source_section_key", sa.String(length=64), nullable=True),
        sa.Column("source_header_label", sa.String(length=255), nullable=True),
        sa.Column("original_value", sa.Text(), nullable=False),
        sa.Column("original_value_hash", sa.String(length=64), nullable=False),
        sa.Column("masked_token", sa.String(length=64), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("snapshot_id", "masked_token", name="uq_snapshot_pii_masked_token"),
    )
    op.create_index("ix_snapshot_pii_token_mappings_snapshot_id", "snapshot_pii_token_mappings", ["snapshot_id"], unique=False)
    op.create_index("ix_snapshot_pii_token_mappings_pii_field_id", "snapshot_pii_token_mappings", ["pii_field_id"], unique=False)
    op.create_index("ix_snapshot_pii_token_mappings_original_value_hash", "snapshot_pii_token_mappings", ["original_value_hash"], unique=False)
    op.create_index("ix_snapshot_pii_token_mappings_created_by_user_id", "snapshot_pii_token_mappings", ["created_by_user_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_snapshot_pii_token_mappings_created_by_user_id", table_name="snapshot_pii_token_mappings")
    op.drop_index("ix_snapshot_pii_token_mappings_original_value_hash", table_name="snapshot_pii_token_mappings")
    op.drop_index("ix_snapshot_pii_token_mappings_pii_field_id", table_name="snapshot_pii_token_mappings")
    op.drop_index("ix_snapshot_pii_token_mappings_snapshot_id", table_name="snapshot_pii_token_mappings")
    op.drop_table("snapshot_pii_token_mappings")

    op.drop_index("ix_snapshot_pii_fields_updated_by_user_id", table_name="snapshot_pii_fields")
    op.drop_index("ix_snapshot_pii_fields_created_by_user_id", table_name="snapshot_pii_fields")
    op.drop_index("ix_snapshot_pii_fields_normalized_header", table_name="snapshot_pii_fields")
    op.drop_index("ix_snapshot_pii_fields_section_key", table_name="snapshot_pii_fields")
    op.drop_index("ix_snapshot_pii_fields_snapshot_id", table_name="snapshot_pii_fields")
    op.drop_table("snapshot_pii_fields")
