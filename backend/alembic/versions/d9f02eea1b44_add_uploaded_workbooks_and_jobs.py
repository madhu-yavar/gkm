"""add uploaded workbooks and jobs

Revision ID: d9f02eea1b44
Revises: c42f1f7bd102
Create Date: 2026-03-20 18:05:00.000000

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "d9f02eea1b44"
down_revision = "c42f1f7bd102"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "uploaded_workbooks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("upload_token", sa.String(length=64), nullable=False),
        sa.Column("source_filename", sa.String(length=512), nullable=False),
        sa.Column("stored_path", sa.String(length=1024), nullable=False),
        sa.Column("workbook_family", sa.String(length=64), nullable=False),
        sa.Column("family_label", sa.String(length=128), nullable=False),
        sa.Column("family_mode", sa.String(length=64), nullable=False),
        sa.Column("preview_json", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="ready"),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("upload_token", name="uq_uploaded_workbooks_upload_token"),
    )
    op.create_index("ix_uploaded_workbooks_upload_token", "uploaded_workbooks", ["upload_token"], unique=True)
    op.create_index("ix_uploaded_workbooks_workbook_family", "uploaded_workbooks", ["workbook_family"], unique=False)
    op.create_index("ix_uploaded_workbooks_status", "uploaded_workbooks", ["status"], unique=False)
    op.create_index("ix_uploaded_workbooks_created_by_user_id", "uploaded_workbooks", ["created_by_user_id"], unique=False)

    op.create_table(
        "document_processing_jobs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("upload_id", sa.Integer(), sa.ForeignKey("uploaded_workbooks.id", ondelete="CASCADE"), nullable=False),
        sa.Column("snapshot_id", sa.Integer(), sa.ForeignKey("snapshots.id", ondelete="SET NULL"), nullable=True),
        sa.Column("workbook_family", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="queued"),
        sa.Column("stage", sa.String(length=64), nullable=False, server_default="queued"),
        sa.Column("progress_percent", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("message", sa.String(length=255), nullable=False, server_default="Queued"),
        sa.Column("error_detail", sa.Text(), nullable=True),
        sa.Column("pii_config_json", sa.JSON(), nullable=True),
        sa.Column("created_by_user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="SET NULL"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_document_processing_jobs_upload_id", "document_processing_jobs", ["upload_id"], unique=False)
    op.create_index("ix_document_processing_jobs_snapshot_id", "document_processing_jobs", ["snapshot_id"], unique=False)
    op.create_index("ix_document_processing_jobs_workbook_family", "document_processing_jobs", ["workbook_family"], unique=False)
    op.create_index("ix_document_processing_jobs_status", "document_processing_jobs", ["status"], unique=False)
    op.create_index("ix_document_processing_jobs_created_by_user_id", "document_processing_jobs", ["created_by_user_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_document_processing_jobs_created_by_user_id", table_name="document_processing_jobs")
    op.drop_index("ix_document_processing_jobs_status", table_name="document_processing_jobs")
    op.drop_index("ix_document_processing_jobs_workbook_family", table_name="document_processing_jobs")
    op.drop_index("ix_document_processing_jobs_snapshot_id", table_name="document_processing_jobs")
    op.drop_index("ix_document_processing_jobs_upload_id", table_name="document_processing_jobs")
    op.drop_table("document_processing_jobs")

    op.drop_index("ix_uploaded_workbooks_created_by_user_id", table_name="uploaded_workbooks")
    op.drop_index("ix_uploaded_workbooks_status", table_name="uploaded_workbooks")
    op.drop_index("ix_uploaded_workbooks_workbook_family", table_name="uploaded_workbooks")
    op.drop_index("ix_uploaded_workbooks_upload_token", table_name="uploaded_workbooks")
    op.drop_table("uploaded_workbooks")
