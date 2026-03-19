"""init

Revision ID: 6382daab13d5
Revises: 
Create Date: 2026-03-19 09:48:38.964175

"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6382daab13d5'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("password_hash", sa.String(length=255), nullable=False),
        sa.Column("role", sa.Enum("admin", "analyst", "client_viewer", name="userrole"), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("email"),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)
    op.create_index("ix_users_role", "users", ["role"], unique=False)

    op.create_table(
        "clients",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("external_id", sa.String(length=64), nullable=False),
        sa.Column("client_type", sa.String(length=64), nullable=False, server_default="CPA"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("external_id", name="uq_clients_external_id"),
    )
    op.create_index("ix_clients_name", "clients", ["name"], unique=False)
    op.create_index("ix_clients_external_id", "clients", ["external_id"], unique=False)

    op.create_table(
        "snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("source_filename", sa.String(length=512), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "client_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("snapshot_id", sa.Integer(), sa.ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False),
        sa.Column("client_id", sa.Integer(), sa.ForeignKey("clients.id", ondelete="CASCADE"), nullable=False),
        sa.Column("contracted_ind", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("contracted_bus", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("contracted_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("received_ind", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("received_bus", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("received_total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("pending_ind", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("pending_bus", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("pending_total", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint("snapshot_id", "client_id", name="uq_client_snapshot"),
    )
    op.create_index("ix_client_snapshots_snapshot_id", "client_snapshots", ["snapshot_id"], unique=False)
    op.create_index("ix_client_snapshots_client_id", "client_snapshots", ["client_id"], unique=False)

    op.create_table(
        "staff_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("snapshot_id", sa.Integer(), sa.ForeignKey("snapshots.id", ondelete="CASCADE"), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("staff_external_id", sa.String(length=64), nullable=False),
        sa.Column("staff_type", sa.String(length=64), nullable=False),
        sa.Column("received_ind", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("received_bus", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("received_total", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint("snapshot_id", "staff_external_id", name="uq_staff_snapshot"),
    )
    op.create_index("ix_staff_snapshots_snapshot_id", "staff_snapshots", ["snapshot_id"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_staff_snapshots_snapshot_id", table_name="staff_snapshots")
    op.drop_table("staff_snapshots")

    op.drop_index("ix_client_snapshots_client_id", table_name="client_snapshots")
    op.drop_index("ix_client_snapshots_snapshot_id", table_name="client_snapshots")
    op.drop_table("client_snapshots")

    op.drop_table("snapshots")

    op.drop_index("ix_clients_external_id", table_name="clients")
    op.drop_index("ix_clients_name", table_name="clients")
    op.drop_table("clients")

    op.drop_index("ix_users_role", table_name="users")
    op.drop_index("ix_users_email", table_name="users")
    op.drop_table("users")

    op.execute("DROP TYPE IF EXISTS userrole")

