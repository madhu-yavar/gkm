"""relax blueprint schema signature uniqueness

Revision ID: 4b7f21c0de91
Revises: f2a7c6d91e34
Create Date: 2026-03-26 18:20:00.000000
"""

from __future__ import annotations

from alembic import op


# revision identifiers, used by Alembic.
revision = "4b7f21c0de91"
down_revision = "f2a7c6d91e34"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("uq_dashboard_blueprints_schema_signature", "dashboard_blueprints", type_="unique")


def downgrade() -> None:
    op.create_unique_constraint(
        "uq_dashboard_blueprints_schema_signature",
        "dashboard_blueprints",
        ["schema_signature"],
    )
