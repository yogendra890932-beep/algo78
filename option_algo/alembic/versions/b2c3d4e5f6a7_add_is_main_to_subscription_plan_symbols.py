"""
Add is_main column to subscription_plan_symbols

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-08-03 00:00:00.000000

Adds:
  - subscription_plan_symbols.is_main (BOOLEAN, NOT NULL, default false)
    Marks a symbol as a selectable "main" symbol on a plan; False means
    an additional symbol traded alongside the chosen main symbol.

This column is defined on the SubscriptionPlanSymbol ORM model
(backend.db.models.SubscriptionPlanSymbol) but was never created in the
database by a migration, so queries referencing it fail with
UndefinedColumnError. This migration backfills existing rows as false.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "b2c3d4e5f6a7"
down_revision: Union[str, None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    is_postgres = bind.engine.name == "postgresql"

    if is_postgres:
        # Multi-step so existing rows get a value before the NOT NULL
        # constraint is applied (mirrors the pattern in 46bcd527f8bf).
        op.add_column(
            "subscription_plan_symbols",
            sa.Column("is_main", sa.Boolean(), nullable=True),
        )
        op.execute(
            "UPDATE subscription_plan_symbols SET is_main = false "
            "WHERE is_main IS NULL"
        )
        op.alter_column(
            "subscription_plan_symbols",
            "is_main",
            existing_type=sa.Boolean(),
            nullable=False,
            existing_server_default=None,
        )
        op.alter_column(
            "subscription_plan_symbols",
            "is_main",
            server_default=sa.text("false"),
        )
    else:
        # SQLite: add with a server default so existing rows are valid.
        op.add_column(
            "subscription_plan_symbols",
            sa.Column(
                "is_main",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("0"),
            ),
        )


def downgrade() -> None:
    op.drop_column("subscription_plan_symbols", "is_main")
