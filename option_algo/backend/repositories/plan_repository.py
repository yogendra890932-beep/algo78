# backend/repositories/plan_repository.py
from typing import Optional, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from backend.db.models import SubscriptionPlan, SubscriptionPlanSymbol


class PlanRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def list(self, active_only: bool = False) -> List[SubscriptionPlan]:
        stmt = select(SubscriptionPlan).options(selectinload(SubscriptionPlan.symbols))
        if active_only:
            stmt = stmt.where(SubscriptionPlan.is_active.is_(True))
        stmt = stmt.order_by(SubscriptionPlan.monthly_price)
        res = await self.db.execute(stmt)
        return list(res.scalars().unique().all())

    async def get_by_id(self, plan_id: int) -> Optional[SubscriptionPlan]:
        stmt = (select(SubscriptionPlan)
                .options(selectinload(SubscriptionPlan.symbols))
                .where(SubscriptionPlan.id == plan_id))
        res = await self.db.execute(stmt)
        return res.scalar_one_or_none()

    async def get_by_code(self, plan_code: str) -> Optional[SubscriptionPlan]:
        stmt = (select(SubscriptionPlan)
                .options(selectinload(SubscriptionPlan.symbols))
                .where(SubscriptionPlan.plan_code == plan_code))
        res = await self.db.execute(stmt)
        return res.scalar_one_or_none()

    async def create(self, plan: SubscriptionPlan) -> SubscriptionPlan:
        self.db.add(plan)
        await self.db.flush()
        return plan

    async def add_symbol(self, symbol_row: SubscriptionPlanSymbol) -> SubscriptionPlanSymbol:
        self.db.add(symbol_row)
        await self.db.flush()
        return symbol_row

    async def delete_symbols(self, plan_id: int) -> None:
        res = await self.db.execute(
            select(SubscriptionPlanSymbol).where(SubscriptionPlanSymbol.plan_id == plan_id))
        for row in res.scalars().all():
            await self.db.delete(row)

    async def delete(self, plan: SubscriptionPlan) -> None:
        await self.db.delete(plan)

    async def count_references(self, plan_id: int) -> int:
        """Count rows that reference a plan (subscriptions history +
        pending upgrades + payments) — used to refuse hard deletes that
        would violate FK constraints / corrupt billing history."""
        from sqlalchemy import func, or_, select
        from backend.db.models import Payment, Subscription
        active = await self.db.execute(
            select(func.count()).select_from(Subscription).where(
                or_(Subscription.plan_id == plan_id,
                    Subscription.pending_plan_id == plan_id)))
        pay = await self.db.execute(
            select(func.count()).select_from(Payment).where(
                Payment.plan_id == plan_id))
        return int(active.scalar_one() or 0) + int(pay.scalar_one() or 0)
