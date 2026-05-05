"""Shared FastAPI dependency that builds a ``TransactionsFilter`` from the URL.

Lives in its own module so all three routers (pages, partials, api) can
inject the same parser via ``Depends(filter_from_query)``.
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from fastapi import HTTPException, Query

from finances.web.services.transactions_query import (
    TransactionsFilter,
)

_ALLOWED_PAGE_SIZES: frozenset[int] = frozenset({25, 50, 100})


def filter_from_query(
    date_from: date | None = Query(default=None),
    date_to: date | None = Query(default=None),
    accounts: list[str] = Query(default_factory=list),
    categories: list[str] = Query(default_factory=list),
    kinds: list[Literal["income", "expense", "transfer", "adjustment"]] = Query(
        default_factory=list
    ),
    sources: list[str] = Query(default_factory=list),
    currencies: list[str] = Query(default_factory=list),
    needs_review: Literal["any", "yes", "no"] = Query(default="any"),
    q: str = Query(default=""),
    sort: Literal["occurred_at", "amount_native", "amount_usd"] = Query(
        default="occurred_at"
    ),
    direction: Literal["asc", "desc"] = Query(default="desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1),
) -> TransactionsFilter:
    if page_size not in _ALLOWED_PAGE_SIZES:
        raise HTTPException(
            status_code=422,
            detail=f"page_size must be one of {sorted(_ALLOWED_PAGE_SIZES)}",
        )
    return TransactionsFilter(
        date_from=date_from,
        date_to=date_to,
        accounts=accounts,
        categories=categories,
        kinds=kinds,
        sources=sources,
        currencies=currencies,
        needs_review=needs_review,
        q=q,
        sort=sort,
        direction=direction,
        page=page,
        page_size=page_size,
    )


__all__ = ["filter_from_query"]
