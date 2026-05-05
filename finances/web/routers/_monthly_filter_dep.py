"""Shared FastAPI dependency that builds a ``MonthlyFilter`` from the URL.

Mirrors :mod:`finances.web.routers._tx_filter_dep` so all three monthly
routers (pages / partials / api) share the same parser.
"""

from __future__ import annotations

import re

from fastapi import HTTPException, Query

from finances.web.services.monthly_view import (
    MonthlyFilter,
    MonthlyKind,
    MonthlyRangePreset,
)

_MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def _validate_month(value: str | None, *, field: str) -> str | None:
    if value is None or value == "":
        return None
    if not _MONTH_RE.match(value):
        raise HTTPException(
            status_code=422,
            detail=f"{field} must be YYYY-MM (got {value!r})",
        )
    return value


def monthly_filter_from_query(
    range_preset: MonthlyRangePreset = Query(default=MonthlyRangePreset.M6),
    since: str | None = Query(default=None),
    until: str | None = Query(default=None),
    kind: MonthlyKind = Query(default=MonthlyKind.EXPENSE),
    accounts: list[str] = Query(default_factory=list),
    categories: list[str] = Query(default_factory=list),
    currencies: list[str] = Query(default_factory=list),
    include_bcv_fallback: bool = Query(default=True),
) -> MonthlyFilter:
    return MonthlyFilter(
        range_preset=range_preset,
        since=_validate_month(since, field="since"),
        until=_validate_month(until, field="until"),
        kind=kind,
        accounts=accounts,
        categories=categories,
        currencies=currencies,
        include_bcv_fallback=include_bcv_fallback,
    )


__all__ = ["monthly_filter_from_query"]
