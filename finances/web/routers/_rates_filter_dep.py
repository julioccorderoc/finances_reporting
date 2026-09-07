"""Shared FastAPI dependency that builds a ``RatesLogFilter`` from the URL.

Its own module so the page route and the log fragment inject the same
parser, exactly as ``_tx_filter_dep`` does for /transactions.

Note what this deliberately does NOT do: reject ``range_days``. The chart
and the log are independent controls that push onto the same /rates
address, so each side's requests carry the other's parameters through in
order to preserve them. A dependency that 422'd on an unknown parameter
would break the filter form the moment the owner had also touched the
range toggle.
"""

from __future__ import annotations

from datetime import date

from fastapi import HTTPException, Query

from finances.web.routers._optional_date import optional_date

from finances.web.services.rates_view import RatesLogFilter

_ALLOWED_PAGE_SIZES: frozenset[int] = frozenset({25, 50, 100, 200})


def rates_filter_from_query(
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    pairs: list[str] = Query(default_factory=list),
    sources: list[str] = Query(default_factory=list),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1),
) -> RatesLogFilter:
    if page_size not in _ALLOWED_PAGE_SIZES:
        raise HTTPException(
            status_code=422,
            detail=f"page_size must be one of {sorted(_ALLOWED_PAGE_SIZES)}",
        )
    return RatesLogFilter(
        date_from=optional_date(date_from, field="date_from"),
        date_to=optional_date(date_to, field="date_to"),
        pairs=pairs,
        sources=sources,
        page=page,
        page_size=page_size,
    )


__all__ = ["rates_filter_from_query", "_ALLOWED_PAGE_SIZES"]
