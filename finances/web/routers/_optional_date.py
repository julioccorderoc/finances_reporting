"""Parsing for a date query parameter that an empty form field may send.

An untouched ``<input type="date">`` serialises as ``date_from=`` — the
name with an empty value — and that is the *default* state of every
filter form in the viewer. FastAPI's own ``date | None`` coercion cannot
parse ``""``, so those forms answered 422 on their own resting state:
/transactions' filters moved nothing at all until 2026-09-07, and the
console error was the only sign.

Empty means "no constraint". Anything else is parsed strictly, so a
malformed date is still a 422 rather than a silently ignored filter.
"""

from __future__ import annotations

from datetime import date

from fastapi import HTTPException


def optional_date(value: str | None, *, field: str) -> date | None:
    """``None`` for absent-or-empty, a real ``date`` otherwise.

    Raises 422 for anything unparseable, matching what FastAPI would have
    done for a non-empty bad value.
    """
    if value is None or value.strip() == "":
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        raise HTTPException(
            status_code=422,
            detail=f"{field} must be a date in YYYY-MM-DD form, got {value!r}",
        ) from None


__all__ = ["optional_date"]
