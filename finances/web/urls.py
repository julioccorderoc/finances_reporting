"""URL builders shared by the routers and the templates.

They live here rather than in ``routers/partials.py`` because templates
need them too, and a template importing from a router module would be a
circular import waiting to happen.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING
from urllib.parse import urlencode

from finances.web.services.triage import TriageItem, TriageType

if TYPE_CHECKING:  # pragma: no cover - import cycle at runtime only
    from finances.web.services.transactions_query import TransactionsFilter


def modal_url_for(item: TriageItem) -> str:
    """The modal URL for ``item``.

    A pair addresses both of its legs; everything else addresses its own
    transaction. Arrow navigation needs no endpoint of its own because of
    this: the neighbour is known at render time, so an arrow points
    straight at that item's click-to-open route.
    """
    if item.type is TriageType.PAIR and item.pair_proposal is not None:
        deposit_id = int(item.pair_proposal.details["bank_transaction_id"])
        sell_id = int(item.pair_proposal.details["binance_transaction_id"])
        return f"/_partial/triage/pair/{deposit_id}/{sell_id}/modal"
    return f"/_partial/triage/{item.item_id.split(':')[1]}/modal"


#: Filter fields the Flow URL carries, in the order they are written.
#: Anything at its default is left out, so a bare list stays a bare URL.
_FLOW_PARAMS: tuple[tuple[str, object], ...] = (
    ("date_from", None),
    ("date_to", None),
    ("q", ""),
    ("needs_review", "any"),
    ("paired", "any"),
    ("accounts", ()),
    ("categories", ()),
    ("kinds", ()),
    ("currencies", ()),
    ("sources", ()),
    ("sort", "occurred_at"),
    ("direction", "desc"),
)


def transactions_url(f: TransactionsFilter, **overrides: object) -> str:
    """``/transactions`` carrying ``f``, with ``overrides`` applied.

    Only non-default values are written, so "the same search over every
    date" is ``transactions_url(f, date_from=None, date_to=None)`` and
    comes back as ``/transactions?q=Hemirla`` rather than a URL restating
    every default the server already holds. Page is deliberately absent:
    a changed filter starts at page 1.
    """
    params: list[tuple[str, str]] = []
    for name, default in _FLOW_PARAMS:
        value = overrides[name] if name in overrides else getattr(f, name)
        if isinstance(value, (list, tuple)):
            # An empty multi-select writes nothing, one entry per value
            # otherwise — the shape FastAPI's list Query parses back.
            params.extend((name, str(v)) for v in value)
            continue
        if value is None or value == default:
            continue
        params.append(
            (name, value.isoformat() if isinstance(value, date) else str(value))
        )
    return "/transactions" + ("?" + urlencode(params) if params else "")


__all__ = ["modal_url_for", "transactions_url"]
