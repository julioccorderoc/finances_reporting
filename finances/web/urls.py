"""URL builders shared by the routers and the templates.

One function today: which dialog opens a given queue item. It lives here
rather than in ``routers/partials.py`` because both a template (the row's
two open controls, the *Sort all N* button) and the advance path need the
same answer, and a template importing from a router module would be a
circular import waiting to happen.
"""

from __future__ import annotations

from finances.web.services.triage import TriageItem, TriageType


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


__all__ = ["modal_url_for"]
