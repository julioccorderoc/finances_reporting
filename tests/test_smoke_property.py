"""Property-based smoke test for the Pydantic domain models.

Proves that the `hypothesis` + `polyfactory` integration is wired correctly
for the rest of EPIC-002b. This is explicitly a *smoke* test per rule-011 —
it exercises round-trip wiring rather than business logic. Real Wave-2 epics
will author fuller hypothesis strategies for rate resolution and
categorization.

Tagged ``@pytest.mark.smoke`` so the TDD-evidence audit ignores it.
"""

from __future__ import annotations

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from finances.domain.models import Transaction
from tests.conftest import TransactionFactory


@pytest.mark.smoke
@given(st.integers(min_value=0, max_value=64))
@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_transaction_json_roundtrip_is_stable(_seed: int) -> None:
    """Every factory-built Transaction survives a JSON round-trip.

    Dumping to JSON and re-validating should produce an equivalent model.
    This proves:

    * ``polyfactory`` is emitting instances that pass Pydantic's strict
      validators (Decimal amounts, tz-aware datetimes, enum kinds).
    * ``hypothesis`` is installed and executes the ``@given`` decorator.
    * ``Transaction.model_dump_json`` / ``Transaction.model_validate_json``
      are symmetric on our own models.

    The ``_seed`` integer is unused at the body level — hypothesis simply
    re-runs the test body 25 times, and each run asks polyfactory for a
    fresh instance.
    """
    txn = TransactionFactory.build()

    raw = txn.model_dump_json()
    restored = Transaction.model_validate_json(raw)

    assert restored.account_id == txn.account_id
    assert restored.kind == txn.kind
    assert restored.amount == txn.amount
    assert restored.currency == txn.currency
    assert restored.source == txn.source
    assert restored.source_ref == txn.source_ref
    # Round-trip through JSON preserves tz-awareness (pydantic emits offset).
    assert restored.occurred_at.tzinfo is not None
    assert restored.occurred_at == txn.occurred_at
