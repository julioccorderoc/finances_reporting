"""Triage payload v2 — what the redesigned queue needs from the server.

`design_handoff_triage/README.md` §"Data the server must supply" plus
acceptance criteria A1-A4, A8, D6, H1 and K1-K6. Three things change from
the payload shipped with EPIC-025:

* **Buckets follow the README's group order** — 0 category, 1 pairs,
  2 priced roughly. Criterion A3 lists category/rate/pair; the README's
  order wins because an approximate rate is explicitly non-blocking (A8,
  D6), so it walks last. Logged in `design_handoff_triage/NOTES.md`.
* **A rate item is computed, not stored** (K2). Membership comes from the
  projection — approximate or unpriceable — never from
  ``transactions.needs_review``, which goes stale the moment a rate lands
  and is wrong on 25 live rows.
* **Each item carries what the row needs to render itself** (K1): its
  account, its money and provenance, a guess from the real categorization
  engine, and for a pair, the numbers behind the confidence.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.db.repos import rates as rates_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Rate,
    Transaction,
    TransactionKind,
)
from finances.web.services.triage import (
    TriageType,
    build_queue,
    neighbours_of,
    next_item_after,
)

NOW = datetime.now(tz=UTC)


def _aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _ago(days: int) -> datetime:
    return NOW - timedelta(days=days)


@pytest.fixture
def queue_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    """One row per state the redesigned queue distinguishes."""
    provincial = accounts_repo.insert(
        web_db,
        Account(
            name="Provincial",
            kind=AccountKind.BANK,
            currency="VES",
            institution="0108",
        ),
    )
    binance = accounts_repo.insert(
        web_db,
        Account(
            name="Binance Spot",
            kind=AccountKind.CRYPTO_SPOT,
            currency="USDT",
            institution="Binance",
        ),
    )
    groceries = categories_repo.get_by_name(
        web_db, TransactionKind.EXPENSE, "Groceries"
    )
    assert groceries is not None

    # One median, dated far enough back to cover every recent row inside
    # the 14-day window. Rows older than that take it as the *nearest*
    # rate instead and become approximations — which is the distinction
    # the whole queue now turns on.
    rates_repo.upsert(
        web_db,
        Rate(
            as_of_date=(NOW - timedelta(days=11)).date(),
            base="USDT",
            quote="VES",
            rate=Decimal("36.50"),
            source="binance_p2p_median",
        ),
    )

    def _txn(**kwargs) -> int:
        defaults = dict(
            account_id=provincial.id,
            kind=TransactionKind.EXPENSE,
            currency="VES",
            source="provincial",
        )
        txn = transactions_repo.insert(web_db, Transaction(**{**defaults, **kwargs}))
        assert txn.id is not None
        return txn.id

    # 1 — needs a category, priced fine.
    _txn(
        occurred_at=_ago(10),
        amount=Decimal("-365.00"),
        description="BODEGA ZULIA",
        source_ref="cat-only",
    )
    # 2 — needs_review=1 in the database, and prices fine anyway. The 25
    #     live rows in this state must produce no rate item (K2).
    _txn(
        occurred_at=_ago(3),
        amount=Decimal("-100.00"),
        description="STALE FLAG",
        category_id=groceries.id,
        source_ref="stale-flag",
        needs_review=True,
    )
    # 3 — categorised, but no rate within 14 days: priced roughly.
    _txn(
        occurred_at=_ago(400),
        amount=Decimal("-730.00"),
        description="ANCIENT ROW",
        category_id=groceries.id,
        source_ref="rough-only",
    )
    # 4 — both problems, one item (A2): the category is what blocks.
    _txn(
        occurred_at=_ago(300),
        amount=Decimal("-1460.00"),
        description="ANCIENT UNCATEGORISED",
        source_ref="cat-and-rough",
    )
    # 5 — native USDT, no category: a category item that can never be a
    #     rate item, whatever the rates table holds.
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=binance.id,
            occurred_at=_ago(5),
            kind=TransactionKind.INCOME,
            amount=Decimal("100.00"),
            currency="USDT",
            description="Earn reward payout",
            source="binance",
            source_ref="usdt-cat",
        ),
    )
    # 6 + 7 — a pair proposal: bank deposit and its P2P sell, both already
    #     categorised so they stay out of the category group.
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=provincial.id,
            occurred_at=_ago(1),
            kind=TransactionKind.INCOME,
            amount=Decimal("36500.00"),
            currency="VES",
            description="ABONO P2P sell",
            category_id=groceries.id,
            source="provincial",
            source_ref="bank-deposit-1",
        ),
    )
    transactions_repo.insert(
        web_db,
        Transaction(
            account_id=binance.id,
            occurred_at=_ago(1),
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-1000.00"),
            currency="USDT",
            description="P2P sell USDT",
            category_id=groceries.id,
            user_rate=Decimal("36.50"),
            source="binance",
            source_ref="binance-sell-1",
        ),
    )
    return web_db


def _by_ref(queue, source_ref: str):
    for item in queue.items:
        if item.txn_card is not None and item.txn_card.source_ref == source_ref:
            return item
    return None


# ---------------------------------------------------------------------------
# Buckets and membership.
# ---------------------------------------------------------------------------


def test_bucket_order_is_category_pairs_roughly(queue_db: sqlite3.Connection) -> None:
    queue = build_queue(queue_db)

    buckets = {
        "cat-only": 0,
        "cat-and-rough": 0,
        "rough-only": 2,
        "usdt-cat": 0,
    }
    for ref, bucket in buckets.items():
        item = _by_ref(queue, ref)
        assert item is not None, ref
        assert item.bucket == bucket, ref

    pair = next(i for i in queue.items if i.type is TriageType.PAIR)
    assert pair.bucket == 1


def test_a_stale_needs_review_flag_produces_no_rate_item(
    queue_db: sqlite3.Connection,
) -> None:
    """K2 — membership is the projection's answer, not the column's.

    And nothing is written to get there: the flag stays exactly as the
    ledger has it.
    """
    queue = build_queue(queue_db)

    assert _by_ref(queue, "stale-flag") is None
    stored = queue_db.execute(
        "SELECT needs_review FROM transactions WHERE source_ref = 'stale-flag'"
    ).fetchone()
    assert stored["needs_review"] == 1


def test_an_unpriceable_row_is_a_rate_item(queue_db: sqlite3.Connection) -> None:
    """The other half of K2: no flag, no rate — still a rate item."""
    queue_db.execute("DELETE FROM rates")

    item = _by_ref(build_queue(queue_db), "stale-flag")

    assert item is not None
    assert item.needs.rate is True
    assert item.bucket == 2
    assert item.txn_card.amount_usd is None


def test_one_row_two_problems_is_one_item(queue_db: sqlite3.Connection) -> None:
    """A2 — one item, two badges, and the blocking problem picks the bucket."""
    item = _by_ref(build_queue(queue_db), "cat-and-rough")

    assert item is not None
    assert (item.needs.cat, item.needs.rate, item.needs.pair) == (True, True, False)
    assert item.txn_issue_badges == ["category", "rate"]
    assert item.bucket == 0
    assert item.type is TriageType.CATEGORY


def test_a_native_row_is_never_a_rate_item(queue_db: sqlite3.Connection) -> None:
    item = _by_ref(build_queue(queue_db), "usdt-cat")

    assert item is not None
    assert item.needs.rate is False
    assert item.txn_card.approximate is False
    assert item.txn_card.rate_source == "native_usd"


def test_ordering_is_bucket_then_age_then_id(queue_db: sqlite3.Connection) -> None:
    """A3/A4 — oldest first inside a bucket, item_id as the real tiebreak."""
    items = build_queue(queue_db).items

    assert [it.bucket for it in items] == sorted(it.bucket for it in items)
    bucket_0 = [it for it in items if it.bucket == 0]
    assert [it.sort_key for it in bucket_0] == sorted(it.sort_key for it in bucket_0)
    assert items == sorted(items, key=lambda it: (it.bucket, it.sort_key, it.item_id))


# ---------------------------------------------------------------------------
# Counts.
# ---------------------------------------------------------------------------


def test_counts_name_the_three_groups(queue_db: sqlite3.Connection) -> None:
    queue = build_queue(queue_db)

    assert queue.category_count == 3  # cat-only, cat-and-rough, usdt-cat
    assert queue.approximate_count == 1  # rough-only
    assert queue.pair_count == 1
    assert queue.parked_count == 0


def test_blocking_count_excludes_approximate_rows(
    queue_db: sqlite3.Connection,
) -> None:
    """A8 — the header answers with what actually needs the owner."""
    queue = build_queue(queue_db)

    assert queue.blocking_count == queue.category_count + queue.pair_count
    assert queue.blocking_count == 4
    assert queue.total == 5


def test_blocking_hits_zero_while_approximate_rows_remain(
    queue_db: sqlite3.Connection,
) -> None:
    """"Nothing needs you" must be sayable with rate items still queued."""
    groceries = categories_repo.get_by_name(
        queue_db, TransactionKind.EXPENSE, "Groceries"
    )
    queue_db.execute(
        "UPDATE transactions SET category_id = ? WHERE category_id IS NULL",
        (groceries.id,),
    )
    queue_db.execute("UPDATE transactions SET transfer_id = 'done' ")

    queue = build_queue(queue_db)

    assert queue.blocking_count == 0
    assert queue.approximate_count >= 1


# ---------------------------------------------------------------------------
# Per-item data (K1).
# ---------------------------------------------------------------------------


def test_item_carries_its_account(queue_db: sqlite3.Connection) -> None:
    item = _by_ref(build_queue(queue_db), "cat-only")

    assert item.account is not None
    assert item.account.name == "Provincial"
    assert item.account.detail == "0108"
    assert item.account.kind == "bank"
    assert item.account.currency == "VES"


def test_item_carries_money_and_provenance(queue_db: sqlite3.Connection) -> None:
    item = _by_ref(build_queue(queue_db), "rough-only")
    card = item.txn_card

    assert card.currency == "VES"
    assert card.amount_native == Decimal("-730.00")
    assert card.amount_usd == Decimal("-730.00") / Decimal("36.50")
    assert card.rate == Decimal("36.50")
    assert card.rate_source == "binance_p2p_median_nearest"
    assert card.approximate is True
    assert card.is_bcv_fallback is False
    assert card.kind == "expense"
    assert card.source_ref == "rough-only"


def test_an_approximate_item_says_how_far_off_the_rate_is(
    queue_db: sqlite3.Connection,
) -> None:
    """The *Priced roughly* group's per-row line — "P2P median, 400 days later"."""
    item = _by_ref(build_queue(queue_db), "rough-only")

    assert item.rough is not None
    assert item.rough.startswith("P2P median, ")
    assert item.rough.endswith(" days later")


def test_a_priced_item_has_no_rough_label(queue_db: sqlite3.Connection) -> None:
    assert _by_ref(build_queue(queue_db), "cat-only").rough is None


def test_item_carries_a_cleaned_merchant_when_there_is_one(
    queue_db: sqlite3.Connection,
) -> None:
    """A name gets title-cased; a bank code is left alone (see NOTES.md)."""
    queue = build_queue(queue_db)

    assert _by_ref(queue, "cat-only").merchant == "Bodega Zulia"

    queue_db.execute(
        "UPDATE transactions SET description = 'CAR.DRV0013196230' "
        "WHERE source_ref = 'cat-only'"
    )
    assert _by_ref(build_queue(queue_db), "cat-only").merchant is None


# ---------------------------------------------------------------------------
# Guesses (K6, G7).
# ---------------------------------------------------------------------------


def _add_rule(
    conn: sqlite3.Connection,
    pattern: str,
    category_id: int,
    *,
    priority: int = 5,
    active: int = 1,
    min_amount: str | None = None,
) -> int:
    cur = conn.execute(
        "INSERT INTO category_rules (pattern, category_id, priority, active, min_amount)"
        " VALUES (?, ?, ?, ?, ?)",
        (pattern, category_id, priority, active, min_amount),
    )
    return int(cur.lastrowid)


def test_guess_comes_from_the_real_rules_engine(
    queue_db: sqlite3.Connection,
) -> None:
    groceries = categories_repo.get_by_name(
        queue_db, TransactionKind.EXPENSE, "Groceries"
    )
    rule_id = _add_rule(queue_db, "bodega|zulia", groceries.id)

    guess = _by_ref(build_queue(queue_db), "cat-only").guess

    assert guess is not None
    assert guess.category_id == groceries.id
    assert guess.label == "Groceries"
    assert guess.rule_id == rule_id
    assert guess.pattern == "bodega|zulia"
    assert guess.times is None


def test_guess_honours_active_and_amount_bounds(
    queue_db: sqlite3.Connection,
) -> None:
    groceries = categories_repo.get_by_name(
        queue_db, TransactionKind.EXPENSE, "Groceries"
    )
    _add_rule(queue_db, "bodega", groceries.id, active=0)
    # abs(amount) is 365; a floor of 1000 must not match (migration 017).
    _add_rule(queue_db, "zulia", groceries.id, min_amount="1000")

    assert _by_ref(build_queue(queue_db), "cat-only").guess is None


def test_guess_respects_rule_priority(queue_db: sqlite3.Connection) -> None:
    groceries = categories_repo.get_by_name(
        queue_db, TransactionKind.EXPENSE, "Groceries"
    )
    leisure = categories_repo.get_by_name(
        queue_db, TransactionKind.EXPENSE, "Going Out"
    )
    assert leisure is not None
    _add_rule(queue_db, "zulia", leisure.id, priority=40)
    winner = _add_rule(queue_db, "bodega", groceries.id, priority=1)

    guess = _by_ref(build_queue(queue_db), "cat-only").guess

    assert guess.rule_id == winner
    assert guess.category_id == groceries.id


def test_learned_guess_when_the_same_row_was_sorted_three_times(
    queue_db: sqlite3.Connection,
) -> None:
    """G7's second tooltip: "You sorted this here 6 times"."""
    groceries = categories_repo.get_by_name(
        queue_db, TransactionKind.EXPENSE, "Groceries"
    )
    account_id = queue_db.execute(
        "SELECT account_id FROM transactions WHERE source_ref = 'cat-only'"
    ).fetchone()["account_id"]
    for n in range(3):
        transactions_repo.insert(
            queue_db,
            Transaction(
                account_id=account_id,
                occurred_at=_ago(20 + n),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-50.00"),
                currency="VES",
                description="BODEGA ZULIA",
                category_id=groceries.id,
                source="provincial",
                source_ref=f"prior-{n}",
            ),
        )

    guess = _by_ref(build_queue(queue_db), "cat-only").guess

    assert guess is not None
    assert guess.category_id == groceries.id
    assert guess.times == 3
    assert guess.rule_id is None


def test_two_prior_sortings_are_not_enough(queue_db: sqlite3.Connection) -> None:
    groceries = categories_repo.get_by_name(
        queue_db, TransactionKind.EXPENSE, "Groceries"
    )
    account_id = queue_db.execute(
        "SELECT account_id FROM transactions WHERE source_ref = 'cat-only'"
    ).fetchone()["account_id"]
    for n in range(2):
        transactions_repo.insert(
            queue_db,
            Transaction(
                account_id=account_id,
                occurred_at=_ago(20 + n),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-50.00"),
                currency="VES",
                description="BODEGA ZULIA",
                category_id=groceries.id,
                source="provincial",
                source_ref=f"prior-{n}",
            ),
        )

    assert _by_ref(build_queue(queue_db), "cat-only").guess is None


def test_no_guess_when_nothing_matches(queue_db: sqlite3.Connection) -> None:
    assert _by_ref(build_queue(queue_db), "cat-and-rough").guess is None


def test_a_guess_never_contradicts_the_row_kind(
    queue_db: sqlite3.Connection,
) -> None:
    """An expense category on an income row is refused by the save path.

    Offering it as a one-click guess would produce a 422 on the click.
    """
    groceries = categories_repo.get_by_name(
        queue_db, TransactionKind.EXPENSE, "Groceries"
    )
    _add_rule(queue_db, "earn reward", groceries.id, priority=1)

    assert _by_ref(build_queue(queue_db), "usdt-cat").guess is None


def test_a_guess_is_only_ever_a_category_the_owner_could_pick(
    queue_db: sqlite3.Connection,
) -> None:
    """Criterion E2, applied to the chip as well as the picker.

    Accepting a guess resolves the row in one click, so it must land
    somewhere the owner could have chosen by hand. ``auto_only``
    categories (migration 021) are system-written — a transfer is
    confirmed as a *pair*, not declared by tagging one leg — and a
    "you sorted this here 3 times" count on one of them is really a count
    of what ``category_rules`` wrote, which is the Fees trap the picker
    notes describe.
    """
    auto_only = queue_db.execute(
        "SELECT id FROM categories WHERE auto_only = 1 AND active = 1 LIMIT 1"
    ).fetchone()
    assert auto_only is not None
    _add_rule(queue_db, "bodega", int(auto_only["id"]), priority=1)

    account_id = queue_db.execute(
        "SELECT account_id FROM transactions WHERE source_ref = 'cat-only'"
    ).fetchone()["account_id"]
    for n in range(3):
        transactions_repo.insert(
            queue_db,
            Transaction(
                account_id=account_id,
                occurred_at=_ago(20 + n),
                kind=TransactionKind.EXPENSE,
                amount=Decimal("-50.00"),
                currency="VES",
                description="BODEGA ZULIA",
                category_id=int(auto_only["id"]),
                source="provincial",
                source_ref=f"auto-{n}",
            ),
        )

    assert _by_ref(build_queue(queue_db), "cat-only").guess is None


def test_only_category_items_carry_a_guess(queue_db: sqlite3.Connection) -> None:
    assert _by_ref(build_queue(queue_db), "rough-only").guess is None


# ---------------------------------------------------------------------------
# Pair items (H1).
# ---------------------------------------------------------------------------


def _pair(queue):
    return next(i for i in queue.items if i.type is TriageType.PAIR)


def test_pair_item_carries_the_numbers_behind_the_confidence(
    queue_db: sqlite3.Connection,
) -> None:
    proposal = _pair(build_queue(queue_db)).pair_proposal

    assert proposal.days_apart == 0
    assert proposal.drift_pct == Decimal("0")
    assert proposal.implied_rate == Decimal("36.50")
    assert 0 < proposal.confidence <= 1
    assert proposal.refused is False
    assert proposal.refuse_reason is None


def test_pair_item_needs_a_pair_and_nothing_else(
    queue_db: sqlite3.Connection,
) -> None:
    item = _pair(build_queue(queue_db))

    assert (item.needs.cat, item.needs.rate, item.needs.pair) == (False, False, True)


def _leg(occurred_at: datetime, amount: str, currency: str, **kw) -> Transaction:
    return Transaction(
        account_id=1,
        occurred_at=occurred_at,
        kind=TransactionKind.INCOME,
        amount=Decimal(amount),
        currency=currency,
        source="test",
        source_ref="leg",
        **kw,
    )


def test_assess_refuses_a_pair_too_far_apart() -> None:
    """H3 — the bounds live in one place, and both readers use it.

    The automatic strategy proposes inside ±1 day, so a refusal never
    reaches the queue from there; the manual picker has a wider window and
    the modal must be able to say *why* the button is dead before the click
    422s.
    """
    from finances.web.services.triage import MANUAL_PAIR_MAX_DAYS, assess_pair

    gap = MANUAL_PAIR_MAX_DAYS + 1
    verdict = assess_pair(
        _leg(_ago(gap), "36500.00", "VES"),
        _leg(_ago(0), "-1000.00", "USDT", user_rate=Decimal("36.50")),
    )

    assert verdict.days_apart == gap
    assert verdict.refused is True
    assert str(MANUAL_PAIR_MAX_DAYS) in verdict.refuse_reason


def test_assess_refuses_a_pair_that_drifts_too_far() -> None:
    from finances.web.services.triage import MANUAL_PAIR_MAX_DRIFT, assess_pair

    verdict = assess_pair(
        _leg(_ago(0), "30000.00", "VES"),
        _leg(_ago(0), "-1000.00", "USDT", user_rate=Decimal("36.50")),
    )

    assert verdict.drift_pct > MANUAL_PAIR_MAX_DRIFT * 100
    assert verdict.refused is True
    assert "%" in verdict.refuse_reason


def test_assess_scores_an_unrated_sell_on_date_alone() -> None:
    """A legacy sell with no user_rate is exactly what the manual path is for."""
    from finances.web.services.triage import assess_pair

    verdict = assess_pair(
        _leg(_ago(0), "36500.00", "VES"),
        _leg(_ago(0), "-1000.00", "USDT"),
    )

    assert verdict.drift_pct is None
    assert verdict.implied_rate == Decimal("36.50")
    assert verdict.refused is False


def test_confirm_pair_and_the_payload_refuse_together(
    queue_db: sqlite3.Connection,
) -> None:
    """One implementation: what the payload calls refused, the write raises on."""
    from finances.web.services.triage import assess_pair, confirm_pair

    deposit = _leg(_ago(20), "36500.00", "VES")
    sell = _leg(_ago(0), "-1000.00", "USDT", user_rate=Decimal("36.50"))
    assert assess_pair(deposit, sell).refused is True

    deposit_id = queue_db.execute(
        "SELECT id FROM transactions WHERE source_ref = 'bank-deposit-1'"
    ).fetchone()["id"]
    sell_id = queue_db.execute(
        "SELECT id FROM transactions WHERE source_ref = 'binance-sell-1'"
    ).fetchone()["id"]
    queue_db.execute(
        "UPDATE transactions SET occurred_at = ? WHERE id = ?",
        (_ago(30).isoformat(), deposit_id),
    )

    with pytest.raises(ValueError, match="refusing to pair"):
        confirm_pair(queue_db, deposit_id=deposit_id, sell_id=sell_id)


# ---------------------------------------------------------------------------
# The advance contract is untouched (MEMORY: triage advance contract).
# ---------------------------------------------------------------------------


def test_advance_still_holds_position(queue_db: sqlite3.Connection) -> None:
    before = build_queue(queue_db).items
    resolved = before[0]

    after = [it for it in before if it.item_id != resolved.item_id]

    assert next_item_after(before, after, resolved.item_id) is after[0]


def test_neighbours_still_positional(queue_db: sqlite3.Connection) -> None:
    items = build_queue(queue_db).items

    prev_item, next_item = neighbours_of(items, items[1].item_id)

    assert prev_item is items[0]
    assert next_item is items[2]


# ---------------------------------------------------------------------------
# The JSON boundary (rule-009, K1).
# ---------------------------------------------------------------------------


def test_api_triage_serialises_the_new_payload(
    queue_db: sqlite3.Connection, web_client_factory
) -> None:
    client: TestClient = web_client_factory()

    body = client.get("/api/triage").json()

    assert body["blocking_count"] == 4
    assert body["category_count"] == 3
    assert body["approximate_count"] == 1
    assert body["pair_count"] == 1
    assert body["parked_count"] == 0

    item = next(
        i
        for i in body["items"]
        if i["txn_card"] and i["txn_card"]["source_ref"] == "rough-only"
    )
    assert item["bucket"] == 2
    assert item["needs"] == {"cat": False, "rate": True, "pair": False}
    assert item["txn_card"]["approximate"] is True
    assert item["account"]["kind"] == "bank"
