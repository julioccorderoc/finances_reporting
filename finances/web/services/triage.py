"""Triage queue builder + pair-confirm wrapper (EPIC-025 / Phase 4 / ADR-012).

Per rule-012, this module reuses existing domain primitives:

* RATE / CATEGORY items are surfaced by reading ``transactions`` rows
  via the canonical ``transactions`` repo + projecting through the
  Phase 2/3 ``_project_card`` helper. We do not re-implement
  rate-resolver logic here; the projection delegates to
  :func:`finances.domain.rates.resolve`.
* PAIR items wrap :class:`finances.domain.transfers.BankAnchoredP2pPairing`
  proposals; we don't re-implement matching logic.
* Guesses come from :func:`finances.domain.categorization.suggest` — the
  same engine ingest runs — so a guess honours regex scope, priority,
  ``active`` and the ADR-017 amount bounds without a second reading of the
  rules table.
* :func:`confirm_pair` delegates to :func:`finances.domain.transfers.create_transfer`
  (mode 3 — both anchors). The web layer never executes its own
  INSERT/UPDATE on ``transactions``.

**Payload v2** (`design_handoff_triage/`, criteria A1-A4, A8, H1, K1-K6).
Two things changed with the redesign:

*Buckets follow the queue's own group order* — 0 needs a category,
1 pair proposals, 2 priced roughly. An approximate rate never blocks a
sitting (A8/D6), so it walks last; ``blocking_count`` is category + pair.
Criterion A3 lists category/rate/pair instead, and the README's order
wins — logged in `design_handoff_triage/NOTES.md`.

*A rate item is computed, never read off the stored flag* (K2).
``transactions.needs_review`` is what a write derives and what
``reports/needs_review`` reads; it goes stale the moment a rate lands, and
is wrong on 25 live rows. Membership here is the projection's answer:
approximate (ADR-021 ``*_nearest``) or unpriceable. Nothing is written to
make that true.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Container, Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, computed_field

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import categories as categories_repo
from finances.domain import categorization, money, realized_rates
from finances.domain import rates as rates_engine
from finances.domain import transfers as transfers_domain
from finances.domain.models import Transaction, TransactionKind
from finances.domain.transfers import BankAnchoredP2pPairing, create_transfer
from finances.format import clean_merchant
from finances.web.services.transactions_query import (
    TXN_QUERY_BASE,
    TransactionCard,
    _project_card,
    _row_to_transaction,
)
from finances.web.services.transactions_write import category_fits


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class TriageType(StrEnum):
    """Discriminator for unified-queue items."""

    RATE = "rate"
    CATEGORY = "category"
    PAIR = "pair"


class PairAssessment(BaseModel):
    """The arithmetic behind "96% confident", and whether it is confirmable.

    One implementation, two readers: the payload renders it (criterion H1,
    the metadata row under the two legs) and
    :func:`_reject_implausible_pair` raises on it. They cannot disagree
    about what is refusable, which is the point — the modal must be able
    to grey the button out *before* the click, and say why.
    """

    model_config = ConfigDict(extra="forbid")

    days_apart: int
    drift_pct: Decimal | None
    """Percentage points — ``Decimal("12.4")`` means 12.4% — not a ratio.

    ``None`` when the sell carries no ``user_rate``: there is nothing to
    score the amounts against, and refusing such a pair outright would
    block exactly the legacy rows the manual path exists to clear."""
    implied_rate: Decimal | None
    """Quote units per dollar implied by the two legs (VES / USDT).

    ``None`` for a same-currency pair, where the ratio would be ~1 and
    mean nothing."""
    refused: bool
    refuse_reason: str | None = None


class PairProposal(BaseModel):
    """One BankAnchoredP2pPairing proposal projected for the UI."""

    model_config = ConfigDict(extra="forbid")

    proposal_id: str
    deposit: TransactionCard
    sell: TransactionCard
    confidence: float
    days_apart: int
    drift_pct: Decimal | None
    implied_rate: Decimal | None
    refused: bool
    refuse_reason: str | None = None
    details: dict[str, Any]


class TriageNeeds(BaseModel):
    """What this row is still missing. More than one may be true (A2)."""

    model_config = ConfigDict(extra="forbid")

    cat: bool = False
    rate: bool = False
    pair: bool = False


class TriageAccount(BaseModel):
    """The row's account, as the queue renders it.

    ``detail`` is ``accounts.institution``. The design shows a masked
    account number beside the name; the schema has no such column, and
    adding one is a migration — see `design_handoff_triage/NOTES.md`.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    detail: str | None
    kind: str
    currency: str


class TriageGuess(BaseModel):
    """A proposed category, and the evidence for it (criteria K6, G7).

    Exactly one of the two shapes is populated:

    * ``rule_id`` + ``pattern`` — the categorization engine matched a rule,
      honouring its regex scope, priority, ``active`` flag and amount
      bounds. The chip's tooltip cites both.
    * ``times`` — no rule matched, but this exact bank string has been
      sorted into one category at least
      :data:`LEARNED_GUESS_MIN` times on this account before.

    A guess whose kind contradicts the row's is never offered: the save
    path refuses it (``category_fits``), and the accept-the-guess chip
    writes without opening the modal, so an offered-then-refused guess is
    a 422 waiting for a click.
    """

    model_config = ConfigDict(extra="forbid")

    category_id: int
    label: str
    rule_id: int | None = None
    pattern: str | None = None
    times: int | None = None


class TriageItem(BaseModel):
    """One item in the unified queue.

    Type-discriminated payload: only ``txn_card`` (for RATE/CATEGORY) or
    ``pair_proposal`` (for PAIR) is populated per item. Everything the
    queue renders is here — the UI computes nothing from guesses (K1).
    """

    model_config = ConfigDict(extra="forbid")

    item_id: str
    type: TriageType
    sort_key: datetime
    bucket: int
    needs: TriageNeeds = Field(default_factory=TriageNeeds)
    txn_card: TransactionCard | None = None
    txn_issue_badges: list[str] = Field(default_factory=list)
    account: TriageAccount | None = None
    merchant: str | None = None
    """Cleaned name, or ``None`` when the raw string is a bank reference.

    See :func:`finances.format.clean_merchant`: a typographic cleanup, not
    a merchant identity."""
    guess: TriageGuess | None = None
    rough: str | None = None
    """How far off an approximate rate is — ``"BCV, 3 days later"``.

    ``None`` for a row priced inside a tier's window, and for one that
    cannot be priced at all: "roughly" and "not at all" are different
    states and the design renders them differently."""
    pair_proposal: PairProposal | None = None


class TriageQueue(BaseModel):
    """Full unified-queue payload."""

    model_config = ConfigDict(extra="forbid")

    items: list[TriageItem]
    counts: dict[TriageType, int]
    bucket_counts: dict[int, int]
    integrity_warnings: list[str]
    parked_items: list[TriageItem]
    parked_count: int

    @computed_field
    @property
    def category_count(self) -> int:
        """Rows needing a category — the first group, and bucket 0."""
        return self.counts.get(TriageType.CATEGORY, 0)

    @computed_field
    @property
    def pair_count(self) -> int:
        """Proposed transfer pairs — the second group, and bucket 1."""
        return self.counts.get(TriageType.PAIR, 0)

    @computed_field
    @property
    def approximate_count(self) -> int:
        """Rows priced roughly — the third group, and bucket 2."""
        return self.counts.get(TriageType.RATE, 0)

    @computed_field
    @property
    def blocking_count(self) -> int:
        """What the header answers with: "N rows need you" (criterion A8).

        Approximate rows are deliberately excluded. They are already
        priced, the figure is usable, and the header must be able to read
        "Nothing needs you" while the *Priced roughly* group still has
        rows in it — otherwise the queue never clears and the sitting
        never ends.

        Derived rather than stored so it cannot drift from ``counts``.
        """
        return self.category_count + self.pair_count

    @property
    def total(self) -> int:
        """Size of the UNFILTERED live queue.

        ``items`` is the filtered list, so it cannot answer "how big is
        the queue" once a chip is active — that is what made the "All"
        chip shrink to the filtered count. ``counts`` is per-type on the
        unfiltered set, so its sum is the honest total.

        Parked rows are excluded, same as ``items`` and ``counts``:
        parking is a durable "not now" and the parked group carries its
        own ``parked_count``.
        """
        return sum(self.counts.values())


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _fetch_txn_with_labels(conn: sqlite3.Connection, txn_id: int):
    return conn.execute(
        TXN_QUERY_BASE + " WHERE t.id = ?", (txn_id,)
    ).fetchone()


def _project_from_row(conn: sqlite3.Connection, row) -> TransactionCard:
    txn = _row_to_transaction(row)
    return _project_card(
        conn,
        txn,
        account_name=row["account_name"] or "",
        category_name=row["category_name"],
    )


# ---------------------------------------------------------------------------
# build_queue
# ---------------------------------------------------------------------------


# How many times the same bank string must already have been sorted into
# one category before the queue offers it as a guess. Three is the point
# where "you always file this here" stops being a coincidence; below it the
# chip would be proposing the owner's last stray click back to them.
LEARNED_GUESS_MIN = 3

# Short, human tier names for the ``rough`` label. Keyed by the *base*
# source, with ADR-021's suffix stripped first.
_TIER_LABELS: dict[str, str] = {
    rates_engine.USER_RATE_SOURCE: "Yours",
    rates_engine.REALIZED_SOURCE: "Realized",
    rates_engine.BINANCE_P2P_SOURCE: "P2P median",
    rates_engine.BCV_SOURCE: "BCV",
}

_LEARNED_GUESS_SQL = """
    SELECT t.category_id AS category_id, c.name AS name, c.kind AS kind,
           COUNT(*) AS n
    FROM transactions t
    JOIN categories c ON c.id = t.category_id
    WHERE t.description = ?
      AND t.account_id = ?
      AND t.kind = ?
      AND t.category_id IS NOT NULL
    GROUP BY t.category_id
    ORDER BY n DESC, t.category_id ASC
    LIMIT 1
"""


def _rough_label(resolution: rates_engine.RateResolution | None) -> str | None:
    """"P2P median, 21 days later" — the *Priced roughly* group's per-row line.

    Direction is the rate's, relative to the transaction: a positive
    ``age_days`` means the rate predates the row (it was carried forward
    too far), a negative one means it was published afterwards, which is
    hindsight. The design says both out loud, so neither is flattened into
    a bare "approximate".
    """
    if resolution is None or not resolution.approximate:
        return None
    if resolution.age_days is None:  # pragma: no cover - defensive
        return None
    base = resolution.source.removesuffix(rates_engine.NEAREST_SUFFIX)
    label = _TIER_LABELS.get(base, base)
    days = abs(resolution.age_days)
    unit = "day" if days == 1 else "days"
    direction = "later" if resolution.age_days < 0 else "earlier"
    return f"{label}, {days} {unit} {direction}"


def _accounts_by_id(conn: sqlite3.Connection) -> dict[int, TriageAccount]:
    """Every account, once per queue build rather than once per row.

    There are six of them and a sitting can carry hundreds of rows; the
    join would be per-row for a table that fits in a breath.
    """
    return {
        account.id: TriageAccount(
            name=account.name,
            detail=account.institution,
            kind=account.kind.value,
            currency=account.currency,
        )
        for account in accounts_repo.list_all(conn, include_inactive=True)
        if account.id is not None
    }


def _pricing_state(
    conn: sqlite3.Connection,
    cache: dict[tuple[str, date], rates_engine.RateResolution],
    txn: Transaction,
) -> rates_engine.RateResolution:
    """Ask the resolver how this row prices, memoised by (currency, day).

    Every row reaching here has ``user_rate IS NULL`` and a non-native
    currency, so its resolution is a pure function of those two values —
    which is what makes the memo safe, and what turns 748 live bolívar
    rows into a few hundred lookups.
    """
    key = (txn.currency, txn.occurred_at.date())
    resolution = cache.get(key)
    if resolution is None:
        resolution = rates_engine.resolve_detail(conn, txn)
        cache[key] = resolution
    return resolution


def _guess_for(
    conn: sqlite3.Connection,
    txn: Transaction,
    pickable: dict[int, str],
) -> TriageGuess | None:
    """Propose a category for ``txn``, or ``None`` (criteria K6, G7).

    Tier one is the real engine — :func:`categorization.suggest`, the same
    call ingest makes — so scope, priority, ``active`` and the ADR-017
    amount bounds are honoured by construction rather than by a second
    reading of the rules table (rule-006 forbids the second reading, and
    G8 forbids this surface writing rules at all).

    Tier two is the owner's own history: the same bank string, on the same
    account, filed the same way at least :data:`LEARNED_GUESS_MIN` times.
    Exact string equality on purpose — a fuzzy match here would propose a
    category from a merchant the owner never linked.

    Both tiers are bounded by ``pickable`` — ``categories_repo.list_pickable``,
    the one definition of "a category a human may choose" (migration 021).
    Accepting a guess resolves the row in one click, so it must land
    somewhere the owner could have chosen by hand; and a "sorted here N
    times" count on an ``auto_only`` category is really a count of what
    ``category_rules`` wrote, which is the trap that keeps ``Fees`` off
    the chips. It also keeps a transfer being confirmed as a *pair*
    rather than declared by tagging one leg.

    A guess whose kind the save path would refuse is likewise never
    offered (:func:`category_fits`): the chip writes without opening the
    modal, so an offered-then-refused guess is a 422 waiting for a click.
    """
    if not txn.description:
        return None

    match = categorization.suggest(
        conn,
        categorization.CategorizationRequest(
            description=txn.description,
            source=txn.source,
            account_id=txn.account_id,
            amount=txn.amount,
        ),
    )
    if match is not None and match.category_id in pickable:
        category = categories_repo.get_by_id(conn, match.category_id)
        if category is not None and category_fits(txn.kind, category.kind):
            return TriageGuess(
                category_id=match.category_id,
                label=category.name,
                rule_id=match.rule_id,
                pattern=match.pattern,
            )

    row = conn.execute(
        _LEARNED_GUESS_SQL,
        (txn.description, txn.account_id, txn.kind.value),
    ).fetchone()
    if row is None or int(row["n"]) < LEARNED_GUESS_MIN:
        return None
    category_id = int(row["category_id"])
    if category_id not in pickable:
        return None
    if not category_fits(txn.kind, TransactionKind(row["kind"])):
        return None
    return TriageGuess(
        category_id=category_id,
        label=row["name"],
        times=int(row["n"]),
    )


def _bucket_for(needs: TriageNeeds) -> int:
    """0 needs a category, 1 a pair, 2 priced roughly.

    The redesign's group order, and the modal's walk order with it. A row
    missing both a category and a rate sits in bucket 0: the category is
    what blocks the sitting, the rate is not (A8/D6).

    Criterion A3 lists category/rate/pair instead; the README's group
    order wins, and the deviation is logged in
    `design_handoff_triage/NOTES.md`.
    """
    if needs.cat:
        return 0
    if needs.pair:
        return 1
    return 2


def _collect_txn_items(
    conn: sqlite3.Connection, *, parked: bool = False
) -> list[TriageItem]:
    """Build one item per transaction that still needs a decision.

    Two surfaces, merged by transaction id so a row missing both a
    category and a trustworthy rate is ONE item with two badges (A2):

    * **category** — ``category_id IS NULL``, income or expense only.
      Transfers and adjustments are never asked about (rule-006,
      ADR-018), so surfacing them would be noise.
    * **rate** — the projection says the row is priced approximately
      (ADR-021 ``*_nearest``) or cannot be priced at all. Candidates are
      narrowed in SQL to the only rows that *can* be either: a native-USD
      row is always 1:1, and a row carrying ``user_rate`` is priced by
      that. Everything else is asked of the resolver.

      This is criterion K2, and it is a change of question rather than a
      change of data: ``needs_review`` stays exactly as stored, including
      on the 25 live rows that carry it while pricing perfectly well.

    ``parked`` selects the other side of the same predicates. A parked row
    keeps its live badges — parking defers a row, it does not resolve it —
    but leaves the queue entirely.
    """
    parked_flag = 1 if parked else 0

    cat_rows = conn.execute(
        TXN_QUERY_BASE
        + """
        WHERE t.category_id IS NULL
          AND t.kind NOT IN ('transfer', 'adjustment')
          AND t.parked = ?
        ORDER BY t.occurred_at, t.id
        """,
        (parked_flag,),
    ).fetchall()

    # The native-USD set is imported, never spelled out again
    # (tests/test_money_is_the_only_definition.py pins that).
    natives = sorted(money.NATIVE_USD_CURRENCIES)
    placeholders = ",".join("?" * len(natives))
    rate_candidates = conn.execute(
        TXN_QUERY_BASE
        + f"""
        WHERE t.parked = ?
          AND t.user_rate IS NULL
          AND t.currency NOT IN ({placeholders})
        ORDER BY t.occurred_at, t.id
        """,  # noqa: S608 - placeholders only, values are bound
        (parked_flag, *natives),
    ).fetchall()

    entries: dict[int, dict[str, Any]] = {}
    for row in cat_rows:
        entry = entries.setdefault(
            int(row["id"]), {"row": row, "cat": False, "rate": False}
        )
        entry["cat"] = True

    cache: dict[tuple[str, date], rates_engine.RateResolution] = {}
    for row in rate_candidates:
        resolution = _pricing_state(conn, cache, _row_to_transaction(row))
        if not resolution.approximate and resolution.rate is not None:
            continue
        entry = entries.setdefault(
            int(row["id"]), {"row": row, "cat": False, "rate": False}
        )
        entry["rate"] = True
        entry["resolution"] = resolution

    accounts = _accounts_by_id(conn)
    # ~21 rows, read once per build rather than per guess.
    pickable = {
        category.id: category.name
        for category in categories_repo.list_pickable(conn)
        if category.id is not None
    }

    items: list[TriageItem] = []
    for txn_id, entry in entries.items():
        row = entry["row"]
        needs = TriageNeeds(cat=entry["cat"], rate=entry["rate"], pair=False)
        txn = _row_to_transaction(row)
        card = _project_from_row(conn, row)
        badges = [
            badge
            for badge, present in (("category", needs.cat), ("rate", needs.rate))
            if present
        ]
        items.append(
            TriageItem(
                item_id=f"txn:{txn_id}",
                # The discriminator names the blocking problem, so the
                # filter chips and the buckets agree with each other.
                type=TriageType.CATEGORY if needs.cat else TriageType.RATE,
                sort_key=card.occurred_at,
                bucket=_bucket_for(needs),
                needs=needs,
                txn_card=card,
                txn_issue_badges=badges,
                account=accounts.get(txn.account_id),
                merchant=clean_merchant(card.description),
                # Only a row being asked for a category gets one; guessing
                # at a row that already has one is answering a question
                # nobody asked.
                guess=_guess_for(conn, txn, pickable) if needs.cat else None,
                rough=_rough_label(entry.get("resolution")),
                pair_proposal=None,
            )
        )

    items.sort(key=lambda it: (it.bucket, it.sort_key, it.item_id))
    return items


def _collect_parked_items(conn: sqlite3.Connection) -> list[TriageItem]:
    """The parked group: same predicates, other side of ``parked``.

    A separate list, never merged into ``items`` or into any count except
    ``parked_count``.
    """
    return _collect_txn_items(conn, parked=True)


def _collect_pair_items(conn: sqlite3.Connection) -> list[TriageItem]:
    """Run BankAnchoredP2pPairing and project proposals.

    The strategy decides *what* to propose; this adds the numbers the
    modal shows underneath — days apart, drift, implied rate — and whether
    the pair is confirmable at all, from the same assessment the write
    path raises on (criterion H1).
    """
    strategy = BankAnchoredP2pPairing(conn)
    proposals = strategy.match()

    accounts = _accounts_by_id(conn)
    items: list[TriageItem] = []
    for proposal in proposals:
        # Per finances/domain/transfers.py, the strategy returns
        # ``bank_transaction_id`` and ``binance_transaction_id`` keys.
        deposit_id = int(proposal.details["bank_transaction_id"])
        sell_id = int(proposal.details["binance_transaction_id"])

        deposit_row = _fetch_txn_with_labels(conn, deposit_id)
        sell_row = _fetch_txn_with_labels(conn, sell_id)
        if deposit_row is None or sell_row is None:
            # Defensive: strategy emitted ids that don't exist anymore.
            continue

        deposit_card = _project_from_row(conn, deposit_row)
        sell_card = _project_from_row(conn, sell_row)
        verdict = assess_pair(
            _row_to_transaction(deposit_row), _row_to_transaction(sell_row)
        )

        needs = TriageNeeds(pair=True)
        proposal_id = f"{deposit_id}:{sell_id}"
        items.append(
            TriageItem(
                item_id=f"pair:{proposal_id}",
                type=TriageType.PAIR,
                sort_key=deposit_card.occurred_at,
                bucket=_bucket_for(needs),
                needs=needs,
                txn_card=None,
                txn_issue_badges=[],
                account=accounts.get(_row_to_transaction(deposit_row).account_id),
                pair_proposal=PairProposal(
                    proposal_id=proposal_id,
                    deposit=deposit_card,
                    sell=sell_card,
                    confidence=proposal.confidence,
                    days_apart=verdict.days_apart,
                    drift_pct=verdict.drift_pct,
                    implied_rate=verdict.implied_rate,
                    refused=verdict.refused,
                    refuse_reason=verdict.refuse_reason,
                    details=dict(proposal.details),
                ),
            )
        )

    return items


def _build_integrity_warnings(conn: sqlite3.Connection) -> list[str]:
    """Surface unreconciled transfers as human-readable warnings.

    Read straight from :func:`finances.domain.transfers.find_unreconciled`
    so the wording stays under the canonical view. That query carries no
    ORDER BY (it is shared, ADR-002-owned SQL), so the row order it returns
    is not guaranteed stable between renders — we sort the rendered strings
    here, in this module only, so the banner's line order is deterministic.
    """
    rows = transfers_domain.find_unreconciled(conn)
    warnings: list[str] = []
    for row in rows:
        tid = row.get("transfer_id")
        leg_count = row.get("leg_count")
        if tid is None:
            warnings.append(
                f"{leg_count} transfer leg(s) carry NULL transfer_id "
                "(missing pairing)"
            )
        else:
            warnings.append(
                f"transfer_id={tid} has only {leg_count} leg(s) (expected 2)"
            )
    return sorted(warnings)


def build_queue(
    conn: sqlite3.Connection,
    *,
    type_filter: TriageType | None = None,
    dismissed: Container[str] = (),
) -> TriageQueue:
    """Assemble the unified triage queue.

    Order of operations:
      1) Collect txn-issue items (CATEGORY / RATE, merging duplicates,
         excluding parked rows — spec §5.3).
      2) Collect pair items (BankAnchoredP2pPairing.match).
      3) Sort all items by (bucket, sort_key, item_id) — group first, then
         oldest-first, with item_id as a mandatory tiebreak for the many
         rows sharing a timestamp.
      4) Compute counts + bucket_counts on the unfiltered set.
      5) Apply ``type_filter`` if provided.
      6) Build integrity warnings from unreconciled transfers.
      7) Collect parked items (Task 3) — a separate surface, never part of
         ``items``, ``counts``, ``bucket_counts``, or the type filter above.

    Buckets are 0 category, 1 pairs, 2 priced roughly — see
    :func:`_bucket_for`. They are server-assigned so the list and the modal
    cannot disagree about order (criterion K5), and the modal walks them in
    this order regardless of which groups the owner has collapsed.
    """
    txn_items = _collect_txn_items(conn)
    pair_items = _collect_pair_items(conn)
    # ``dismissed`` is what "Not a pair" leaves behind. The strategy is a
    # pure function of the ledger and would propose the same two rows on
    # the very next build, so a refusal that wrote nothing would be a
    # button that does nothing; the write path is deliberately not the
    # answer either, since declining a GUESS is not a fact about the
    # money. It is dropped before the counts so the header, the group and
    # the run all agree it is gone.
    all_items = [
        item
        for item in txn_items + pair_items
        if item.item_id not in dismissed
    ]
    # Difficulty first, then chronology, then a mandatory id tiebreak.
    # ADR-012 Amendment 2026-07-21. The item_id component is load-bearing:
    # 204 of 243 live items share a timestamp (Provincial CSV has no time
    # component), so without it the order inside a bucket is undefined.
    all_items.sort(key=lambda it: (it.bucket, it.sort_key, it.item_id))

    # Counts always reflect the UNFILTERED queue, so the chip badges stay
    # accurate when the user selects a single type.
    counts = {t: 0 for t in TriageType}
    for item in all_items:
        counts[item.type] += 1

    bucket_counts: dict[int, int] = {0: 0, 1: 0, 2: 0}
    for item in all_items:
        bucket_counts[item.bucket] += 1

    if type_filter is not None:
        filtered = [it for it in all_items if it.type == type_filter]
    else:
        filtered = list(all_items)

    warnings = _build_integrity_warnings(conn)
    parked_items = _collect_parked_items(conn)

    return TriageQueue(
        items=filtered,
        counts=counts,
        bucket_counts=bucket_counts,
        integrity_warnings=warnings,
        parked_items=parked_items,
        parked_count=len(parked_items),
    )


def count_blocking(
    conn: sqlite3.Connection, *, dismissed: Container[str] = ()
) -> int:
    """What ``build_queue(...).blocking_count`` would say, without the pricing.

    The rail badge asks this on every page load. ``build_queue`` prices
    every non-native row that lacks a ``user_rate`` to find the
    approximate ones — and approximate rows do not block (criterion A8),
    so the badge never needed that work. Categories are the same
    predicate ``_collect_txn_items`` uses, counted in SQL; pairs are the
    strategy's proposals minus this run's refusals, exactly as the queue
    drops them. ``tests/web/test_shell.py`` pins the two equal.
    """
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM transactions t
        WHERE t.category_id IS NULL
          AND t.kind NOT IN ('transfer', 'adjustment')
          AND t.parked = 0
        """
    ).fetchone()
    categories = int(row["c"]) if row is not None else 0
    pairs = sum(
        1 for item in _collect_pair_items(conn) if item.item_id not in dismissed
    )
    return categories + pairs


def _index_of(items: Sequence[TriageItem], item_id: str) -> int | None:
    return next(
        (n for n, item in enumerate(items) if item.item_id == item_id),
        None,
    )


def _first_surviving(
    before: Sequence[TriageItem],
    surviving: dict[str, TriageItem],
    index: int,
    *,
    step: int,
) -> TriageItem | None:
    """Scan ``before`` from ``index`` in ``step`` direction for a survivor.

    Shared by the two things that move around the queue: the post-write
    advance (which hunts past every row the write removed) and arrow
    navigation (which walks exactly one slot, because nothing was
    removed). Returns the ``surviving`` instance, never the ``before``
    snapshot, so callers render post-write state.
    """
    n = index + step
    while 0 <= n < len(before):
        item_id = before[n].item_id
        if item_id in surviving:
            return surviving[item_id]
        n += step
    return None


def neighbours_of(
    items: Sequence[TriageItem],
    item_id: str,
) -> tuple[TriageItem | None, TriageItem | None]:
    """Return ``(previous, next)`` — the items adjacent to ``item_id``.

    Positional navigation for the modal's arrows (ADR-012 Amendment
    2026-07-21). Both ends return ``None``, which the template renders as
    a disabled arrow rather than a dead one. An ``item_id`` that is not in
    ``items`` yields ``(None, None)``: the open modal is not part of this
    queue, so both arrows go dead instead of guessing.

    Navigation performs no write, so ``items`` is both the snapshot and
    the live queue — unlike :func:`next_item_after`, which reconciles two
    different queues and therefore keeps scanning past removed rows.
    """
    index = _index_of(items, item_id)
    if index is None:
        return (None, None)

    surviving = {it.item_id: it for it in items if it.item_id != item_id}
    return (
        _first_surviving(items, surviving, index, step=-1),
        _first_surviving(items, surviving, index, step=1),
    )


def next_item_after(
    before: Sequence[TriageItem],
    after: Sequence[TriageItem],
    resolved_id: str,
) -> TriageItem | None:
    """Pick the item that now occupies the resolved item's slot.

    Hold-position advance (ADR-012 Amendment 2026-07-26): the owner keeps
    working down the queue from where they were, instead of being thrown
    back to the top. ``before`` is the queue as the owner saw it,
    ``after`` the same queue rebuilt post-write; both must carry the same
    ``type_filter`` or the successor can be an item the active filter
    chip hides.

    Three rules, in order:

    * The resolved item is never the answer. A partial fix (saving a rate
      on a row that still lacks a category) leaves the row queued, and
      re-opening the modal the owner just dismissed reads as a failed
      save. The row keeps its place for a later pass.
    * Otherwise the answer is the nearest item that was BELOW the
      resolved one and is still queued.
    * Resolving the bottom row has nothing below it, so it steps up to
      the nearest surviving item above instead.

    Selection is by identity, never by index. One write can remove more
    than one row — confirming a pair promotes both legs to
    ``kind='transfer'``, evicting them from the CATEGORY surface, and
    legs (bucket 0/1) always sort above the pair (bucket 2). Carrying the
    ``before`` index into ``after`` would overshoot by one per evicted
    row and silently skip the proposals in between.

    The returned item is the ``after`` instance, so the modal renders
    post-write state rather than the snapshot.

    Returns ``None`` when nothing is left to advance to, or when
    ``resolved_id`` was not in ``before`` (the caller then closes the
    modal rather than guessing).
    """
    index = _index_of(before, resolved_id)
    if index is None:
        return None

    surviving = {
        item.item_id: item for item in after if item.item_id != resolved_id
    }
    if not surviving:
        return None

    below = _first_surviving(before, surviving, index, step=1)
    if below is not None:
        return below

    above = _first_surviving(before, surviving, index, step=-1)
    if above is not None:
        return above

    # Everything the owner could see is gone, but the queue is not empty
    # (a write can surface items that were not proposable before).
    return next(iter(surviving.values()))


# ---------------------------------------------------------------------------
# confirm_pair — wraps create_transfer
# ---------------------------------------------------------------------------


# How far apart a manually-confirmed pair may be before it is refused.
# Deliberately looser than the automatic matcher's ±1 day / 2%: the whole
# point of the manual path is the cases the matcher would not take, and the
# owner can see the amounts. These bounds only catch a mis-click.
MANUAL_PAIR_MAX_DAYS = 5
MANUAL_PAIR_MAX_DRIFT = Decimal("0.10")


def assess_pair(deposit: Transaction, sell: Transaction) -> PairAssessment:
    """Score a candidate pairing: how far apart, how far off, confirmable?

    The single implementation of the plausibility bounds. The payload
    renders it under the two legs (criterion H1) and
    :func:`_reject_implausible_pair` raises on it, so the modal's disabled
    button and the write path's 400 always agree — the design's danger
    banner has to state the reason *before* the click.

    ``implied_rate`` is the pair's own arithmetic: the bank leg's bolívars
    over the exchange leg's dollars. Same-currency pairs get ``None``,
    where the ratio would be ~1 and say nothing.
    """
    days_apart = abs((sell.occurred_at.date() - deposit.occurred_at.date()).days)

    implied_rate: Decimal | None = None
    if sell.amount != 0 and deposit.currency != sell.currency:
        implied_rate = abs(deposit.amount) / abs(sell.amount)

    drift_pct: Decimal | None = None
    drift: Decimal | None = None
    if sell.user_rate is not None and sell.user_rate > 0 and deposit.amount != 0:
        expected = abs(sell.amount) * sell.user_rate
        drift = abs(abs(deposit.amount) - expected) / abs(deposit.amount)
        drift_pct = drift * 100

    if days_apart > MANUAL_PAIR_MAX_DAYS:
        reason = (
            f"refusing to pair: {days_apart} days apart "
            f"({deposit.occurred_at.date()} vs {sell.occurred_at.date()}), "
            f"limit is {MANUAL_PAIR_MAX_DAYS}"
        )
    elif drift is not None and drift > MANUAL_PAIR_MAX_DRIFT:
        expected = abs(sell.amount) * sell.user_rate
        reason = (
            f"refusing to pair: the sell is worth {expected:.2f} "
            f"{deposit.currency} at its recorded rate but the deposit is "
            f"{abs(deposit.amount):.2f} — {drift:.0%} apart, limit is "
            f"{MANUAL_PAIR_MAX_DRIFT:.0%}"
        )
    else:
        reason = None

    return PairAssessment(
        days_apart=days_apart,
        drift_pct=drift_pct,
        implied_rate=implied_rate,
        refused=reason is not None,
        refuse_reason=reason,
    )


def _reject_implausible_pair(deposit: Transaction, sell: Transaction) -> None:
    """Refuse a pairing that cannot plausibly be the same movement of money.

    ``create_transfer`` drift-checks only same-currency pairs, ``doctor``
    exempted cross-currency ones, and ``transfers.validate`` is not called
    from any write path. That left the manual path with no check at all: the
    review confirmed a 2 261 Bs deposit dated 2025-11-06 could be paired
    with a 200.44 USDT sell dated 2026-07-30 — eight months and a factor of
    75 apart — after which both rows leave income and expense permanently,
    silently and with no way to notice.

    The pair-picker already computes this drift for display
    (``web/services/pairing.py``); this is the same arithmetic made
    load-bearing at the point of the write.

    A sell with no ``user_rate`` cannot be scored on amount, so only the
    date bound applies — refusing it outright would block exactly the
    legacy rows the manual path exists to clear.

    The arithmetic itself lives in :func:`assess_pair`, which the queue
    payload renders; this is the same verdict, raised.
    """
    verdict = assess_pair(deposit, sell)
    if verdict.refused:
        raise ValueError(verdict.refuse_reason)


def confirm_pair(
    conn: sqlite3.Connection,
    *,
    deposit_id: int,
    sell_id: int,
) -> dict[str, Any]:
    """Pair a bank deposit + Binance sell via :func:`create_transfer`.

    Validates that both transactions exist and neither already carries a
    transfer_id; otherwise raises ``LookupError`` (404 surface) or
    ``ValueError`` (400 surface).

    Then rebuilds the realized cost basis — criterion H5, and the same
    bargain ``transactions_write.apply_edit`` step 3 keeps for a rate
    edit (ADR-013 Amendment 2026-07-26). Confirming the pairing is the
    owner asserting *these bolívars came from that sell*, so it is the
    moment the ``binance_p2p_realized`` tier should encode it. The write
    itself changes no rate — ``SQL_P2P_SELLS`` keys off ``source_ref``
    and sign, not ``kind``, so promoting both legs leaves the set of
    fills alone — but nothing guarantees that set was ever
    *materialised*. The tier is only as fresh as the last ingest,
    backfill or ``finances rates rebuild-realized``, and a fill that
    arrived by any other path prices a fortnight of bolívar rows off the
    market median instead of what those bolívars cost.

    Ordered after the guards, so a refused or already-paired
    confirmation writes nothing at all. Idempotent and derived wholly
    from ``transactions`` (~5 ms on the live ledger), so firing it on
    every confirmation is cheap and always recoverable.

    Returns a dict ready for JSON serialization::

        {"transfer_id": str, "from_transaction_id": int, "to_transaction_id": int}
    """
    from finances.db.repos import transactions as transactions_repo

    deposit = transactions_repo.get_by_id(conn, deposit_id)
    sell = transactions_repo.get_by_id(conn, sell_id)

    if deposit is None:
        raise LookupError(f"transaction id={deposit_id} not found")
    if sell is None:
        raise LookupError(f"transaction id={sell_id} not found")

    if deposit.transfer_id is not None:
        raise ValueError(
            f"transaction id={deposit_id} is already part of a transfer "
            f"(transfer_id={deposit.transfer_id})"
        )
    if sell.transfer_id is not None:
        raise ValueError(
            f"transaction id={sell_id} is already part of a transfer "
            f"(transfer_id={sell.transfer_id})"
        )

    _reject_implausible_pair(deposit, sell)

    pair = create_transfer(
        conn,
        anchor_transaction_id=deposit_id,
        counterpart_transaction_id=sell_id,
    )

    realized_rates.rebuild(conn)

    return {
        "transfer_id": pair.transfer_id,
        "from_transaction_id": pair.from_transaction_id,
        "to_transaction_id": pair.to_transaction_id,
    }


__all__ = [
    "LEARNED_GUESS_MIN",
    "MANUAL_PAIR_MAX_DAYS",
    "MANUAL_PAIR_MAX_DRIFT",
    "PairAssessment",
    "PairProposal",
    "TriageAccount",
    "TriageGuess",
    "TriageItem",
    "TriageNeeds",
    "TriageQueue",
    "TriageType",
    "assess_pair",
    "build_queue",
    "confirm_pair",
    "count_blocking",
    "neighbours_of",
    "next_item_after",
]
