"""Dashboard aggregation services (EPIC-023, Phase 2a).

Composes existing reports/repos to build the four KPI tiles, the sync
status strip, the recent-activity card list, and the 6-month spend-trend
chart data. Per rule-012 we never reimplement domain logic — every
USD-equivalence call here ultimately routes through
:func:`finances.domain.rates.resolve` (via :mod:`finances.reports.monthly`)
or the dedicated USDT-only :func:`finances.web.services.net_worth.compute_net_worth`.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict

from finances.db.repos import accounts as accounts_repo
from finances.reports import monthly as monthly_report
from finances.web.services.net_worth import (
    NetWorth,
    compute_net_worth,
)
from finances.web.services.transactions_query import (
    TransactionCard,
    TransactionsFilter,
    query_transactions,
)


# ---------------------------------------------------------------------------
# KPI tiles.
# ---------------------------------------------------------------------------


_Severity = Literal["normal", "warning", "alert"]


class KpiTile(BaseModel):
    """A single dashboard KPI tile."""

    model_config = ConfigDict(extra="forbid")

    label: str
    value: str
    hint: str | None = None
    severity: _Severity = "normal"


class KpiTiles(BaseModel):
    """The four headline KPI tiles."""

    model_config = ConfigDict(extra="forbid")

    net_worth: KpiTile
    month_spend: KpiTile
    month_income: KpiTile
    needs_review: KpiTile


def _format_money(value: Decimal) -> str:
    """Format a Decimal as ``$X,XXX.XX`` using locale-free thousands grouping."""
    quant = value.quantize(Decimal("0.01"))
    sign = "-" if quant < 0 else ""
    abs_val = abs(quant)
    int_part, _, frac_part = format(abs_val, "f").partition(".")
    if not frac_part:
        frac_part = "00"
    elif len(frac_part) == 1:
        frac_part = frac_part + "0"
    else:
        frac_part = frac_part[:2]
    # Group integer part with commas.
    grouped = "{:,}".format(int(int_part))
    return f"{sign}${grouped}.{frac_part}"


_BANK_KINDS = frozenset({"bank"})
_CRYPTO_KINDS = frozenset({"crypto_spot", "crypto_funding", "crypto_earn"})
_CASH_KINDS = frozenset({"cash"})


def _net_worth_hint(conn: sqlite3.Connection, nw: NetWorth) -> str:
    """Build the "Bank $X · Crypto $Y · Cash $Z" hint line."""
    accounts = {a.id: a for a in accounts_repo.list_all(conn, include_inactive=False)}
    bank = Decimal("0")
    crypto = Decimal("0")
    cash = Decimal("0")
    for c in nw.contributions:
        if c.contribution_usdt is None:
            continue
        kind = accounts[c.account_id].kind.value if c.account_id in accounts else "other"
        if kind in _BANK_KINDS:
            bank += c.contribution_usdt
        elif kind in _CRYPTO_KINDS:
            crypto += c.contribution_usdt
        elif kind in _CASH_KINDS:
            cash += c.contribution_usdt
    return (
        f"Bank {_format_money(bank)} · "
        f"Crypto {_format_money(crypto)} · "
        f"Cash {_format_money(cash)}"
    )


def _build_net_worth_tile(conn: sqlite3.Connection, today: date) -> KpiTile:
    nw = compute_net_worth(conn, as_of_date=today)
    hint = _net_worth_hint(conn, nw)
    severity: _Severity = "warning" if nw.missing_pairs else "normal"
    if nw.missing_pairs:
        hint = f"{hint} — missing: {', '.join(nw.missing_pairs)}"
    return KpiTile(
        label="Net worth",
        value=_format_money(nw.total_usdt),
        hint=hint,
        severity=severity,
    )


def _build_month_kind_tile(
    conn: sqlite3.Connection,
    *,
    today: date,
    kind: str,
    label: str,
) -> KpiTile:
    """Sum monthly headline (``total_usd``, NOT ``fallback_usd``) per kind."""
    month_str = today.strftime("%Y-%m")
    report = monthly_report.build_report(conn, month=month_str)
    total = sum(
        (row.total_usd for row in report.rows if row.kind == kind),
        Decimal("0"),
    )
    fallback = sum(
        (row.fallback_usd for row in report.rows if row.kind == kind),
        Decimal("0"),
    )
    needs_review = sum(
        (row.needs_review_count for row in report.rows if row.kind == kind),
        0,
    )
    hint_parts: list[str] = []
    if fallback != 0:
        hint_parts.append(f"BCV-only fallback {_format_money(fallback)}")
    if needs_review:
        hint_parts.append(f"{needs_review} need review")
    hint = " · ".join(hint_parts) if hint_parts else None
    return KpiTile(
        label=label,
        value=_format_money(total),
        hint=hint,
    )


def _build_needs_review_tile(conn: sqlite3.Connection) -> KpiTile:
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM transactions WHERE needs_review = 1"
    ).fetchone()
    n = int(row["c"]) if row is not None else 0
    severity: _Severity = "alert" if n > 0 else "normal"
    return KpiTile(
        label="Needs review",
        value=str(n),
        hint=("transactions awaiting triage" if n > 0 else "all clear"),
        severity=severity,
    )


def build_kpis(conn: sqlite3.Connection, *, today: date) -> KpiTiles:
    return KpiTiles(
        net_worth=_build_net_worth_tile(conn, today),
        month_spend=_build_month_kind_tile(
            conn, today=today, kind="expense", label="This month spend"
        ),
        month_income=_build_month_kind_tile(
            conn, today=today, kind="income", label="This month income"
        ),
        needs_review=_build_needs_review_tile(conn),
    )


# ---------------------------------------------------------------------------
# Sync status strip.
# ---------------------------------------------------------------------------


_CANONICAL_SOURCES: tuple[str, ...] = ("binance", "provincial", "bcv", "p2p_rates")
_FRESHNESS_HOURS: dict[str, int | None] = {
    "binance": 24,
    "provincial": None,  # never auto-stales
    "bcv": 48,
    "p2p_rates": 24,
}


_SyncStatus = Literal["success", "running", "error", "never"]
_ChipSeverity = Literal["green", "yellow", "red"]


class SyncChip(BaseModel):
    """One sync status chip rendered into the dashboard strip."""

    model_config = ConfigDict(extra="forbid")

    source: str
    last_run_at: datetime | None
    last_status: _SyncStatus
    severity: _ChipSeverity


def _list_recent_runs(
    conn: sqlite3.Connection, source: str, *, limit: int = 3
) -> list[dict]:
    """Return the most recent ``limit`` import_runs rows for ``source``.

    Internal helper — kept here per Phase 2a's instruction (do NOT add a
    "list recent runs per source" helper to the import_state repo).
    """
    rows = conn.execute(
        """
        SELECT id, source, started_at, finished_at, status, error
        FROM import_runs
        WHERE source = ?
        ORDER BY started_at DESC, id DESC
        LIMIT ?
        """,
        (source, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def _parse_dt(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        try:
            # SQLite CURRENT_TIMESTAMP is naive UTC: "YYYY-MM-DD HH:MM:SS".
            cleaned = value.replace("T", " ")
            try:
                dt = datetime.fromisoformat(cleaned)
            except ValueError:
                dt = datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except ValueError:
            return None
    return None


def _chip_for_source(
    conn: sqlite3.Connection, source: str, *, now: datetime
) -> SyncChip:
    runs = _list_recent_runs(conn, source, limit=3)
    if not runs:
        return SyncChip(
            source=source,
            last_run_at=None,
            last_status="never",
            severity="red",
        )

    last = runs[0]
    last_run_at = _parse_dt(last.get("finished_at") or last.get("started_at"))
    raw_status = (last.get("status") or "").strip()
    last_status: _SyncStatus
    if raw_status in ("success", "running", "error"):
        last_status = raw_status  # type: ignore[assignment]
    else:
        last_status = "never"

    # Three failures in a row → red, regardless of freshness.
    last_three_failed = (
        len(runs) >= 3
        and all((r.get("status") == "error") for r in runs[:3])
    )

    severity: _ChipSeverity
    if last_status == "error" or last_three_failed:
        severity = "red"
    else:
        window_hours = _FRESHNESS_HOURS.get(source)
        if last_status == "success":
            if window_hours is None or last_run_at is None:
                severity = "green"
            else:
                age = now - last_run_at
                severity = (
                    "green" if age <= timedelta(hours=window_hours) else "yellow"
                )
        elif last_status == "running":
            severity = "yellow"
        else:  # never (defensive)
            severity = "red"

    return SyncChip(
        source=source,
        last_run_at=last_run_at,
        last_status=last_status,
        severity=severity,
    )


def build_sync_status(conn: sqlite3.Connection) -> list[SyncChip]:
    """Build one chip per canonical source (binance, provincial, bcv, p2p_rates)."""
    now = datetime.now(tz=UTC)
    return [_chip_for_source(conn, src, now=now) for src in _CANONICAL_SOURCES]


# ---------------------------------------------------------------------------
# Recent activity.
# ---------------------------------------------------------------------------


def build_recent_activity(
    conn: sqlite3.Connection, *, limit: int = 10
) -> list[TransactionCard]:
    """Return the ``limit`` most recent income/expense transactions as cards.

    Reuses the canonical query module from Phase 2b — no parallel logic
    here. Transfers and adjustments are excluded by filter.
    """
    # Generous date window to surface real activity even on sparsely-fed
    # local DBs. The query is inherently bounded by ``page_size``.
    today = datetime.now(tz=UTC).date()
    f = TransactionsFilter(
        date_from=today - timedelta(days=365 * 5),
        date_to=today,
        kinds=["income", "expense"],
        sort="occurred_at",
        direction="desc",
        page=1,
        page_size=25,
    )
    page = query_transactions(conn, f)
    return list(page.rows[:limit])


# ---------------------------------------------------------------------------
# 6-month spend trend stacked bar.
# ---------------------------------------------------------------------------


class SpendSeries(BaseModel):
    """One stacked-bar series: a category and its values across months."""

    model_config = ConfigDict(extra="forbid")

    category: str
    values: list[Decimal]


class SpendTrend(BaseModel):
    """6-month stacked-bar dataset."""

    model_config = ConfigDict(extra="forbid")

    months: list[str]
    series: list[SpendSeries]
    fallback_total_per_month: list[Decimal]


def _months_back_iter(today: date, months_back: int) -> list[str]:
    """Return ``months_back`` ``YYYY-MM`` labels ending with today's month."""
    # Compute starting from ``today.year, today.month`` and walk back.
    months: list[tuple[int, int]] = []
    y, m = today.year, today.month
    for _ in range(months_back):
        months.append((y, m))
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    months.reverse()
    return [f"{yy:04d}-{mm:02d}" for (yy, mm) in months]


_OTHER_BUCKET = "Other"


def build_spend_trend(
    conn: sqlite3.Connection,
    *,
    today: date,
    months_back: int = 6,
) -> SpendTrend:
    """Build the 6-month stacked-bar dataset.

    * Filter to ``kind == 'expense'``.
    * Top 5 categories by total over the window become their own series;
      the rest collapse into ``"Other"``.
    * ``fallback_total_per_month`` is the BCV-mixed shadow series — the
      sum of ``fallback_usd`` per month (one value per month).
    """
    months = _months_back_iter(today, months_back)
    if not months:
        return SpendTrend(months=[], series=[], fallback_total_per_month=[])

    since = months[0]
    until = months[-1]

    report = monthly_report.build_report(conn, since=since, until=until)
    expense_rows = [r for r in report.rows if r.kind == "expense"]

    # Total per category over window.
    cat_total: dict[str, Decimal] = {}
    for r in expense_rows:
        key = r.category_name or _OTHER_BUCKET
        cat_total[key] = cat_total.get(key, Decimal("0")) + r.total_usd

    sorted_cats = sorted(
        cat_total.items(),
        key=lambda kv: (kv[1], kv[0]),
        reverse=True,
    )
    top5 = [name for (name, _) in sorted_cats[:5]]
    bucketed: set[str] = set(top5)

    # Per-month per-category accumulation.
    per_month: dict[str, dict[str, Decimal]] = {m: {} for m in months}
    fallback_per_month: dict[str, Decimal] = {m: Decimal("0") for m in months}
    for r in expense_rows:
        if r.month not in per_month:
            continue
        cat = r.category_name or _OTHER_BUCKET
        bucket = cat if cat in bucketed else _OTHER_BUCKET
        per_month[r.month][bucket] = (
            per_month[r.month].get(bucket, Decimal("0")) + r.total_usd
        )
        fallback_per_month[r.month] += r.fallback_usd

    series_names = list(top5)
    has_other = any(
        per_month[m].get(_OTHER_BUCKET, Decimal("0")) != 0 for m in months
    ) or len(sorted_cats) > 5
    if has_other:
        series_names.append(_OTHER_BUCKET)

    series = [
        SpendSeries(
            category=name,
            values=[per_month[m].get(name, Decimal("0")) for m in months],
        )
        for name in series_names
    ]

    return SpendTrend(
        months=months,
        series=series,
        fallback_total_per_month=[fallback_per_month[m] for m in months],
    )


__all__ = [
    "KpiTile",
    "KpiTiles",
    "SpendSeries",
    "SpendTrend",
    "SyncChip",
    "build_kpis",
    "build_recent_activity",
    "build_spend_trend",
    "build_sync_status",
]
