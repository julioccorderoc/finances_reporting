"""Tests for the /rates date-pivot table and the chart's shared date axis.

Per rule-011 these land before the implementation. Two things are under
test here, both of which the previous /rates page got wrong:

* **The chart's x axis.** Chart.js is given a ``category`` scale, which
  places points by *index* unless it is handed an explicit label list.
  With a 13-row P2P median and a 245-row BCV series that means the two
  lines are drawn against different days — the bug that made the median
  appear to stop a third of the way across. The union-of-dates axis is
  built on the server, not in the browser, precisely so these tests can
  see it.

* **The table.** One row per date, one column per (base, quote, source),
  newest first, gaps left empty. Columns are discovered from the data and
  ordered by the resolver's own preference (rule-005), so a source added
  later needs no change here.

The seed is deliberately ragged: the P2P median has holes, one source
sits entirely outside the window, and BCV carries a forward-dated row
(the real feed publishes tomorrow's rate today).
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import rates as rates_repo
from finances.domain.models import Rate


TODAY = date.today()


def _rate(conn: sqlite3.Connection, day: date, base: str, quote: str, source: str, value: str) -> None:
    rates_repo.upsert(
        conn,
        Rate(
            as_of_date=day,
            base=base,
            quote=quote,
            rate=Decimal(value),
            source=source,
        ),
    )


@pytest.fixture
def ragged_rates_db(seeded_web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Rate history with holes, a stale source, and a forward-dated row.

    * BCV USD/VES — every day for the last 10, plus tomorrow.
    * BCV EUR/VES — every day for the last 10.
    * USDT/VES P2P median — only days 0, 3 and 7 back. Holes on purpose.
    * USDT/VES realized — day 1 back only.
    * USDT/VES P2P buy — a single row 400 days back, far outside any
      window the page offers, so it must not earn a column.
    """
    for offset in range(0, 10):
        day = TODAY - timedelta(days=offset)
        _rate(seeded_web_db, day, "USD", "VES", "bcv", f"{800 + offset}.5000")
        _rate(seeded_web_db, day, "EUR", "VES", "bcv", f"{940 + offset}.2500")

    # BCV publishes ahead of itself.
    _rate(seeded_web_db, TODAY + timedelta(days=1), "USD", "VES", "bcv", "812.0000")

    for offset in (0, 3, 7):
        day = TODAY - timedelta(days=offset)
        _rate(seeded_web_db, day, "USDT", "VES", "binance_p2p_median", f"{960 + offset}.1000")

    _rate(seeded_web_db, TODAY - timedelta(days=1), "USDT", "VES", "binance_p2p_realized", "958.5600")
    _rate(seeded_web_db, TODAY - timedelta(days=400), "USDT", "VES", "binance_p2p_median_buy", "500.0000")

    return seeded_web_db


# ---------------------------------------------------------------------------
# The chart's shared date axis.
# ---------------------------------------------------------------------------


def test_chart_labels_are_every_day_in_the_window_ascending(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    """The axis is a date domain, not whichever days happened to have rows.

    A category axis built from one series' dates would place the other
    series' points against the wrong days.
    """
    from finances.web.services.rates_view import build_rates_chart

    chart = build_rates_chart(ragged_rates_db, range_days=10)

    assert chart.labels == sorted(chart.labels)
    assert chart.labels[0] == TODAY - timedelta(days=9)
    assert chart.labels[-1] == TODAY
    assert len(chart.labels) == 10


def test_chart_labels_include_a_forward_dated_row(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    """BCV publishes tomorrow's rate today; the axis must reach it."""
    from finances.web.services.rates_view import build_rates_chart

    chart = build_rates_chart(ragged_rates_db, range_days=10)

    assert TODAY + timedelta(days=1) in chart.labels


def test_chart_series_points_all_land_on_a_label(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    """Every plotted point must have a slot on the axis, or it is misplaced."""
    from finances.web.services.rates_view import build_rates_chart

    chart = build_rates_chart(ragged_rates_db, range_days=10)
    axis = set(chart.labels)

    for series in chart.series:
        for point in series.points:
            assert point.as_of_date in axis


def test_chart_payload_carries_the_labels_to_the_browser(
    ragged_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """The axis is useless if it stops at the DTO."""
    import json
    import re

    client: TestClient = web_client_factory()
    body = client.get("/rates", params={"range_days": 10}).text

    match = re.search(
        r'<script id="rates-chart-data" type="application/json">(.*?)</script>',
        body,
        re.DOTALL,
    )
    assert match is not None, "chart payload block missing"
    payload = json.loads(match.group(1))

    assert "labels" in payload
    assert payload["labels"] == sorted(payload["labels"])
    assert TODAY.isoformat() in payload["labels"]


def test_chart_script_spans_gaps_so_a_sparse_series_still_draws_a_line() -> None:
    """The median has holes; without spanGaps it renders as loose dots."""
    from pathlib import Path

    source = (
        Path("finances/web/templates/partials/rates_chart.html")
        .read_text(encoding="utf-8")
    )
    assert "spanGaps" in source


def test_rates_page_states_the_series_once_not_twice(
    ragged_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Chart.js draws its own legend; the toolbar note repeated it."""
    client: TestClient = web_client_factory()
    body = client.get("/rates").text

    assert "rpt-chart-legend-note" not in body


# ---------------------------------------------------------------------------
# The table: columns.
# ---------------------------------------------------------------------------


def test_table_columns_follow_the_resolver_ladder_then_the_rest(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    """rule-005's ladder, left to right, taken from the resolver itself.

    The order is the chain's own — realized outranks the median, which
    outranks BCV — so a re-ordering of ``_FALLBACK_TIERS`` moves these
    columns too. Pairs the ladder does not price (BCV in euros) follow,
    stably sorted.
    """
    from finances.web.services.rates_view import build_rates_table

    table = build_rates_table(ragged_rates_db, range_days=10)

    assert [(c.pair, c.source) for c in table.columns] == [
        ("USDT/VES", "binance_p2p_realized"),
        ("USDT/VES", "binance_p2p_median"),
        ("USD/VES", "bcv"),
        ("EUR/VES", "bcv"),
    ]


def test_table_column_order_is_read_off_the_resolver_not_copied(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    """A second copy of the ladder here is exactly what rule-012 forbids."""
    from finances.domain import rates as rates_domain
    from finances.web.services.rates_view import build_rates_table

    table = build_rates_table(ragged_rates_db, range_days=10)
    ladder = [
        (base, quote, source) for base, quote, source in rates_domain.ladder_tiers()
    ]
    leading = [
        (c.base, c.quote, c.source)
        for c in table.columns
        if (c.base, c.quote, c.source) in set(ladder)
    ]

    assert leading == ladder


def test_table_drops_a_source_with_no_rows_in_the_window(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    """The P2P buy row is 400 days old; an empty column is noise."""
    from finances.web.services.rates_view import build_rates_table

    table = build_rates_table(ragged_rates_db, range_days=10)

    assert "binance_p2p_median_buy" not in {c.source for c in table.columns}


def test_table_gives_an_unknown_source_a_column_at_the_end(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    """``transactions.source`` is open-ended; the table must not hard-code."""
    from finances.web.services.rates_view import build_rates_table

    _rate(ragged_rates_db, TODAY, "USDT", "VES", "some_new_oracle", "961.0000")

    table = build_rates_table(ragged_rates_db, range_days=10)

    assert table.columns[-1].source == "some_new_oracle"


def test_table_marks_bcv_columns_reference_only(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    """rule-005: BCV is never a headline, and the table must say so."""
    from finances.web.services.rates_view import build_rates_table

    table = build_rates_table(ragged_rates_db, range_days=10)
    by_source = {(c.source, c.pair): c for c in table.columns}

    assert by_source[("bcv", "USD/VES")].is_reference_only is True
    assert by_source[("binance_p2p_median", "USDT/VES")].is_reference_only is False


# ---------------------------------------------------------------------------
# The table: rows and cells.
# ---------------------------------------------------------------------------


def test_table_rows_are_newest_first(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    from finances.web.services.rates_view import build_rates_table

    table = build_rates_table(ragged_rates_db, range_days=10)
    dates = [r.as_of_date for r in table.rows]

    assert dates == sorted(dates, reverse=True)


def test_table_leaves_a_missing_rate_empty_rather_than_carrying_it(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    """The median has no row 5 days back. That is a fact, not a value.

    Carrying yesterday's number forward here would make the table lie
    about how sparse the P2P feed is.
    """
    from finances.web.services.rates_view import build_rates_table

    table = build_rates_table(ragged_rates_db, range_days=10)
    column = next(c for c in table.columns if c.source == "binance_p2p_median")
    row = next(r for r in table.rows if r.as_of_date == TODAY - timedelta(days=5))

    assert row.cells[column.key] is None


def test_table_cell_holds_the_rate_for_that_day(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    from finances.web.services.rates_view import build_rates_table

    table = build_rates_table(ragged_rates_db, range_days=10)
    column = next(c for c in table.columns if c.source == "binance_p2p_median")
    row = next(r for r in table.rows if r.as_of_date == TODAY - timedelta(days=3))

    assert row.cells[column.key] == Decimal("963.1000")


def test_table_has_no_row_for_a_day_with_no_rates_at_all(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """Rows come from the data, not from the calendar."""
    from finances.web.services.rates_view import build_rates_table

    _rate(seeded_web_db, TODAY, "USDT", "VES", "binance_p2p_median", "960.0000")

    table = build_rates_table(seeded_web_db, range_days=10)

    assert [r.as_of_date for r in table.rows] == [TODAY]


def test_table_honours_the_range_window(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    from finances.web.services.rates_view import build_rates_table

    table = build_rates_table(ragged_rates_db, range_days=3)
    oldest = min(r.as_of_date for r in table.rows)

    assert oldest >= TODAY - timedelta(days=2)


def test_table_flags_a_forward_dated_row(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    """A rate dated after today is real, shown, and marked as ahead."""
    from finances.web.services.rates_view import build_rates_table

    table = build_rates_table(ragged_rates_db, range_days=10)
    tomorrow = next(
        (r for r in table.rows if r.as_of_date == TODAY + timedelta(days=1)), None
    )

    assert tomorrow is not None
    assert tomorrow.is_future is True
    assert table.rows[0].as_of_date == TODAY + timedelta(days=1)


def test_table_does_not_flag_todays_row_as_future(
    ragged_rates_db: sqlite3.Connection,
) -> None:
    from finances.web.services.rates_view import build_rates_table

    table = build_rates_table(ragged_rates_db, range_days=10)
    row = next(r for r in table.rows if r.as_of_date == TODAY)

    assert row.is_future is False


# ---------------------------------------------------------------------------
# Rendering.
# ---------------------------------------------------------------------------


def test_rates_page_renders_the_table(
    ragged_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    body = client.get("/rates", params={"range_days": 10}).text

    assert 'class="rpt-rate-table"' in body
    assert "963.1000" in body


def test_rates_page_no_longer_renders_the_latest_per_pair_tiles(
    ragged_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """The table's newest row is the latest per pair. Tiles were a duplicate."""
    client: TestClient = web_client_factory()
    body = client.get("/rates").text

    assert "rpt-rate-tiles" not in body
    assert "data-rate-card" not in body


def test_table_renders_an_empty_cell_for_a_gap(
    ragged_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    import re

    client: TestClient = web_client_factory()
    body = client.get("/rates", params={"range_days": 10}).text

    assert re.search(r'data-cell[^>]*data-empty', body) is not None


def test_rates_table_is_not_an_html_table_element(
    ragged_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """The viewer's data surfaces are CSS Grid, not <table> (see .flow-rows)."""
    client: TestClient = web_client_factory()
    body = client.get("/rates", params={"range_days": 10}).text

    assert "<table" not in body.replace('<table class="today-sr"', "")


# ---------------------------------------------------------------------------
# The range toggle now swaps chart AND table together.
# ---------------------------------------------------------------------------


def test_panel_partial_returns_chart_and_table(
    ragged_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get(
        "/_partial/rates/panel",
        params={"range_days": 10},
        headers={"HX-Request": "true"},
    )

    assert resp.status_code == 200
    body = resp.text
    assert "<html" not in body.lower()
    assert "<canvas" in body
    assert 'class="rpt-rate-table"' in body


def test_range_toggle_targets_the_panel_not_the_chart_alone(
    ragged_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """A range change that moved only the chart left the table stale."""
    client: TestClient = web_client_factory()
    body = client.get("/rates").text

    assert 'hx-get="/_partial/rates/panel"' in body
    assert 'hx-target="#rates-panel"' in body


def test_panel_partial_honours_the_range(
    ragged_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """A 3-day window must not render a row from 7 days back."""
    client: TestClient = web_client_factory()
    resp = client.get(
        "/_partial/rates/panel",
        params={"range_days": 3},
        headers={"HX-Request": "true"},
    )

    assert resp.status_code == 200
    body = resp.text
    stale = (TODAY - timedelta(days=7)).isoformat()
    assert stale not in body
