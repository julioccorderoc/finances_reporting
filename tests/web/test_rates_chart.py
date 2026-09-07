"""Tests for the /rates chart: its shared date axis and its hover overlay.

Chart.js is given a ``category`` scale, which places points by *index*
unless it is handed an explicit label list. With a 13-row P2P median and
a 245-row BCV series that means the two lines are drawn against different
days — the bug that made the median appear to stop a third of the way
across, and, with no categories to label, collapsed the plot into the top
of its box. The union-of-dates axis is built on the server, not in the
browser, precisely so these tests can see it.

The overlay is tested through the payload rather than the pixels: it
shows every stream recorded on the hovered day, not only the two the plot
plots, each marked reference-only or not per rule-005.

The seed is deliberately ragged: the P2P median has holes, one source
sits entirely outside the window, and BCV carries a forward-dated row
(the real feed publishes tomorrow's rate today).

The history log below the chart has its own file, test_rates_log.py.
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
    # Ten days of window, plus the day BCV has already published ahead.
    assert chart.labels[-1] == TODAY + timedelta(days=1)
    assert len(chart.labels) == 11


def test_chart_axis_holds_a_slot_for_a_day_with_no_rows(
    seeded_web_db: sqlite3.Connection,
) -> None:
    """A gap in the feed is a gap on the axis, not a squeezed-out day."""
    from finances.web.services.rates_view import build_rates_chart

    _rate(seeded_web_db, TODAY - timedelta(days=4), "USDT", "VES", "binance_p2p_median", "955.0000")
    _rate(seeded_web_db, TODAY, "USDT", "VES", "binance_p2p_median", "960.0000")

    chart = build_rates_chart(seeded_web_db, range_days=5)

    assert chart.labels == [TODAY - timedelta(days=offset) for offset in range(4, -1, -1)]


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


def test_chart_payload_carries_every_stream_for_a_day_not_only_the_plotted_two(
    ragged_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """The hover overlay answers "what was recorded that day", in full.

    The chart plots two lines. The overlay shows all of them, so the
    realized rate and the euro reference are visible without leaving the
    plot for the table below it.
    """
    import json
    import re

    client: TestClient = web_client_factory()
    body = client.get("/rates", params={"range_days": 10}).text
    payload = json.loads(
        re.search(
            r'<script id="rates-chart-data" type="application/json">(.*?)</script>',
            body,
            re.DOTALL,
        ).group(1)
    )

    yesterday = (TODAY - timedelta(days=1)).isoformat()
    entries = payload["details"][yesterday]
    labelled = {(e["pair"], e["label"]): e["value"] for e in entries}

    # Recorded that day but NOT plotted by either line.
    assert labelled[("USDT/VES", "realized")] == "958.5600"
    assert labelled[("EUR/VES", "BCV")] == "941.2500"
    # And the plotted one is there too.
    assert ("USD/VES", "BCV") in labelled


def test_chart_detail_omits_a_day_nothing_was_recorded_on(
    seeded_web_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    import json
    import re

    _rate(seeded_web_db, TODAY, "USDT", "VES", "binance_p2p_median", "960.0000")

    client: TestClient = web_client_factory()
    body = client.get("/rates", params={"range_days": 5}).text
    payload = json.loads(
        re.search(
            r'<script id="rates-chart-data" type="application/json">(.*?)</script>',
            body,
            re.DOTALL,
        ).group(1)
    )

    assert list(payload["details"]) == [TODAY.isoformat()]


def test_chart_detail_marks_a_reference_only_stream(
    ragged_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """rule-005 again: the overlay may not present BCV as a headline."""
    import json
    import re

    client: TestClient = web_client_factory()
    body = client.get("/rates", params={"range_days": 10}).text
    payload = json.loads(
        re.search(
            r'<script id="rates-chart-data" type="application/json">(.*?)</script>',
            body,
            re.DOTALL,
        ).group(1)
    )

    entries = payload["details"][(TODAY - timedelta(days=1)).isoformat()]
    bcv = next(e for e in entries if e["pair"] == "USD/VES")
    p2p = next(e for e in entries if e["label"] == "realized")

    assert bcv["reference"] is True
    assert p2p["reference"] is False


def test_chart_card_hosts_an_overlay_element(
    ragged_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """A Chart.js in-canvas tooltip clips at the canvas floor. This one is
    a DOM node, so it can spill out of the plot."""
    client: TestClient = web_client_factory()
    body = client.get("/rates").text

    assert 'data-rates-tip' in body
    assert "rpt-chart-tip" in body


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
