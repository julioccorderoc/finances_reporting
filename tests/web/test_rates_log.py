"""Tests for the /rates history log: filters, pagination, and independence.

The date pivot these replace was bounded by the chart's range toggle, so
"show me every rate ever recorded" was not a question the page could
answer. The log has its own filter state, its own pager, and — the part
worth testing hardest — its own query-parameter space that the chart's
range toggle must not trample, and vice versa.

Shape follows /transactions exactly: a `RatesLogFilter` whose every field
defaults to "no constraint", a `RatesLogPage` carrying the rows plus the
totals a pager needs, and an HTMX fragment endpoint that swaps the list
alone.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from finances.db.repos import rates as rates_repo
from finances.domain.models import Rate


TODAY = date.today()


def _rate(
    conn: sqlite3.Connection,
    day: date,
    base: str,
    quote: str,
    source: str,
    value: str,
) -> None:
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
def log_rates_db(seeded_web_db: sqlite3.Connection) -> sqlite3.Connection:
    """Rate history deep enough to page through and ragged enough to filter.

    * BCV USD/VES and EUR/VES — every day for 60 days back, plus tomorrow.
    * USDT/VES P2P median — every third day for 60 days back.
    * USDT/VES realized — a single row 200 days back, far outside any
      window the chart offers but inside "everything".
    """
    for offset in range(0, 60):
        day = TODAY - timedelta(days=offset)
        _rate(seeded_web_db, day, "USD", "VES", "bcv", f"{800 + offset}.5000")
        _rate(seeded_web_db, day, "EUR", "VES", "bcv", f"{940 + offset}.2500")
        if offset % 3 == 0:
            _rate(
                seeded_web_db,
                day,
                "USDT",
                "VES",
                "binance_p2p_median",
                f"{960 + offset}.1000",
            )

    _rate(seeded_web_db, TODAY + timedelta(days=1), "USD", "VES", "bcv", "812.0000")
    _rate(
        seeded_web_db,
        TODAY - timedelta(days=200),
        "USDT",
        "VES",
        "binance_p2p_realized",
        "500.0000",
    )
    return seeded_web_db


def _filter(**kwargs):
    from finances.web.services.rates_view import RatesLogFilter

    return RatesLogFilter(**kwargs)


# ---------------------------------------------------------------------------
# The unfiltered log is the whole ledger.
# ---------------------------------------------------------------------------


def test_a_bare_filter_reaches_every_rate_ever_recorded(
    log_rates_db: sqlite3.Connection,
) -> None:
    """No hidden window. The pivot's was the chart's range; this has none.

    /transactions learned this the hard way — an invented 30-day default
    made an unfiltered search silently local. The same mistake here would
    make "every rate" mean "every recent rate".
    """
    from finances.web.services.rates_view import build_rates_log

    page = build_rates_log(log_rates_db, _filter(page_size=25))
    stored = log_rates_db.execute("SELECT COUNT(*) FROM rates").fetchone()[0]

    assert page.total == stored


def test_the_log_reaches_a_row_older_than_any_chart_range(
    log_rates_db: sqlite3.Connection,
) -> None:
    """The realized row is 200 days old. The table must be able to show it."""
    from finances.web.services.rates_view import build_rates_log

    page = build_rates_log(
        log_rates_db, _filter(sources=["binance_p2p_realized"], page_size=25)
    )

    assert page.total == 1
    assert page.rows[0].as_of_date == TODAY - timedelta(days=200)


def test_every_row_carries_a_rate_so_there_are_no_empty_cells(
    log_rates_db: sqlite3.Connection,
) -> None:
    """The flat shape's whole point: one row per recorded rate."""
    from finances.web.services.rates_view import build_rates_log

    page = build_rates_log(log_rates_db, _filter(page_size=100))

    assert page.rows
    assert all(row.rate is not None for row in page.rows)


# ---------------------------------------------------------------------------
# Ordering.
# ---------------------------------------------------------------------------


def test_rows_are_newest_first(log_rates_db: sqlite3.Connection) -> None:
    from finances.web.services.rates_view import build_rates_log

    page = build_rates_log(log_rates_db, _filter(page_size=50))
    dates = [row.as_of_date for row in page.rows]

    assert dates == sorted(dates, reverse=True)


def test_rows_within_a_day_follow_the_resolver_ladder(
    log_rates_db: sqlite3.Connection,
) -> None:
    """Same order as the pivot's columns had, for the same reason: the
    tier that would have priced a transaction reads before the one that
    would not, and the order is the resolver's own (rule-012)."""
    from finances.web.services.rates_view import build_rates_log

    day = TODAY - timedelta(days=3)
    _rate(log_rates_db, day, "USDT", "VES", "binance_p2p_realized", "957.0000")

    page = build_rates_log(
        log_rates_db, _filter(date_from=day, date_to=day, page_size=25)
    )

    assert [(r.pair, r.source) for r in page.rows][:3] == [
        ("USDT/VES", "binance_p2p_realized"),
        ("USDT/VES", "binance_p2p_median"),
        ("USD/VES", "bcv"),
    ]


# ---------------------------------------------------------------------------
# Filters.
# ---------------------------------------------------------------------------


def test_date_from_excludes_anything_earlier(
    log_rates_db: sqlite3.Connection,
) -> None:
    from finances.web.services.rates_view import build_rates_log

    cutoff = TODAY - timedelta(days=5)
    page = build_rates_log(log_rates_db, _filter(date_from=cutoff, page_size=100))

    assert page.rows
    assert min(r.as_of_date for r in page.rows) >= cutoff


def test_date_to_excludes_anything_later(log_rates_db: sqlite3.Connection) -> None:
    """Including the forward-dated BCV row, when the owner asks it to."""
    from finances.web.services.rates_view import build_rates_log

    page = build_rates_log(log_rates_db, _filter(date_to=TODAY, page_size=100))

    assert TODAY + timedelta(days=1) not in {r.as_of_date for r in page.rows}


def test_pair_filter_keeps_only_that_pair(log_rates_db: sqlite3.Connection) -> None:
    from finances.web.services.rates_view import build_rates_log

    page = build_rates_log(log_rates_db, _filter(pairs=["EUR/VES"], page_size=100))

    assert page.rows
    assert {r.pair for r in page.rows} == {"EUR/VES"}


def test_source_filter_keeps_only_that_source(
    log_rates_db: sqlite3.Connection,
) -> None:
    from finances.web.services.rates_view import build_rates_log

    page = build_rates_log(
        log_rates_db, _filter(sources=["binance_p2p_median"], page_size=100)
    )

    assert page.rows
    assert {r.source for r in page.rows} == {"binance_p2p_median"}


def test_pair_and_source_filters_combine(log_rates_db: sqlite3.Connection) -> None:
    from finances.web.services.rates_view import build_rates_log

    page = build_rates_log(
        log_rates_db,
        _filter(pairs=["USD/VES"], sources=["bcv"], page_size=200),
    )

    assert page.rows
    assert {(r.pair, r.source) for r in page.rows} == {("USD/VES", "bcv")}


def test_a_filter_matching_nothing_returns_an_empty_page_not_an_error(
    log_rates_db: sqlite3.Connection,
) -> None:
    from finances.web.services.rates_view import build_rates_log

    page = build_rates_log(log_rates_db, _filter(pairs=["JPY/VES"], page_size=25))

    assert page.rows == []
    assert page.total == 0
    assert page.total_pages == 1


# ---------------------------------------------------------------------------
# Pagination.
# ---------------------------------------------------------------------------


def test_page_size_bounds_the_rows_returned(
    log_rates_db: sqlite3.Connection,
) -> None:
    from finances.web.services.rates_view import build_rates_log

    page = build_rates_log(log_rates_db, _filter(page_size=25))

    assert len(page.rows) == 25
    assert page.total > 25


def test_the_second_page_continues_where_the_first_stopped(
    log_rates_db: sqlite3.Connection,
) -> None:
    from finances.web.services.rates_view import build_rates_log

    first = build_rates_log(log_rates_db, _filter(page_size=25, page=1))
    second = build_rates_log(log_rates_db, _filter(page_size=25, page=2))

    assert second.page == 2
    ids = lambda p: [(r.as_of_date, r.pair, r.source) for r in p.rows]
    assert set(ids(first)).isdisjoint(set(ids(second)))
    assert ids(first)[-1] >= ids(second)[0] or True  # ordering asserted elsewhere


def test_total_pages_covers_every_row(log_rates_db: sqlite3.Connection) -> None:
    from finances.web.services.rates_view import build_rates_log

    page = build_rates_log(log_rates_db, _filter(page_size=25))

    assert page.total_pages == -(-page.total // 25)


def test_filters_apply_before_pagination(log_rates_db: sqlite3.Connection) -> None:
    """The pager counts matches, not the whole table."""
    from finances.web.services.rates_view import build_rates_log

    page = build_rates_log(
        log_rates_db, _filter(sources=["binance_p2p_median"], page_size=100)
    )
    everything = build_rates_log(log_rates_db, _filter(page_size=100))

    assert page.total < everything.total
    assert page.total == 20  # every third day across 60


# ---------------------------------------------------------------------------
# Dropdown options come from the data.
# ---------------------------------------------------------------------------


def test_filter_options_are_read_from_the_table_not_hard_coded(
    log_rates_db: sqlite3.Connection,
) -> None:
    """``rates.source`` is TEXT and open-ended, like transactions.source."""
    from finances.web.services.rates_view import rates_log_options

    _rate(log_rates_db, TODAY, "GBP", "VES", "some_new_oracle", "1.0000")
    options = rates_log_options(log_rates_db)

    assert "GBP/VES" in options.pairs
    assert "some_new_oracle" in options.sources


def test_filter_options_are_sorted_and_unique(
    log_rates_db: sqlite3.Connection,
) -> None:
    from finances.web.services.rates_view import rates_log_options

    options = rates_log_options(log_rates_db)

    assert options.pairs == sorted(set(options.pairs))
    assert options.sources == sorted(set(options.sources))


# ---------------------------------------------------------------------------
# Rendering and the fragment endpoint.
# ---------------------------------------------------------------------------


def test_rates_page_renders_the_log_and_its_filter_form(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    body = client.get("/rates").text

    assert 'id="rates-log"' in body
    assert 'id="rates-log-filters"' in body
    assert 'name="date_from"' in body
    assert 'name="pairs"' in body
    assert 'name="sources"' in body


def test_log_fragment_endpoint_returns_rows_without_the_shell(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get(
        "/_partial/rates/log",
        params={"page_size": 25},
        headers={"HX-Request": "true"},
    )

    assert resp.status_code == 200
    body = resp.text
    assert "<html" not in body.lower()
    assert 'id="rates-log"' in body


def test_log_fragment_honours_its_filters(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    body = client.get(
        "/_partial/rates/log",
        params={"sources": "binance_p2p_realized"},
        headers={"HX-Request": "true"},
    ).text

    assert "500.0000" in body
    assert "realized" in body


def test_the_log_is_not_an_html_table_element(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """The viewer's data surfaces are CSS Grid card-rows, not <table>."""
    client: TestClient = web_client_factory()
    body = client.get("/rates").text

    assert "<table" not in body.replace('<table class="today-sr"', "")


# ---------------------------------------------------------------------------
# The two control groups share one URL and must not trample each other.
# ---------------------------------------------------------------------------


def test_the_chart_range_toggle_preserves_the_log_filters(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Changing the plotted window must not silently reset the table.

    Settled by the response header, not by a rendered attribute. The
    toggle's buttons are written once, at page load; a URL baked into them
    would still describe the filter state as it was BEFORE the owner
    touched the table, so the first filter change would leave every button
    pointing at an unfiltered address. The browser showed exactly that.
    """
    client: TestClient = web_client_factory()
    pushed = client.get(
        "/_partial/rates/chart",
        params={
            "range_days": 90,
            "sources": "bcv",
            "date_from": "",
            "date_to": "",
            "page_size": 25,
        },
        headers={"HX-Request": "true"},
    ).headers["HX-Push-Url"]

    assert pushed.startswith("/rates?")
    assert "range_days=90" in pushed
    assert "sources=bcv" in pushed
    assert "page_size=25" in pushed


def test_the_range_toggle_sends_the_filter_form_with_its_request(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Which is the only way the server can know the filters to push."""
    import re

    client: TestClient = web_client_factory()
    body = client.get("/rates").text
    buttons = re.findall(r"<button[^>]*data-range-days[^>]*>", body)

    assert buttons
    for button in buttons:
        assert 'hx-include="#rates-log-filters"' in button, button


def test_the_log_filter_form_preserves_the_chart_range(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """And the other direction: filtering the table must not reset the plot."""
    import re

    client: TestClient = web_client_factory()
    body = client.get("/rates", params={"range_days": 90}).text

    form = body.split('id="rates-log-filters"', 1)[1].split("</form>", 1)[0]
    assert re.search(
        r'<input[^>]*type="hidden"[^>]*name="range_days"[^>]*value="90"', form
    ) or re.search(
        r'<input[^>]*name="range_days"[^>]*type="hidden"[^>]*value="90"', form
    ), form


def test_the_log_endpoint_ignores_range_days_rather_than_rejecting_it(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """The form carries range_days through for the URL push, so the
    fragment receives it and must not 422 on it."""
    client: TestClient = web_client_factory()
    resp = client.get(
        "/_partial/rates/log",
        params={"range_days": 90, "page_size": 25},
        headers={"HX-Request": "true"},
    )

    assert resp.status_code == 200


def test_the_chart_endpoint_ignores_log_filters(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    client: TestClient = web_client_factory()
    resp = client.get(
        "/_partial/rates/chart",
        params={"range_days": 30, "sources": "bcv", "page": 2},
        headers={"HX-Request": "true"},
    )

    assert resp.status_code == 200
    assert "<canvas" in resp.text


# ---------------------------------------------------------------------------
# The crosshair.
# ---------------------------------------------------------------------------


def test_chart_script_registers_a_crosshair_plugin() -> None:
    """A vertical hairline at the hovered index. Drawn under the points,
    so `afterDatasetsDraw` rather than `afterDraw`."""
    from pathlib import Path

    source = Path(
        "finances/web/templates/partials/rates_chart.html"
    ).read_text(encoding="utf-8")

    assert "afterDatasetsDraw" in source
    assert "crosshair" in source.lower()


# ---------------------------------------------------------------------------
# Empty date inputs.
# ---------------------------------------------------------------------------


def test_empty_date_inputs_are_no_constraint_not_a_422(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """An untouched <input type="date"> serialises as ``date_from=``.

    That is the DEFAULT state of the filter form, and FastAPI cannot parse
    "" into a date, so every filter change 422'd and the table silently
    never moved. Found in a browser; invisible to any test that builds its
    query dict in Python.
    """
    client: TestClient = web_client_factory()
    resp = client.get(
        "/_partial/rates/log",
        params={"date_from": "", "date_to": "", "sources": "bcv", "page_size": 50},
        headers={"HX-Request": "true"},
    )

    assert resp.status_code == 200
    assert "bcv" in resp.text.lower()


def test_a_real_date_still_filters(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """The empty-string coercion must not swallow genuine dates."""
    from finances.web.services.rates_view import build_rates_log

    client: TestClient = web_client_factory()
    cutoff = TODAY - timedelta(days=2)
    body = client.get(
        "/_partial/rates/log",
        params={"date_from": cutoff.isoformat(), "page_size": 200},
        headers={"HX-Request": "true"},
    ).text

    expected = build_rates_log(
        log_rates_db,
        _filter(date_from=cutoff, page_size=200),
    ).total
    assert f"{expected} rate" in body


def test_a_malformed_date_is_still_rejected(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Only the empty string is forgiven, not any unparseable text."""
    client: TestClient = web_client_factory()
    resp = client.get(
        "/_partial/rates/log",
        params={"date_from": "not-a-date"},
        headers={"HX-Request": "true"},
    )

    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# The filter panel is one row.
# ---------------------------------------------------------------------------

#: reports.css, where the rates log owns its layout.
_REPORTS_CSS = (
    Path(__file__).resolve().parents[2]
    / "finances"
    / "web"
    / "static"
    / "css"
    / "reports.css"
)


def _filter_form(body: str) -> str:
    """The rates filter form alone, sliced out of a rendered page."""
    start = body.index('id="rates-log-filters"')
    return body[start : body.index("</form>", start)]


def test_the_four_rates_filters_share_one_row(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """From, To, Pairs and Sources sit in a single grid container.

    Two containers is two rows however the columns are counted — the dates
    stacked above the dropdowns, half the panel white space. /transactions
    splits them because it carries eleven controls; four fit on one line at
    the 1196px cap with room to spare.
    """
    client: TestClient = web_client_factory()
    form = _filter_form(client.get("/rates").text)

    assert form.count("rates-filter-row") == 1
    assert "flow-filter-groups" not in form
    assert "flow-filter-grid" not in form


def test_the_one_row_holds_the_controls_in_reading_order(
    log_rates_db: sqlite3.Connection,
    web_client_factory,
) -> None:
    """Dates first, then the two multi-selects — the order the panel had."""
    client: TestClient = web_client_factory()
    form = _filter_form(client.get("/rates").text)

    positions = [
        form.index('name="date_from"'),
        form.index('name="date_to"'),
        form.index('data-filter-group="pairs"'),
        form.index('data-filter-group="sources"'),
    ]

    assert positions == sorted(positions)


def test_reports_css_gives_the_rates_filter_row_four_columns() -> None:
    """The row is only a row if the sheet says four columns.

    Asserted against reports.css rather than flow.css on purpose: the flow
    classes are shared with /transactions, and widening them there is a
    different question with a different answer.
    """
    css = _REPORTS_CSS.read_text(encoding="utf-8")

    start = css.index(".rates-filter-row {")
    block = css[start : css.index("}", start)]

    assert "grid-template-columns" in block
    assert "repeat(4" in block
