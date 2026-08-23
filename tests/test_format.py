"""Unit contract for finances.format (UX overhaul WP1).

Written before the implementation per rule-011. Pins the four shared
formatting functions every surface (viewer, static report, CLI) must
agree on. Real expense amounts are NEGATIVE (project sign convention),
so negative cases are first-class here.

Weekday facts used below (verified against the proleptic Gregorian
calendar): 2026-07-06, 2025-07-07, 2024-01-15 are Mondays;
2025-12-31 is a Wednesday; 2026-01-01 is a Thursday.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from finances.format import (
    fmt_date,
    fmt_date_short,
    fmt_money,
    fmt_month,
    fmt_native,
    fmt_number,
    fmt_usd,
)

EM_DASH = "—"  # em dash "—" (the None placeholder)


# ---------------------------------------------------------------------------
# fmt_number
# ---------------------------------------------------------------------------


def test_number_negative_decimal_grouped() -> None:
    assert fmt_number(Decimal("-1234.56")) == "-1,234.56"


def test_number_positive_pads_places() -> None:
    assert fmt_number(Decimal("1234.5")) == "1,234.50"


def test_number_over_one_million() -> None:
    assert fmt_number(Decimal("-1234567.891")) == "-1,234,567.89"


def test_number_zero() -> None:
    assert fmt_number(Decimal("0")) == "0.00"


def test_number_negative_zero_normalized() -> None:
    # A tiny negative that rounds to zero must not render "-0.00".
    assert fmt_number(Decimal("-0.001")) == "0.00"


def test_number_none_em_dash() -> None:
    assert fmt_number(None) == EM_DASH


def test_number_float_input() -> None:
    assert fmt_number(-1234.56) == "-1,234.56"


def test_number_int_input() -> None:
    assert fmt_number(1_000_000) == "1,000,000.00"


def test_number_places_four() -> None:
    # /rates uses 4 decimal places (rates_latest_per_pair.html).
    assert fmt_number(Decimal("36.5"), places=4) == "36.5000"


def test_number_places_zero() -> None:
    assert fmt_number(Decimal("-1234.56"), places=0) == "-1,235"


def test_number_half_up_rounding() -> None:
    assert fmt_number(Decimal("2.005")) == "2.01"


# ---------------------------------------------------------------------------
# fmt_money
# ---------------------------------------------------------------------------


def test_money_sign_before_symbol() -> None:
    assert fmt_money(Decimal("-1200")) == "-$1,200.00"


def test_money_positive() -> None:
    assert fmt_money(Decimal("3450")) == "$3,450.00"


def test_money_over_one_million() -> None:
    assert fmt_money(Decimal("-1234567.89")) == "-$1,234,567.89"


def test_money_zero() -> None:
    assert fmt_money(Decimal("0")) == "$0.00"


def test_money_none_em_dash() -> None:
    assert fmt_money(None) == EM_DASH


def test_money_label_currency_sign_first() -> None:
    assert fmt_money(Decimal("-45231.10"), symbol="Bs. ") == "-Bs. 45,231.10"


def test_money_never_dollar_minus() -> None:
    for value in (Decimal("-0.01"), Decimal("-1"), Decimal("-999999.99")):
        assert "$-" not in fmt_money(value)


# ---------------------------------------------------------------------------
# fmt_date
# ---------------------------------------------------------------------------

_TODAY = date(2026, 7, 11)


def test_date_same_year_no_year_suffix() -> None:
    assert fmt_date(date(2026, 7, 6), today=_TODAY) == "Mon, Jul 6"


def test_date_other_year_appends_year() -> None:
    assert fmt_date(date(2025, 7, 7), today=_TODAY) == "Mon, Jul 7, 2025"


def test_date_year_boundary_previous_year() -> None:
    # Dec 31 viewed on Jan 1 of the next year → year suffix required.
    assert (
        fmt_date(date(2025, 12, 31), today=date(2026, 1, 1))
        == "Wed, Dec 31, 2025"
    )


def test_date_year_boundary_next_year() -> None:
    # Jan 1 viewed on Dec 31 of the previous year → year suffix required.
    assert (
        fmt_date(date(2026, 1, 1), today=date(2025, 12, 31))
        == "Thu, Jan 1, 2026"
    )


def test_date_accepts_datetime() -> None:
    dt = datetime(2024, 1, 15, 23, 59, tzinfo=UTC)
    assert fmt_date(dt, today=_TODAY) == "Mon, Jan 15, 2024"


def test_date_accepts_iso_string() -> None:
    assert fmt_date("2024-01-15", today=_TODAY) == "Mon, Jan 15, 2024"


def test_date_none_em_dash() -> None:
    assert fmt_date(None) == EM_DASH


def test_date_default_today_omits_current_year() -> None:
    # Default today comes from finances.config.CARACAS_TZ; today's own date
    # never carries a year suffix, so exactly one comma appears.
    label = fmt_date(datetime.now(tz=UTC).date())
    assert label.count(",") == 1


# ---------------------------------------------------------------------------
# fmt_month
# ---------------------------------------------------------------------------


def test_month_from_string() -> None:
    assert fmt_month("2026-07") == "Jul 2026"


def test_month_from_string_january() -> None:
    assert fmt_month("2024-01") == "Jan 2024"


def test_month_from_date() -> None:
    assert fmt_month(date(2025, 12, 1)) == "Dec 2025"


def test_month_from_datetime() -> None:
    assert fmt_month(datetime(2025, 12, 1, 23, 59, tzinfo=UTC)) == "Dec 2025"


def test_month_none_em_dash() -> None:
    assert fmt_month(None) == EM_DASH


# ---------------------------------------------------------------------------
# fmt_date_short — the triage queue's 64px date column
# ---------------------------------------------------------------------------


def test_short_date_is_month_and_day() -> None:
    assert fmt_date_short(date(2026, 7, 7), today=date(2026, 7, 20)) == "Jul 7"


def test_short_date_never_carries_a_weekday() -> None:
    """Bank rows have no time component and 204 of 243 live rows share a
    timestamp, so a weekday carries no signal in the dense list."""
    label = fmt_date_short(date(2026, 7, 7), today=date(2026, 7, 20))
    assert "," not in label
    assert "Tue" not in label


def test_short_date_appends_a_two_digit_year_off_the_current_one() -> None:
    assert fmt_date_short(date(2024, 11, 3), today=date(2026, 7, 20)) == "Nov 3 24"


def test_short_date_accepts_a_datetime() -> None:
    got = fmt_date_short(datetime(2026, 3, 2, 14, 30, tzinfo=UTC), today=date(2026, 7, 1))
    assert got == "Mar 2"


def test_short_date_accepts_an_iso_string() -> None:
    assert fmt_date_short("2026-03-02T00:00:00", today=date(2026, 7, 1)) == "Mar 2"


def test_short_date_none_em_dash() -> None:
    assert fmt_date_short(None) == EM_DASH


# ---------------------------------------------------------------------------
# fmt_usd / fmt_native — the SIGNAL money pair (triage redesign)
# ---------------------------------------------------------------------------


def test_usd_negative_uses_a_true_minus_before_the_symbol() -> None:
    assert fmt_usd(Decimal("-1200")) == "−$1,200.00"


def test_usd_positive_is_bare_unless_signed_is_asked_for() -> None:
    assert fmt_usd(Decimal("18.40")) == "$18.40"
    assert fmt_usd(Decimal("18.40"), signed=True) == "+$18.40"


def test_usd_zero_never_carries_a_sign() -> None:
    assert fmt_usd(Decimal("0"), signed=True) == "$0.00"


def test_usd_none_em_dash() -> None:
    assert fmt_usd(None) == EM_DASH


def test_native_ves_uses_a_non_breaking_space() -> None:
    assert fmt_native(Decimal("-45231.10"), "VES") == "−Bs. 45,231.10"


def test_native_usdt_puts_the_ticker_after_the_figure() -> None:
    assert fmt_native(Decimal("277.90"), "USDT") == "277.90 USDT"


def test_native_usd_is_the_dollar_form() -> None:
    assert fmt_native(Decimal("-12.50"), "USD") == "−$12.50"


def test_native_signed_marks_a_credit_with_a_plus() -> None:
    assert fmt_native(Decimal("96.40"), "USDT", signed=True) == "+96.40 USDT"
    assert fmt_native(Decimal("36500"), "VES", signed=True) == "+Bs. 36,500.00"


def test_native_none_em_dash() -> None:
    assert fmt_native(None, "VES") == EM_DASH


def test_the_signal_pair_never_emits_an_ascii_hyphen() -> None:
    """D11 — the design's minus is U+2212. fmt_money keeps the ASCII form
    that the older surfaces and the CSV/CLI exports already render.
    """
    assert "-" not in fmt_usd(Decimal("-1"))
    assert "-" not in fmt_native(Decimal("-1"), "VES")
    assert fmt_money(Decimal("-1")).startswith("-")
