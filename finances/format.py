"""Shared display formatting — single source of truth (UX overhaul WP1).

Every user-facing surface (web viewer templates, static ``report.html``
export, CLI summaries) formats numbers, money, dates and month labels
through these four functions. Never reimplement grouping/sign/date
logic at a call site (same spirit as rule-012: no parallel logic).

Locked display decisions (docs/plans/ux-overhaul/00-design.md):

* US grouping: ``1,234.56``.
* Sign before symbol: ``-$1,200.00`` (never ``$-1,200.00``); label
  currencies too: ``-Bs. 45,231.10``.
* Dates: English abbreviated weekday, ``Mon, Jul 7``; ``, YYYY`` is
  appended only when the year differs from today's.
* Months: ``Jul 2026``.
* ``None`` renders as an em dash.

Weekday/month names are hard-coded English tables (not ``strftime``)
so output never depends on the process locale.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal

from finances import config

EM_DASH = "—"  # em dash "—"

_DAY_ABBR = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
_MONTH_ABBR = (
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
)


def fmt_number(value: Decimal | float | int | None, places: int = 2) -> str:
    """Grouped, sign-preserving number: ``-1,234.56``. ``None`` → em dash."""
    if value is None:
        return EM_DASH
    dec = value if isinstance(value, Decimal) else Decimal(str(value))
    quantum = Decimal(1).scaleb(-places)  # places=2 → Decimal("0.01")
    quantized = dec.quantize(quantum, rounding=ROUND_HALF_UP)
    if quantized.is_zero():
        quantized = abs(quantized)  # never render "-0.00"
    return format(quantized, ",f")


def fmt_money(
    value: Decimal | float | int | None,
    symbol: str = "$",
    places: int = 2,
) -> str:
    """Money with the sign BEFORE the symbol: ``-$1,200.00``.

    ``fmt_money(v, symbol="Bs. ")`` → ``-Bs. 45,231.10``. ``None`` → em dash.
    """
    if value is None:
        return EM_DASH
    number = fmt_number(value, places)
    if number.startswith("-"):
        return f"-{symbol}{number[1:]}"
    return f"{symbol}{number}"


#: The typographic minus (U+2212), not the hyphen a keyboard produces. It
#: is the width of a plus and sits on the digit baseline, which is why the
#: design specifies it for every figure on the triage surface (D11).
MINUS = "−"

#: ``Bs.`` and its amount are one token and must not break across a line.
NBSP = " "

#: Currencies whose ticker follows the figure instead of leading it.
_SUFFIX_CURRENCIES = ("USDT", "USDC")


def _signed(number: str, *, signed: bool) -> tuple[str, str]:
    """Split ``fmt_number`` output into (sign glyph, digits)."""
    if number.startswith("-"):
        return MINUS, number[1:]
    if signed and any(ch in "123456789" for ch in number):
        return "+", number
    return "", number


def fmt_usd(
    value: Decimal | float | int | None,
    *,
    signed: bool = False,
    places: int = 2,
) -> str:
    """``−$1,200.00`` / ``+$18.40`` — the SIGNAL dollar figure.

    Three differences from :func:`fmt_money`, all of them design
    decisions rather than taste (README §"Money", criteria D11 and I2):

    * the minus is U+2212, not an ASCII hyphen;
    * ``signed=True`` prefixes a credit with ``+``, because positive
      money is rendered in ink and needs the sign to read as a credit —
      colouring it green (or red) is what this system refuses to do;
    * zero is never signed.

    ``fmt_money`` keeps the ASCII form: the transactions, monthly and
    accounts pages, the static report, the CSV export and the CLI all
    render through it, and none of them is being redesigned here.
    """
    if value is None:
        return EM_DASH
    sign, digits = _signed(fmt_number(value, places), signed=signed)
    return f"{sign}${digits}"


def fmt_native(
    value: Decimal | float | int | None,
    currency: str,
    *,
    signed: bool = False,
    places: int = 2,
) -> str:
    """The amount as the account holds it: ``Bs. 45,231.10``, ``277.90 USDT``.

    The second line of the money block, under the consolidated dollar
    figure. Bolívares lead with ``Bs.`` and a non-breaking space; USDT
    and USDC trail their ticker; everything else is a dollar figure.
    """
    if value is None:
        return EM_DASH
    ticker = (currency or "").upper()
    if ticker in _SUFFIX_CURRENCIES:
        sign, digits = _signed(fmt_number(value, places), signed=signed)
        return f"{sign}{digits}{NBSP}{ticker}"
    if ticker == "VES":
        sign, digits = _signed(fmt_number(value, places), signed=signed)
        return f"{sign}Bs.{NBSP}{digits}"
    return fmt_usd(value, signed=signed, places=places)


def fmt_date(
    value: date | datetime | str | None,
    today: date | None = None,
) -> str:
    """``Mon, Jul 7``; appends ``, YYYY`` only when year != today's year.

    Accepts ``date``, ``datetime`` (its ``.date()`` is used, no timezone
    conversion — same behaviour as the old macro), or an ISO string
    (``YYYY-MM-DD...``). ``None`` → em dash. ``today`` is injectable for
    tests; defaults to today in Caracas (``finances.config.CARACAS_TZ``).
    """
    if value is None:
        return EM_DASH
    if isinstance(value, datetime):  # must precede the date check (subclass)
        d = value.date()
    elif isinstance(value, date):
        d = value
    else:
        d = date.fromisoformat(str(value)[:10])
    if today is None:
        today = datetime.now(tz=config.CARACAS_TZ).date()
    label = f"{_DAY_ABBR[d.weekday()]}, {_MONTH_ABBR[d.month - 1]} {d.day}"
    if d.year != today.year:
        label = f"{label}, {d.year}"
    return label


def fmt_date_short(
    value: date | datetime | str | None,
    today: date | None = None,
) -> str:
    """``Jul 7`` — the dense form, with ``24`` appended off the current year.

    The triage queue's date column is 64px of mono and must never wrap
    (criterion A10). It also carries no weekday and no "Today /
    Yesterday": Provincial rows have no time component and 204 of 243
    live rows share a timestamp, so a day label is decoration that
    happens to look like information (criterion A5).

    Same accepted inputs and same ``today`` injection as :func:`fmt_date`,
    so the two cannot disagree about which year is the current one.
    """
    if value is None:
        return EM_DASH
    if isinstance(value, datetime):  # must precede the date check (subclass)
        d = value.date()
    elif isinstance(value, date):
        d = value
    else:
        d = date.fromisoformat(str(value)[:10])
    if today is None:
        today = datetime.now(tz=config.CARACAS_TZ).date()
    label = f"{_MONTH_ABBR[d.month - 1]} {d.day}"
    if d.year != today.year:
        label = f"{label} {d.year % 100:02d}"
    return label


def fmt_month(value: date | datetime | str | None) -> str:
    """``"2026-07"`` (or a date/datetime) → ``Jul 2026``. ``None`` → em dash."""
    if value is None:
        return EM_DASH
    if isinstance(value, datetime):
        value = value.date()
    if isinstance(value, date):
        return f"{_MONTH_ABBR[value.month - 1]} {value.year}"
    year_str, month_str = str(value).split("-")[:2]
    return f"{_MONTH_ABBR[int(month_str) - 1]} {int(year_str)}"


# A run of this many digits or more marks a token as a reference number
# rather than a name. Three is too aggressive (a street number survives);
# four is where Provincial's own codes start (``CAR.DRV0013196230``).
_CODE_DIGIT_RUN = 4

_CODE_RUN = re.compile(rf"\d{{{_CODE_DIGIT_RUN},}}")
_WORD = re.compile(r"[^\W\d_]{3,}", re.UNICODE)


def clean_merchant(raw: str | None) -> str | None:
    """A readable merchant name from a bank string, or ``None``.

    Provincial writes every description in caps, and three different kinds
    of thing land in the same column: real names
    (``LUNCHERIA MILY GOURMET``), bank jargon (``COM. PAGO MOVIL``) and
    pure references (``CAR.DRV0013196230``, ``TRAV0031264379000156203``).
    Only the first two read as anything when title-cased; the third is a
    number wearing letters.

    So this is deliberately a *typographic* cleanup and nothing more. It
    title-cases a string that already looks like a name and declines
    everything else, rather than inferring a canonical merchant identity —
    the repo has no merchant table, no mapping, and no basis for guessing
    which of two strings is the same shop. ``None`` is a supported answer:
    the raw string then stands on its own, which is what the triage design
    specifies for a row with no cleaned name.

    Deliberately not applied to a string that is already mixed-case: that
    is a Binance memo or a hand-typed cash note, and it is already
    readable.
    """
    if raw is None:
        return None
    text = raw.strip()
    if not text or text != text.upper():
        return None
    if _CODE_RUN.search(text):
        return None
    if len(_WORD.findall(text)) < 2:
        return None
    return text.title()


__all__ = [
    "EM_DASH",
    "MINUS",
    "NBSP",
    "clean_merchant",
    "fmt_date",
    "fmt_date_short",
    "fmt_money",
    "fmt_month",
    "fmt_native",
    "fmt_number",
    "fmt_usd",
]
