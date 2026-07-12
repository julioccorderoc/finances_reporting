# WP1 Formatting Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One shared formatting module (`finances/format.py`) becomes the single source of truth for numbers, money, dates and month labels across the web viewer, the static `report.html`, and the dashboard KPI service — grouped US numbers, sign-before-symbol money, weekday dates.

**Architecture:** A new pure-function module `finances/format.py` exports four contract functions (`fmt_number`, `fmt_money`, `fmt_date`, `fmt_month`). They are registered as Jinja filters in the web app factory and in `html_export._jinja_env()`. The existing `_macros.html` helpers (`format_amount`, `format_date`) keep their names but delegate to the filters, so all card rows/modals/triage cards inherit automatically; the remaining inline `'%.2f' | format(...)` and `$`-prefixed sites are swept file by file.

**Tech Stack:** Python 3.13, `decimal.Decimal`, Jinja2 (via FastAPI `Jinja2Templates` and the report's own `Environment`), pytest with the existing tmp-DB fixtures in `tests/web/conftest.py`.

## Global Constraints

- TDD per rule-011 + CLAUDE.md execution rule 5: for every task, the failing-test commit lands BEFORE the implementation commit (`test(scope): ...` then `feat|fix|refactor(scope): ...`).
- Run tests with `uv run pytest -q <path>` — never bare `pytest`.
- Every commit message ends with a second `-m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"`.
- Contract signatures (consumed by WP2–WP6 — do not rename): `fmt_number(value, places=2) -> str`, `fmt_money(value, symbol="$", places=2) -> str`, `fmt_date(value, today=None) -> str`, `fmt_month(value) -> str`; all four registered as Jinja filters with the SAME names in `finances/web/app.py`.
- Display decisions (locked): US grouping `1,234.56`; sign before symbol `-$1,200.00` (never `$-1,200.00`); label currencies `-Bs. 45,231.10`; dates `Mon, Jul 7` with `, YYYY` appended only when year ≠ today's year; months `Jul 2026`; `None` → `—` (em dash, U+2014).
- `fmt_date`'s `today` param exists for testability; it defaults to today in Caracas TZ (`finances.config.CARACAS_TZ`).
- Real expense amounts are NEGATIVE. The `seeded_web_db` fixture stores them positive (known wart) — every formatting test here seeds its own NEGATIVE amounts on the plain `web_db` fixture instead.
- Tests never touch the real `finances.db` — only the tmp-DB fixtures from `tests/conftest.py` / `tests/web/conftest.py`.
- No new dependencies, no CDN assets. No `<table>` for data lists (card-rows only). No new UPDATE SQL anywhere (this WP is read/display only).
- Machine-readable attributes (`data-month="2026-07"`, `datetime="..."`, URLs, chart-JSON rate labels) keep raw ISO values; only human-visible text changes.
- `finances/web/templates/_macros.html` is at the templates ROOT (not `partials/`). The macro names `format_amount` / `format_date` must not change — 20+ call sites import them.

---

### Task 1: `finances/format.py` — the four contract functions

**Files:**
- Create: `finances/format.py`
- Test: `tests/test_format.py` (new)

**Interfaces:**
- Consumes: `finances.config.CARACAS_TZ` (existing, `finances/config.py:19`).
- Produces: `fmt_number(value, places=2) -> str`, `fmt_money(value, symbol="$", places=2) -> str`, `fmt_date(value, today=None) -> str`, `fmt_month(value) -> str`, `EM_DASH: str` — consumed by every later task and by WP2–WP6.

**Steps:**

- [ ] Create the work branch:

```bash
git checkout -b ux-wp1-formatting
```

- [ ] Write the failing unit-test file `tests/test_format.py` (full content):

```python
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

from finances.format import fmt_date, fmt_money, fmt_month, fmt_number

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


def test_month_none_em_dash() -> None:
    assert fmt_month(None) == EM_DASH
```

- [ ] Run it and confirm it fails on the missing module:

```bash
uv run pytest -q tests/test_format.py
```

Expected: collection error — `ModuleNotFoundError: No module named 'finances.format'`.

- [ ] Commit the test:

```bash
git add tests/test_format.py
git commit -m "test(format): pin fmt_number/fmt_money/fmt_date/fmt_month display contract" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] Create `finances/format.py` (full content):

```python
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


__all__ = ["EM_DASH", "fmt_date", "fmt_money", "fmt_month", "fmt_number"]
```

- [ ] Run the tests, expect all green:

```bash
uv run pytest -q tests/test_format.py
```

Expected: `30 passed` (11 fmt_number + 7 fmt_money + 8 fmt_date + 4 fmt_month), 0 failed.

- [ ] Commit the implementation:

```bash
git add finances/format.py
git commit -m "feat(format): shared formatting module (US grouping, sign-before-symbol, weekday dates)" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Register the four Jinja filters in the web app factory

**Files:**
- Modify: `finances/web/app.py` (import block ~line 28; after `app.state.templates = Jinja2Templates(...)` at line 82)
- Test: `tests/web/test_formatting.py` (new)

**Interfaces:**
- Consumes: `finances.format.fmt_number/fmt_money/fmt_date/fmt_month` (Task 1); `create_app(settings)` and `WebSettings` (existing).
- Produces: Jinja filters named exactly `fmt_number`, `fmt_money`, `fmt_date`, `fmt_month` on `app.state.templates.env.filters` — consumed by Tasks 3–5 and by WP2/WP4/WP6 templates.

**Steps:**

- [ ] Write the failing test — create `tests/web/test_formatting.py` (full content):

```python
"""Rendered-formatting tests for the web viewer (UX overhaul WP1).

Covers: fmt_* Jinja filter registration, the rewired _macros.html
helpers, the monthly pivot/mobile sweep, and the sign-before-symbol
fix on $-prefixed USD sites. All tests use the tmp-DB web fixtures
from tests/web/conftest.py — never the real finances.db.

Expenses are seeded NEGATIVE (real sign convention). The shared
``seeded_web_db`` fixture stores expenses positive — a known wart —
so these tests seed their own rows on the plain ``web_db`` fixture.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.format import fmt_date, fmt_money, fmt_month, fmt_number
from finances.web.app import create_app
from finances.web.settings import WebSettings


def _seed_negative_usd_expense(
    conn: sqlite3.Connection,
    *,
    amount: Decimal = Decimal("-1234.56"),
    occurred_at: datetime = datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
    source_ref: str = "fmt-smoke-1",
) -> None:
    """One Cash-USD expense, NEGATIVE per the real sign convention.

    USD is the native_usd rate path — amount_usd == amount, so no rate
    rows are needed. 2024-01-15 is a Monday in a past year, so fmt_date
    must render "Mon, Jan 15, 2024" (weekday + year) deterministically.
    """
    account = accounts_repo.insert(
        conn, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    transactions_repo.insert(
        conn,
        Transaction(
            account_id=account.id,
            occurred_at=occurred_at,
            kind=TransactionKind.EXPENSE,
            amount=amount,
            currency="USD",
            description="formatting smoke",
            source="cash_cli",
            source_ref=source_ref,
        ),
    )


# ---------------------------------------------------------------------------
# Task 2 — filter registration.
# ---------------------------------------------------------------------------


def test_fmt_filters_registered_on_app_templates(web_db_path: Path) -> None:
    app = create_app(WebSettings(host="127.0.0.1", db_path=web_db_path))
    filters = app.state.templates.env.filters
    assert filters["fmt_number"] is fmt_number
    assert filters["fmt_money"] is fmt_money
    assert filters["fmt_date"] is fmt_date
    assert filters["fmt_month"] is fmt_month
```

- [ ] Run it and confirm the failure:

```bash
uv run pytest -q tests/web/test_formatting.py
```

Expected: `1 failed` with `KeyError: 'fmt_number'`.

- [ ] Commit the test:

```bash
git add tests/web/test_formatting.py
git commit -m "test(web): fmt_* Jinja filters registered on app templates" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] Edit `finances/web/app.py`. Add the import (before the `finances.web.*` imports):

```python
from finances.format import fmt_date, fmt_money, fmt_month, fmt_number
from finances.web.auth import BearerTokenMiddleware
```

Then replace the single line `app.state.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))` with:

```python
    app.state.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    # Shared display filters (UX overhaul WP1) — the SAME four names are
    # the cross-plan contract; templates use them directly and via macros.
    app.state.templates.env.filters.update(
        {
            "fmt_number": fmt_number,
            "fmt_money": fmt_money,
            "fmt_date": fmt_date,
            "fmt_month": fmt_month,
        }
    )
```

- [ ] Run, expect green (plus no regression in the app foundation tests):

```bash
uv run pytest -q tests/web/test_formatting.py tests/test_web_app.py
```

Expected: all passed, 0 failed.

- [ ] Commit the implementation:

```bash
git add finances/web/app.py
git commit -m "feat(web): register shared fmt_* formatting filters in create_app" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Rewire `format_amount` / `format_date` macros to the filters

**Files:**
- Modify: `finances/web/templates/_macros.html` (lines 10–27 — note: templates ROOT, not `partials/`)
- Test: `tests/web/test_formatting.py` (append)

**Interfaces:**
- Consumes: Jinja filters from Task 2.
- Produces: macros `format_amount(value, places=2)` and `format_date(dt)` with UNCHANGED names/signatures — all existing importers (`card_transaction.html`, `account_card.html`, `transactions_list.html`, `modal_transaction.html`, `modal_transaction_triage.html`, `modal_pair_confirm.html`, `triage_card_pair.html`, `rates_latest_per_pair.html`) inherit the new formatting with zero call-site churn.

**Steps:**

- [ ] Append the failing smoke test to `tests/web/test_formatting.py`:

```python
# ---------------------------------------------------------------------------
# Task 3 — _macros.html format_amount/format_date delegate to the filters.
# ---------------------------------------------------------------------------


def test_macros_render_grouped_amount_and_weekday_date(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_negative_usd_expense(web_db)
    client = web_client_factory()
    resp = client.get(
        "/transactions",
        params={"date_from": "2024-01-01", "date_to": "2024-01-31"},
    )
    assert resp.status_code == 200
    body = resp.text
    # format_amount → fmt_number: grouped, sign preserved (native column).
    assert "-1,234.56" in body
    # format_date → fmt_date: weekday + year (2024 != current year).
    assert "Mon, Jan 15, 2024" in body
```

- [ ] Run it and confirm the failure:

```bash
uv run pytest -q tests/web/test_formatting.py
```

Expected: `1 failed` — `AssertionError` on `assert '-1,234.56' in body` (the old macro renders ungrouped `-1234.56`).

- [ ] Commit the test:

```bash
git add tests/web/test_formatting.py
git commit -m "test(web): macros render grouped amounts and weekday dates" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] Edit `finances/web/templates/_macros.html`. Replace the two formatting macros (lines 10–27). Old:

```jinja
{%- macro format_amount(value, places=2) -%}
{#- Decimal/float -> human-readable string. None becomes em dash. -#}
{%- if value is none -%}
&mdash;
{%- else -%}
{{ "%.{}f".format(places) | format(value) }}
{%- endif -%}
{%- endmacro -%}

{%- macro format_date(dt) -%}
{%- if dt is none -%}
&mdash;
{%- elif dt.strftime is defined -%}
{{ dt.strftime("%Y-%m-%d") }}
{%- else -%}
{{ dt }}
{%- endif -%}
{%- endmacro -%}
```

New (names/signatures unchanged; None handling lives inside the filters):

```jinja
{%- macro format_amount(value, places=2) -%}
{#- Decimal/float -> grouped, sign-preserving string (finances.format).
    None -> em dash. -#}
{{ value | fmt_number(places) }}
{%- endmacro -%}

{%- macro format_date(dt) -%}
{#- date/datetime/ISO string -> "Mon, Jul 7" (finances.format).
    None -> em dash. -#}
{{ dt | fmt_date }}
{%- endmacro -%}
```

- [ ] Run, expect green, then run the whole web suite to prove no call site broke:

```bash
uv run pytest -q tests/web/test_formatting.py
uv run pytest -q tests/web/
```

Expected: all passed, 0 failed.

- [ ] Commit the implementation:

```bash
git add finances/web/templates/_macros.html
git commit -m "feat(web): rewire format_amount/format_date macros to shared fmt_* filters" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Monthly templates sweep — `'%.2f'` sites + `fmt_month` labels

**Files:**
- Modify: `finances/web/templates/partials/monthly_pivot.html` (lines 19, 47)
- Modify: `finances/web/templates/_macros.html` (`pivot_cell`, lines ~197 and ~200)
- Modify: `finances/web/templates/partials/monthly_mobile_inner.html` (lines 13, 17, 23, 32, 34)
- Modify: `finances/web/templates/partials/monthly_mobile_card.html` (lines 17, 19)
- Test: `tests/web/test_formatting.py` (append)

**Interfaces:**
- Consumes: Jinja filters from Task 2 (`fmt_number`, `fmt_money`, `fmt_month`).
- Produces: no new names — human-visible month labels become `Jan 2024`-style; USD cells/totals become grouped; `data-month`/href values stay raw `YYYY-MM`.

**Steps:**

- [ ] Append the failing tests to `tests/web/test_formatting.py`:

```python
# ---------------------------------------------------------------------------
# Task 4 — /monthly pivot + mobile formatting sweep.
# ---------------------------------------------------------------------------


def test_pivot_month_labels_and_totals_formatted(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_negative_usd_expense(
        web_db, amount=Decimal("-2345.67"), source_ref="fmt-monthly-1"
    )
    client = web_client_factory()
    resp = client.get(
        "/monthly",
        params={
            "layout": "desktop",
            "range_preset": "custom",
            "since": "2024-01",
            "until": "2024-02",
        },
    )
    assert resp.status_code == 200
    body = resp.text
    assert "Jan 2024" in body                # pivot header via fmt_month
    assert "Feb 2024" in body
    assert 'data-month="2024-01"' in body    # machine-readable key untouched
    assert "-2,345.67" in body               # cell + column total via fmt_number


def test_mobile_month_nav_and_total_formatted(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_negative_usd_expense(
        web_db, amount=Decimal("-2345.67"), source_ref="fmt-monthly-2"
    )
    client = web_client_factory()
    resp = client.get(
        "/monthly", params={"layout": "mobile", "month": "2024-01"}
    )
    assert resp.status_code == 200
    body = resp.text
    assert "Jan 2024" in body                # centre month label via fmt_month
    assert "-$2,345.67" in body              # month total via fmt_money
    assert "$-2,345.67" not in body          # sign never after the symbol
```

- [ ] Run and confirm both fail:

```bash
uv run pytest -q tests/web/test_formatting.py
```

Expected: `2 failed` — `assert 'Jan 2024' in body` (headers currently render raw `2024-01`).

- [ ] Commit the tests:

```bash
git add tests/web/test_formatting.py
git commit -m "test(web): monthly pivot and mobile use grouped money and Jul 2026 month labels" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] Edit `finances/web/templates/partials/monthly_pivot.html`. Line 19, old:

```jinja
    <div class="pivot-head" data-month="{{ m }}">{{ m }}</div>
```

New:

```jinja
    <div class="pivot-head" data-month="{{ m }}">{{ m | fmt_month }}</div>
```

Line 47 (footer column total), old:

```jinja
      {{ '%.2f' | format(pivot.totals.column_total_usd[m]) }}
```

New:

```jinja
      {{ pivot.totals.column_total_usd[m] | fmt_number }}
```

- [ ] Edit `finances/web/templates/_macros.html` `pivot_cell`. The `title=` attribute (line ~197), old:

```jinja
  title="USD {{ '%.2f' | format(amount or 0) }}{% if fallback_amount and fallback_amount != 0 %} (+ {{ '%.2f' | format(fallback_amount) }} BCV fallback){% endif %}{% if needs_review_count and needs_review_count > 0 %} · {{ needs_review_count }} needs review{% endif %}"
```

New:

```jinja
  title="USD {{ (amount or 0) | fmt_number }}{% if fallback_amount and fallback_amount != 0 %} (+ {{ fallback_amount | fmt_number }} BCV fallback){% endif %}{% if needs_review_count and needs_review_count > 0 %} · {{ needs_review_count }} needs review{% endif %}"
```

The cell body (lines ~199–201), old:

```jinja
  {%- if amount is none -%}&mdash;
  {%- else -%}{{ '%.2f' | format(amount) }}
  {%- endif -%}
```

New (fmt_number renders None as an em dash itself):

```jinja
  {{ amount | fmt_number }}
```

- [ ] Edit `finances/web/templates/partials/monthly_mobile_inner.html`. Nav labels — hrefs stay raw. Line 13, old:

```jinja
      >&larr; {{ mobile.prev_month }}</a>
```

New:

```jinja
      >&larr; {{ mobile.prev_month | fmt_month }}</a>
```

Line 17, old:

```jinja
    <span class="text-sm font-semibold tabular-nums">{{ mobile.month }}</span>
```

New:

```jinja
    <span class="text-sm font-semibold tabular-nums">{{ mobile.month | fmt_month }}</span>
```

Line 23, old:

```jinja
      >{{ mobile.next_month }} &rarr;</a>
```

New:

```jinja
      >{{ mobile.next_month | fmt_month }} &rarr;</a>
```

Lines 32–34 (month total; `$` prefix folds into fmt_money), old:

```jinja
      ${{ '%.2f' | format(mobile.month_total_usd) }}
      {% if mobile.month_fallback_usd and mobile.month_fallback_usd != 0 %}
        <span class="text-xs text-amber-600 ml-1">(+{{ '%.2f' | format(mobile.month_fallback_usd) }} bcv)</span>
```

New:

```jinja
      {{ mobile.month_total_usd | fmt_money }}
      {% if mobile.month_fallback_usd and mobile.month_fallback_usd != 0 %}
        <span class="text-xs text-amber-600 ml-1">(+{{ mobile.month_fallback_usd | fmt_number }} bcv)</span>
```

- [ ] Edit `finances/web/templates/partials/monthly_mobile_card.html`. Lines 17–19, old:

```jinja
      ${{ '%.2f' | format(category.total_usd) }}
      {% if category.fallback_usd and category.fallback_usd != 0 %}
        <span class="text-[10px] text-amber-600 ml-1">(+{{ '%.2f' | format(category.fallback_usd) }} bcv)</span>
```

New:

```jinja
      {{ category.total_usd | fmt_money }}
      {% if category.fallback_usd and category.fallback_usd != 0 %}
        <span class="text-[10px] text-amber-600 ml-1">(+{{ category.fallback_usd | fmt_number }} bcv)</span>
```

- [ ] Verify the sweep is complete — this must print nothing:

```bash
grep -rn "'%.2f'" finances/web/templates/
```

Expected: no output (exit code 1).

- [ ] Run, expect green:

```bash
uv run pytest -q tests/web/test_formatting.py tests/web/test_monthly.py
```

Expected: all passed, 0 failed.

- [ ] Commit the implementation:

```bash
git add finances/web/templates/partials/monthly_pivot.html finances/web/templates/_macros.html finances/web/templates/partials/monthly_mobile_inner.html finances/web/templates/partials/monthly_mobile_card.html
git commit -m "feat(web): monthly templates use fmt_money/fmt_number/fmt_month" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Sign-before-symbol fix — the `${{ format_amount(...) }}` sites

This is where the `$-1,234.56` bug actually lives in the viewer (verified: `dashboard._format_money` already emits `-$...`; the templates hard-prefix `$` in front of a signed number). Each site becomes a `fmt_money` call.

**Files:**
- Modify: `finances/web/templates/partials/card_transaction.html` (line 52)
- Modify: `finances/web/templates/partials/account_card.html` (line 36)
- Modify: `finances/web/templates/partials/modal_transaction.html` (line 43)
- Modify: `finances/web/templates/partials/modal_transaction_triage.html` (line 36)
- Modify: `finances/web/templates/partials/modal_pair_confirm.html` (lines 45, 64)
- Test: `tests/web/test_formatting.py` (append)

**Interfaces:**
- Consumes: `fmt_money` filter (Task 2).
- Produces: no new names — USD-equivalent amounts on transaction cards, account cards and modals render `-$1,234.56`.

**Steps:**

- [ ] Append the failing tests to `tests/web/test_formatting.py`:

```python
# ---------------------------------------------------------------------------
# Task 5 — sign before symbol on $-prefixed USD sites.
# ---------------------------------------------------------------------------


def test_transactions_list_usd_sign_before_symbol(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_negative_usd_expense(web_db, source_ref="fmt-sign-1")
    client = web_client_factory()
    resp = client.get(
        "/transactions",
        params={"date_from": "2024-01-01", "date_to": "2024-01-31"},
    )
    assert resp.status_code == 200
    body = resp.text
    assert "-$1,234.56" in body
    assert "$-1,234.56" not in body


def test_transaction_modal_usd_sign_before_symbol(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_negative_usd_expense(web_db, source_ref="fmt-sign-2")
    txn_id = web_db.execute(
        "SELECT id FROM transactions WHERE source_ref = 'fmt-sign-2'"
    ).fetchone()["id"]
    client = web_client_factory()
    resp = client.get(f"/_partial/transactions/{txn_id}/modal")
    assert resp.status_code == 200
    assert "-$1,234.56" in resp.text
    assert "$-1,234.56" not in resp.text


def test_accounts_page_usd_sign_before_symbol(
    web_db: sqlite3.Connection, web_client_factory
) -> None:
    _seed_negative_usd_expense(web_db, source_ref="fmt-sign-3")
    client = web_client_factory()
    resp = client.get("/accounts")
    assert resp.status_code == 200
    body = resp.text
    assert "-$1,234.56" in body      # balance_usdt via fmt_money
    assert "$-1,234.56" not in body
```

- [ ] Run and confirm all three fail:

```bash
uv run pytest -q tests/web/test_formatting.py
```

Expected: `3 failed` — each on `assert '-$1,234.56' in ...` (bodies currently contain `$-1,234.56`).

- [ ] Commit the tests:

```bash
git add tests/web/test_formatting.py
git commit -m "test(web): USD amounts render sign before symbol on cards, modals, accounts" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] Edit `finances/web/templates/partials/card_transaction.html` line 52. Old:

```jinja
        <span>${{ format_amount(card.amount_usd) }}</span>
```

New:

```jinja
        <span>{{ card.amount_usd | fmt_money }}</span>
```

- [ ] Edit `finances/web/templates/partials/account_card.html` line 36. Old:

```jinja
        ${{ format_amount(card.balance_usdt) }} <span class="not-italic">USDT</span>
```

New:

```jinja
        {{ card.balance_usdt | fmt_money }} <span class="not-italic">USDT</span>
```

- [ ] Edit `finances/web/templates/partials/modal_transaction.html` line 43. Old:

```jinja
          <span class="ml-2 text-sm text-slate-500">${{ format_amount(card.amount_usd) }}</span>
```

New:

```jinja
          <span class="ml-2 text-sm text-slate-500">{{ card.amount_usd | fmt_money }}</span>
```

- [ ] Edit `finances/web/templates/partials/modal_transaction_triage.html` line 36 — identical old/new strings as the previous step.

- [ ] Edit `finances/web/templates/partials/modal_pair_confirm.html`. Line 45, old:

```jinja
            ≈ ${{ format_amount(deposit.amount_usd) }}
```

New:

```jinja
            ≈ {{ deposit.amount_usd | fmt_money }}
```

Line 64, old:

```jinja
            ≈ ${{ format_amount(sell.amount_usd) }}
```

New:

```jinja
            ≈ {{ sell.amount_usd | fmt_money }}
```

- [ ] Verify no `$`-prefixed macro call remains — this must print nothing:

```bash
grep -rn '\${{ format_amount' finances/web/templates/
```

Expected: no output (exit code 1).

- [ ] Run, expect green (whole web suite — modals/triage/accounts have existing tests):

```bash
uv run pytest -q tests/web/
```

Expected: all passed, 0 failed.

- [ ] Commit the implementation:

```bash
git add finances/web/templates/partials/card_transaction.html finances/web/templates/partials/account_card.html finances/web/templates/partials/modal_transaction.html finances/web/templates/partials/modal_transaction_triage.html finances/web/templates/partials/modal_pair_confirm.html
git commit -m "fix(web): sign before symbol for USD amounts (-\$1,234.56, never \$-1,234.56)" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: `services/dashboard.py` — replace `_format_money` with shared `fmt_money`

`dashboard._format_money` (lines 63–77) already emits `-$1,200.00`, but it is parallel formatting logic; it gets deleted and every call site switches to `finances.format.fmt_money`. The regression tests lock the `-$` ordering and grouping so the swap can never silently change tile output.

**Files:**
- Modify: `finances/web/services/dashboard.py` (delete lines 63–77; 6 call sites at lines 102, 103, 104, 116, 146, 152; add one import)
- Test: `tests/web/test_formatting.py` (append)

**Interfaces:**
- Consumes: `finances.format.fmt_money` (Task 1); `build_kpis(conn, *, today) -> KpiTiles` (existing).
- Produces: `dashboard.fmt_money` (module attribute, the shared function) — the identity assertion below is the single-source-of-truth guard.

**Steps:**

- [ ] Append the failing tests to `tests/web/test_formatting.py`:

```python
# ---------------------------------------------------------------------------
# Task 6 — dashboard KPI money via the shared formatter.
# ---------------------------------------------------------------------------


def test_dashboard_money_is_shared_formatter() -> None:
    from finances.web.services import dashboard

    # Single source of truth: no module-private formatter left behind.
    assert dashboard.fmt_money is fmt_money
    assert not hasattr(dashboard, "_format_money")


def test_kpi_tiles_sign_before_symbol_and_grouped(
    web_db: sqlite3.Connection,
) -> None:
    from finances.web.services.dashboard import build_kpis

    _seed_negative_usd_expense(
        web_db,
        amount=Decimal("-1234567.89"),
        occurred_at=datetime.now(tz=UTC),
        source_ref="fmt-kpi-1",
    )
    kpis = build_kpis(web_db, today=datetime.now(tz=UTC).date())
    # >1M, negative, grouped, sign BEFORE the symbol.
    assert kpis.month_spend.value == "-$1,234,567.89"
    for tile in (kpis.net_worth, kpis.month_spend, kpis.month_income):
        assert "$-" not in tile.value
        assert "$-" not in (tile.hint or "")
```

- [ ] Run and confirm the failure:

```bash
uv run pytest -q tests/web/test_formatting.py
```

Expected: `1 failed` — `AttributeError: module 'finances.web.services.dashboard' has no attribute 'fmt_money'` (the behavioural test passes today and stands as the regression lock for the swap).

- [ ] Commit the tests:

```bash
git add tests/web/test_formatting.py
git commit -m "test(web): dashboard KPI money comes from finances.format, -\$ ordering locked" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] Edit `finances/web/services/dashboard.py`. Add the import between the existing repo and report imports:

```python
from finances.db.repos import accounts as accounts_repo
from finances.format import fmt_money
from finances.reports import monthly as monthly_report
```

Delete the whole `_format_money` function (lines 63–77):

```python
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
```

Replace all six call sites `_format_money(` → `fmt_money(`; the two multi-line spots become:

```python
    return (
        f"Bank {fmt_money(bank)} · "
        f"Crypto {fmt_money(crypto)} · "
        f"Cash {fmt_money(cash)}"
    )
```

```python
    return KpiTile(
        label="Net worth",
        value=fmt_money(nw.total_usdt),
        hint=hint,
        severity=severity,
    )
```

and in `_build_month_kind_tile`:

```python
    if fallback != 0:
        hint_parts.append(f"BCV-only fallback {fmt_money(fallback)}")
```

```python
    return KpiTile(
        label=label,
        value=fmt_money(total),
        hint=hint,
    )
```

- [ ] Confirm nothing references the deleted helper — this must print nothing:

```bash
grep -rn "_format_money" finances/web/
```

Expected: no output (exit code 1).

- [ ] Run, expect green including the existing dashboard suite:

```bash
uv run pytest -q tests/web/test_formatting.py tests/web/test_dashboard.py
```

Expected: all passed, 0 failed.

- [ ] Commit the implementation:

```bash
git add finances/web/services/dashboard.py
git commit -m "refactor(web): dashboard KPI tiles format via finances.format.fmt_money" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Static report — `html_export.py` + `report.html.j2` use the shared helpers

**Files:**
- Modify: `finances/reports/html_export.py` (delete `_format_money` lines 352–361; rewire `_jinja_env` ~line 376; month labels in `_chart_data_json` ~line 401; add one import)
- Modify: `finances/reports/templates/report.html.j2` (all `| money` sites; date/native/month text sites)
- Test: `tests/web/test_html_export_formatting.py` (new)

**Interfaces:**
- Consumes: all four `finances.format` functions (Task 1); `html_export.build_report_context` / `render_html` (existing).
- Produces: report Jinja env filters `fmt_number`/`fmt_money`/`fmt_date`/`fmt_month` (the `money` filter is removed; `report.html.j2` is its only consumer and is updated in the same commit).

**Steps:**

- [ ] Write the failing test file `tests/web/test_html_export_formatting.py` (full content; lives under `tests/web/` to reuse the `web_db` tmp-DB fixture):

```python
"""Static report formatting matches the viewer (UX overhaul WP1).

The static ``report.html`` must format money/dates/months through
``finances.format`` — the same single source of truth the live viewer
uses — so both surfaces render identically by construction.
"""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal

from finances import format as fmt
from finances.db.repos import accounts as accounts_repo
from finances.db.repos import transactions as transactions_repo
from finances.domain.models import (
    Account,
    AccountKind,
    Transaction,
    TransactionKind,
)
from finances.reports import html_export


def test_export_env_registers_shared_filters() -> None:
    env = html_export._jinja_env()
    assert env.filters["fmt_money"] is fmt.fmt_money
    assert env.filters["fmt_number"] is fmt.fmt_number
    assert env.filters["fmt_date"] is fmt.fmt_date
    assert env.filters["fmt_month"] is fmt.fmt_month


def _seed(conn: sqlite3.Connection) -> None:
    account = accounts_repo.insert(
        conn, Account(name="Cash USD", kind=AccountKind.CASH, currency="USD")
    )
    transactions_repo.insert(
        conn,
        Transaction(
            account_id=account.id,
            occurred_at=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),  # a Monday
            kind=TransactionKind.EXPENSE,
            amount=Decimal("-1234.56"),  # real convention: expenses negative
            currency="USD",
            description="report formatting smoke",
            source="cash_cli",
            source_ref="fmt-report-1",
        ),
    )


def test_report_renders_shared_formatting(web_db: sqlite3.Connection) -> None:
    _seed(web_db)
    now = datetime.now(tz=UTC)
    ctx = html_export.build_report_context(web_db, now=now)
    html = html_export.render_html(ctx, chartjs_source="/* stub */")
    # Recent-transaction date: weekday + year (2024 is a past year).
    assert "Mon, Jan 15, 2024" in html
    # USD money grouped, sign BEFORE the symbol (recent row + account tile).
    assert "-$1,234.56" in html
    assert "$-1,234.56" not in html
    # Native amount grouped.
    assert "-1,234.56" in html
    # Month headings via fmt_month ("Jul 2026"-style; %b is English in the
    # C locale, so this expectation is independent of finances.format).
    assert now.strftime("%b %Y") in html
```

- [ ] Run and confirm both fail:

```bash
uv run pytest -q tests/web/test_html_export_formatting.py
```

Expected: `2 failed` — `KeyError: 'fmt_money'` and `AssertionError` on `assert 'Mon, Jan 15, 2024' in html`.

- [ ] Commit the test:

```bash
git add tests/web/test_html_export_formatting.py
git commit -m "test(reports): static report formats via finances.format" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

- [ ] Edit `finances/reports/html_export.py`. Add the import above the existing `finances.reports` import:

```python
from finances.format import fmt_date, fmt_money, fmt_month, fmt_number
from finances.reports import monthly as monthly_report
```

Delete `_format_money` entirely (lines 352–361):

```python
def _format_money(value: Decimal | None) -> str:
    """Format a Decimal as ``$X,XXX.XX`` (or ``-$…``); ``None`` → em dash."""
    if value is None:
        return "—"
    quant = value.quantize(Decimal("0.01"))
    sign = "-" if quant < 0 else ""
    int_part, _, frac_part = format(abs(quant), "f").partition(".")
    frac_part = (frac_part + "00")[:2]
    grouped = "{:,}".format(int(int_part))
    return f"{sign}${grouped}.{frac_part}"
```

In `_jinja_env()` replace `env.filters["money"] = _format_money` with:

```python
        env.filters.update(
            {
                "fmt_number": fmt_number,
                "fmt_money": fmt_money,
                "fmt_date": fmt_date,
                "fmt_month": fmt_month,
            }
        )
```

In `_chart_data_json()` make the monthly bar-chart labels human ("Jul 2026") — old:

```python
        "monthly": {
            "labels": [f.month for f in context.monthly_flows],
```

New:

```python
        "monthly": {
            "labels": [fmt_month(f.month) for f in context.monthly_flows],
```

(The rates chart keeps ISO date labels — daily granularity, not months.)

- [ ] Edit `finances/reports/templates/report.html.j2`. Replace every `| money` with `| fmt_money` (8 sites — lines 115, 149, 161, 165, 170, 183, 191, 226), e.g. old:

```jinja
          <div class="netfig {{ 'neg' if ctx.net_worth_usd < 0 else '' }}">{{ ctx.net_worth_usd | money }}</div>
```

New:

```jinja
          <div class="netfig {{ 'neg' if ctx.net_worth_usd < 0 else '' }}">{{ ctx.net_worth_usd | fmt_money }}</div>
```

Freshness chip date (line 134), old:

```jinja
          <span>{% if f.last_txn_date %}{{ f.last_txn_date.isoformat() }} ({{ f.days_old }}d){% else %}never{% endif %}</span>
```

New:

```jinja
          <span>{% if f.last_txn_date %}{{ f.last_txn_date | fmt_date }} ({{ f.days_old }}d){% else %}never{% endif %}</span>
```

Account tile native balance (line 148), old:

```jinja
        <div class="sub">{{ a.balance_native }} {{ a.currency }}</div>
```

New:

```jinja
        <div class="sub">{{ a.balance_native | fmt_number }} {{ a.currency }}</div>
```

Category-breakdown month headings (lines 181 and 189), old:

```jinja
        <div class="row-between"><strong>{{ ctx.current_month }}</strong><span class="muted">current</span></div>
```

```jinja
        <div class="row-between"><strong>{{ ctx.prev_month }}</strong><span class="muted">previous</span></div>
```

New:

```jinja
        <div class="row-between"><strong>{{ ctx.current_month | fmt_month }}</strong><span class="muted">current</span></div>
```

```jinja
        <div class="row-between"><strong>{{ ctx.prev_month | fmt_month }}</strong><span class="muted">previous</span></div>
```

Recent-transaction row date + native amount (lines 219 and 225), old:

```jinja
        <span class="date">{{ t.occurred_at.strftime('%Y-%m-%d') }}</span>
```

```jinja
          {{ t.amount_native }} {{ t.currency }}
```

New:

```jinja
        <span class="date">{{ t.occurred_at | fmt_date }}</span>
```

```jinja
          {{ t.amount_native | fmt_number }} {{ t.currency }}
```

Deliberately UNCHANGED (technical timestamps, not dates): the `<title>` (`ctx.generated_at.strftime('%Y-%m-%d')`, line 6) and the header `Generated {{ ctx.generated_at.strftime('%Y-%m-%d %H:%M') }}` (line 118).

- [ ] Verify no old filter usage remains — both must print nothing:

```bash
grep -n "| money" finances/reports/templates/report.html.j2
grep -rn "_format_money" finances/
```

Expected: no output from either (exit code 1).

- [ ] Run, expect green including the existing export + CLI suites:

```bash
uv run pytest -q tests/web/test_html_export_formatting.py tests/web/test_html_export.py tests/test_html_cli.py
```

Expected: all passed, 0 failed.

- [ ] Commit the implementation:

```bash
git add finances/reports/html_export.py finances/reports/templates/report.html.j2
git commit -m "feat(reports): static report formats via shared fmt_* filters" -m "Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: Full-suite gate + verification checklist

**Files:** none created/modified — verification only.

**Interfaces:** none — confirms every earlier task's product coexists with the whole suite.

**Steps:**

- [ ] Full unit suite green:

```bash
uv run pytest -q
```

Expected: `0 failed` (the integration tests in `tests/integration/` run too — they are hermetic (mocked HTTP, tmp DBs) and must also pass; nothing in the suite touches the real `finances.db` or live credentials).

- [ ] Sweep-complete greps — each must print nothing (exit code 1):

```bash
grep -rn "'%.2f'" finances/web/templates/
grep -rn "_format_money" finances/
grep -rn '\${{ format_amount' finances/web/templates/
```

- [ ] Coverage guard on the new module:

```bash
uv run pytest --cov=finances.format -q tests/test_format.py
```

Expected: `finances/format.py` at 100% (it is pure branching logic; every branch has a test above).

- [ ] Manual verification for Julio (execution rule 3 — the agent never marks this complete): start the viewer against the real DB read-only (`uv run finances serve`, or double-click `Finances.command`), then eyeball: dashboard KPI tiles show `-$…` grouped; `/transactions` dates show weekday (`Mon, Jul 7`) and native amounts grouped; `/monthly` headers show `Jul 2026`; `/accounts` negative USDT balances show `-$…`; regenerate `report.html` (happens automatically on server shutdown) and confirm it matches the viewer.
- [ ] STOP. Julio decides merge (per superpowers:finishing-a-development-branch): the branch is `ux-wp1-formatting`.

## Verification Criteria (gate for the whole work package)

- [ ] `finances/format.py` exports exactly the four contract functions with the contract signatures; `uv run pytest -q tests/test_format.py` green.
- [ ] The four filters are registered under the SAME names in `create_app` (WP2/WP4/WP6 templates will use them).
- [ ] `format_amount` / `format_date` macro names unchanged; zero call-site churn outside the `$`-prefix fixes.
- [ ] No `'%.2f'`, no `_format_money`, no `${{ format_amount` anywhere under `finances/`.
- [ ] Negative amounts render `-$1,234.56` / `-1,234.56` everywhere (viewer pages, modals, KPI tiles, static report); a seeded-negative regression test covers each surface.
- [ ] Dates render `Mon, Jul 7` (year appended only across year boundaries); month labels render `Jul 2026`; `data-month`/hrefs/`datetime=` attributes stay ISO.
- [ ] Full `uv run pytest -q` green.
