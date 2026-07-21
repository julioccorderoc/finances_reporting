"""Every resolver-emittable rate_source has a styled badge (spec §5.1).

Each known source is checked against its exact expected display label (an
explicit expected-label table, not a proxy comparison). An unmapped source
falls through to the raw snake_case source string with default slate
styling — that fallback path is intentionally NOT exercised here by label
comparison; the point of this table is that a future rate source added to
the resolver but forgotten in the macro's ``label_map`` will render its raw
source string, which will not equal whatever expected label a contributor
adds for it, so the test fails loudly instead of quietly passing.

Rendered through a bare Jinja environment rather than create_app: the macro
uses no custom filters, so there is no need for an app or a database here.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

from finances.format import fmt_date, fmt_money, fmt_month, fmt_number

TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "finances" / "web" / "templates"

# (source, expected visible label) — the resolver-emittable sources
# (including the ADR-013 realized tier that arrives with the
# manual-pair-picker merge, spec §3.1a) paired with the exact label
# label_map is expected to render for each.
KNOWN_SOURCES = [
    ("user_rate", "user"),
    ("binance_p2p_realized", "real"),
    ("binance_p2p_realized_carry", "real~"),
    ("binance_p2p_median", "p2p"),
    ("binance_p2p_median_carry", "p2p~"),
    ("bcv", "bcv"),
    ("bcv_carry", "bcv~"),
    ("native_usd", "native"),
    ("needs_review", "?"),
]


def _render_badge(source: str) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)), autoescape=True)
    # Register the display filters so that _macros.html can be loaded.
    env.filters.update(
        {
            "fmt_number": fmt_number,
            "fmt_money": fmt_money,
            "fmt_date": fmt_date,
            "fmt_month": fmt_month,
        }
    )
    template = env.from_string(
        "{% from '_macros.html' import rate_source_badge %}"
        "{{ rate_source_badge(source) }}"
    )
    return template.render(source=source)


def _label_of(html: str) -> str:
    """Pull the visible label text out of a rendered badge span."""
    match = re.search(r">\s*([^<>]+?)\s*</span>", html)
    assert match is not None, f"no label found in: {html!r}"
    return match.group(1)


@pytest.mark.parametrize("source, expected_label", KNOWN_SOURCES)
def test_known_source_renders_expected_label(source: str, expected_label: str) -> None:
    html = _render_badge(source)

    assert f'data-rate-source="{source}"' in html
    assert _label_of(html) == expected_label
