"""Every resolver-emittable rate_source has a styled badge (spec §5.1).

Unknown sources fall through to the raw snake_case string with default slate
styling. That is an acceptable last resort but a bad default for sources we
know the resolver can produce.

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

# Sources rates.resolve can return, including the ADR-013 realized tier that
# arrives with the manual-pair-picker merge (spec §3.1a).
KNOWN_SOURCES = [
    "user_rate",
    "binance_p2p_realized",
    "binance_p2p_realized_carry",
    "binance_p2p_median",
    "binance_p2p_median_carry",
    "bcv",
    "bcv_carry",
    "native_usd",
    "needs_review",
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


@pytest.mark.parametrize("source", KNOWN_SOURCES)
def test_every_known_source_has_a_styled_label(source: str) -> None:
    html = _render_badge(source)

    assert f'data-rate-source="{source}"' in html
    # An unmapped source echoes its own raw name as the label. No mapped
    # source has a label equal to its source string, so this is a clean
    # discriminator.
    assert _label_of(html) != source
