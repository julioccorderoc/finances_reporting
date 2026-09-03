"""The disambiguating test for each category, read from the doc.

``docs/architecture/category-definitions.md`` is authoritative for what
each category *means* — it exists because ADR-006 named the categories
but never wrote down their edges, and the undefined edges (not the
count) were what caused the mis-tagging. The triage picker shows that
sentence at the moment of choosing, which only helps if it is the same
sentence: retyping it into a template would create a second source of
truth that drifts silently the first time an edge is re-ruled.

So the sentences are parsed out of the doc and cached for the process
(criterion K7). The doc's tables are the interface:

    | **Groceries** | Food consumed at home. Supermarkets, ... |

The ``## Expense``, ``## Income`` and ``## Transfer`` sections are read
(the third since migration 022 made the two transfer categories
pickable). The adjustment table lists *meanings* for categories no human
ever picks, and its one row names two categories at once — nothing in a
picker needs it.
"""

from __future__ import annotations

import re
from functools import lru_cache
from types import MappingProxyType
from typing import Mapping

from finances.config import PROJECT_ROOT

DEFINITIONS_PATH = PROJECT_ROOT / "docs" / "architecture" / "category-definitions.md"

#: Sections whose tables define a pickable category. Anything else in the
#: file (history, edge rulings, the adjustment and retired tables) is prose
#: for humans.
_PICKABLE_SECTIONS = ("Expense", "Income", "Transfer")

_HEADING_RE = re.compile(r"^##\s+(?P<title>.+?)\s*$")
_ROW_RE = re.compile(r"^\|\s*\*\*(?P<name>[^*|]+)\*\*\s*\|\s*(?P<test>.+?)\s*\|\s*$")


def _strip_emphasis(text: str) -> str:
    """Drop markdown bold/italic markers, keeping the words."""
    return re.sub(r"\*{1,2}(.+?)\*{1,2}", r"\1", text)


@lru_cache(maxsize=1)
def category_tests() -> Mapping[str, str]:
    """``{category name: disambiguating test}``, parsed once per process.

    Read-only: the mapping is shared by every caller, and the fix for a
    wrong sentence is an edit to the doc, not to a dict at runtime.
    """
    tests: dict[str, str] = {}
    section: str | None = None

    for line in DEFINITIONS_PATH.read_text(encoding="utf-8").splitlines():
        heading = _HEADING_RE.match(line)
        if heading:
            section = heading.group("title")
            continue
        if section not in _PICKABLE_SECTIONS:
            continue
        row = _ROW_RE.match(line)
        if row:
            tests[row.group("name").strip()] = _strip_emphasis(row.group("test"))

    return MappingProxyType(tests)


def definition_for(name: str) -> str | None:
    """The test sentence for ``name``, or ``None`` when the doc has none."""
    return category_tests().get(name)


def missing_tests(names: list[str] | tuple[str, ...]) -> list[str]:
    """Which of ``names`` the doc does not define, in the order given.

    The suite calls this over the pickable set so a category added
    without its edge written down fails loudly and by name. The fix is
    always to write the sentence in the doc.
    """
    defined = category_tests()
    return [name for name in names if name not in defined]


__all__ = ["DEFINITIONS_PATH", "category_tests", "missing_tests", "definition_for"]
