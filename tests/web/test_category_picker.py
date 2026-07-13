"""WP4 — shared category picker partial (tests precede impl per rule-011).

Direct template rendering, same pattern as
tests/web/test_transactions_write.py::test_card_partial_*. DOM contract:

* hidden <input name="set_category" value="false"> — an untouched picker
  can never wipe a category (WP2 safety contract),
* hidden <input name="category_id"> initialised from ``picker_selected``,
* one [data-chip] button per top_categories entry (DOM order = keys 1-8),
* [data-picker-search] type-to-filter input that swallows Enter,
* [data-picker-remove] explicit clear control,
* every active category rendered in the full list ([data-picker-item]).
"""

from __future__ import annotations

from finances.domain.models import Category, TransactionKind
from finances.web.app import create_app
from finances.web.settings import WebSettings


def _mk_cats() -> list[Category]:
    names = [
        "Groceries",
        "Transport",
        "Health",
        "Leisure",
        "Subscriptions",
        "Purchases",
        "Fees",
        "Clothing",
        "Dating",
        "Gifts",
    ]
    return [
        Category(id=i + 1, kind=TransactionKind.EXPENSE, name=name, active=True)
        for i, name in enumerate(names)
    ]


def _render(
    categories: list[Category],
    top: list[Category],
    selected: int | None = None,
) -> str:
    app = create_app(WebSettings(host="127.0.0.1"))
    return app.state.templates.get_template(
        "partials/category_picker.html"
    ).render(categories=categories, top_categories=top, picker_selected=selected)


def test_renders_hidden_inputs_with_safe_defaults() -> None:
    cats = _mk_cats()
    rendered = _render(cats, cats[:8])

    assert "data-category-picker" in rendered
    assert 'name="set_category" value="false"' in rendered  # untouched → never wipes
    assert 'name="category_id"' in rendered


def test_initial_selection_prefills_hidden_input() -> None:
    cats = _mk_cats()
    rendered = _render(cats, cats[:8], selected=3)

    assert 'name="category_id" value="3"' in rendered


def test_renders_one_chip_per_top_category_in_order() -> None:
    cats = _mk_cats()
    rendered = _render(cats, cats[:8])

    assert rendered.count("data-chip=") == 8
    for n in range(1, 9):
        assert f'data-chip="{n}"' in rendered
    # Chip order follows top_categories order.
    assert rendered.index("Groceries") < rendered.index("Transport")


def test_renders_search_input_and_full_list() -> None:
    cats = _mk_cats()
    rendered = _render(cats, cats[:8])

    assert "data-picker-search" in rendered
    # Enter in the search box must never submit the surrounding form.
    assert "@keydown.enter.prevent" in rendered
    for cat in cats:
        assert cat.name in rendered
    assert rendered.count("data-picker-item=") == len(cats)


def test_renders_explicit_remove_control() -> None:
    cats = _mk_cats()
    rendered = _render(cats, cats[:8], selected=1)

    assert "data-picker-remove" in rendered
    assert "remove category" in rendered
