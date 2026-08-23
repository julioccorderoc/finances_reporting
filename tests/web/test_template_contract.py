"""Every template's context requirements, checked against every call site.

This is the layer that makes the 2026-07-26 pair impossible rather than
merely survivable. That outage needed two commits that were individually
fine: a template began reading ``prev_url``, and a router began supplying
it. Ship them together and all is well; ship the template into a process
running the old router and every modal 500s.

A static check kills the whole class. Walk the routers for
``TemplateResponse`` call sites, walk each template (through its
``{% include %}`` and ``{% extends %}`` edges) for the variables it
actually dereferences, and require the second to be covered by the first.
The template half of that pair would have failed CI on its own commit.

The include traversal is the load-bearing part and the reason a naive
version of this test would have missed the incident entirely:
``find_undeclared_variables`` on ``modal_transaction_triage.html`` alone
never mentions ``prev_url``, because ``prev_url`` is read by the
``triage_modal_nav.html`` it includes.

``test_the_contract_detects_the_2026_07_26_regression`` is what keeps this
file honest — a static analyzer that has quietly stopped analyzing passes
every other test here.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Callable, Iterator, NamedTuple

import pytest
from jinja2 import Environment, FileSystemLoader, meta, nodes
from starlette.testclient import TestClient

from finances.web.app import TEMPLATES_DIR

ROUTERS_DIR = Path(__file__).resolve().parents[2] / "finances" / "web" / "routers"

# Names Jinja or Starlette always provides, so no call site has to.
_AMBIENT = {
    "request",
    "url_for",
    "loop",
    "range",
    "dict",
    "namespace",
    "cycler",
    "joiner",
    "lipsum",
}


class CallSite(NamedTuple):
    file: str
    lineno: int
    template: str
    supplied: frozenset[str]

    def __str__(self) -> str:  # pragma: no cover - failure output only
        return f"{self.file}:{self.lineno} -> {self.template}"


def _call_sites() -> list[CallSite]:
    sites: list[CallSite] = []
    for path in sorted(ROUTERS_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not (
                isinstance(func, ast.Attribute)
                and func.attr == "TemplateResponse"
            ):
                continue
            sites.append(
                CallSite(
                    file=path.name,
                    lineno=node.lineno,
                    template=_template_name(node),
                    supplied=frozenset(_context_keys(node)),
                )
            )
    return sites


def _template_name(node: ast.Call) -> str:
    """``TemplateResponse(request, "x.html", {...})`` — the modern signature."""
    for arg in node.args:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            return arg.value
    return ""


def _context_keys(node: ast.Call) -> Iterator[str]:
    for arg in node.args:
        if not isinstance(arg, ast.Dict):
            continue
        for key in arg.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                yield key.value


def _env() -> Environment:
    """The app's own environment, filters included.

    ``find_undeclared_variables`` compiles, so a template referencing a
    filter that no longer exists fails here too.
    """
    from finances.web.app import create_app
    from finances.web.settings import WebSettings

    app = create_app(WebSettings())
    env = app.state.templates.env
    env.loader = FileSystemLoader(str(TEMPLATES_DIR))
    return env


def _target_names(target: nodes.Node) -> set[str]:
    """Names bound by an assignment target.

    ``find_all`` walks children only, so a bare ``{% set x = ... %}`` —
    whose target IS a Name — yields nothing unless the node itself is
    considered. Missing that turns every loop variable into a phantom
    missing-context report.
    """
    names = {node.name for node in target.find_all(nodes.Name)}
    if isinstance(target, nodes.Name):
        names.add(target.name)
    return names


def _locally_bound(tree: nodes.Template) -> set[str]:
    """Names a template binds itself: {% set %}, {% for %}, {% with %}, macros."""
    bound: set[str] = set()
    for node in tree.find_all((nodes.Assign, nodes.AssignBlock)):
        bound |= _target_names(node.target)
    for node in tree.find_all(nodes.For):
        bound |= _target_names(node.target)
    for node in tree.find_all(nodes.With):
        for target in node.targets:
            bound |= _target_names(target)
    for node in tree.find_all(nodes.Macro):
        bound.add(node.name)
    for node in tree.find_all(nodes.FromImport):
        for name in node.names:
            bound.add(name if isinstance(name, str) else name[1])
    for node in tree.find_all(nodes.Import):
        bound.add(node.target)
    return bound


def _optional(tree: nodes.Template) -> set[str]:
    """Names the template itself probes with ``is defined``/``is undefined``.

    A template that asks whether a variable exists has declared it
    optional, so no allowlist has to be maintained by hand.
    """
    optional: set[str] = set()
    for test in tree.find_all(nodes.Test):
        if test.name in {"defined", "undefined"} and isinstance(
            test.node, nodes.Name
        ):
            optional.add(test.node.name)
    return optional


def _child_templates(tree: nodes.Template) -> tuple[list[str], list[str]]:
    """Constant ``{% include %}`` and ``{% extends %}`` targets."""
    includes: list[str] = []
    for node in tree.find_all(nodes.Include):
        target = node.template
        if isinstance(target, nodes.Const):
            value = target.value
            includes.extend(value if isinstance(value, list) else [value])
        elif isinstance(target, nodes.List):
            includes.extend(
                item.value
                for item in target.items
                if isinstance(item, nodes.Const)
            )
    parents = [
        node.template.value
        for node in tree.find_all(nodes.Extends)
        if isinstance(node.template, nodes.Const)
    ]
    return includes, parents


def _requires(name: str, env: Environment, seen: frozenset[str] = frozenset()) -> set[str]:
    """Context variables ``name`` needs, following includes and extends."""
    if name in seen:
        return set()
    seen = seen | {name}

    source = (TEMPLATES_DIR / name).read_text()
    tree = env.parse(source, name=name)

    required = set(meta.find_undeclared_variables(tree))
    bound = _locally_bound(tree)
    required -= _optional(tree) | _AMBIENT | bound

    includes, parents = _child_templates(tree)
    for child in includes:
        # An include inherits the including template's context, so anything
        # the parent binds locally is already available to it.
        required |= _requires(child, env, seen) - bound
    for parent in parents:
        required |= _requires(parent, env, seen)

    return required


@pytest.fixture(scope="module")
def env() -> Environment:
    return _env()


def test_call_sites_supply_every_required_variable(env: Environment) -> None:
    missing: list[str] = []
    for site in _call_sites():
        gap = _requires(site.template, env) - set(site.supplied)
        if gap:
            missing.append(f"{site} missing {sorted(gap)}")

    assert not missing, "template/router context mismatch:\n" + "\n".join(missing)


def test_the_contract_detects_the_2026_07_26_regression(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Prove the checker is not vacuous — the failure mode of static tests.

    Reproduces the incident's exact shape: a template that reads nothing
    unusual itself, but includes one that reads ``prev_url``.
    """
    (tmp_path / "nav.html").write_text("<a>{{ prev_url }}</a>")
    (tmp_path / "modal.html").write_text(
        "<div>{{ txn }}{% include 'nav.html' %}</div>"
    )

    local_env = Environment(loader=FileSystemLoader(str(tmp_path)))
    monkeypatch.setattr(
        "tests.web.test_template_contract.TEMPLATES_DIR", tmp_path
    )

    required = _requires("modal.html", local_env)

    assert "prev_url" in required
    assert required - {"txn"} == {"prev_url"}


def test_every_call_site_is_statically_parseable() -> None:
    """A ``**context`` refactor must fail here, not silently shrink coverage."""
    unparseable = [
        f"{site.file}:{site.lineno}"
        for site in _call_sites()
        if not site.template
    ]

    assert not unparseable, (
        "TemplateResponse call sites whose template name is not a literal: "
        + ", ".join(unparseable)
    )


def test_every_template_file_is_reachable(env: Environment) -> None:
    """Orphans and typo'd include paths.

    A mistyped ``{% include %}`` is a TemplateNotFound at render time
    today — i.e. discovered by the owner, in the middle of a triage run.
    """
    reachable: set[str] = set()
    for site in _call_sites():
        if site.template:
            reachable.add(site.template)

    frontier = list(reachable)
    while frontier:
        name = frontier.pop()
        path = TEMPLATES_DIR / name
        assert path.exists(), f"referenced template does not exist: {name}"
        includes, parents = _child_templates(env.parse(path.read_text(), name=name))
        for child in includes + parents:
            if child not in reachable:
                reachable.add(child)
                frontier.append(child)

    on_disk = {
        str(path.relative_to(TEMPLATES_DIR))
        for path in TEMPLATES_DIR.rglob("*.html")
    }
    orphans = on_disk - reachable - _MACRO_ONLY

    assert not orphans, f"templates nothing renders or includes: {sorted(orphans)}"


# Macro libraries are pulled in with {% import %}, which binds a namespace
# rather than splicing a context — they are reachable, just not by the
# include/extends edges the walk above follows.
_MACRO_ONLY = {"_macros.html", "_icons.html", "_triage.html"}


def test_the_analyzer_sees_the_real_template_tree(env: Environment) -> None:
    """Guard against the walk silently finding nothing to check."""
    sites = _call_sites()

    assert len(sites) >= 15
    assert any(site.template.startswith("partials/") for site in sites)
    assert "prev_url" in _requires("partials/triage_modal.html", env)
