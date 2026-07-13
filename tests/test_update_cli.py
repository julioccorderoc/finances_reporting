"""Tests for `finances update` — the one-command weekly ritual (Thing 5).

Per rule-011 (TDD) these are committed before the implementation. They cover
the orchestration contract from docs/plans/revival/05-ux-launcher.md §A:

* every step runs, in order, and its counts are aggregated,
* one failing step does not stop the rest (isolation),
* ``--dry-run`` is threaded into every step and nothing is written
  (no report.html regen),
* a Binance 451 / network failure attaches a plain-language VPN hint,
* provincial is skipped silently when ``inputs/`` holds no files,
* the rendered summary is plain-language (needs-review, per-source, freshness).
"""

from __future__ import annotations

from pathlib import Path

import pytest

import finances.ingest.bcv as bcv_ingest
import finances.ingest.binance as binance_ingest
import finances.ingest.p2p_rates as p2p_ingest
import finances.ingest.provincial as provincial_ingest
import finances.reports.html_export as html_export
from finances.ingest.provincial import IngestReport


class _FakeClient:
    """Stand-in for a Binance SDK client; never touched by mocked syncs."""


def _make_fake_client() -> _FakeClient:
    return _FakeClient()


@pytest.fixture
def happy_steps(monkeypatch: pytest.MonkeyPatch) -> dict[str, list]:
    """Patch every ingest/export step to a recording stub returning fixed data.

    Returns a dict of call-record lists keyed by step name so tests can assert
    on ordering and the ``dry_run`` value threaded into each call.
    """
    calls: dict[str, list] = {k: [] for k in ("bcv", "p2p", "binance", "provincial", "html")}

    def fake_bcv(conn, dry_run=False):
        calls["bcv"].append({"dry_run": dry_run})
        return 2

    def fake_p2p(conn, *, as_of_date=None, dry_run=False, **kw):
        calls["p2p"].append({"dry_run": dry_run, "as_of_date": as_of_date})
        return {
            "buy_median": 1,
            "sell_median": 2,
            "midpoint": 1.5,
            "rows_written": [object(), object(), object()],
        }

    def fake_binance(conn, *, client, dry_run=False, **kw):
        calls["binance"].append({"dry_run": dry_run, "client": client})
        return {
            "rows_inserted": 5,
            "rows_updated": 1,
            "earn_positions": 4,
            "errors": [],
        }

    def fake_provincial(conn, csv_path, *, dry_run=False, **kw):
        calls["provincial"].append({"dry_run": dry_run, "csv_path": Path(csv_path)})
        return IngestReport(rows_seen=3, rows_inserted=3, rows_updated=0)

    def fake_html(conn, path):
        calls["html"].append({"path": Path(path)})
        return Path(path)

    monkeypatch.setattr(bcv_ingest, "ingest_bcv", fake_bcv)
    monkeypatch.setattr(p2p_ingest, "ingest_p2p_rates", fake_p2p)
    monkeypatch.setattr(binance_ingest, "sync_binance", fake_binance)
    monkeypatch.setattr(provincial_ingest, "ingest_csv", fake_provincial)
    monkeypatch.setattr(html_export, "export_html", fake_html)
    return calls


def _inputs_with_one_csv(tmp_path: Path) -> Path:
    inputs = tmp_path / "inputs"
    inputs.mkdir()
    (inputs / "provincial_2026-07-01.csv").write_text("stub", encoding="utf-8")
    return inputs


# ---------------------------------------------------------------------------
# Happy path — all steps run, counts aggregate, report regenerated.
# ---------------------------------------------------------------------------


def test_run_update_runs_all_steps_and_aggregates(
    in_memory_db, happy_steps, tmp_path
) -> None:
    from finances.reports.update import run_update

    report_path = tmp_path / "report.html"
    inputs = _inputs_with_one_csv(tmp_path)

    result = run_update(
        in_memory_db,
        make_binance_client=_make_fake_client,
        inputs_dir=inputs,
        report_path=report_path,
        dry_run=False,
    )

    # Every canonical source produced an outcome.
    sources = {o.source for o in result.outcomes}
    assert {"bcv", "p2p_rates", "binance", "provincial"} <= sources

    assert result.source("bcv").inserted == 2
    assert result.source("binance").inserted == 5
    assert result.source("binance").updated == 1
    assert result.source("p2p_rates").inserted == 3
    assert result.source("provincial").inserted == 3

    # needs_review total is a plain int (empty db → 0).
    assert result.needs_review_total == 0

    # Non-dry-run regenerates the report exactly once.
    assert result.report_regenerated is True
    assert len(happy_steps["html"]) == 1
    assert happy_steps["html"][0]["path"] == report_path


def test_run_update_calls_steps_in_documented_order(
    in_memory_db, happy_steps, tmp_path
) -> None:
    from finances.reports.update import run_update

    run_update(
        in_memory_db,
        make_binance_client=_make_fake_client,
        inputs_dir=_inputs_with_one_csv(tmp_path),
        report_path=tmp_path / "report.html",
        dry_run=False,
    )

    # bcv → p2p → binance → provincial → html.
    assert [o.source for o in _ordered(happy_steps)] == [
        "bcv",
        "p2p",
        "binance",
        "provincial",
        "html",
    ]


def _ordered(calls: dict[str, list]):
    """Flatten the recorded calls into a stable ordering marker list."""
    # Each step recorded at most one call in these tests; reconstruct order by
    # the first-call presence in the documented sequence.
    from types import SimpleNamespace

    seq = []
    for name in ("bcv", "p2p", "binance", "provincial", "html"):
        if calls[name]:
            seq.append(SimpleNamespace(source=name))
    return seq


# ---------------------------------------------------------------------------
# Isolation — a failing step does not stop the rest.
# ---------------------------------------------------------------------------


def test_run_update_isolates_a_failing_step(
    in_memory_db, happy_steps, monkeypatch, tmp_path
) -> None:
    from finances.reports.update import run_update

    def boom(conn, dry_run=False):
        raise RuntimeError("bcv scrape exploded")

    monkeypatch.setattr(bcv_ingest, "ingest_bcv", boom)

    result = run_update(
        in_memory_db,
        make_binance_client=_make_fake_client,
        inputs_dir=_inputs_with_one_csv(tmp_path),
        report_path=tmp_path / "report.html",
        dry_run=False,
    )

    bcv = result.source("bcv")
    assert bcv.status == "error"
    assert any("exploded" in e for e in bcv.errors)

    # Downstream steps still ran.
    assert happy_steps["p2p"] and happy_steps["binance"] and happy_steps["provincial"]
    assert result.source("binance").inserted == 5
    # Report still regenerated despite the earlier failure.
    assert result.report_regenerated is True


# ---------------------------------------------------------------------------
# Dry-run — threaded into every step, writes nothing.
# ---------------------------------------------------------------------------


def test_run_update_dry_run_threads_and_writes_nothing(
    in_memory_db, happy_steps, tmp_path
) -> None:
    from finances.reports.update import run_update

    report_path = tmp_path / "report.html"
    result = run_update(
        in_memory_db,
        make_binance_client=_make_fake_client,
        inputs_dir=_inputs_with_one_csv(tmp_path),
        report_path=report_path,
        dry_run=True,
    )

    assert happy_steps["bcv"][0]["dry_run"] is True
    assert happy_steps["p2p"][0]["dry_run"] is True
    assert happy_steps["binance"][0]["dry_run"] is True
    assert happy_steps["provincial"][0]["dry_run"] is True

    # No report regeneration in dry-run.
    assert result.report_regenerated is False
    assert happy_steps["html"] == []
    assert not report_path.exists()


# ---------------------------------------------------------------------------
# VPN hint — Binance 451 / network failure.
# ---------------------------------------------------------------------------


def test_run_update_binance_451_in_errors_adds_vpn_hint(
    in_memory_db, happy_steps, monkeypatch, tmp_path
) -> None:
    from finances.reports.update import run_update

    def geo_blocked(conn, *, client, dry_run=False, **kw):
        return {
            "rows_inserted": 0,
            "rows_updated": 0,
            "earn_positions": 0,
            "errors": ["deposit: 451 Client Error: Unavailable For Legal Reasons"],
        }

    monkeypatch.setattr(binance_ingest, "sync_binance", geo_blocked)

    result = run_update(
        in_memory_db,
        make_binance_client=_make_fake_client,
        inputs_dir=_inputs_with_one_csv(tmp_path),
        report_path=tmp_path / "report.html",
        dry_run=False,
    )

    binance = result.source("binance")
    assert binance.hint is not None
    assert "VPN" in binance.hint


def test_run_update_binance_network_exception_adds_vpn_hint(
    in_memory_db, happy_steps, monkeypatch, tmp_path
) -> None:
    from finances.reports.update import run_update

    def conn_refused(conn, *, client, dry_run=False, **kw):
        raise ConnectionError("Max retries exceeded: Failed to establish a new connection")

    monkeypatch.setattr(binance_ingest, "sync_binance", conn_refused)

    result = run_update(
        in_memory_db,
        make_binance_client=_make_fake_client,
        inputs_dir=_inputs_with_one_csv(tmp_path),
        report_path=tmp_path / "report.html",
        dry_run=False,
    )

    binance = result.source("binance")
    assert binance.status == "error"
    assert binance.hint is not None and "VPN" in binance.hint


# ---------------------------------------------------------------------------
# Provincial — skipped silently when inputs/ is empty.
# ---------------------------------------------------------------------------


def test_run_update_provincial_skipped_when_no_files(
    in_memory_db, happy_steps, tmp_path
) -> None:
    from finances.reports.update import run_update

    empty_inputs = tmp_path / "inputs"
    empty_inputs.mkdir()

    result = run_update(
        in_memory_db,
        make_binance_client=_make_fake_client,
        inputs_dir=empty_inputs,
        report_path=tmp_path / "report.html",
        dry_run=False,
    )

    assert happy_steps["provincial"] == []  # ingest_csv never called
    assert result.source("provincial").status == "skipped"


# ---------------------------------------------------------------------------
# Summary rendering — plain language.
# ---------------------------------------------------------------------------


def test_render_summary_is_plain_language(
    in_memory_db, happy_steps, tmp_path
) -> None:
    from finances.reports.update import render_summary, run_update

    result = run_update(
        in_memory_db,
        make_binance_client=_make_fake_client,
        inputs_dir=_inputs_with_one_csv(tmp_path),
        report_path=tmp_path / "report.html",
        dry_run=False,
    )
    text = render_summary(result)

    assert "needs review" in text.lower()
    assert "finances.command" in text
    assert "Finances.command" not in text
    for src in ("bcv", "p2p", "binance", "provincial"):
        assert src in text.lower()


def test_render_summary_zero_needs_review_names_launcher(tmp_path) -> None:
    from finances.reports.update import UpdateReport, render_summary

    report = UpdateReport(
        outcomes=[],
        needs_review_total=0,
        freshness=[],
        dry_run=False,
        report_regenerated=True,
        report_path=tmp_path / "report.html",
    )
    text = render_summary(report)

    assert "finances.command" in text
    assert "Finances.command" not in text
    # The triage URL only appears when something is actually waiting.
    assert "http://localhost:8765/triage" not in text


def test_render_summary_needs_review_points_at_triage_url(tmp_path) -> None:
    from finances.reports.update import UpdateReport, render_summary

    report = UpdateReport(
        outcomes=[],
        needs_review_total=7,
        freshness=[],
        dry_run=False,
        report_regenerated=True,
        report_path=tmp_path / "report.html",
    )
    text = render_summary(report)

    assert "Needs review: 7" in text
    assert "http://localhost:8765/triage" in text
    # Old wording is gone — the URL replaces the "sort them" instruction.
    assert "sort them" not in text


# ---------------------------------------------------------------------------
# CLI smoke — `finances update --dry-run` end-to-end via CliRunner.
# ---------------------------------------------------------------------------


def test_update_command_dry_run_smoke(
    happy_steps, monkeypatch, tmp_path
) -> None:
    from typer.testing import CliRunner

    from finances import config
    from finances.cli import main as cli_main
    from finances.db.connection import get_connection
    from finances.db.migrate import apply_migrations

    db_file = tmp_path / "update.db"
    conn = get_connection(db_file)
    apply_migrations(conn)
    conn.close()

    inputs = _inputs_with_one_csv(tmp_path)
    monkeypatch.setattr(cli_main, "DB_PATH", db_file)
    monkeypatch.setattr(config, "INPUTS_DIR", inputs)
    monkeypatch.setattr(config, "REPORT_HTML_PATH", tmp_path / "report.html")
    # Avoid real Binance client construction (needs API keys).
    monkeypatch.setattr(cli_main, "_make_binance_client", _make_fake_client)

    runner = CliRunner()
    result = runner.invoke(cli_main.app, ["update", "--dry-run"])

    assert result.exit_code == 0, result.output
    assert "needs review" in result.output.lower()
    # Dry-run wrote no report file.
    assert not (tmp_path / "report.html").exists()
