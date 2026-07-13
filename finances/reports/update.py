"""`finances update` — the weekly ritual as one command (Thing 5A).

Runs each ingest step in the documented order, *isolated* so one failing
step never stops the rest, then regenerates the static ``report.html`` and
returns a structured :class:`UpdateReport`. The CLI command renders that
report as a plain-language summary block.

Design constraints (docs/plans/revival/05-ux-launcher.md):

* **No forked logic.** Every step calls the exact same production function the
  individual ``finances ingest …`` commands call — this module only sequences
  them and aggregates their results.
* **Isolation.** Each step is wrapped so an exception (or a non-empty
  ``errors`` list from Binance) is captured as an error outcome; the remaining
  steps still run.
* **Dry-run.** ``dry_run`` is threaded into every step and the report
  regeneration is skipped entirely, so ``--dry-run`` writes nothing.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from finances.config import CARACAS_TZ
from finances.ingest import bcv as bcv_ingest
from finances.ingest import binance as binance_ingest
from finances.ingest import p2p_rates as p2p_ingest
from finances.ingest import provincial as provincial_ingest
from finances.reports import html_export

#: Provincial file extensions `finances update` will feed through ingest. The
#: bank's web export is an HTML table saved as ``.xls`` (sniffed downstream).
_PROVINCIAL_SUFFIXES = {".csv", ".xls"}

#: Attached to the Binance outcome when a failure looks like the geo-block the
#: owner hits without a VPN (Binance is 451-blocked from Venezuela).
VPN_HINT = (
    "Binance looks geo-blocked (HTTP 451 / network error). "
    "Connect your VPN and re-run `finances update`."
)


def _fmt_earn(earn: Any) -> str:
    """Render the Binance earn-refresh result — a dict or a plain int — briefly."""
    if isinstance(earn, dict):
        ins = earn.get("inserted", 0)
        closed = earn.get("closed", 0)
        return f"{ins} opened / {closed} closed"
    return str(earn)


def _looks_geo_blocked(text: str) -> bool:
    """Heuristic: does this error message read like a 451 or network failure?"""
    t = text.lower()
    needles = (
        "451",
        "unavailable for legal reasons",
        "max retries",
        "failed to establish",
        "connection refused",
        "connectionerror",
        "timed out",
        "timeout",
        "getaddrinfo",
        "network is unreachable",
        "name or service not known",
        "temporary failure in name resolution",
    )
    return any(n in t for n in needles)


@dataclass
class SourceOutcome:
    """Result of one ingest step, in a shape the summary renderer can print."""

    source: str
    status: str = "ok"  # "ok" | "error" | "skipped"
    inserted: int = 0
    updated: int = 0
    summary: str = ""
    errors: list[str] = field(default_factory=list)
    hint: str | None = None


@dataclass
class UpdateReport:
    """Everything the summary block needs — one place, no re-querying."""

    outcomes: list[SourceOutcome]
    needs_review_total: int
    freshness: list[Any]
    dry_run: bool
    report_regenerated: bool
    report_path: Path
    regen_error: str | None = None

    def source(self, name: str) -> SourceOutcome:
        for outcome in self.outcomes:
            if outcome.source == name:
                return outcome
        raise KeyError(name)


def _discover_provincial_files(inputs_dir: Path) -> list[Path]:
    """Return sorted Provincial statement files awaiting ingest.

    Idempotency (``UNIQUE(source, source_ref)``) makes re-ingesting an
    already-loaded file a no-op, so "not-yet-ingested" needs no state table —
    we hand every candidate file to ingest and let dedup absorb the repeats.
    """
    if not inputs_dir.exists():
        return []
    return sorted(
        p
        for p in inputs_dir.iterdir()
        if p.is_file() and p.suffix.lower() in _PROVINCIAL_SUFFIXES
    )


# ---------------------------------------------------------------------------
# Individual steps. Each returns a SourceOutcome and never raises.
# ---------------------------------------------------------------------------


def _step_bcv(conn: sqlite3.Connection, *, dry_run: bool) -> SourceOutcome:
    try:
        inserted = bcv_ingest.ingest_bcv(conn, dry_run=dry_run)
        return SourceOutcome(
            "bcv", inserted=inserted, summary=f"{inserted} rate row(s)"
        )
    except Exception as exc:  # noqa: BLE001 - isolate the step
        return SourceOutcome(
            "bcv", status="error", errors=[str(exc)], summary=f"failed: {exc}"
        )


def _step_p2p(conn: sqlite3.Connection, *, dry_run: bool) -> SourceOutcome:
    try:
        today = datetime.now(tz=CARACAS_TZ).date()
        res = p2p_ingest.ingest_p2p_rates(conn, as_of_date=today, dry_run=dry_run)
        n = len(res.get("rows_written") or [])
        return SourceOutcome(
            "p2p_rates",
            inserted=n,
            summary=(
                f"{n} rate row(s) "
                f"(buy={res.get('buy_median')} sell={res.get('sell_median')} "
                f"mid={res.get('midpoint')})"
            ),
        )
    except Exception as exc:  # noqa: BLE001 - isolate the step
        return SourceOutcome(
            "p2p_rates", status="error", errors=[str(exc)], summary=f"failed: {exc}"
        )


def _step_binance(
    conn: sqlite3.Connection,
    *,
    make_binance_client: Callable[[], Any],
    dry_run: bool,
) -> SourceOutcome:
    outcome = SourceOutcome("binance")
    try:
        client = make_binance_client()
        res = binance_ingest.sync_binance(conn, client=client, dry_run=dry_run)
        outcome.inserted = int(res.get("rows_inserted", 0))
        outcome.updated = int(res.get("rows_updated", 0))
        errs = list(res.get("errors") or [])
        if errs:
            outcome.status = "error"
            outcome.errors = errs
            if any(_looks_geo_blocked(e) for e in errs):
                outcome.hint = VPN_HINT
            outcome.summary = (
                f"{outcome.inserted} new, {outcome.updated} updated, "
                f"{len(errs)} error(s)"
            )
        else:
            outcome.summary = (
                f"{outcome.inserted} new, {outcome.updated} updated, "
                f"{_fmt_earn(res.get('earn_positions', 0))} earn"
            )
    except Exception as exc:  # noqa: BLE001 - isolate the step
        outcome.status = "error"
        outcome.errors = [str(exc)]
        if _looks_geo_blocked(str(exc)):
            outcome.hint = VPN_HINT
        outcome.summary = f"failed: {exc}"
    return outcome


def _archive_processed(path: Path, inputs_dir: Path) -> Path:
    """Move a successfully ingested file into ``inputs/processed/``.

    Creates the directory on first use. Collision-safe: if a file of the
    same name already sits in ``processed/`` (e.g. a re-dropped statement),
    the incoming file gets a ``-1``, ``-2``, … suffix rather than clobbering
    the earlier archive. Returns the final destination path.
    """
    processed = inputs_dir / "processed"
    processed.mkdir(parents=True, exist_ok=True)
    dest = processed / path.name
    if dest.exists():
        counter = 1
        while (dest := processed / f"{path.stem}-{counter}{path.suffix}").exists():
            counter += 1
    path.rename(dest)
    return dest


def _step_provincial(
    conn: sqlite3.Connection, *, inputs_dir: Path, dry_run: bool
) -> SourceOutcome:
    files = _discover_provincial_files(inputs_dir)
    if not files:
        return SourceOutcome(
            "provincial", status="skipped", summary="no files in inputs/"
        )

    inserted = updated = 0
    errors: list[str] = []
    archived = 0
    for path in files:
        try:
            report = provincial_ingest.ingest_csv(conn, path, dry_run=dry_run)
            inserted += report.rows_inserted
            updated += report.rows_updated
        except Exception as exc:  # noqa: BLE001 - isolate a bad file
            errors.append(f"{path.name}: {exc}")
            continue
        # Only a real (non-dry) run that ingested cleanly retires the file, so
        # a dry-run leaves inputs/ untouched and a bad file stays for a retry.
        if not dry_run:
            _archive_processed(path, inputs_dir)
            archived += 1

    status = "error" if errors else "ok"
    summary = f"{len(files)} file(s): {inserted} new, {updated} updated"
    if errors:
        summary += f", {len(errors)} error(s)"
    if archived:
        summary += f"; archived {archived} file(s) → inputs/processed/"
    return SourceOutcome(
        "provincial",
        status=status,
        inserted=inserted,
        updated=updated,
        errors=errors,
        summary=summary,
    )


# ---------------------------------------------------------------------------
# Orchestrator.
# ---------------------------------------------------------------------------


def run_update(
    conn: sqlite3.Connection,
    *,
    make_binance_client: Callable[[], Any],
    inputs_dir: Path,
    report_path: Path,
    dry_run: bool = False,
) -> UpdateReport:
    """Run every ingest step, regenerate ``report.html``, return the report.

    Steps run in the documented order — bcv, p2p-rates, binance, provincial —
    each isolated. On a real (non-dry) run the static report is regenerated
    last; a regen failure is captured, never raised.
    """
    outcomes = [
        _step_bcv(conn, dry_run=dry_run),
        _step_p2p(conn, dry_run=dry_run),
        _step_binance(
            conn, make_binance_client=make_binance_client, dry_run=dry_run
        ),
        _step_provincial(conn, inputs_dir=inputs_dir, dry_run=dry_run),
    ]

    report_regenerated = False
    regen_error: str | None = None
    if not dry_run:
        try:
            html_export.export_html(conn, report_path)
            report_regenerated = True
        except Exception as exc:  # noqa: BLE001 - warn-only, never fail the run
            regen_error = str(exc)

    row = conn.execute(
        "SELECT COUNT(*) AS c FROM transactions WHERE needs_review = 1"
    ).fetchone()
    needs_review_total = int(row["c"] if isinstance(row, sqlite3.Row) else row[0])

    freshness = _build_freshness(conn)

    return UpdateReport(
        outcomes=outcomes,
        needs_review_total=needs_review_total,
        freshness=freshness,
        dry_run=dry_run,
        report_regenerated=report_regenerated,
        report_path=report_path,
        regen_error=regen_error,
    )


def _build_freshness(conn: sqlite3.Connection) -> list[Any]:
    """Per-source last-run chips, reusing the viewer's sync-status service."""
    try:
        from finances.web.services import dashboard

        return dashboard.build_sync_status(conn)
    except Exception:  # noqa: BLE001 - freshness is decorative, never fatal
        return []


# ---------------------------------------------------------------------------
# Plain-language summary rendering.
# ---------------------------------------------------------------------------


def render_summary(report: UpdateReport) -> str:
    """Render the owner-facing summary block for :func:`run_update`."""
    lines: list[str] = []
    mode = " (dry run — nothing written)" if report.dry_run else ""
    lines.append(f"finances update{mode}")
    lines.append("")
    lines.append("Sources:")
    for outcome in report.outcomes:
        lines.append(f"  {outcome.source:<11} {outcome.status:<8} {outcome.summary}")
        for err in outcome.errors:
            lines.append(f"      err: {err}")
        if outcome.hint:
            lines.append(f"      → {outcome.hint}")

    lines.append("")
    if report.needs_review_total:
        lines.append(
            f"Needs review: {report.needs_review_total} row(s) waiting for "
            "triage → http://localhost:8765/triage"
        )
    else:
        lines.append(
            "Needs review: 0 rows — nothing waiting. (triage via finances.command)"
        )

    if report.freshness:
        lines.append("")
        lines.append("Data freshness:")
        for chip in report.freshness:
            last = getattr(chip, "last_run_at", None)
            when = last.strftime("%Y-%m-%d %H:%M") if last else "never"
            status = getattr(chip, "last_status", "?")
            src = getattr(chip, "source", "?")
            lines.append(f"  {src:<11} {status:<8} last run {when}")

    lines.append("")
    if report.dry_run:
        lines.append("Report: not regenerated (dry run).")
    elif report.report_regenerated:
        lines.append(f"Report: regenerated {report.report_path}.")
    else:
        lines.append(f"Report: regen FAILED — {report.regen_error}")

    return "\n".join(lines)


__all__ = [
    "SourceOutcome",
    "UpdateReport",
    "run_update",
    "render_summary",
    "VPN_HINT",
]
