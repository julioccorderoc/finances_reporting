from __future__ import annotations

from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import typer

from finances.config import CARACAS_TZ, DB_PATH, binance_credentials
from finances.db.connection import get_connection
from finances.db.migrate import apply_migrations

app = typer.Typer(help="Finances reporting CLI")

cash_app = typer.Typer(help="Cash USD entries (rule-008 — v1 supports USD cash only).")
app.add_typer(cash_app, name="cash")

ingest_app = typer.Typer(help="Ingest data from external sources.")
app.add_typer(ingest_app, name="ingest")

report_app = typer.Typer(help="Reports over the ledger (EPIC-013).")
app.add_typer(report_app, name="report")

sync_app = typer.Typer(help="Mirror the ledger out to external read-only surfaces.")
app.add_typer(sync_app, name="sync")

rates_app = typer.Typer(help="Exchange-rate maintenance (ADR-005, ADR-013).")
app.add_typer(rates_app, name="rates")

reconcile_app = typer.Typer(
    help="Re-run reconciliation passes over rows already in the ledger "
    "(ADR-002, ADR-017)."
)
app.add_typer(reconcile_app, name="reconcile")


@app.callback()
def _root() -> None:
    """Finances reporting CLI."""


def _regenerate_default_report() -> None:
    """Regenerate the default ``report.html`` after a successful ingest.

    Best-effort (Thing 4, rule 2): any failure is logged to stderr and
    swallowed so it never fails the ingest that triggered it. Read-only —
    :func:`export_html` issues SELECTs only.
    """
    from finances import config as _config
    from finances.reports.html_export import export_html

    try:
        conn = get_connection(_config.DB_PATH)
        try:
            export_html(conn, _config.REPORT_HTML_PATH)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - warn-only by design
        typer.echo(f"warning: report.html regen failed: {exc}", err=True)


@app.command("html")
def html_cmd(
    output: Path = typer.Option(
        None,
        "--output",
        "-o",
        help="Destination file. Defaults to report.html at the repo root.",
    ),
) -> None:
    """Render the single self-contained static report file (Thing 4)."""
    from finances import config as _config
    from finances.reports.html_export import export_html

    out = output if output is not None else _config.REPORT_HTML_PATH
    conn = get_connection(_config.DB_PATH)
    apply_migrations(conn)
    try:
        result = export_html(conn, out)
    finally:
        conn.close()
    typer.echo(f"html: wrote {result}")


def _make_binance_client() -> Any:
    from binance.spot import Spot

    api_key, api_secret = binance_credentials()
    return Spot(api_key=api_key, api_secret=api_secret)


@app.command("update")
def update_cmd(
    dry_run: bool = typer.Option(
        False,
        "--dry-run/--no-dry-run",
        help="Thread dry-run through every step; fetch and validate but write nothing.",
    ),
) -> None:
    """Run the weekly ritual — bcv, p2p, binance, provincial — then regen report.html."""
    from finances import config as _config
    from finances.domain.integrity import render_report as render_integrity
    from finances.domain.integrity import run_checks
    from finances.reports.update import render_summary, run_update

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        report = run_update(
            conn,
            make_binance_client=_make_binance_client,
            inputs_dir=_config.INPUTS_DIR,
            report_path=_config.REPORT_HTML_PATH,
            dry_run=dry_run,
        )
        integrity = run_checks(conn)
    finally:
        conn.close()

    typer.echo(render_summary(report))

    # The ritual is the only moment this ledger is reliably looked at, so
    # it is where drift should announce itself. Advisory on purpose: a
    # finding must never fail an ingest that already succeeded. Run
    # `finances doctor` for the same output on demand.
    if integrity.findings:
        typer.echo("")
        typer.echo(render_integrity(integrity))


@app.command("doctor")
def doctor_cmd(
    strict: bool = typer.Option(
        False,
        "--strict",
        help="Exit non-zero when any invariant is violated (warnings never do).",
    ),
) -> None:
    """Check the ledger against the invariants in CLAUDE.md.

    The test suite verifies code against seeded fixtures; this verifies
    the actual data. Read-only — it reports, it never repairs.
    """
    from finances.domain.integrity import render_report as render_integrity
    from finances.domain.integrity import run_checks

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        report = run_checks(conn)
    finally:
        conn.close()

    typer.echo(render_integrity(report))

    if strict and report.has_errors:
        raise typer.Exit(code=1)


@reconcile_app.command("converts")
def reconcile_converts(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Report what would be paired, then roll back without writing.",
    ),
) -> None:
    """Pair Binance conversion legs into transfers (ADR-017).

    A conversion moves value between two positions inside one account, so
    both legs are movement, not spending. The Binance sync pairs new ones;
    this repairs those already recorded, including the legacy rows whose
    halves were hashed separately and so never shared an order id.

    Idempotent — eligibility is ``transfer_id IS NULL``, so re-running it
    once everything is paired does nothing.
    """
    from finances.domain.reconciliation import run_reconciliation_pass
    from finances.domain.transfers import SameAccountConvertPairing

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        conn.execute("BEGIN")
        try:
            report = run_reconciliation_pass(SameAccountConvertPairing(conn))
            if dry_run:
                conn.execute("ROLLBACK")
            else:
                conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()

    suffix = " (dry run — rolled back)" if dry_run else ""
    typer.echo(
        f"conversion pairing: {report.proposals_applied}/{report.proposals_found} "
        f"pair(s) applied{suffix}"
    )
    for err in report.errors:
        typer.echo(f"  error: {err}", err=True)
    if report.errors:
        raise typer.Exit(code=1)


@reconcile_app.command("categories")
def reconcile_categories(
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Report what would be categorised, then roll back."
    ),
) -> None:
    """Apply the category rules to every still-uncategorised row (rule-006).

    The rules engine runs during backfill but not during live ingest, so a
    rule added after a row was imported never reaches it. This sweeps.

    Only fills blanks — a category already set, by a rule or by hand, is
    never overwritten.
    """
    from finances.migration.backfill import apply_category_rules

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        before = conn.execute(
            "SELECT COUNT(*) FROM transactions WHERE category_id IS NULL "
            "AND kind IN ('income', 'expense')"
        ).fetchone()[0]
        conn.execute("BEGIN")
        try:
            # commit=False: this command owns the transaction, so --dry-run
            # can actually roll back. Without it the sweep committed and the
            # dry run wrote.
            categorized = apply_category_rules(conn, commit=False)
            if dry_run:
                conn.execute("ROLLBACK")
            else:
                conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()

    suffix = " (dry run — rolled back)" if dry_run else ""
    typer.echo(
        f"categorised {categorized} row(s); {before} income/expense rows "
        f"were uncategorised{suffix}"
    )


@reconcile_app.command("park")
def reconcile_park(
    before: str = typer.Option(
        ...,
        "--before",
        help="Park uncategorised income/expense rows dated before this YYYY-MM-DD.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Report what would be parked, then roll back."
    ),
) -> None:
    """Defer a period you have decided not to triage.

    Parking is durable and survives re-ingest. It changes no amount and no
    balance — it only answers "do you want to categorise this?" with no, so
    the rows stop appearing in the queue and in `finances doctor`.
    """
    from finances.domain.triage_admin import park_before

    try:
        cutoff = datetime.strptime(before, "%Y-%m-%d").replace(tzinfo=CARACAS_TZ)
    except ValueError:
        typer.echo(f"error: --before {before!r} is not a YYYY-MM-DD date", err=True)
        raise typer.Exit(code=2) from None

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        conn.execute("BEGIN")
        try:
            parked = park_before(conn, before=cutoff)
            if dry_run:
                conn.execute("ROLLBACK")
            else:
                conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()

    suffix = " (dry run — rolled back)" if dry_run else ""
    typer.echo(f"parked {parked} row(s) dated before {before}{suffix}")


@reconcile_app.command("balances")
def reconcile_balances(
    account: str = typer.Option(..., "--account", help="Account name, e.g. 'Binance Spot'."),
    currency: str = typer.Option(..., "--currency", help="Asset, e.g. USDT."),
    actual: str = typer.Option(
        ...,
        "--actual",
        help="The balance the custodian reports for this position, right now.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show the adjustment, then roll back."
    ),
) -> None:
    """Bring one position to the balance its custodian reports (ADR-018).

    For a gap the ledger can no longer explain because the source history is
    gone — Binance serves internal transfers for six months only. Writes one
    ``kind='adjustment'`` row carrying the difference, dated today. Balances
    include it; income and expense never do.

    Do NOT use this for a gap a pending ingest would close: adjusting
    un-synced interest double-counts it the moment the rows arrive. Run
    ``finances ingest binance`` first, then reconcile what is left.

    The custodian figure is yours to supply. Reading it from the API would
    make the ledger agree with the API by construction.
    """
    from finances.db.repos import accounts as accounts_repo
    from finances.domain.reconciliation_adjustments import record_adjustment

    try:
        actual_amount = Decimal(actual)
    except InvalidOperation:
        typer.echo(f"error: --actual {actual!r} is not a number", err=True)
        raise typer.Exit(code=2) from None

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        target = accounts_repo.get_by_name(conn, account)
        if target is None or target.id is None:
            typer.echo(f"error: no account named {account!r}", err=True)
            raise typer.Exit(code=2)

        conn.execute("BEGIN")
        try:
            result = record_adjustment(
                conn,
                account_id=target.id,
                currency=currency,
                actual=actual_amount,
                occurred_at=datetime.now(tz=CARACAS_TZ),
            )
            if dry_run:
                conn.execute("ROLLBACK")
            else:
                conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()

    if result is None:
        typer.echo(f"{account} {currency.upper()}: already matches — nothing written")
        return

    suffix = " (dry run — rolled back)" if dry_run else ""
    typer.echo(
        f"{account} {result.currency}: ledger {result.ledger_balance:f} → "
        f"custodian {result.actual_balance:f} "
        f"(adjustment {result.delta:+f}){suffix}"
    )


@ingest_app.command("binance")
def ingest_binance(
    since: datetime | None = typer.Option(
        None,
        "--since",
        help=(
            "ISO timestamp to start ingest from (overrides lookback and stored "
            "state). Naive values are interpreted as America/Caracas."
        ),
    ),
    lookback_days: int = typer.Option(
        35,
        "--lookback-days",
        help="Fallback window when no --since and no stored import_state (default 35).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run/--no-dry-run",
        help="Fetch and validate but do not write to the DB.",
    ),
) -> None:
    """Incrementally sync Binance endpoints into the ledger (EPIC-007)."""
    from finances.ingest.binance import sync_binance

    if since is not None and since.tzinfo is None:
        since = since.replace(tzinfo=CARACAS_TZ)

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        client = _make_binance_client()
        result = sync_binance(
            conn,
            client=client,
            since=since,
            lookback_days=lookback_days,
            dry_run=dry_run,
        )
    finally:
        conn.close()

    label = "binance sync (dry-run)" if dry_run else "binance sync"
    typer.echo(
        f"{label}: inserted={result['rows_inserted']} "
        f"updated={result['rows_updated']} "
        f"earn={result['earn_positions']} errors={len(result['errors'])}"
    )
    for err in result["errors"]:
        typer.echo(f"  err: {err}", err=True)
    if not dry_run and not result["errors"]:
        _regenerate_default_report()
    if result["errors"]:
        raise typer.Exit(code=1)


@ingest_app.command("provincial")
def ingest_provincial(
    csv_path: Path = typer.Argument(
        ...,
        exists=True,
        dir_okay=False,
        readable=True,
        help="Path to a Provincial statement CSV (semicolon-delimited).",
    ),
    pairing_window_days: int = typer.Option(
        1,
        "--pairing-window-days",
        help="±Day window used for bank-anchored P2P pairing (default 1).",
    ),
    no_pairing: bool = typer.Option(
        False,
        "--no-pairing",
        help="Skip the bank-anchored P2P pairing pass after upserts.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run/--no-dry-run",
        help="Parse and validate the CSV but do not write to the DB.",
    ),
) -> None:
    """Ingest a Provincial bank CSV and run the P2P pairing pass (EPIC-008)."""
    from finances.ingest.provincial import ingest_csv

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        report = ingest_csv(
            conn,
            csv_path,
            pairing_window_days=pairing_window_days,
            run_pairing=not no_pairing,
            dry_run=dry_run,
        )
    finally:
        conn.close()

    label = "provincial ingest (dry-run)" if dry_run else "provincial ingest"
    typer.echo(
        f"{label}: seen={report.rows_seen} "
        f"inserted={report.rows_inserted} updated={report.rows_updated}"
    )
    # The bank's export truncates at a page limit without saying so. These
    # go to stderr and read as prose because acting on them means re-exporting
    # a date range, which only the owner can do.
    for warning in report.warnings:
        typer.echo(f"  WARNING: {warning}", err=True)
    if report.reconciliation is not None:
        rec = report.reconciliation
        typer.echo(
            f"  pairing ({rec.strategy}): "
            f"found={rec.proposals_found} applied={rec.proposals_applied} "
            f"errors={len(rec.errors)}"
        )
        for err in rec.errors:
            typer.echo(f"    err: {err}", err=True)
    if not dry_run:
        _regenerate_default_report()


@ingest_app.command("bcv")
def ingest_bcv(
    dry_run: bool = typer.Option(
        False,
        "--dry-run/--no-dry-run",
        help="Fetch and validate but do not write to the DB.",
    ),
) -> None:
    """Fetch BCV reference rates (USD/VES, EUR/VES) and upsert into rates (EPIC-009)."""
    from finances.ingest.bcv import ingest_bcv as run_bcv

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        inserted = run_bcv(conn, dry_run=dry_run)
    finally:
        conn.close()
    prefix = "bcv (dry-run): would insert" if dry_run else "bcv: inserted"
    typer.echo(f"{prefix} {inserted} rate rows")
    if not dry_run:
        _regenerate_default_report()


@ingest_app.command("p2p-rates")
def ingest_p2p_rates_cmd(
    as_of: str | None = typer.Option(
        None,
        "--as-of",
        help="Date to stamp on the rate rows in YYYY-MM-DD (Caracas TZ). Defaults to today.",
    ),
    asset: str = typer.Option("USDT", "--asset", help="Base asset code."),
    fiat: str = typer.Option("VES", "--fiat", help="Quote fiat code."),
    rows: int = typer.Option(10, "--rows", help="Top-N adverts per side."),
    dry_run: bool = typer.Option(
        False,
        "--dry-run/--no-dry-run",
        help="Fetch and compute medians but do not write to the DB.",
    ),
) -> None:
    """Fetch Binance P2P medians and upsert buy/sell/midpoint rate rows (EPIC-010)."""
    from datetime import date as _date

    from finances import config as _config
    from finances.ingest.p2p_rates import ingest_p2p_rates

    if as_of is None:
        as_of_date = datetime.now(tz=_config.CARACAS_TZ).date()
    else:
        try:
            as_of_date = _date.fromisoformat(as_of)
        except ValueError as exc:
            raise typer.BadParameter(f"--as-of must be YYYY-MM-DD: {as_of!r}") from exc

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        result = ingest_p2p_rates(
            conn,
            as_of_date=as_of_date,
            asset=asset,
            fiat=fiat,
            rows=rows,
            dry_run=dry_run,
        )
    finally:
        conn.close()

    label = "P2P (dry-run)" if dry_run else "P2P"
    typer.echo(
        f"{label} {asset}/{fiat} @ {as_of_date.isoformat()}: "
        f"buy={result['buy_median']} sell={result['sell_median']} "
        f"midpoint={result['midpoint']} "
        f"(n_buy={result['buy_adverts_used']}, n_sell={result['sell_adverts_used']})"
    )
    if not dry_run:
        _regenerate_default_report()


def _parse_cash_amount(raw: str) -> Decimal:
    try:
        return Decimal(raw)
    except InvalidOperation as exc:
        raise typer.BadParameter(f"amount must be a number: {raw!r}") from exc


def _parse_cash_date(raw: str | None) -> datetime:
    from finances import config as _config

    if raw is None:
        today = datetime.now(tz=_config.CARACAS_TZ).date()
        return datetime.combine(today, time(0, 0), tzinfo=_config.CARACAS_TZ)
    try:
        parsed_date = datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise typer.BadParameter(f"--date must be YYYY-MM-DD: {raw!r}") from exc
    return datetime.combine(parsed_date, time(0, 0), tzinfo=_config.CARACAS_TZ)


@cash_app.command("add")
def cash_add(
    amount: str = typer.Option(
        ..., "--amount", "-a", help="Positive USD amount (expense sign is applied)."
    ),
    description: str = typer.Option(
        ..., "--description", "-d", help="Short note describing the expense."
    ),
    date: str | None = typer.Option(
        None,
        "--date",
        help="Date of expense in YYYY-MM-DD (Caracas TZ). Defaults to today.",
    ),
    category: str | None = typer.Option(
        None, "--category", help="Optional expense category name."
    ),
    account: str | None = typer.Option(
        None,
        "--account",
        help="Reserved; must be 'Cash USD' (rule-008). Leave unset for the default.",
    ),
) -> None:
    """Record a USD cash expense on the Cash USD account (EPIC-011)."""
    from finances import config as _config
    from finances.db.connection import get_connection
    from finances.db.repos import categories as categories_repo
    from finances.domain.models import TransactionKind
    from finances.ingest.cash_cli import (
        CASH_USD_ACCOUNT_NAME,
        add_cash_expense,
        suggest_recent_categories,
    )

    if account is not None and account != CASH_USD_ACCOUNT_NAME:
        typer.echo(
            f"--account must be {CASH_USD_ACCOUNT_NAME!r} (rule-008); got {account!r}.",
            err=True,
        )
        raise typer.Exit(code=2)

    amount_decimal = _parse_cash_amount(amount)
    if amount_decimal <= 0:
        typer.echo("--amount must be a positive number of USD.", err=True)
        raise typer.Exit(code=2)

    occurred_at = _parse_cash_date(date)

    conn = get_connection(_config.DB_PATH)
    try:
        category_id: int | None = None
        if category is not None:
            found = categories_repo.get_by_name(
                conn, TransactionKind.EXPENSE, category
            )
            if found is None:
                typer.echo(
                    f"category {category!r} not found among expense categories.",
                    err=True,
                )
                raise typer.Exit(code=2)
            category_id = found.id

        txn = add_cash_expense(
            conn,
            amount=amount_decimal,
            description=description,
            occurred_at=occurred_at,
            category_id=category_id,
        )
        recent = suggest_recent_categories(conn, txn.account_id, limit=3)
    finally:
        conn.close()

    typer.echo(
        f"Recorded cash expense id={txn.id} amount={txn.amount} {txn.currency} "
        f"on {CASH_USD_ACCOUNT_NAME} ({txn.occurred_at.date().isoformat()})."
    )
    if recent:
        hints = ", ".join(c.name for c in recent)
        typer.echo(f"Recent categories on this account: {hints}")
    _regenerate_default_report()


@app.command("backfill")
def backfill(
    from_dir: Path = typer.Option(
        ...,
        "--from",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        help="Directory holding the legacy CSVs (Finanzas - *.csv).",
    ),
    pairing_window_days: int = typer.Option(
        1,
        "--pairing-window-days",
        help="±Day window used for bank-anchored P2P pairing (default 1).",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Re-run even if the ledger already has transactions (wipes cleanup state).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run/--no-dry-run",
        help="Run the full backfill in a transaction and roll back; no DB writes persist.",
    ),
) -> None:
    """One-time backfill of historical CSVs through production ingest (EPIC-012)."""
    from finances.migration.backfill import run_backfill

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        report = run_backfill(
            conn,
            from_dir,
            pairing_window_days=pairing_window_days,
            force=force,
            dry_run=dry_run,
        )
    finally:
        conn.close()

    label = "backfill (dry-run)" if dry_run else "backfill"
    typer.echo(
        f"{label}: binance={report.binance_rows_inserted}/{report.binance_rows_seen} "
        f"provincial={report.provincial_rows_inserted}/{report.provincial_rows_seen} "
        f"bcv_rates={report.bcv_rates_inserted}/{report.bcv_rows_seen} "
        f"errors={len(report.errors)}"
    )
    if report.reconciliation is not None:
        rec = report.reconciliation
        typer.echo(
            f"  pairing ({rec.strategy}): "
            f"found={rec.proposals_found} applied={rec.proposals_applied}"
        )
    for err in report.errors:
        typer.echo(f"  err: {err}", err=True)
    if not dry_run and not report.errors:
        _regenerate_default_report()
    if report.errors:
        raise typer.Exit(code=1)


def _resolve_report_format(json_flag: bool, csv_flag: bool) -> str:
    if json_flag and csv_flag:
        raise typer.BadParameter("pass --json or --csv, not both")
    if json_flag:
        return "json"
    if csv_flag:
        return "csv"
    return "table"


@rates_app.command("rebuild-realized")
def rates_rebuild_realized() -> None:
    """Recompute realized VES cost-basis rates from Binance P2P sells (ADR-013).

    Binance ingest already does this on every run. Use this for the one-time
    backfill over existing P2P history, or to recover after a P2P transaction
    is edited or deleted.

    Idempotent — it derives from the fills and upserts, so re-running is safe
    and no transaction rows are touched. Because ``amount_usd`` is computed on
    every read, the next report reflects the new rates immediately.
    """
    from finances import config as _config
    from finances.domain import realized_rates

    conn = get_connection(_config.DB_PATH)
    apply_migrations(conn)
    try:
        written = realized_rates.rebuild(conn)
        conn.commit()
    finally:
        conn.close()

    typer.echo(f"realized rates rebuilt: {written} day(s)")


@report_app.command("balances")
def report_balances(
    json_flag: bool = typer.Option(False, "--json", help="Emit JSON instead of table."),
    csv_flag: bool = typer.Option(False, "--csv", help="Emit CSV instead of table."),
) -> None:
    """Per-account balances in native currency (EPIC-013)."""
    from finances.reports import balances as balances_report

    fmt = _resolve_report_format(json_flag, csv_flag)
    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        rows = balances_report.get_balances(conn)
    finally:
        conn.close()

    if fmt == "json":
        typer.echo(balances_report.render_json(rows))
    elif fmt == "csv":
        typer.echo(balances_report.render_csv(rows), nl=False)
    else:
        typer.echo(balances_report.render_table(rows), nl=False)


@report_app.command("consolidated")
def report_consolidated(
    json_flag: bool = typer.Option(False, "--json", help="Emit JSON instead of table."),
    csv_flag: bool = typer.Option(False, "--csv", help="Emit CSV instead of table."),
    strict: bool = typer.Option(
        False,
        "--strict",
        help="Exit non-zero if any headline row uses a BCV-sourced rate (ADR-005 amendment).",
    ),
) -> None:
    """Consolidated USD view of every non-transfer transaction (EPIC-013)."""
    from finances.reports import consolidated_usd as report_module

    fmt = _resolve_report_format(json_flag, csv_flag)
    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        report = report_module.build_report(conn, strict=strict)
    finally:
        conn.close()

    if fmt == "json":
        typer.echo(report_module.render_json(report))
    elif fmt == "csv":
        typer.echo(report_module.render_csv(report), nl=False)
    else:
        typer.echo(report_module.render_table(report))

    if strict and report.strict_violations:
        typer.echo(
            f"--strict: {len(report.strict_violations)} row(s) use a BCV-sourced rate "
            f"(ids={report.strict_violations[:10]}"
            f"{'…' if len(report.strict_violations) > 10 else ''}).",
            err=True,
        )
        raise typer.Exit(code=1)


@report_app.command("monthly")
def report_monthly(
    month: str | None = typer.Option(
        None,
        "--month",
        help="Single YYYY-MM bucket. Mutually exclusive with --since/--until.",
    ),
    since: str | None = typer.Option(
        None, "--since", help="Inclusive lower bound YYYY-MM."
    ),
    until: str | None = typer.Option(
        None, "--until", help="Inclusive upper bound YYYY-MM."
    ),
    json_flag: bool = typer.Option(False, "--json", help="Emit JSON instead of table."),
    csv_flag: bool = typer.Option(False, "--csv", help="Emit CSV instead of table."),
) -> None:
    """Monthly aggregate per (account, category, kind) (EPIC-013)."""
    from finances.reports import monthly as monthly_report

    fmt = _resolve_report_format(json_flag, csv_flag)
    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        try:
            report = monthly_report.build_report(
                conn, month=month, since=since, until=until
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    finally:
        conn.close()

    if fmt == "json":
        typer.echo(monthly_report.render_json(report))
    elif fmt == "csv":
        typer.echo(monthly_report.render_csv(report), nl=False)
    else:
        typer.echo(monthly_report.render_table(report))


@report_app.command("needs-review")
def report_needs_review(
    json_flag: bool = typer.Option(False, "--json", help="Emit JSON instead of table."),
    csv_flag: bool = typer.Option(False, "--csv", help="Emit CSV instead of table."),
) -> None:
    """Every transaction with needs_review=1 (EPIC-013)."""
    from finances.reports import needs_review as needs_review_report

    fmt = _resolve_report_format(json_flag, csv_flag)
    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        rows = needs_review_report.get_needs_review(conn)
    finally:
        conn.close()

    if fmt == "json":
        typer.echo(needs_review_report.render_json(rows))
    elif fmt == "csv":
        typer.echo(needs_review_report.render_csv(rows), nl=False)
    else:
        typer.echo(needs_review_report.render_table(rows), nl=False)


@sync_app.command("sheets")
def sync_sheets(
    spreadsheet_id: str = typer.Option(
        ...,
        "--spreadsheet-id",
        help="Google Sheets spreadsheet ID. Must be shared with the service account as editor.",
    ),
) -> None:
    """Destructively mirror the ledger into four read-only tabs (EPIC-014)."""
    from finances.reports.sheets_sync import sync_to_sheets

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        report = sync_to_sheets(conn, spreadsheet_id=spreadsheet_id)
    finally:
        conn.close()

    counts = " ".join(f"{name}={report.rows_written[name]}" for name in report.tabs)
    typer.echo(
        f"sheets sync: spreadsheet={spreadsheet_id} "
        f"tabs={len(report.tabs)} "
        f"rows={{{counts}}} "
        f"duration={report.duration_s:.2f}s"
    )


@app.command("serve")
def serve_cmd(
    host: str = typer.Option(
        "127.0.0.1",
        "--host",
        help="Address to bind. 127.0.0.1 (default) is localhost-only and "
        "skips auth. Anything else requires --token / FINANCES_WEB_TOKEN.",
    ),
    port: int = typer.Option(8765, "--port", help="TCP port to bind."),
    open_browser: bool = typer.Option(
        False,
        "--open/--no-open",
        help="Open the viewer in the default browser after boot.",
    ),
    token: str | None = typer.Option(
        None,
        "--token",
        envvar="FINANCES_WEB_TOKEN",
        help="Bearer token required for non-localhost binds (ADR-012).",
    ),
    reload: bool | None = typer.Option(
        None,
        "--reload/--no-reload",
        help="Restart the server when finances/** changes. Defaults ON for "
        "the 127.0.0.1 bind; unavailable on LAN binds.",
    ),
) -> None:
    """Run the local web viewer (EPIC-022)."""
    import os
    import threading
    import webbrowser

    import uvicorn

    from finances.web.app import (
        IMPORT_STRING,
        PACKAGE_DIR,
        RELOAD_DIRS,
        RELOAD_EXCLUDES,
        RELOAD_INCLUDES,
        create_app,
    )
    from finances.web.settings import WebSettings

    try:
        settings = WebSettings(
            host=host,
            port=port,
            token=token,
        )
    except ValueError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=2) from exc

    # Reload is localhost-only (ADR-012 Amendment 2026-07-26). The reloader
    # spawns a fresh interpreter that rebuilds WebSettings from the
    # environment; if that ever fell back to defaults it would come up with
    # host="127.0.0.1" — the value BearerTokenMiddleware short-circuits on —
    # behind a socket the parent bound to the LAN. from_env fails closed,
    # but refusing the combination outright means the question never arises.
    is_local = settings.host == "127.0.0.1"
    if reload is None:
        reload = is_local
    if reload and not is_local:
        typer.echo(
            "--reload is localhost-only: a spawned child cannot safely "
            "rebuild a LAN bind (it would default host to 127.0.0.1 and "
            "bypass the bearer middleware). Re-run without --reload.",
            err=True,
        )
        raise typer.Exit(code=2)

    # Under reload this process outlives every child, so it owns the one
    # report.html regen; the child would otherwise run a full export on
    # every source edit.
    # Only the real server reaches the network on boot. Under the reload
    # supervisor the parent never serves — the child does — so the flag rides
    # across the spawn in the env like every other setting, and the staleness
    # gate absorbs the restarts watchfiles triggers on each source edit.
    settings = settings.model_copy(
        update={
            "regen_report_on_shutdown": not reload,
            "refresh_on_start": True,
        }
    )

    base_url = f"http://localhost:{settings.port}/"
    if settings.token:
        # Print the URL with token query for first-hop convenience
        # (ADR-012); subsequent visits use the cookie.
        from urllib.parse import urlencode

        typer.echo(
            f"finances serve: {base_url}?{urlencode({'token': settings.token})}"
        )
    else:
        typer.echo(f"finances serve: {base_url}")

    if reload:
        typer.echo(
            f"finances serve: watching {PACKAGE_DIR} (*.py, *.html, *.j2) — "
            "code edits restart the server automatically."
        )

    if open_browser:
        threading.Timer(
            1.0,
            lambda: webbrowser.open(base_url),
        ).start()

    if not reload:
        uvicorn.run(
            create_app(settings),
            host=settings.host,
            port=settings.port,
            log_level="info",
        )
        return

    # The reloader re-imports the app in a spawn()'ed child, so it gets an
    # import string and a factory — and the environment is the only thing
    # that crosses into it.
    os.environ.update(settings.to_env())
    try:
        uvicorn.run(
            IMPORT_STRING,
            factory=True,
            reload=True,
            reload_dirs=list(RELOAD_DIRS),
            reload_includes=list(RELOAD_INCLUDES),
            reload_excludes=list(RELOAD_EXCLUDES),
            host=settings.host,
            port=settings.port,
            log_level="info",
            # Bounds the terminate()+join() the supervisor does on every
            # restart, so a request wedged on a SQLite lock cannot hang the
            # reloader indefinitely.
            timeout_graceful_shutdown=10,
        )
    finally:
        # The regen the child no longer does, run once, here, on the way out.
        from finances.web import app as web_app

        web_app._regen_report_on_shutdown(settings)


if __name__ == "__main__":
    app()
