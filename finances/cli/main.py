from __future__ import annotations

from datetime import datetime, time
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import typer

from finances.config import DB_PATH, binance_credentials
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


@app.callback()
def _root() -> None:
    """Finances reporting CLI."""


def _make_binance_client() -> Any:
    from binance.spot import Spot

    api_key, api_secret = binance_credentials()
    return Spot(api_key=api_key, api_secret=api_secret)


@ingest_app.command("binance")
def ingest_binance(
    since: datetime | None = typer.Option(
        None,
        "--since",
        help="ISO timestamp to start ingest from (overrides lookback and stored state).",
    ),
    lookback_days: int = typer.Option(
        35,
        "--lookback-days",
        help="Fallback window when no --since and no stored import_state (default 35).",
    ),
) -> None:
    """Incrementally sync Binance endpoints into the ledger (EPIC-007)."""
    from finances.ingest.binance import sync_binance

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        client = _make_binance_client()
        result = sync_binance(
            conn, client=client, since=since, lookback_days=lookback_days
        )
    finally:
        conn.close()

    typer.echo(
        f"binance sync: inserted={result['rows_inserted']} "
        f"updated={result['rows_updated']} "
        f"earn={result['earn_positions']} errors={len(result['errors'])}"
    )
    for err in result["errors"]:
        typer.echo(f"  err: {err}", err=True)
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
        2,
        "--pairing-window-days",
        help="±Day window used for bank-anchored P2P pairing (default 2).",
    ),
    no_pairing: bool = typer.Option(
        False,
        "--no-pairing",
        help="Skip the bank-anchored P2P pairing pass after upserts.",
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
        )
    finally:
        conn.close()

    typer.echo(
        f"provincial ingest: seen={report.rows_seen} "
        f"inserted={report.rows_inserted} updated={report.rows_updated}"
    )
    if report.reconciliation is not None:
        rec = report.reconciliation
        typer.echo(
            f"  pairing ({rec.strategy}): "
            f"found={rec.proposals_found} applied={rec.proposals_applied} "
            f"errors={len(rec.errors)}"
        )
        for err in rec.errors:
            typer.echo(f"    err: {err}", err=True)


@ingest_app.command("bcv")
def ingest_bcv() -> None:
    """Fetch BCV reference rates (USD/VES, EUR/VES) and upsert into rates (EPIC-009)."""
    from finances.ingest.bcv import ingest_bcv as run_bcv

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        inserted = run_bcv(conn)
    finally:
        conn.close()
    typer.echo(f"bcv: inserted {inserted} rate rows")


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
            conn, as_of_date=as_of_date, asset=asset, fiat=fiat, rows=rows
        )
    finally:
        conn.close()

    typer.echo(
        f"P2P {asset}/{fiat} @ {as_of_date.isoformat()}: "
        f"buy={result['buy_median']} sell={result['sell_median']} "
        f"midpoint={result['midpoint']} "
        f"(n_buy={result['buy_adverts_used']}, n_sell={result['sell_adverts_used']})"
    )


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
        2,
        "--pairing-window-days",
        help="±Day window used for bank-anchored P2P pairing (default 2).",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Re-run even if the ledger already has transactions (wipes cleanup state).",
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
        )
    finally:
        conn.close()

    typer.echo(
        f"backfill: binance={report.binance_rows_inserted}/{report.binance_rows_seen} "
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
    if report.errors:
        raise typer.Exit(code=1)


@app.command("cleanup-export")
def cleanup_export(
    to: Path = typer.Option(
        Path("needs_review.csv"),
        "--to",
        help="CSV destination. Review + fill `category` column in Sheets, then cleanup-apply.",
    ),
    legacy_from: Path = typer.Option(
        None,
        "--legacy-from",
        exists=True,
        file_okay=False,
        dir_okay=True,
        readable=True,
        help=(
            "Optional legacy CSV dir (e.g. data/). Adds `legacy_sub_category` "
            "and `legacy_category` reference columns pulled from the original sheets."
        ),
    ),
) -> None:
    """Dump every needs_review=1 row to a CSV for batch review (EPIC-012)."""
    from finances.migration.interactive_cleanup import export_needs_review

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        count = export_needs_review(conn, to, legacy_dir=legacy_from)
    finally:
        conn.close()
    typer.echo(f"cleanup-export: wrote {count} rows to {to}")


@app.command("cleanup-apply")
def cleanup_apply(
    from_csv: Path = typer.Option(
        ...,
        "--from",
        exists=True,
        dir_okay=False,
        readable=True,
        help="CSV produced by `cleanup-export`, with the `category` column filled.",
    ),
) -> None:
    """Apply a user-edited cleanup CSV back into the ledger (EPIC-012)."""
    from finances.migration.interactive_cleanup import import_cleanup_csv

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    try:
        report = import_cleanup_csv(conn, from_csv)
    finally:
        conn.close()
    typer.echo(
        f"cleanup-apply: seen={report.rows_seen} "
        f"resolved={report.rows_resolved} skipped={report.rows_skipped}"
    )


@app.command("cleanup")
def cleanup(
    limit: int = typer.Option(
        0,
        "--limit",
        help="Stop after N rows (0 = walk every needs_review row).",
    ),
) -> None:
    """Interactive pass that resolves `needs_review=1` rows (EPIC-012)."""
    from finances.migration.interactive_cleanup import run_cleanup

    conn = get_connection(DB_PATH)
    apply_migrations(conn)
    processed = {"n": 0}

    def _prompt(row: Any) -> tuple[str | None, str | None]:
        processed["n"] += 1
        typer.echo("")
        typer.echo(
            f"[{processed['n']}] {row['occurred_at']}  "
            f"{row['source']}  {row['kind']}  "
            f"{row['amount']} {row['currency']}  — {row['description']}"
        )
        category = typer.prompt(
            "  category (blank to skip)", default="", show_default=False
        )
        if not category.strip():
            return (None, None)
        rate = typer.prompt(
            "  user_rate (blank to leave unset)",
            default="",
            show_default=False,
        )
        return (category.strip(), rate.strip() or None)

    def _bounded_prompt(row: Any) -> tuple[str | None, str | None]:
        if limit and processed["n"] >= limit:
            return (None, None)
        return _prompt(row)

    try:
        report = run_cleanup(conn, prompt=_bounded_prompt)
    finally:
        conn.close()

    typer.echo(
        f"cleanup: seen={report.rows_seen} resolved={report.rows_resolved} "
        f"skipped={report.rows_skipped} errors={len(report.errors)}"
    )
    for err in report.errors:
        typer.echo(f"  err: {err}", err=True)


@app.command("categorize")
def categorize(
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Report what would match without writing category_id changes.",
    ),
    source: str | None = typer.Option(
        None,
        "--source",
        help="Restrict to transactions with this source (e.g. 'provincial', 'binance').",
    ),
    db_path: Path = typer.Option(
        None,
        "--db-path",
        help="Path to the SQLite database. Defaults to finances.config.DB_PATH.",
    ),
) -> None:
    """Run the categorization engine over stored transactions (EPIC-004).

    In ``--dry-run`` mode, prints how many rows the seeded rules would classify
    without mutating any data — used to measure rule coverage before running
    the backfill.
    """
    from finances.domain.categorization import CategorizationRequest, suggest

    target_db = Path(db_path) if db_path is not None else Path(DB_PATH)
    conn = get_connection(target_db)
    apply_migrations(conn)
    try:
        params: tuple[Any, ...]
        if source is not None:
            query = (
                "SELECT id, description, source, account_id "
                "FROM transactions WHERE source = ?"
            )
            params = (source,)
        else:
            query = "SELECT id, description, source, account_id FROM transactions"
            params = ()
        rows = conn.execute(query, params).fetchall()

        total = len(rows)
        matched = 0
        for row in rows:
            if suggest(
                conn,
                CategorizationRequest(
                    description=row["description"],
                    source=row["source"],
                    account_id=row["account_id"],
                ),
            ) is not None:
                matched += 1
    finally:
        conn.close()

    scope = source or "all sources"
    if total == 0:
        typer.echo(f"categorize dry-run [{scope}]: no transactions to categorize.")
        return

    pct = matched / total * 100.0
    if dry_run:
        typer.echo(
            f"categorize dry-run [{scope}]: {matched}/{total} = {pct:.1f}% "
            "would auto-classify."
        )
        return

    typer.echo(
        "Refusing to write: re-categorization is owned by EPIC-012 backfill.\n"
        "Re-run with --dry-run to see preview counts.",
        err=True,
    )
    raise typer.Exit(code=2)


def _resolve_report_format(json_flag: bool, csv_flag: bool) -> str:
    if json_flag and csv_flag:
        raise typer.BadParameter("pass --json or --csv, not both")
    if json_flag:
        return "json"
    if csv_flag:
        return "csv"
    return "table"


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


if __name__ == "__main__":
    app()
