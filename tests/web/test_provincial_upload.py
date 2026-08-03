"""Drop a Provincial statement on the viewer (WP3).

The owner downloads a statement and today has to move it into ``inputs/`` by
hand and run ``finances update`` in a terminal. This is the browser path:
drop → dry-run preview → confirm → ingest.

Contract these tests pin:

* Preview writes nothing. Import writes exactly what preview promised.
* A staged-but-unconfirmed file must be invisible to ``finances update`` —
  otherwise previewing a file would queue it for ingest behind the owner's
  back.
* The filename is irrelevant; the suffix is not. The bank's "``.xls``" is
  really an HTML table, already handled by ``provincial.iter_raw_rows``.
* Re-importing the same statement inserts nothing new
  (``UNIQUE(source, source_ref)``).
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from finances.db.repos import accounts as accounts_repo
from finances.domain.models import Account, AccountKind
from finances.web.services import uploads as uploads_svc

CSV_BODY = (
    "Fecha;Descripcion;Monto;Saldo\n"
    "14/07/2026;COM. PAGO MOVIL;-86,88;1.000,00\n"
    "15/07/2026;DR OB V17194172 102BANCO;-28.960,00;900,00\n"
    "16/07/2026;DR OB 04124423729 102BAN;20.000,00;20.900,00\n"
)

XLS_BODY = """<html><body><table>
<tr><th>Fecha</th><th>Descripcion</th><th>Monto</th><th>Saldo</th></tr>
<tr><td>14/07/2026</td><td>COM. PAGO MOVIL</td><td>-86,88</td><td>1.000,00</td></tr>
<tr><td>15/07/2026</td><td>DR OB V17194172</td><td>-28.960,00</td><td>900,00</td></tr>
</table></body></html>"""


@pytest.fixture
def provincial_db(web_db: sqlite3.Connection) -> sqlite3.Connection:
    accounts_repo.insert(
        web_db,
        Account(
            name="Provincial Bolivares",
            kind=AccountKind.BANK,
            currency="VES",
        ),
    )
    return web_db


@pytest.fixture
def staging(tmp_path: Path) -> Path:
    d = tmp_path / "staging"
    d.mkdir()
    return d


@pytest.fixture
def inputs_dir(tmp_path: Path) -> Path:
    d = tmp_path / "inputs"
    d.mkdir()
    return d


@pytest.fixture(autouse=True)
def _isolated_inputs_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Point the routes' ``inputs/`` at tmp_path.

    Autouse and unconditional: without it the route tests would stage and
    archive real files inside the repo's own ``inputs/``, and an archived
    fixture statement would then be waiting for the next ``finances update``.
    """
    from finances import config as _config

    d = tmp_path / "route-inputs"
    d.mkdir()
    monkeypatch.setattr(_config, "INPUTS_DIR", d)
    return d


def _stage(staging: Path, body: str = CSV_BODY, name: str = "statement.csv"):
    return uploads_svc.stage_upload(
        body.encode("utf-8"), filename=name, staging_dir=staging
    )


# ---------------------------------------------------------------------------
# Accepting the file.
# ---------------------------------------------------------------------------


def test_rejects_an_unsupported_suffix(staging: Path) -> None:
    with pytest.raises(uploads_svc.UploadRejected, match="csv"):
        _stage(staging, name="statement.txt")


def test_rejects_a_file_over_the_size_cap(staging: Path) -> None:
    oversized = "x" * (uploads_svc.MAX_UPLOAD_BYTES + 1)

    with pytest.raises(uploads_svc.UploadRejected, match="too large"):
        _stage(staging, body=oversized)


def test_accepts_any_filename_with_a_supported_suffix(staging: Path) -> None:
    """No naming convention — the owner drops whatever the bank produced."""
    staged = _stage(staging, name="provincial-july-2026 (1).csv")

    assert staged.filename == "provincial-july-2026 (1).csv"


def test_a_traversal_filename_cannot_escape_the_staging_dir(
    staging: Path,
) -> None:
    staged = _stage(staging, name="../../evil.csv")

    assert staging in uploads_svc.staged_path(staged.token, staging).parents


def test_an_unknown_token_is_rejected(staging: Path) -> None:
    with pytest.raises(uploads_svc.UploadRejected):
        uploads_svc.staged_path("../../../etc/passwd", staging)


# ---------------------------------------------------------------------------
# Preview — reads, never writes.
# ---------------------------------------------------------------------------


def test_preview_reports_rows_and_date_range(
    provincial_db: sqlite3.Connection, staging: Path
) -> None:
    staged = _stage(staging)

    preview = uploads_svc.preview_upload(
        provincial_db, staged.token, staging_dir=staging
    )

    assert preview.rows_seen == 3
    assert preview.rows_new == 3
    assert preview.rows_known == 0
    assert preview.date_from.isoformat() == "2026-07-14"
    assert preview.date_to.isoformat() == "2026-07-16"


def test_preview_writes_nothing_to_the_ledger(
    provincial_db: sqlite3.Connection, staging: Path
) -> None:
    staged = _stage(staging)

    uploads_svc.preview_upload(
        provincial_db, staged.token, staging_dir=staging
    )

    n = provincial_db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    assert n == 0


def test_preview_records_no_import_run(
    provincial_db: sqlite3.Connection, staging: Path
) -> None:
    staged = _stage(staging)

    uploads_svc.preview_upload(
        provincial_db, staged.token, staging_dir=staging
    )

    n = provincial_db.execute("SELECT COUNT(*) FROM import_runs").fetchone()[0]
    assert n == 0


def test_preview_parses_the_html_masquerading_as_xls(
    provincial_db: sqlite3.Connection, staging: Path
) -> None:
    """The bank's web export is an HTML table named .xls."""
    staged = _stage(staging, body=XLS_BODY, name="provincial-july-2026.xls")

    preview = uploads_svc.preview_upload(
        provincial_db, staged.token, staging_dir=staging
    )

    assert preview.rows_seen == 2


def test_preview_reports_a_parse_failure_instead_of_raising(
    provincial_db: sqlite3.Connection, staging: Path
) -> None:
    staged = _stage(staging, body="not;a;statement\n1;2;3\n")

    preview = uploads_svc.preview_upload(
        provincial_db, staged.token, staging_dir=staging
    )

    assert preview.error is not None
    assert preview.rows_seen == 0


def test_preview_counts_already_known_rows_separately(
    provincial_db: sqlite3.Connection, staging: Path, inputs_dir: Path
) -> None:
    first = _stage(staging)
    uploads_svc.commit_upload(
        provincial_db, first.token, staging_dir=staging, inputs_dir=inputs_dir
    )
    second = _stage(staging)

    preview = uploads_svc.preview_upload(
        provincial_db, second.token, staging_dir=staging
    )

    assert preview.rows_new == 0
    assert preview.rows_known == 3


# ---------------------------------------------------------------------------
# Import — writes, then archives.
# ---------------------------------------------------------------------------


def test_import_writes_the_rows_preview_promised(
    provincial_db: sqlite3.Connection, staging: Path, inputs_dir: Path
) -> None:
    staged = _stage(staging)
    preview = uploads_svc.preview_upload(
        provincial_db, staged.token, staging_dir=staging
    )

    result = uploads_svc.commit_upload(
        provincial_db, staged.token, staging_dir=staging, inputs_dir=inputs_dir
    )

    assert result.rows_inserted == preview.rows_new
    n = provincial_db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    assert n == 3


def test_import_archives_the_file_under_processed(
    provincial_db: sqlite3.Connection, staging: Path, inputs_dir: Path
) -> None:
    staged = _stage(staging, name="provincial-july-2026.csv")

    uploads_svc.commit_upload(
        provincial_db, staged.token, staging_dir=staging, inputs_dir=inputs_dir
    )

    archived = list((inputs_dir / "processed").iterdir())
    assert [p.name for p in archived] == ["provincial-july-2026.csv"]


def test_reimporting_the_same_statement_inserts_nothing_new(
    provincial_db: sqlite3.Connection, staging: Path, inputs_dir: Path
) -> None:
    uploads_svc.commit_upload(
        provincial_db,
        _stage(staging).token,
        staging_dir=staging,
        inputs_dir=inputs_dir,
    )

    result = uploads_svc.commit_upload(
        provincial_db,
        _stage(staging).token,
        staging_dir=staging,
        inputs_dir=inputs_dir,
    )

    assert result.rows_inserted == 0
    n = provincial_db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    assert n == 3


def test_a_failed_import_leaves_the_ledger_untouched(
    provincial_db: sqlite3.Connection, staging: Path, inputs_dir: Path
) -> None:
    staged = _stage(staging, body="not;a;statement\n1;2;3\n")

    with pytest.raises(Exception):
        uploads_svc.commit_upload(
            provincial_db,
            staged.token,
            staging_dir=staging,
            inputs_dir=inputs_dir,
        )

    n = provincial_db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    assert n == 0
    assert not (inputs_dir / "processed").exists()


def test_staged_files_are_invisible_to_finances_update(
    staging: Path, inputs_dir: Path
) -> None:
    """Previewing must not queue a file for the next ritual run."""
    from finances.reports.update import _discover_provincial_files

    nested = inputs_dir / uploads_svc.STAGING_DIR_NAME
    nested.mkdir()
    _stage(nested)

    assert _discover_provincial_files(inputs_dir) == []


# ---------------------------------------------------------------------------
# Routes.
# ---------------------------------------------------------------------------


def test_preview_route_renders_the_summary(
    provincial_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    response = client.post(
        "/_partial/uploads/provincial/preview",
        files={"file": ("statement.csv", CSV_BODY, "text/csv")},
    )

    assert response.status_code == 200
    assert "statement.csv" in response.text
    assert "3" in response.text
    assert 'data-upload-token' in response.text


def test_preview_route_rejects_a_bad_suffix_without_a_500(
    provincial_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    response = client.post(
        "/_partial/uploads/provincial/preview",
        files={"file": ("notes.txt", "hello", "text/plain")},
    )

    assert response.status_code == 200
    assert "csv" in response.text.lower()


def test_import_route_ingests_and_toasts(
    provincial_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()
    token = client.post(
        "/_partial/uploads/provincial/preview",
        files={"file": ("statement.csv", CSV_BODY, "text/csv")},
    ).text.split('data-upload-token="', 1)[1].split('"', 1)[0]

    response = client.post(
        "/_partial/uploads/provincial/import", data={"token": token}
    )

    assert response.status_code == 200
    # base.html re-dispatches the "toast" key as the show-toast window event.
    trigger = json.loads(response.headers["HX-Trigger"])
    assert trigger["toast"]["level"] == "success"
    assert "3 new" in trigger["toast"]["message"]
    n = provincial_db.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    assert n == 3


def test_transactions_page_offers_the_dropzone(
    provincial_db: sqlite3.Connection, web_client_factory
) -> None:
    client = web_client_factory()

    html = client.get("/transactions").text

    assert "data-provincial-dropzone" in html
