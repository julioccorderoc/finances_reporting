"""Browser-dropped Provincial statements (WP3).

Three steps, deliberately separate so the owner sees what they are about to
commit before it lands in the ledger:

1. :func:`stage_upload` — validate the bytes and park them outside ``inputs/``
   proper. Staging lives in ``inputs/<STAGING_DIR_NAME>/``, a *directory*, so
   ``reports.update._discover_provincial_files`` (which only picks up
   ``is_file()`` entries) cannot sweep an unconfirmed file into the next
   ritual run.
2. :func:`preview_upload` — ``ingest_csv(dry_run=True)``. Writes nothing to
   ``transactions``, ``import_runs`` or ``import_state``.
3. :func:`commit_upload` — the real ingest, then archive to
   ``inputs/processed/``.

Parsing is entirely ``finances.ingest.provincial``'s job — including the
bank's web export, an HTML table named ``.xls``, which ``iter_raw_rows``
already sniffs. Nothing here re-implements a statement reader (rule-004).
"""

from __future__ import annotations

import re
import shutil
import sqlite3
from datetime import date
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, ConfigDict

from finances.ingest import provincial as provincial_ingest
from finances.reports.update import _archive_processed

#: Suffixes the bank actually produces. ``.xls`` is HTML in disguise.
ALLOWED_SUFFIXES: frozenset[str] = frozenset({".csv", ".xls"})

#: A Provincial month is ~14 KB. Five megabytes is a generous ceiling that
#: still stops a stray upload from filling the disk.
MAX_UPLOAD_BYTES: int = 5 * 1024 * 1024

#: Subdirectory of ``inputs/`` holding previewed-but-unconfirmed uploads.
#: A directory, not a prefix, precisely so file-discovery skips it.
STAGING_DIR_NAME: str = ".staging"

_TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")


class UploadRejected(Exception):
    """The upload was refused before anything was parsed."""


class StagedUpload(BaseModel):
    """A file accepted and parked, not yet previewed or ingested."""

    model_config = ConfigDict(extra="forbid")

    token: str
    filename: str
    size_bytes: int


class UploadPreview(BaseModel):
    """What a dry-run ingest of a staged file would do.

    ``error`` non-``None`` means the file could not be parsed; every count is
    then zero and the owner is shown the reason instead of a summary.
    """

    model_config = ConfigDict(extra="forbid")

    token: str
    filename: str
    rows_seen: int = 0
    rows_new: int = 0
    rows_known: int = 0
    date_from: date | None = None
    date_to: date | None = None
    error: str | None = None


class UploadResult(BaseModel):
    """What a committed ingest actually did."""

    model_config = ConfigDict(extra="forbid")

    filename: str
    rows_inserted: int
    rows_updated: int
    archived_to: Path


def _safe_name(filename: str) -> str:
    """Strip any directory component. ``../../evil.csv`` -> ``evil.csv``."""
    name = Path(filename).name.strip()
    return name or "upload"


def stage_upload(
    data: bytes, *, filename: str, staging_dir: Path
) -> StagedUpload:
    """Validate ``data`` and park it under a fresh token.

    Raises:
        UploadRejected: unsupported suffix, or larger than
            :data:`MAX_UPLOAD_BYTES`.
    """
    name = _safe_name(filename)
    suffix = Path(name).suffix.lower()
    if suffix not in ALLOWED_SUFFIXES:
        allowed = ", ".join(sorted(ALLOWED_SUFFIXES))
        raise UploadRejected(
            f"{name!r} is not a Provincial statement — expected {allowed}."
        )
    if len(data) > MAX_UPLOAD_BYTES:
        limit_mb = MAX_UPLOAD_BYTES // (1024 * 1024)
        raise UploadRejected(
            f"{name!r} is too large ({len(data) / 1024 / 1024:.1f} MB); "
            f"the limit is {limit_mb} MB."
        )

    token = uuid4().hex
    target_dir = staging_dir / token
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / name).write_bytes(data)
    return StagedUpload(token=token, filename=name, size_bytes=len(data))


def staged_path(token: str, staging_dir: Path) -> Path:
    """Resolve ``token`` to its staged file.

    The token is the only thing that crosses back from the browser, so it is
    validated as a bare 32-hex string rather than trusted as a path segment.

    Raises:
        UploadRejected: malformed or unknown token.
    """
    if not _TOKEN_RE.match(token or ""):
        raise UploadRejected("that upload is no longer available — drop it again.")
    target_dir = staging_dir / token
    if not target_dir.is_dir():
        raise UploadRejected("that upload is no longer available — drop it again.")
    files = sorted(p for p in target_dir.iterdir() if p.is_file())
    if not files:
        raise UploadRejected("that upload is no longer available — drop it again.")
    return files[0]


def _date_span(path: Path) -> tuple[date | None, date | None]:
    days = [raw.to_datetime().date() for raw in provincial_ingest.iter_raw_rows(path)]
    return (min(days), max(days)) if days else (None, None)


def preview_upload(
    conn: sqlite3.Connection, token: str, *, staging_dir: Path
) -> UploadPreview:
    """Dry-run the staged file and summarise what importing would do.

    A parse failure is reported in :attr:`UploadPreview.error`, not raised:
    the owner dropped the wrong file, which is an answer, not a crash.
    """
    path = staged_path(token, staging_dir)
    try:
        report = provincial_ingest.ingest_csv(conn, path, dry_run=True)
        date_from, date_to = _date_span(path)
    except Exception as exc:  # noqa: BLE001 - surfaced to the owner as text
        # Plain words, then the parser's own reason (it names the missing
        # columns). The exception class is for the log, not the dropzone.
        return UploadPreview(
            token=token,
            filename=path.name,
            error=f"Could not read {path.name}: {exc}",
        )
    return UploadPreview(
        token=token,
        filename=path.name,
        rows_seen=report.rows_seen,
        rows_new=report.rows_inserted,
        rows_known=report.rows_updated,
        date_from=date_from,
        date_to=date_to,
    )


def commit_upload(
    conn: sqlite3.Connection,
    token: str,
    *,
    staging_dir: Path,
    inputs_dir: Path,
) -> UploadResult:
    """Ingest the staged file for real, then archive it.

    The file is archived only after a clean ingest, so a statement that blew
    up stays put and the ledger is untouched — ``ingest_csv`` rolls back and
    records the failure in ``import_runs`` itself.
    """
    path = staged_path(token, staging_dir)
    report = provincial_ingest.ingest_csv(conn, path)
    archived = _archive_processed(path, inputs_dir)
    shutil.rmtree(path.parent, ignore_errors=True)
    return UploadResult(
        filename=path.name,
        rows_inserted=report.rows_inserted,
        rows_updated=report.rows_updated,
        archived_to=archived,
    )


__all__ = [
    "ALLOWED_SUFFIXES",
    "MAX_UPLOAD_BYTES",
    "STAGING_DIR_NAME",
    "StagedUpload",
    "UploadPreview",
    "UploadRejected",
    "UploadResult",
    "commit_upload",
    "preview_upload",
    "stage_upload",
    "staged_path",
]
