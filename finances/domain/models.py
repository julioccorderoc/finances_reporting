from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator

# NOTE: per ADR-009 + rule-009, every model in this file must remain a Pydantic
# v2 BaseModel subclass. Replacing them with @dataclass / TypedDict is
# forbidden. A grep-style guard in tests/test_db_schema.py enforces this.


class TransactionKind(str, Enum):
    INCOME = "income"
    EXPENSE = "expense"
    TRANSFER = "transfer"
    ADJUSTMENT = "adjustment"


class AccountKind(str, Enum):
    BANK = "bank"
    CRYPTO_SPOT = "crypto_spot"
    CRYPTO_FUNDING = "crypto_funding"
    CRYPTO_EARN = "crypto_earn"
    CASH = "cash"
    OTHER = "other"


def _coerce_decimal(v: Any) -> Decimal:
    """Accept Decimal, int, or str; reject float (per ADR-009)."""
    if isinstance(v, Decimal):
        return v
    if isinstance(v, bool):
        raise ValueError("bool is not a valid monetary value")
    if isinstance(v, float):
        raise ValueError("float monetary inputs are forbidden; use Decimal or str")
    if isinstance(v, (int, str)):
        return Decimal(str(v))
    raise ValueError(f"cannot coerce {type(v).__name__} to Decimal")


def _require_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        raise ValueError("datetime must be timezone-aware")
    return dt


class Account(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    id: int | None = None
    name: str
    kind: AccountKind
    currency: str
    institution: str | None = None
    active: bool = True
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @field_validator("currency")
    @classmethod
    def _upper_currency(cls, v: str) -> str:
        return v.upper()

    @field_validator("created_at", "updated_at")
    @classmethod
    def _ensure_aware_timestamps(cls, v: datetime | None) -> datetime | None:
        return None if v is None else _require_aware(v)


class Category(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    id: int | None = None
    kind: TransactionKind
    name: str
    active: bool = True
    created_at: datetime | None = None

    @field_validator("created_at")
    @classmethod
    def _ensure_aware_created_at(cls, v: datetime | None) -> datetime | None:
        return None if v is None else _require_aware(v)


class Transaction(BaseModel):
    # strict mode would normally reject Decimal coercion of str; field
    # validators handle normalization explicitly before strict type check.
    model_config = ConfigDict(strict=False, extra="forbid")

    id: int | None = None
    account_id: int
    occurred_at: datetime
    kind: TransactionKind
    amount: Decimal
    currency: str
    description: str | None = None
    category_id: int | None = None
    transfer_id: str | None = None
    user_rate: Decimal | None = None
    source: str
    source_ref: str | None = None
    needs_review: bool = False
    parked: bool = False
    notes: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @field_validator("amount", "user_rate", mode="before")
    @classmethod
    def _decimal_fields(cls, v: Any) -> Any:
        if v is None:
            return v
        return _coerce_decimal(v)

    @field_validator("occurred_at", "created_at", "updated_at")
    @classmethod
    def _aware_datetimes(cls, v: datetime | None) -> datetime | None:
        return None if v is None else _require_aware(v)

    @field_validator("currency")
    @classmethod
    def _upper_currency(cls, v: str) -> str:
        return v.upper()


class Rate(BaseModel):
    model_config = ConfigDict(strict=False, extra="forbid")

    id: int | None = None
    as_of_date: date
    base: str
    quote: str
    rate: Decimal
    source: str
    created_at: datetime | None = None

    @field_validator("rate", mode="before")
    @classmethod
    def _decimal_rate(cls, v: Any) -> Decimal:
        return _coerce_decimal(v)

    @field_validator("base", "quote")
    @classmethod
    def _upper_currency(cls, v: str) -> str:
        return v.upper()

    @field_validator("created_at")
    @classmethod
    def _aware_created_at(cls, v: datetime | None) -> datetime | None:
        return None if v is None else _require_aware(v)


class EarnPosition(BaseModel):
    model_config = ConfigDict(strict=False, extra="forbid")

    id: int | None = None
    account_id: int
    product_id: str
    asset: str
    principal: Decimal
    apy: Decimal | None = None
    started_at: datetime
    ended_at: datetime | None = None
    snapshot_at: datetime | None = None

    @field_validator("principal", "apy", mode="before")
    @classmethod
    def _decimal_fields(cls, v: Any) -> Any:
        if v is None:
            return v
        return _coerce_decimal(v)

    @field_validator("started_at", "ended_at", "snapshot_at")
    @classmethod
    def _aware_datetimes(cls, v: datetime | None) -> datetime | None:
        return None if v is None else _require_aware(v)

    @field_validator("asset")
    @classmethod
    def _upper_asset(cls, v: str) -> str:
        return v.upper()


class SavedView(BaseModel):
    """A named /transactions filter combination (Wave 2 Thing 2).

    ``query_string`` is the raw querystring without the leading ``?``;
    chips rebuild the URL as ``/transactions?<query_string>``.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    id: int | None = None
    name: str
    query_string: str
    created_at: datetime | None = None

    @field_validator("created_at")
    @classmethod
    def _aware_created_at(cls, v: datetime | None) -> datetime | None:
        return None if v is None else _require_aware(v)


class TransactionEdit(BaseModel):
    """One recorded change to a manually-editable transaction field.

    Rows are written only from inside ``transactions_repo.update()`` (the
    single sanctioned write path, rule-012) — one per field that actually
    changed. ``field`` is constrained to the manual-enrichment columns;
    ``needs_review`` is resolver-derived (ADR-005) and never recorded.
    ``old_value`` / ``new_value`` are stored as TEXT (``None`` = the field
    was empty on that side of the change).
    """

    model_config = ConfigDict(strict=False, extra="forbid")

    id: int | None = None
    transaction_id: int
    edited_at: datetime | None = None
    field: Literal["category_id", "user_rate", "notes"]
    old_value: str | None = None
    new_value: str | None = None

    @field_validator("edited_at")
    @classmethod
    def _aware_edited_at(cls, v: datetime | None) -> datetime | None:
        return None if v is None else _require_aware(v)


__all__ = [
    "Account",
    "AccountKind",
    "Category",
    "EarnPosition",
    "Rate",
    "SavedView",
    "Transaction",
    "TransactionEdit",
    "TransactionKind",
]
