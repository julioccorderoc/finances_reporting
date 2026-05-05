"""Service layer for the local web viewer (EPIC-022 / ADR-012).

Each module here is a thin projection over the existing repos / domain
modules. Per rule-012 the viewer adds no parallel business logic; these
services only assemble query parameters, call existing functions, and
project the results into Pydantic DTOs that are convenient for templates
and the JSON API.
"""

from __future__ import annotations
