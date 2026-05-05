"""HTMX fragment endpoints (EPIC-022 / ADR-012).

Phase 1 only stubs the router; Phase 2 wires the per-page fragments.
"""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/_partial")
