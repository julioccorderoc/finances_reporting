"""JSON API endpoints (EPIC-022 / ADR-012).

Phase 1 only stubs the router; Phase 2/3 wire the read + write surfaces.
This is also the foundation for the deferred EPIC-016 mobile API.
"""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/api")
