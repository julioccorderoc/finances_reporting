"""Full HTML page routes (EPIC-022 / ADR-012).

Phase 1 ships a single placeholder route at ``/`` so the foundation
end-to-end test (``finances serve`` → curl) returns a real Jinja
render. Phase 2 fills in the rest of the page surface.
"""

from __future__ import annotations

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/", include_in_schema=False)
def dashboard(request: Request):
    templates = request.app.state.templates
    return templates.TemplateResponse(
        request,
        "pages/dashboard.html",
        {"title": "Finances"},
    )
