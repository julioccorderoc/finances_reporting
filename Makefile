# EPIC-021 convenience targets.
#
# `make unit`        — fast unit/smoke suite (everything except integration).
# `make integration` — end-to-end pipeline tests from tests/integration/.
# `make test`        — both, back-to-back, matching CI's coverage of the suite.
#
# These wrap `uv run pytest`; pass additional flags via `PYTEST_ARGS=...`.

PYTEST ?= uv run pytest
PYTEST_ARGS ?=

.PHONY: unit integration test

unit:
	$(PYTEST) -m "not integration" $(PYTEST_ARGS)

integration:
	$(PYTEST) -m integration $(PYTEST_ARGS)

test: unit integration
