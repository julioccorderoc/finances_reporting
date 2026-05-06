# Web Viewer — LAN access from iPhone Safari

Goal: open the local web viewer from your iPhone over the home Wi-Fi.

## TL;DR

```bash
# On the Mac:
export FINANCES_WEB_TOKEN=$(python -c 'import secrets; print(secrets.token_urlsafe(24))')
finances serve --host 0.0.0.0 --port 8765
```

The CLI prints a URL like `http://<your-mac-hostname>.local:8765/?token=...`.
Open it on your iPhone (same Wi-Fi). The token is stored as a session cookie
on the first request, so subsequent requests don't need the query string.

## What `finances serve` does

- Default `--host 127.0.0.1` is localhost-only. No auth required.
- `--host 0.0.0.0` requires `--token` or the `FINANCES_WEB_TOKEN` env var. Without
  one, the command exits with a non-zero error.
- `--open` opens your default browser to the URL on the Mac itself.
- `--port 8765` is the default; pick anything free if 8765 is taken.

## iPhone Safari specifics

- Use `http://`, not `https://`. Self-signed TLS is not in v1 scope.
- The hostname `<your-mac-hostname>.local` is published via Bonjour and iOS
  resolves it natively. If it doesn't, find your Mac's IP under System Settings
  → Network and use that instead.
- "Add to Home Screen" works — the viewer's `<meta name="viewport">` is set.
- Mobile breakpoints kick in at `≤640px`: card-row layouts stack vertically,
  pair-confirm modals become full-screen sheets, the nav wraps. Tested on
  iPhone 13 Mini, iPhone 15.

## Troubleshooting

| Symptom | Fix |
|---|---|
| 401 Unauthorized | Token mismatch. Re-copy the URL the CLI printed at startup. |
| `finances` command not found | Run `uv pip install -e .` from the project root. |
| `finances serve` fails to import | Stale editable-install path. Re-run `uv pip install -e .`. |
| Port already in use | Pass `--port 8766` (or any free port). |
| Skip-store resets | The "Skip → bottom" affordance is per-process and per-app-instance. Restarting `finances serve` clears it. Intentional v1 behavior. |
| Bonjour `.local` doesn't resolve | Use the Mac's IP from System Settings → Network instead. |
| Modal won't close on iPhone | Reload the page once. The `HX-Trigger: closeModal` event flow depends on a small Alpine listener loaded from `/static/vendor/alpine.min.js`; stale Safari caches occasionally lose the binding. |

## Security notes

- LAN access is plaintext HTTP. Acceptable on a trusted home network only.
- The token is shared. Anyone with the URL has full read+write access to the
  ledger. Don't paste it into shared chats. Rotate by setting a new
  `FINANCES_WEB_TOKEN` and restarting.
- `--host 0.0.0.0` binds all interfaces, which on a home router means your
  home subnet. On public Wi-Fi (cafés, airports), keep the default
  `--host 127.0.0.1`.
- Single-process / single-worker only. Skip-store and any in-memory state are
  not shared across workers; this is intentional for v1.

## Related docs

- [ADR-012](../ADR/ADR-012-local-web-viewer.md) — architecture decisions for the viewer.
- [rule-012](../architecture/rules/rule-012-web-viewer-uses-existing-domain.md) — viewer must reuse existing repo/domain APIs.
- [docs/plans/web-viewer-v1.md](../plans/web-viewer-v1.md) — the v1 plan.
