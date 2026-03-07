# TODO

## Enhancement Rollout Status

- [x] 1. Canonical-work matching for missing reports
  - Implemented Open Library work-key matching in missing-author endpoint.
  - Falls back to normalized title matching when work key is unavailable.

- [x] 2. Series-level tracking
  - Added series and series-number inference from title/subtitle patterns.
  - Persists into `Series` and `Series Number` properties when present in Notion.

- [x] 3. Author alias normalization
  - Added alias map support via `AUTHOR_ALIASES_JSON` env var.
  - Alias canonicalization is applied before author relation upsert.

- [x] 4. Missing books sync database workflow
  - Added `sync=1` mode to `/api/authors/missing`.
  - Upserts missing works into `NOTION_MISSING_BOOKS_DATA_SOURCE_ID`.

- [x] 5. Confidence scoring
  - Added metadata confidence score and reason list to scan metadata.
  - Returned in verbose scan responses.

- [x] 6. Fast shortcut response format
  - Added compact mode via `compact=1` query parameter.
  - Returns minimized fields for Apple Shortcuts parsing.

- [x] 7. Background enrichment jobs
  - Added `enrichAsync=1` mode for best-effort asynchronous enrichment pass.
  - Initial scan responds quickly, then updates secondary metadata fields.

- [x] 8. Rate-limit handling and provider failover
  - Added retry/backoff fetch wrapper for provider calls.
  - Added Google/Open Library metadata fallback merge.

- [x] 9. Idempotency and write guards
  - Added idempotency support via `x-idempotency-key`.
  - Added write-guard mode via `dryRun=1`.

- [x] 10. API auth and audit events
  - Added optional API key auth (`API_KEY`) for scan and missing endpoints.
  - Added audit logging for scan flow, with optional Notion audit sink.

## Follow-up Hardening (Optional)

- [ ] Add integration tests for compact, dry run, idempotency replay, and sync mode.
- [ ] Add dedicated queue for durable background enrichment beyond in-process execution.

## Apple Shortcut #2: Check-Only Scanner

- [ ] Create a second Apple Shortcut named "Check Book In Library".
- [ ] Add `Scan Barcode` action and pass scanned value as `isbn`.
- [ ] Add `Get Contents of URL` action:
  - Method: `POST`
  - URL: `/api/scan?mode=check&compact=1`
  - Headers: `Content-Type: application/json`
  - Headers: `x-api-key: <API_KEY>` (if API auth enabled)
  - Body JSON: `{ "isbn": "<scanned value>" }`
- [ ] Add response handling branch:
  - If `exists=true`: show "Already in library" and open `notionUrl` optionally.
  - If `exists=false`: show "Not in library" and offer handoff to import shortcut.
- [ ] Add spoken confirmation using returned `speechText`.
- [ ] Add idempotency header for repeated taps (`x-idempotency-key`) if desired.
- [ ] Validate round-trip latency and on-device reliability from lock screen.

## Reliability and Scale Plan

- [ ] Add Vitest unit tests for:
  - `extractIsbn`, `parseSeriesFromTitle`, `scoreMetadataConfidence`, and title normalization logic.
- [ ] Extract pure utility modules from route handlers for easier isolated testing.
- [ ] Add route integration tests with mocked upstream APIs (Google Books, Open Library, Notion).
- [ ] Add response contract tests (e.g., zod schemas in tests) for shortcut-facing payloads.
- [ ] Add CI workflow to run `npm run check` on PRs and main merges.
- [ ] Add optional coverage thresholds for changed files.
- [ ] Add smoke tests for production env var completeness and auth header behavior.
