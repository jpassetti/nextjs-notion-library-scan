# Notion Library Scan

Next.js API and dashboard for scanning ISBNs into a Notion books database.

## Core Endpoints

- `POST /api/scan`
	- Default behavior: add or update a book by ISBN.
	- Body example: `{ "isbn": "9780143127741", "onDuplicate": "update" }`
	- Optional query flags:
		- `compact=1`: minimized shortcut-friendly response payload.
		- `dryRun=1`: compute write payload without mutating Notion.
		- `enrichAsync=1`: best-effort background metadata enrichment.

- `POST /api/scan?mode=check&verbose=1`
	- Check-only behavior: verifies whether an ISBN already exists in Notion.
	- No book page is created or updated in this mode.
	- Body example: `{ "isbn": "9780143127741" }`

- `GET /api/authors/missing?author=Ursula%20K.%20Le%20Guin`
	- Compares books for an author in your Notion database against Open Library author works.
	- Uses canonical Open Library work-key matching when available.
	- Optional `sync=1` writes missing works into a separate Notion data source.

- `POST /api/scan/backfill`
	- Backfills metadata for previously scanned books.

## Environment Variables

Required:

- `NOTION_TOKEN`: Notion integration token.
- `NOTION_DATABASE_ID`: Notion books database ID.

Optional:

- `GOOGLE_BOOKS_API_KEY`: Google Books API key.
- `API_KEY`: Optional API key required via `x-api-key` header (or `Authorization: Bearer <key>`).
- `AUTHOR_ALIASES_JSON`: JSON map for author aliases.
	- Example: `{ "jk rowling": "J. K. Rowling", "joanne rowling": "J. K. Rowling" }`
- `NOTION_AUDIT_DATA_SOURCE_ID`: Optional Notion data source for audit events.
- `NOTION_AUDIT_REQUEST_ID_PROPERTY`: Audit property name for request id (default `Request ID`).
- `NOTION_AUDIT_ACTION_PROPERTY`: Audit property name for action (default `Action`).
- `NOTION_AUDIT_CODE_PROPERTY`: Audit property name for code (default `Code`).
- `NOTION_AUDIT_STATUS_PROPERTY`: Audit property name for status (default `Status`).
- `NOTION_AUDIT_DETAILS_PROPERTY`: Audit property name for details (default `Details`).
- `AUDIT_LOG_TO_CONSOLE`: Console audit toggle (default `1`).

Optional variables for author relation syncing:

- `NOTION_AUTHORS_DATABASE_ID`: Separate Notion authors database ID.
- `NOTION_BOOK_AUTHOR_RELATION_PROPERTY`: Relation property name on books database.
	- If omitted, the app auto-detects a relation property that points to the authors data source.
- `NOTION_AUTHOR_NAME_PROPERTY`: Name field on authors database (default: `Name`).

Optional variables for missing-books sync table:

- `NOTION_MISSING_BOOKS_DATA_SOURCE_ID`: Data source id for storing missing works.
- `NOTION_MISSING_TITLE_PROPERTY`: Missing-books title field (default `Name`).
- `NOTION_MISSING_AUTHOR_PROPERTY`: Missing-books author field (default `Author`).
- `NOTION_MISSING_WORK_KEY_PROPERTY`: Missing-books canonical work key field (default `OpenLibrary Work Key`).

## Getting Started

Run the development server:

```bash
npm run dev
```

Then open one of these URLs:

- Local: http://localhost:3000
- Production: https://nextjs-notion-library-scan.vercel.app/

## Quality Gates

- `npm run lint:strict`: ESLint with zero-warnings policy.
- `npm run typecheck`: strict TypeScript check with no emit.
- `npm run check`: runs lint, typecheck, and production build in sequence.
