import { NextResponse } from "next/server";
import { Client } from "@notionhq/client";

export const runtime = "nodejs"; // important for Notion SDK

const notion = new Client({ auth: process.env.NOTION_TOKEN });

type JsonObject = Record<string, unknown>;
type NotionQueryArgs = Parameters<typeof notion.dataSources.query>[0];
type NotionCreateArgs = Parameters<typeof notion.pages.create>[0];
type NotionUpdateArgs = Parameters<typeof notion.pages.update>[0];

function asRecord(value: unknown): JsonObject | null {
    if (!value || typeof value !== "object" || Array.isArray(value)) return null;
    return value as JsonObject;
}

function asArray(value: unknown): unknown[] {
    return Array.isArray(value) ? value : [];
}

function getString(value: unknown) {
    return typeof value === "string" ? value : null;
}

type IdempotentEntry = {
    status: number;
    body: JsonObject;
    storedAt: number;
};

const IDEMPOTENCY_CACHE = new Map<string, IdempotentEntry>();
const IDEMPOTENCY_TTL_MS = 5 * 60 * 1000;

function parseBool(input: unknown) {
    if (typeof input === "boolean") return input;
    if (typeof input === "number") return input !== 0;
    const s = String(input ?? "").trim().toLowerCase();
    return s === "1" || s === "true" || s === "yes" || s === "on";
}

function parseAliasesMap(raw: string | undefined) {
    if (!raw) return new Map<string, string>();
    try {
        const parsed = JSON.parse(raw) as Record<string, string>;
        const out = new Map<string, string>();
        for (const [k, v] of Object.entries(parsed)) {
            out.set(k.trim().toLowerCase(), String(v).trim());
        }
        return out;
    } catch {
        return new Map<string, string>();
    }
}

const AUTHOR_ALIASES = parseAliasesMap(process.env.AUTHOR_ALIASES_JSON);

function makeCompactPayload(input: {
    ok: boolean;
    code: string;
    message: string;
    isbn?: string | null;
    title?: string | null;
    firstAuthor?: string | null;
    notionUrl?: string | null;
    exists?: boolean;
    speechText?: string | null;
    requestId: string;
    durationMs: number;
}) {
    return {
        ok: input.ok,
        code: input.code,
        message: input.message,
        isbn: input.isbn ?? null,
        title: input.title ?? null,
        author: input.firstAuthor ?? null,
        notionUrl: input.notionUrl ?? null,
        exists: input.exists ?? null,
        speechText: input.speechText ?? null,
        requestId: input.requestId,
        durationMs: input.durationMs,
    };
}

function getApiKeyFromRequest(req: Request) {
    const header = req.headers.get("x-api-key") || req.headers.get("authorization") || "";
    if (header.startsWith("Bearer ")) return header.slice(7).trim();
    return header.trim();
}

function isApiAuthorized(req: Request) {
    const required = process.env.API_KEY;
    if (!required) return true;
    return getApiKeyFromRequest(req) === required;
}

function maybeStoreIdempotent(key: string | null, status: number, body: JsonObject) {
    if (!key) return;
    IDEMPOTENCY_CACHE.set(key, {
        status,
        body,
        storedAt: Date.now(),
    });
}

function maybeGetIdempotent(key: string | null) {
    if (!key) return null;
    const hit = IDEMPOTENCY_CACHE.get(key);
    if (!hit) return null;
    if (Date.now() - hit.storedAt > IDEMPOTENCY_TTL_MS) {
        IDEMPOTENCY_CACHE.delete(key);
        return null;
    }
    return hit;
}

function pruneIdempotencyCache() {
    const now = Date.now();
    for (const [k, v] of IDEMPOTENCY_CACHE.entries()) {
        if (now - v.storedAt > IDEMPOTENCY_TTL_MS) {
            IDEMPOTENCY_CACHE.delete(k);
        }
    }
}

function toAuditLog(event: {
    requestId: string;
    route: string;
    isbn?: string | null;
    action: string;
    status: number;
    code: string;
    durationMs: number;
    metadata?: JsonObject;
}) {
    return {
        ts: new Date().toISOString(),
        ...event,
    };
}

async function writeAuditEvent(event: {
    requestId: string;
    route: string;
    isbn?: string | null;
    action: string;
    status: number;
    code: string;
    durationMs: number;
    metadata?: JsonObject;
}) {
    const payload = toAuditLog(event);
    if (parseBool(process.env.AUDIT_LOG_TO_CONSOLE ?? "1")) {
        console.info("scan_audit", payload);
    }

    const auditDataSourceId = process.env.NOTION_AUDIT_DATA_SOURCE_ID;
    const auditRequestIdProperty = process.env.NOTION_AUDIT_REQUEST_ID_PROPERTY ?? "Request ID";
    const auditActionProperty = process.env.NOTION_AUDIT_ACTION_PROPERTY ?? "Action";
    const auditCodeProperty = process.env.NOTION_AUDIT_CODE_PROPERTY ?? "Code";
    const auditStatusProperty = process.env.NOTION_AUDIT_STATUS_PROPERTY ?? "Status";
    const auditDetailsProperty = process.env.NOTION_AUDIT_DETAILS_PROPERTY ?? "Details";

    if (!auditDataSourceId) return;
    try {
        await notion.pages.create({
            parent: { data_source_id: auditDataSourceId },
            properties: {
                [auditRequestIdProperty]: { rich_text: [{ text: { content: event.requestId } }] },
                [auditActionProperty]: { rich_text: [{ text: { content: event.action.slice(0, 2000) } }] },
                [auditCodeProperty]: { rich_text: [{ text: { content: event.code.slice(0, 2000) } }] },
                [auditStatusProperty]: { number: event.status },
                [auditDetailsProperty]: {
                    rich_text: [{ text: { content: JSON.stringify(payload).slice(0, 2000) } }],
                },
            } as unknown as NotionCreateArgs["properties"],
        });
    } catch {
        // Swallow audit failures so scan UX is unaffected.
    }
}

function normalizeHeaderIdempotencyKey(req: Request) {
    const direct = req.headers.get("x-idempotency-key") || req.headers.get("idempotency-key") || "";
    const normalized = direct.trim();
    return normalized.length ? normalized : null;
}

async function sleep(ms: number) {
    await new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(
    input: string,
    init: RequestInit,
    options?: { retries?: number; baseDelayMs?: number; retryOnStatuses?: number[] }
) {
    const retries = options?.retries ?? 2;
    const baseDelayMs = options?.baseDelayMs ?? 160;
    const retryOnStatuses = new Set(options?.retryOnStatuses ?? [429, 500, 502, 503, 504]);

    let lastErr: unknown;
    for (let attempt = 0; attempt <= retries; attempt += 1) {
        try {
            const res = await fetch(input, init);
            if (retryOnStatuses.has(res.status) && attempt < retries) {
                await sleep(baseDelayMs * (attempt + 1));
                continue;
            }
            return res;
        } catch (err) {
            lastErr = err;
            if (attempt < retries) {
                await sleep(baseDelayMs * (attempt + 1));
                continue;
            }
        }
    }
    throw lastErr instanceof Error ? lastErr : new Error("Request failed after retries");
}

function errorResponse(params: {
    status: number;
    code: string;
    message: string;
    suggestion?: string;
    retryable?: boolean;
    details?: Record<string, unknown>;
    requestId: string;
    durationMs: number;
    verbose?: boolean;
}) {
    const { status, code, message, suggestion, retryable = false, details, requestId, durationMs, verbose = false } = params;

    const basePayload = {
        ok: false,
        status,
        code,
        message,
        suggestion: suggestion ?? null,
        retryable,
        requestId,
        durationMs,
        timestamp: new Date().toISOString(),
    };

    return NextResponse.json(
        verbose ? { ...basePayload, details: details ?? null } : basePayload,
        { status }
    );
}

function extractIsbn(raw: unknown) {
    if (!raw) return null;
    const s = String(raw).toUpperCase();

    // Prefer ISBN-13 if present
    const m13 = s.match(/97[89]\d{10}/);
    if (m13) return m13[0];

    // Otherwise ISBN-10 (may end in X)
    const m10 = s.match(/\b\d{9}[\dX]\b/);
    if (m10) return m10[0];

    // Fallback: strip non ISBN-ish chars
    const cleaned = s.replace(/[^0-9X]/g, "");
    return cleaned.length >= 10 ? cleaned : null;
}

function upgradeGoogleThumb(url: string) {
    // Commonly works for Google Books image links
    return url.replace("zoom=0", "zoom=2").replace("zoom=1", "zoom=2");
}

function isKnownUnavailableCoverUrl(url: string) {
    const u = url.toLowerCase();
    return u.includes("image_not_available") || u.includes("no_cover") || u.includes("nophoto");
}

async function urlLooksLikeImage(url: string) {
    try {
        const head = await fetch(url, { method: "HEAD", cache: "no-store", redirect: "follow" });
        if (head.ok) {
            const contentType = head.headers.get("content-type") || "";
            if (contentType.startsWith("image/")) return true;
        }

        // Some hosts do not support HEAD consistently.
        if (head.status === 405 || head.status === 403) {
            const get = await fetch(url, { method: "GET", cache: "no-store", redirect: "follow" });
            if (!get.ok) return false;
            const contentType = get.headers.get("content-type") || "";
            return contentType.startsWith("image/");
        }

        return false;
    } catch {
        return false;
    }
}

async function resolveBestCoverUrl(isbn: string, googleCoverUrl: string | null) {
    const openLibraryLarge = `https://covers.openlibrary.org/b/isbn/${encodeURIComponent(isbn)}-L.jpg?default=false`;
    const openLibraryMedium = `https://covers.openlibrary.org/b/isbn/${encodeURIComponent(isbn)}-M.jpg?default=false`;

    // Prefer Google first to reduce Open Library archive proxy redirects.
    if (googleCoverUrl && !isKnownUnavailableCoverUrl(googleCoverUrl) && await urlLooksLikeImage(googleCoverUrl)) {
        return { coverUrl: googleCoverUrl, coverSource: "google_books" };
    }

    if (await urlLooksLikeImage(openLibraryLarge)) {
        return { coverUrl: openLibraryLarge, coverSource: "openlibrary" };
    }
    if (await urlLooksLikeImage(openLibraryMedium)) {
        return { coverUrl: openLibraryMedium, coverSource: "openlibrary" };
    }

    return { coverUrl: null, coverSource: null };
}

function cleanCategories(cats: string[]) {
    const uniq = Array.from(
        new Set(
            cats
                .map((c) => String(c).trim())
                .filter(Boolean)
        )
    );
    return uniq.slice(0, 8).map((name) => ({ name: name.slice(0, 50) }));
}

function todayIsoDate() {
    return new Date().toISOString().slice(0, 10);
}

function notionPageIdCompact(pageId: string) {
    // Notion URLs use the page id without dashes
    return pageId.replace(/-/g, "");
}

function notionPageUrl(pageId: string) {
    const clean = notionPageIdCompact(pageId);
    return `https://www.notion.so/${clean}`;
}

function notionDeepLinkUrl(pageId: string) {
    // This scheme typically opens the Notion app on iOS/macOS
    const clean = notionPageIdCompact(pageId);
    return `notion://www.notion.so/${clean}`;
}

async function fetchGoogleBooksByIsbn(isbn: string) {
    const key = process.env.GOOGLE_BOOKS_API_KEY;
    const url =
        `https://www.googleapis.com/books/v1/volumes?q=isbn:${encodeURIComponent(isbn)}` +
        (key ? `&key=${encodeURIComponent(key)}` : "");

    const res = await fetchWithRetry(url, { cache: "no-store" }, { retries: 2 });
    if (!res.ok) throw new Error(`Google Books error: ${res.status}`);
    const data = await res.json();

    const item = data.items?.[0];
    if (!item) return null;

    const v = item.volumeInfo ?? {};
    const rawCoverUrl = v.imageLinks?.thumbnail ?? v.imageLinks?.smallThumbnail ?? null;
    const upgradedGoogleCoverUrl = rawCoverUrl ? upgradeGoogleThumb(rawCoverUrl) : null;
    const { coverUrl, coverSource } = await resolveBestCoverUrl(isbn, upgradedGoogleCoverUrl);
    return {
        provider: "google_books",
        googleId: item.id ?? null,
        title: v.title ?? null,
        subtitle: v.subtitle ?? null,
        authors: Array.isArray(v.authors) ? v.authors : [],
        publisher: v.publisher ?? null,
        publishedDate: v.publishedDate ?? null, // YYYY or YYYY-MM or YYYY-MM-DD
        pageCount: typeof v.pageCount === "number" ? v.pageCount : null,
        categories: Array.isArray(v.categories) ? v.categories : [],
        series: null as string | null,
        seriesNumber: null as number | null,
        coverUrl,
        coverSource,
        description: v.description ?? null,
        sourceUrl: v.infoLink ?? v.previewLink ?? null,
        openLibraryWorkKey: null as string | null,
        confidenceScore: 0,
        confidenceReasons: [] as string[],
    };
}

function parseSeriesFromTitle(title: string | null, subtitle: string | null) {
    const combined = `${title ?? ""} ${subtitle ?? ""}`.trim();
    if (!combined) return { series: null as string | null, seriesNumber: null as number | null };

    const m = combined.match(/^(.*?)\s*[:\-]?\s*(?:book|vol(?:ume)?|#)\s*(\d+(?:\.\d+)?)/i);
    if (m) {
        return {
            series: m[1].trim() || null,
            seriesNumber: Number.isFinite(Number(m[2])) ? Number(m[2]) : null,
        };
    }

    const p = combined.match(/\(([^()]+?)\s*(?:book|vol(?:ume)?|#)\s*(\d+(?:\.\d+)?)\)/i);
    if (p) {
        return {
            series: p[1].trim() || null,
            seriesNumber: Number.isFinite(Number(p[2])) ? Number(p[2]) : null,
        };
    }

    return { series: null as string | null, seriesNumber: null as number | null };
}

async function fetchOpenLibraryByIsbn(isbn: string) {
    const url = `https://openlibrary.org/isbn/${encodeURIComponent(isbn)}.json`;
    const res = await fetchWithRetry(url, { cache: "no-store" }, { retries: 2 });
    if (!res.ok) return null;

    const data = await res.json();
    let workKey: string | null = null;
    if (Array.isArray(data?.works) && data.works.length) {
        const first = data.works[0]?.key;
        if (typeof first === "string" && first.startsWith("/works/")) {
            workKey = first.replace("/works/", "");
        }
    }

    const title = typeof data?.title === "string" ? data.title : null;
    const subtitle = typeof data?.subtitle === "string" ? data.subtitle : null;
    const seriesGuess = parseSeriesFromTitle(title, subtitle);

    return {
        provider: "openlibrary",
        googleId: null,
        title,
        subtitle,
        authors: [] as string[],
        publisher: null,
        publishedDate: typeof data?.publish_date === "string" ? data.publish_date : null,
        pageCount: typeof data?.number_of_pages === "number" ? data.number_of_pages : null,
        categories: [] as string[],
        series: seriesGuess.series,
        seriesNumber: seriesGuess.seriesNumber,
        coverUrl: null,
        coverSource: null,
        description: null,
        sourceUrl: `https://openlibrary.org/isbn/${encodeURIComponent(isbn)}`,
        openLibraryWorkKey: workKey,
        confidenceScore: 0,
        confidenceReasons: [] as string[],
    };
}

function scoreMetadataConfidence(input: {
    isbn: string;
    metadata: {
        title: string | null;
        authors: string[];
        publishedDate: string | null;
        coverUrl: string | null;
        googleId: string | null;
        openLibraryWorkKey: string | null;
        provider: string;
    };
}) {
    const reasons: string[] = [];
    let score = 0;

    if (input.isbn.length >= 10) {
        score += 0.25;
        reasons.push("Valid ISBN extracted");
    }
    if (input.metadata.title) {
        score += 0.25;
        reasons.push("Title present");
    }
    if (input.metadata.authors.length) {
        score += 0.2;
        reasons.push("Author present");
    }
    if (input.metadata.coverUrl) {
        score += 0.1;
        reasons.push("Cover present");
    }
    if (input.metadata.googleId || input.metadata.openLibraryWorkKey) {
        score += 0.15;
        reasons.push("Provider ID present");
    }
    if (input.metadata.publishedDate) {
        score += 0.05;
        reasons.push("Published date present");
    }

    if (input.metadata.provider === "openlibrary") {
        score = Math.max(0, score - 0.05);
        reasons.push("Fallback provider used");
    }

    return {
        confidenceScore: Math.max(0, Math.min(1, Number(score.toFixed(3)))),
        confidenceReasons: reasons,
    };
}

async function fetchBookMetadataByIsbn(isbn: string) {
    const google = await fetchGoogleBooksByIsbn(isbn).catch(() => null);
    const openLibrary = await fetchOpenLibraryByIsbn(isbn).catch(() => null);

    if (!google && !openLibrary) {
        throw new Error("Metadata providers unavailable");
    }

    const base = google ?? openLibrary;
    if (!base) return null;

    const series = base.series ?? parseSeriesFromTitle(base.title, base.subtitle).series;
    const seriesNumber = base.seriesNumber ?? parseSeriesFromTitle(base.title, base.subtitle).seriesNumber;

    const merged = {
        ...base,
        sourceUrl: base.sourceUrl ?? openLibrary?.sourceUrl ?? null,
        openLibraryWorkKey: base.openLibraryWorkKey ?? openLibrary?.openLibraryWorkKey ?? null,
        series,
        seriesNumber,
    };

    const scoring = scoreMetadataConfidence({
        isbn,
        metadata: {
            title: merged.title,
            authors: merged.authors,
            publishedDate: merged.publishedDate,
            coverUrl: merged.coverUrl,
            googleId: merged.googleId,
            openLibraryWorkKey: merged.openLibraryWorkKey,
            provider: merged.provider,
        },
    });

    return {
        ...merged,
        confidenceScore: scoring.confidenceScore,
        confidenceReasons: scoring.confidenceReasons,
    };
}

function notionDate(publishedDate: string | null) {
    if (!publishedDate) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(publishedDate)) return publishedDate;
    if (/^\d{4}-\d{2}$/.test(publishedDate)) return `${publishedDate}-01`;
    if (/^\d{4}$/.test(publishedDate)) return `${publishedDate}-01-01`;
    return null;
}

type DatabaseMeta = {
    dataSourceId: string;
    propertyNames: Set<string>;
    propertySchemas: Record<string, unknown>;
};

let DB_META_CACHE: { databaseId: string; meta: DatabaseMeta; fetchedAt: number } | null = null;
let AUTHORS_META_CACHE: { databaseId: string; meta: DatabaseMeta; fetchedAt: number } | null = null;
const AUTHOR_OPENLIBRARY_KEY_CACHE = new Map<string, string | null>();

async function getDatabaseMeta(databaseId: string): Promise<DatabaseMeta> {
    const now = Date.now();
    if (DB_META_CACHE && DB_META_CACHE.databaseId === databaseId && now - DB_META_CACHE.fetchedAt < 5 * 60 * 1000) {
        return DB_META_CACHE.meta;
    }

    const database = await notion.databases.retrieve({ database_id: databaseId });

    const databaseObj = asRecord(database);
    const dataSources = asArray(databaseObj?.data_sources);
    const firstDataSource = asRecord(dataSources[0]);
    const firstDataSourceId = getString(firstDataSource?.id);
    if (!firstDataSourceId) {
        throw new Error("No Notion data source found for NOTION_DATABASE_ID");
    }

    // In Notion's current model, property schema primarily lives on the data source.
    const dataSource = await notion.dataSources.retrieve({ data_source_id: firstDataSourceId });
    const dataSourceObj = asRecord(dataSource);
    const propsObj =
        asRecord(dataSourceObj?.properties) ??
        asRecord(databaseObj?.properties) ??
        {};
    const propertyNames = new Set(Object.keys(propsObj));

    const meta = { dataSourceId: firstDataSourceId, propertyNames, propertySchemas: propsObj };
    DB_META_CACHE = { databaseId, meta, fetchedAt: now };
    return meta;
}

function normalizeAuthorName(raw: unknown) {
    const cleaned = String(raw ?? "")
        .replace(/\s+/g, " ")
        .trim();
    const alias = AUTHOR_ALIASES.get(cleaned.toLowerCase());
    return alias ?? cleaned;
}

function uniqueAuthorNames(authors: string[]) {
    const seen = new Set<string>();
    const out: string[] = [];
    for (const author of authors) {
        const normalized = normalizeAuthorName(author);
        const key = normalized.toLowerCase();
        if (!normalized || seen.has(key)) continue;
        seen.add(key);
        out.push(normalized);
    }
    return out;
}

function findRelationPropertyToDataSource(meta: DatabaseMeta, targetDataSourceId: string) {
    for (const [name, schema] of Object.entries(meta.propertySchemas)) {
        const schemaObj = asRecord(schema);
        if (schemaObj?.type !== "relation") continue;
        const relation = asRecord(schemaObj?.relation);
        if (!relation) continue;
        if (relation.data_source_id === targetDataSourceId || relation.database_id === targetDataSourceId) {
            return name;
        }
    }
    return null;
}

async function getAuthorsDatabaseMeta(): Promise<DatabaseMeta | null> {
    const authorsDb = process.env.NOTION_AUTHORS_DATABASE_ID;
    if (!authorsDb) return null;

    const now = Date.now();
    if (
        AUTHORS_META_CACHE &&
        AUTHORS_META_CACHE.databaseId === authorsDb &&
        now - AUTHORS_META_CACHE.fetchedAt < 5 * 60 * 1000
    ) {
        return AUTHORS_META_CACHE.meta;
    }

    const database = await notion.databases.retrieve({ database_id: authorsDb });
    const databaseObj = asRecord(database);
    const dataSources = asArray(databaseObj?.data_sources);
    const firstDataSource = asRecord(dataSources[0]);
    const firstDataSourceId = getString(firstDataSource?.id);
    if (!firstDataSourceId) {
        throw new Error("No Notion data source found for NOTION_AUTHORS_DATABASE_ID");
    }

    const dataSource = await notion.dataSources.retrieve({ data_source_id: firstDataSourceId });
    const dataSourceObj = asRecord(dataSource);
    const propsObj =
        asRecord(dataSourceObj?.properties) ??
        asRecord(databaseObj?.properties) ??
        {};
    const meta = {
        dataSourceId: firstDataSourceId,
        propertyNames: new Set(Object.keys(propsObj)),
        propertySchemas: propsObj,
    };
    AUTHORS_META_CACHE = { databaseId: authorsDb, meta, fetchedAt: now };
    return meta;
}

function resolveAuthorNameProperty(authorsMeta: DatabaseMeta) {
    const configured = process.env.NOTION_AUTHOR_NAME_PROPERTY ?? "Name";
    if (authorsMeta.propertyNames.has(configured)) return configured;

    for (const [name, schema] of Object.entries(authorsMeta.propertySchemas)) {
        if (asRecord(schema)?.type === "title") return name;
    }
    for (const [name, schema] of Object.entries(authorsMeta.propertySchemas)) {
        if (asRecord(schema)?.type === "rich_text") return name;
    }
    return null;
}

function resolveAuthorOpenLibraryKeyProperty(authorsMeta: DatabaseMeta) {
    const configured = process.env.NOTION_AUTHOR_OPENLIBRARY_KEY_PROPERTY ?? "OpenLibrary Author Key";
    if (authorsMeta.propertyNames.has(configured)) return configured;
    return null;
}

function normalizeAuthorLookupName(name: string) {
    return name
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();
}

function extractOpenLibraryAuthorKey(raw: unknown) {
    const key = String(raw ?? "").trim();
    if (!key) return null;
    return key.startsWith("/authors/") ? key.replace("/authors/", "") : key;
}

async function fetchOpenLibraryAuthorKeyByName(authorName: string) {
    const cacheKey = normalizeAuthorLookupName(authorName);
    if (AUTHOR_OPENLIBRARY_KEY_CACHE.has(cacheKey)) {
        return AUTHOR_OPENLIBRARY_KEY_CACHE.get(cacheKey) ?? null;
    }

    const url = `https://openlibrary.org/search/authors.json?q=${encodeURIComponent(authorName)}`;
    try {
        const res = await fetchWithRetry(url, { cache: "no-store" }, { retries: 2 });
        if (!res.ok) {
            AUTHOR_OPENLIBRARY_KEY_CACHE.set(cacheKey, null);
            return null;
        }

        const data = await res.json();
        const docs = asArray(asRecord(data)?.docs);
        if (!docs.length) {
            AUTHOR_OPENLIBRARY_KEY_CACHE.set(cacheKey, null);
            return null;
        }

        const target = normalizeAuthorLookupName(authorName);
        const exact = docs.find((doc) => {
            const docObj = asRecord(doc);
            const name = normalizeAuthorLookupName(String(docObj?.name ?? ""));
            return Boolean(name) && name === target;
        });
        const chosen = asRecord(exact ?? docs[0]);
        const key = extractOpenLibraryAuthorKey(chosen?.key);

        AUTHOR_OPENLIBRARY_KEY_CACHE.set(cacheKey, key);
        return key;
    } catch {
        AUTHOR_OPENLIBRARY_KEY_CACHE.set(cacheKey, null);
        return null;
    }
}

function readTextPropertyValue(prop: unknown, schemaType: string | null) {
    const propObj = asRecord(prop);
    if (!propObj || !schemaType) return "";

    if (schemaType === "rich_text") {
        const text = asArray(propObj.rich_text)
            .map((part) => getString(asRecord(part)?.plain_text) ?? "")
            .join("")
            .trim();
        return text;
    }

    if (schemaType === "title") {
        const text = asArray(propObj.title)
            .map((part) => getString(asRecord(part)?.plain_text) ?? "")
            .join("")
            .trim();
        return text;
    }

    if (schemaType === "url") {
        return getString(propObj.url) ?? "";
    }

    return "";
}

function buildOpenLibraryAuthorKeyPropertyValue(schemaType: string | null, key: string) {
    const content = key.slice(0, 2000);
    if (!content || !schemaType) return null;

    if (schemaType === "rich_text") {
        return { rich_text: [{ text: { content } }] };
    }
    if (schemaType === "title") {
        return { title: [{ text: { content } }] };
    }
    if (schemaType === "url") {
        return { url: `https://openlibrary.org/authors/${encodeURIComponent(content)}` };
    }

    return null;
}

async function findOrCreateAuthorPageId(params: {
    authorName: string;
    authorsMeta: DatabaseMeta;
    nameProperty: string;
    openLibraryAuthorKeyProperty: string | null;
    openLibraryAuthorKey: string | null;
}) {
    const {
        authorName,
        authorsMeta,
        nameProperty,
        openLibraryAuthorKeyProperty,
        openLibraryAuthorKey,
    } = params;
    const schemaType = asRecord(authorsMeta.propertySchemas?.[nameProperty])?.type;
    if (schemaType !== "title" && schemaType !== "rich_text") {
        return null;
    }

    const filter =
        schemaType === "title"
            ? {
                property: nameProperty,
                title: { equals: authorName },
            }
            : {
                property: nameProperty,
                rich_text: { equals: authorName },
            };

    const query = await notion.dataSources.query({
        data_source_id: authorsMeta.dataSourceId,
        filter: filter as NotionQueryArgs["filter"],
        page_size: 1,
    });

    const existingObj = asRecord(query.results?.[0]);
    const existingId = getString(existingObj?.id);
    if (existingId) {
        const keySchemaType = openLibraryAuthorKeyProperty
            ? getString(asRecord(authorsMeta.propertySchemas?.[openLibraryAuthorKeyProperty])?.type)
            : null;
        if (openLibraryAuthorKeyProperty && openLibraryAuthorKey && keySchemaType) {
            const existingProps = asRecord(existingObj?.properties) ?? {};
            const existingKey = readTextPropertyValue(existingProps[openLibraryAuthorKeyProperty], keySchemaType);
            if (!existingKey) {
                const keyValue = buildOpenLibraryAuthorKeyPropertyValue(keySchemaType, openLibraryAuthorKey);
                if (keyValue) {
                    await notion.pages.update({
                        page_id: existingId,
                        properties: {
                            [openLibraryAuthorKeyProperty]: keyValue,
                        } as NotionUpdateArgs["properties"],
                    });
                }
            }
        }
        return existingId;
    }

    const content = authorName.slice(0, 2000);
    const properties: Record<string, unknown> =
        schemaType === "title"
            ? {
                [nameProperty]: {
                    title: [{ text: { content } }],
                },
            }
            : {
                [nameProperty]: {
                    rich_text: [{ text: { content } }],
                },
            };

    const keySchemaType = openLibraryAuthorKeyProperty
        ? getString(asRecord(authorsMeta.propertySchemas?.[openLibraryAuthorKeyProperty])?.type)
        : null;
    if (openLibraryAuthorKeyProperty && openLibraryAuthorKey && keySchemaType) {
        const keyValue = buildOpenLibraryAuthorKeyPropertyValue(keySchemaType, openLibraryAuthorKey);
        if (keyValue) {
            properties[openLibraryAuthorKeyProperty] = keyValue;
        }
    }

    const created = await notion.pages.create({
        parent: { data_source_id: authorsMeta.dataSourceId },
        properties: properties as NotionCreateArgs["properties"],
    });

    const createdObj = asRecord(created);
    return String(createdObj?.id ?? "");
}

async function buildAuthorRelationUpdate(authors: string[], bookMeta: DatabaseMeta) {
    const dedupedAuthors = uniqueAuthorNames(authors);
    if (!dedupedAuthors.length) {
        return {
            relationProperty: null as string | null,
            authorPageIds: [] as string[],
            enabled: false,
            reason: "No authors found in metadata.",
        };
    }

    const authorsMeta = await getAuthorsDatabaseMeta();
    if (!authorsMeta) {
        return {
            relationProperty: null as string | null,
            authorPageIds: [] as string[],
            enabled: false,
            reason: "NOTION_AUTHORS_DATABASE_ID is not configured.",
        };
    }

    const configuredRelationProperty = process.env.NOTION_BOOK_AUTHOR_RELATION_PROPERTY;
    const relationProperty = configuredRelationProperty
        ? (bookMeta.propertyNames.has(configuredRelationProperty) ? configuredRelationProperty : null)
        : findRelationPropertyToDataSource(bookMeta, authorsMeta.dataSourceId);

    if (!relationProperty) {
        return {
            relationProperty: null as string | null,
            authorPageIds: [] as string[],
            enabled: false,
            reason: configuredRelationProperty
                ? `Configured relation property not found: ${configuredRelationProperty}`
                : "No relation property found in book database for authors data source.",
        };
    }

    const nameProperty = resolveAuthorNameProperty(authorsMeta);
    if (!nameProperty) {
        return {
            relationProperty,
            authorPageIds: [] as string[],
            enabled: false,
            reason: "Authors database has no title/rich_text field for author names.",
        };
    }

    const openLibraryAuthorKeyProperty = resolveAuthorOpenLibraryKeyProperty(authorsMeta);

    const authorPageIds: string[] = [];
    for (const authorName of dedupedAuthors) {
        const openLibraryAuthorKey = openLibraryAuthorKeyProperty
            ? await fetchOpenLibraryAuthorKeyByName(authorName)
            : null;
        const id = await findOrCreateAuthorPageId({
            authorName,
            authorsMeta,
            nameProperty,
            openLibraryAuthorKeyProperty,
            openLibraryAuthorKey,
        });
        if (id) authorPageIds.push(id);
    }

    return {
        relationProperty,
        authorPageIds,
        enabled: true,
        reason: null as string | null,
    };
}

async function findExistingByIsbn(isbn: string) {
    const db = process.env.NOTION_DATABASE_ID!;
    const { dataSourceId } = await getDatabaseMeta(db);
    const resp = await notion.dataSources.query({
        data_source_id: dataSourceId,
        filter: {
            property: "ISBN",
            rich_text: { equals: isbn },
        },
        page_size: 1,
    });
    return resp.results?.[0] ?? null;
}

export async function POST(req: Request) {
    const startedAt = Date.now();
    const requestId = globalThis.crypto?.randomUUID?.() ?? `req_${Date.now()}`;

    try {
        pruneIdempotencyCache();

        if (!isApiAuthorized(req)) {
            const status = 401;
            const code = "UNAUTHORIZED";
            const message = "Unauthorized request.";
            await writeAuditEvent({
                requestId,
                route: "/api/scan",
                action: "reject",
                status,
                code,
                durationMs: Date.now() - startedAt,
            });
            return NextResponse.json({ ok: false, status, code, message, requestId }, { status });
        }

        const contentType = req.headers.get("content-type") || "";
        let body: JsonObject = {};

        if (contentType.includes("application/json")) {
            body = await req.json().catch(() => ({}));
        } else if (contentType.includes("application/x-www-form-urlencoded")) {
            const text = await req.text().catch(() => "");
            const params = new URLSearchParams(text);
            body = Object.fromEntries(params.entries());
        } else {
            // Fallback: treat entire body as the ISBN text
            const text = await req.text().catch(() => "");
            body = { isbn: text };
        }

        // Also allow ISBN to come in via query string (?isbn=...)
        const { searchParams } = new URL(req.url);
        const verbose = searchParams.get("verbose") === "1";
        const compact = parseBool(searchParams.get("compact") ?? body?.compact);
        const dryRun = parseBool(searchParams.get("dryRun") ?? body?.dryRun);
        const enrichAsync = parseBool(searchParams.get("enrichAsync") ?? body?.enrichAsync);

        if (!body?.isbn && searchParams.get("isbn")) {
            body.isbn = searchParams.get("isbn");
        }

        const isbn = extractIsbn(body?.isbn);
        if (!isbn) {
            return errorResponse({
                status: 400,
                code: "INVALID_ISBN",
                message: "Missing or invalid ISBN.",
                suggestion: "Scan the barcode again or enter a valid ISBN-10/ISBN-13.",
                retryable: true,
                requestId,
                durationMs: Date.now() - startedAt,
                verbose,
            });
        }

        const db = process.env.NOTION_DATABASE_ID;
        if (!db) {
            return errorResponse({
                status: 500,
                code: "SERVER_MISCONFIGURED",
                message: "Server misconfigured: NOTION_DATABASE_ID not set.",
                suggestion: "Set NOTION_DATABASE_ID in .env.local and redeploy/restart the app.",
                requestId,
                durationMs: Date.now() - startedAt,
                verbose,
            });
        }

        const onDuplicateRaw = String(body?.onDuplicate ?? "update").toLowerCase();
        const onDuplicate: "update" | "skip" = onDuplicateRaw === "skip" ? "skip" : "update";
        const modeRaw = String(body?.mode ?? searchParams.get("mode") ?? "").toLowerCase();
        const checkOnly =
            modeRaw === "check" ||
            body?.checkOnly === true ||
            searchParams.get("check") === "1" ||
            searchParams.get("checkOnly") === "1";
        const idempotencyHeader = normalizeHeaderIdempotencyKey(req);
        const idempotencyKey = idempotencyHeader
            ? `${idempotencyHeader}:${isbn}:${checkOnly ? "check" : "write"}:${onDuplicate}`
            : null;

        const idempotentHit = maybeGetIdempotent(idempotencyKey);
        if (idempotentHit) {
            const replayBody = {
                ...idempotentHit.body,
                idempotencyReplay: true,
            };
            return NextResponse.json(replayBody, { status: idempotentHit.status });
        }

        const { dataSourceId, propertyNames, propertySchemas } = await getDatabaseMeta(db);

        // Debug: return database property names without creating/updating anything
        if (searchParams.get("debug") === "1") {
            const payload = {
                ok: true,
                status: 200,
                code: "DEBUG_PROPERTIES",
                action: "exists",
                message: "Resolved Notion property names.",
                suggestion: null,
                requestId,
                durationMs: Date.now() - startedAt,
                timestamp: new Date().toISOString(),
                data: {
                    databaseId: db,
                    propertyNames: Array.from(propertyNames).sort(),
                },
            };
            maybeStoreIdempotent(idempotencyKey, 200, payload);
            return NextResponse.json(payload, { status: 200 });
        }

        const existing = await findExistingByIsbn(isbn);
        const isDuplicate = Boolean(existing);

        if (checkOnly) {
            const existingObj = asRecord(existing);
            const pageId = isDuplicate ? getString(existingObj?.id) : null;
            const url = pageId ? notionPageUrl(pageId) : null;
            const deepLinkUrl = pageId ? notionDeepLinkUrl(pageId) : null;

            const code = isDuplicate ? "BOOK_EXISTS" : "BOOK_NOT_FOUND";
            const message = isDuplicate ? `Already in library: ${isbn}` : `Not found in library: ${isbn}`;
            const speechText = isDuplicate
                ? `Already in library. ISBN ${isbn}.`
                : `Not in library yet. ISBN ${isbn}.`;

            const payload = compact
                ? makeCompactPayload({
                    ok: true,
                    code,
                    message,
                    isbn,
                    notionUrl: url,
                    exists: isDuplicate,
                    speechText,
                    requestId,
                    durationMs: Date.now() - startedAt,
                })
                : {
                    ok: true,
                    status: 200,
                    code,
                    action: "exists",
                    message,
                    suggestion: isDuplicate
                        ? "Open existing item or run normal scan to update metadata."
                        : "Run a normal scan to add this book.",
                    requestId,
                    durationMs: Date.now() - startedAt,
                    timestamp: new Date().toISOString(),
                    data: {
                        isbn,
                        exists: isDuplicate,
                        checkOnly: true,
                        notionPageId: pageId,
                        notionUrl: url,
                        notionDeepLinkUrl: deepLinkUrl,
                        speechText,
                    },
                };

            await writeAuditEvent({
                requestId,
                route: "/api/scan",
                isbn,
                action: "check",
                status: 200,
                code,
                durationMs: Date.now() - startedAt,
                metadata: { exists: isDuplicate },
            });
            maybeStoreIdempotent(idempotencyKey, 200, payload);
            return NextResponse.json(payload, { status: 200 });
        }

        if (isDuplicate && onDuplicate === "skip") {
            const existingObj = asRecord(existing);
            const pageId = String(existingObj?.id ?? "");
            const url = notionPageUrl(pageId);
            const deepLinkUrl = notionDeepLinkUrl(pageId);
            const message = `Already in library: ${isbn}`;
            const payload = compact
                ? makeCompactPayload({
                    ok: true,
                    code: "BOOK_ALREADY_EXISTS",
                    message,
                    isbn,
                    notionUrl: url,
                    exists: true,
                    speechText: `Already in library. ISBN ${isbn}.`,
                    requestId,
                    durationMs: Date.now() - startedAt,
                })
                : {
                    ok: true,
                    status: 200,
                    code: "BOOK_ALREADY_EXISTS",
                    action: "exists",
                    message,
                    suggestion: "Open existing item or rescan with onDuplicate=update to refresh metadata.",
                    requestId,
                    durationMs: Date.now() - startedAt,
                    timestamp: new Date().toISOString(),
                    ...(verbose
                        ? {
                            data: {
                                isbn,
                                duplicated: true,
                                onDuplicate,
                                notionPageId: pageId,
                                notionUrl: url,
                                notionDeepLinkUrl: deepLinkUrl,
                                speechText: `Already in library. ISBN ${isbn}.`,
                            },
                        }
                        : {}),
                };
            await writeAuditEvent({
                requestId,
                route: "/api/scan",
                isbn,
                action: "skip_duplicate",
                status: 200,
                code: "BOOK_ALREADY_EXISTS",
                durationMs: Date.now() - startedAt,
            });
            maybeStoreIdempotent(idempotencyKey, 200, payload);
            return NextResponse.json(payload, { status: 200 });
        }

        const book = await fetchBookMetadataByIsbn(isbn);

        const notionImage = book?.coverUrl
            ? {
                type: "external" as const,
                external: { url: book.coverUrl },
            }
            : undefined;

        const scannedDate = todayIsoDate();

        const title = book?.title
            ? (book.subtitle ? `${book.title}: ${book.subtitle}` : book.title)
            : `Unknown title (${isbn})`;

        // NOTE: Property names must match your Notion database exactly.
        const properties: Record<string, unknown> = {
            Name: { title: [{ text: { content: title } }] },
            ISBN: { rich_text: [{ text: { content: isbn } }] },
        };

        if (book?.authors?.length && propertyNames.has("Authors")) {
            properties.Authors = { rich_text: [{ text: { content: book.authors.join(", ") } }] };
        }

        const authorRelation = await buildAuthorRelationUpdate(book?.authors ?? [], {
            dataSourceId,
            propertyNames,
            propertySchemas,
        });

        if (authorRelation.enabled && authorRelation.relationProperty && authorRelation.authorPageIds.length) {
            properties[authorRelation.relationProperty] = {
                relation: authorRelation.authorPageIds.map((id) => ({ id })),
            };
        }

        if (book?.publisher && propertyNames.has("Publisher")) {
            properties.Publisher = { rich_text: [{ text: { content: book.publisher } }] };
        }
        if (book?.pageCount != null && propertyNames.has("Page Count")) {
            properties["Page Count"] = { number: book.pageCount };
        }
        if (book?.categories?.length && propertyNames.has("Categories")) {
            properties.Categories = {
                multi_select: cleanCategories(book.categories),
            };
        }
        if (book?.coverUrl && propertyNames.has("Cover URL")) {
            properties["Cover URL"] = { url: book.coverUrl };
        }
        if (book?.googleId && propertyNames.has("Google Books ID")) {
            properties["Google Books ID"] = { rich_text: [{ text: { content: book.googleId } }] };
        }
        if (book?.openLibraryWorkKey && propertyNames.has("OpenLibrary Work Key")) {
            properties["OpenLibrary Work Key"] = { rich_text: [{ text: { content: book.openLibraryWorkKey } }] };
        }
        if (book?.series && propertyNames.has("Series")) {
            properties.Series = { rich_text: [{ text: { content: book.series.slice(0, 2000) } }] };
        }
        if (book?.seriesNumber != null && propertyNames.has("Series Number")) {
            properties["Series Number"] = { number: book.seriesNumber };
        }
        if (book?.sourceUrl && propertyNames.has("Source URL")) {
            properties["Source URL"] = { url: book.sourceUrl };
        }
        if (book?.description && propertyNames.has("Description")) {
            properties.Description = {
                rich_text: [{ text: { content: book.description.slice(0, 2000) } }],
            };
        }

        const published = notionDate(book?.publishedDate ?? null);
        if (published && propertyNames.has("Published")) {
            properties.Published = { date: { start: published } };
        }

        // Scan metadata
        if (propertyNames.has("Last Scanned")) {
            properties["Last Scanned"] = { date: { start: scannedDate } };
        }
        if (!existing && propertyNames.has("Date Added")) {
            properties["Date Added"] = { date: { start: scannedDate } };
        }

        if (dryRun) {
            const status = existing ? 200 : 201;
            const code = existing ? "BOOK_DRY_RUN_UPDATE" : "BOOK_DRY_RUN_CREATE";
            const message = existing
                ? `Dry run: would update ${title}`
                : `Dry run: would add ${title}`;
            const payload = compact
                ? makeCompactPayload({
                    ok: true,
                    code,
                    message,
                    isbn,
                    title,
                    firstAuthor: book?.authors?.[0] ?? null,
                    exists: Boolean(existing),
                    requestId,
                    durationMs: Date.now() - startedAt,
                })
                : {
                    ok: true,
                    status,
                    code,
                    action: "exists",
                    message,
                    suggestion: "Set dryRun=false to apply changes.",
                    requestId,
                    durationMs: Date.now() - startedAt,
                    timestamp: new Date().toISOString(),
                    data: {
                        isbn,
                        title,
                        duplicated: Boolean(existing),
                        properties,
                        dryRun: true,
                    },
                };
            await writeAuditEvent({
                requestId,
                route: "/api/scan",
                isbn,
                action: "dry_run",
                status,
                code,
                durationMs: Date.now() - startedAt,
            });
            maybeStoreIdempotent(idempotencyKey, status, payload);
            return NextResponse.json(payload, { status });
        }

        // If exists, update; otherwise create
        let pageId: string;

        if (existing) {
            const updated = await notion.pages.update({
                page_id: String(asRecord(existing)?.id ?? ""),
                cover: notionImage,
                icon: notionImage,
                properties: properties as NotionUpdateArgs["properties"],
            });
            pageId = String(asRecord(updated)?.id ?? "");
        } else {
            const created = await notion.pages.create({
                parent: { data_source_id: dataSourceId },
                cover: notionImage,
                icon: notionImage,
                properties: properties as NotionCreateArgs["properties"],
                // optional: put description into the page content only if you're not storing it in a Description property
                children:
                    book?.description && !propertyNames.has("Description")
                        ? [
                            {
                                object: "block",
                                type: "paragraph",
                                paragraph: {
                                    rich_text: [
                                        { type: "text", text: { content: book.description.slice(0, 1800) } },
                                    ],
                                },
                            },
                        ]
                        : [],
            });
            pageId = String(asRecord(created)?.id ?? "");
        }

        const url = notionPageUrl(pageId);
        const deepLinkUrl = notionDeepLinkUrl(pageId);
        const message = `${existing ? "Updated" : "Added"}: ${title}${book?.authors?.length ? ` — ${book.authors.join(", ")}` : ""}`;
        const action = existing ? "updated" : "created";
        const status = existing ? 200 : 201;
        const metadataFound = Boolean(book);
        const notes = metadataFound ? [] : ["No Google Books metadata found; stored ISBN and fallback title only."];

        const speechText = existing
            ? `Updated ${title}${book?.authors?.[0] ? ` by ${book.authors[0]}` : ""}.`
            : `Added ${title}${book?.authors?.[0] ? ` by ${book.authors[0]}` : ""}.`;

        const payload = compact
            ? makeCompactPayload({
                ok: true,
                code: existing ? "BOOK_UPDATED" : "BOOK_CREATED",
                message,
                isbn,
                title,
                firstAuthor: book?.authors?.[0] ?? null,
                notionUrl: url,
                exists: Boolean(existing),
                speechText,
                requestId,
                durationMs: Date.now() - startedAt,
            })
            : {
                ok: true,
                status,
                code: existing ? "BOOK_UPDATED" : "BOOK_CREATED",
                action,
                message,
                suggestion: existing ? "Entry refreshed successfully." : "Book added successfully.",
                requestId,
                durationMs: Date.now() - startedAt,
                timestamp: new Date().toISOString(),
                ...(verbose
                    ? {
                        data: {
                            isbn,
                            title,
                            authors: book?.authors ?? [],
                            publisher: book?.publisher ?? null,
                            publishedDate: book?.publishedDate ?? null,
                            pageCount: book?.pageCount ?? null,
                            categories: book?.categories ?? [],
                            coverSource: book?.coverSource ?? null,
                            metadataFound,
                            notes,
                            duplicated: false,
                            onDuplicate,
                            checkOnly: false,
                            provider: book?.provider ?? null,
                            openLibraryWorkKey: book?.openLibraryWorkKey ?? null,
                            series: book?.series ?? null,
                            seriesNumber: book?.seriesNumber ?? null,
                            confidenceScore: book?.confidenceScore ?? null,
                            confidenceReasons: book?.confidenceReasons ?? [],
                            authorsRelation: {
                                enabled: authorRelation.enabled,
                                relationProperty: authorRelation.relationProperty,
                                linkedAuthorCount: authorRelation.authorPageIds.length,
                                reason: authorRelation.reason,
                            },
                            notionPageId: pageId,
                            notionUrl: url,
                            notionDeepLinkUrl: deepLinkUrl,
                            speechText,
                        },
                    }
                    : {}),
            };

        await writeAuditEvent({
            requestId,
            route: "/api/scan",
            isbn,
            action,
            status,
            code: existing ? "BOOK_UPDATED" : "BOOK_CREATED",
            durationMs: Date.now() - startedAt,
            metadata: {
                provider: book?.provider ?? null,
                confidenceScore: book?.confidenceScore ?? null,
                relationLinked: authorRelation.authorPageIds.length,
            },
        });

        if (enrichAsync && pageId) {
            setTimeout(async () => {
                try {
                    const refreshed = await fetchBookMetadataByIsbn(isbn);
                    if (!refreshed) return;
                    const asyncProps: Record<string, unknown> = {};
                    if (refreshed.description && propertyNames.has("Description")) {
                        asyncProps.Description = {
                            rich_text: [{ text: { content: refreshed.description.slice(0, 2000) } }],
                        };
                    }
                    if (refreshed.categories?.length && propertyNames.has("Categories")) {
                        asyncProps.Categories = { multi_select: cleanCategories(refreshed.categories) };
                    }
                    if (Object.keys(asyncProps).length) {
                        await notion.pages.update({
                            page_id: pageId,
                            properties: asyncProps as NotionUpdateArgs["properties"],
                        });
                    }
                } catch {
                    // Best-effort background enrichment.
                }
            }, 0);
        }

        maybeStoreIdempotent(idempotencyKey, status, payload);
        return NextResponse.json(payload, { status });
    } catch (err: unknown) {
        const { searchParams } = new URL(req.url);
        const verbose = searchParams.get("verbose") === "1";
        const compact = parseBool(searchParams.get("compact"));
        const rawMessage = err instanceof Error ? err.message : "Server error";
        const status = rawMessage.includes("Metadata providers unavailable") ? 502 : 500;
        const code = rawMessage.includes("Metadata providers unavailable")
            ? "METADATA_UNAVAILABLE"
            : "SERVER_ERROR";
        const message = rawMessage.includes("Metadata providers unavailable")
            ? "Could not retrieve metadata from providers."
            : "Something went wrong while scanning this book.";

        await writeAuditEvent({
            requestId,
            route: "/api/scan",
            action: "error",
            status,
            code,
            durationMs: Date.now() - startedAt,
            metadata: { error: rawMessage },
        });

        if (compact) {
            return NextResponse.json(
                makeCompactPayload({
                    ok: false,
                    code,
                    message,
                    requestId,
                    durationMs: Date.now() - startedAt,
                }),
                { status }
            );
        }

        return errorResponse({
            status,
            code,
            message,
            suggestion: "Try again. If it continues, check Notion integration permissions and env vars.",
            retryable: true,
            details: verbose ? { error: rawMessage } : undefined,
            requestId,
            durationMs: Date.now() - startedAt,
            verbose,
        });
    }
}