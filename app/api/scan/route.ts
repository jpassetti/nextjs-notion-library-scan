import { NextResponse } from "next/server";
import { Client } from "@notionhq/client";

export const runtime = "nodejs"; // important for Notion SDK

const notion = new Client({ auth: process.env.NOTION_TOKEN });

type SuccessAction = "created" | "updated" | "exists";

function successResponse(params: {
    status?: number;
    code: string;
    action: SuccessAction;
    message: string;
    suggestion?: string;
    data: Record<string, unknown>;
    requestId: string;
    durationMs: number;
    verbose?: boolean;
}) {
    const { status = 200, code, action, message, suggestion, data, requestId, durationMs, verbose = false } = params;

    const basePayload = {
        ok: true,
        status,
        code,
        action,
        message,
        suggestion: suggestion ?? null,
        requestId,
        durationMs,
        timestamp: new Date().toISOString(),
    };

    return NextResponse.json(
        verbose ? { ...basePayload, data } : basePayload,
        { status }
    );
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

    if (await urlLooksLikeImage(openLibraryLarge)) {
        return { coverUrl: openLibraryLarge, coverSource: "openlibrary" };
    }
    if (await urlLooksLikeImage(openLibraryMedium)) {
        return { coverUrl: openLibraryMedium, coverSource: "openlibrary" };
    }

    if (googleCoverUrl && !isKnownUnavailableCoverUrl(googleCoverUrl) && await urlLooksLikeImage(googleCoverUrl)) {
        return { coverUrl: googleCoverUrl, coverSource: "google_books" };
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

    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`Google Books error: ${res.status}`);
    const data = await res.json();

    const item = data.items?.[0];
    if (!item) return null;

    const v = item.volumeInfo ?? {};
    const rawCoverUrl = v.imageLinks?.thumbnail ?? v.imageLinks?.smallThumbnail ?? null;
    const upgradedGoogleCoverUrl = rawCoverUrl ? upgradeGoogleThumb(rawCoverUrl) : null;
    const { coverUrl, coverSource } = await resolveBestCoverUrl(isbn, upgradedGoogleCoverUrl);
    return {
        googleId: item.id ?? null,
        title: v.title ?? null,
        subtitle: v.subtitle ?? null,
        authors: Array.isArray(v.authors) ? v.authors : [],
        publisher: v.publisher ?? null,
        publishedDate: v.publishedDate ?? null, // YYYY or YYYY-MM or YYYY-MM-DD
        pageCount: typeof v.pageCount === "number" ? v.pageCount : null,
        categories: Array.isArray(v.categories) ? v.categories : [],
        coverUrl,
        coverSource,
        description: v.description ?? null,
        sourceUrl: v.infoLink ?? v.previewLink ?? null,
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
};

let DB_META_CACHE: { databaseId: string; meta: DatabaseMeta; fetchedAt: number } | null = null;

async function getDatabaseMeta(databaseId: string): Promise<DatabaseMeta> {
    const now = Date.now();
    if (DB_META_CACHE && DB_META_CACHE.databaseId === databaseId && now - DB_META_CACHE.fetchedAt < 5 * 60 * 1000) {
        return DB_META_CACHE.meta;
    }

    const database = await notion.databases.retrieve({ database_id: databaseId });

    const dataSources = (database as any)?.data_sources;
    const firstDataSourceId = Array.isArray(dataSources) ? dataSources[0]?.id : null;
    if (!firstDataSourceId) {
        throw new Error("No Notion data source found for NOTION_DATABASE_ID");
    }

    // In Notion's current model, property schema primarily lives on the data source.
    const dataSource = await notion.dataSources.retrieve({ data_source_id: firstDataSourceId });
    const propsObj = (dataSource as any)?.properties ?? (database as any)?.properties ?? {};
    const propertyNames = new Set(Object.keys(propsObj));

    const meta = { dataSourceId: firstDataSourceId, propertyNames };
    DB_META_CACHE = { databaseId, meta, fetchedAt: now };
    return meta;
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
        const contentType = req.headers.get("content-type") || "";
        let body: any = {};

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

        const { dataSourceId, propertyNames } = await getDatabaseMeta(db);

        // Debug: return database property names without creating/updating anything
        if (searchParams.get("debug") === "1") {
            return successResponse({
                code: "DEBUG_PROPERTIES",
                action: "exists",
                message: "Resolved Notion property names.",
                data: {
                    databaseId: db,
                    propertyNames: Array.from(propertyNames).sort(),
                },
                requestId,
                durationMs: Date.now() - startedAt,
                verbose: true,
            });
        }

        const existing = await findExistingByIsbn(isbn);
        const isDuplicate = Boolean(existing);

        if (isDuplicate && onDuplicate === "skip") {
            const pageId = (existing as any).id as string;
            const url = notionPageUrl(pageId);
            const deepLinkUrl = notionDeepLinkUrl(pageId);
            return successResponse({
                status: 200,
                code: "BOOK_ALREADY_EXISTS",
                action: "exists",
                message: `Already in library: ${isbn}`,
                suggestion: "Open existing item or rescan with onDuplicate=update to refresh metadata.",
                data: {
                    isbn,
                    duplicated: true,
                    onDuplicate,
                    notionPageId: pageId,
                    notionUrl: url,
                    notionDeepLinkUrl: deepLinkUrl,
                    speechText: `Already in library. ISBN ${isbn}.`,
                },
                requestId,
                durationMs: Date.now() - startedAt,
                verbose,
            });
        }

        const book = await fetchGoogleBooksByIsbn(isbn);

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
        const properties: any = {
            Name: { title: [{ text: { content: title } }] },
            ISBN: { rich_text: [{ text: { content: isbn } }] },
        };

        if (book?.authors?.length && propertyNames.has("Authors")) {
            properties.Authors = { rich_text: [{ text: { content: book.authors.join(", ") } }] };
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

        // If exists, update; otherwise create
        let pageId: string;

        if (existing) {
            const updated = await notion.pages.update({
                page_id: (existing as any).id,
                cover: notionImage,
                icon: notionImage,
                properties,
            });
            pageId = (updated as any).id;
        } else {
            const created = await notion.pages.create({
                parent: { data_source_id: dataSourceId },
                cover: notionImage,
                icon: notionImage,
                properties,
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
            pageId = (created as any).id;
        }

        const url = notionPageUrl(pageId);
        const deepLinkUrl = notionDeepLinkUrl(pageId);
        const message = `${existing ? "Updated" : "Added"}: ${title}${book?.authors?.length ? ` — ${book.authors.join(", ")}` : ""}`;
        const action = existing ? "updated" : "created";
        const status = existing ? 200 : 201;
        const metadataFound = Boolean(book);
        const notes = metadataFound ? [] : ["No Google Books metadata found; stored ISBN and fallback title only."];

        return successResponse({
            status,
            code: existing ? "BOOK_UPDATED" : "BOOK_CREATED",
            action,
            message,
            suggestion: existing ? "Entry refreshed successfully." : "Book added successfully.",
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
                notionPageId: pageId,
                notionUrl: url,
                notionDeepLinkUrl: deepLinkUrl,
                    speechText: existing
                        ? `Updated ${title}${book?.authors?.[0] ? ` by ${book.authors[0]}` : ""}.`
                        : `Added ${title}${book?.authors?.[0] ? ` by ${book.authors[0]}` : ""}.`,
            },
                requestId,
                durationMs: Date.now() - startedAt,
                verbose,
        });
    } catch (err: any) {
        const { searchParams } = new URL(req.url);
        const verbose = searchParams.get("verbose") === "1";
        const rawMessage = String(err?.message ?? "Server error");
        const isGoogleBooksError = rawMessage.startsWith("Google Books error:");

        if (isGoogleBooksError) {
            return errorResponse({
                status: 502,
                code: "GOOGLE_BOOKS_UNAVAILABLE",
                message: "Could not retrieve metadata from Google Books.",
                suggestion: "Try scanning again in a few seconds.",
                retryable: true,
                details: { upstream: rawMessage },
                requestId,
                durationMs: Date.now() - startedAt,
                verbose,
            });
        }

        return errorResponse({
            status: 500,
            code: "SERVER_ERROR",
            message: "Something went wrong while scanning this book.",
            suggestion: "Try again. If it continues, check Notion integration permissions and env vars.",
            retryable: true,
            details: { error: rawMessage },
            requestId,
            durationMs: Date.now() - startedAt,
            verbose,
        });
    }
}