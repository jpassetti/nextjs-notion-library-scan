import { NextResponse } from "next/server";
import { Client } from "@notionhq/client";

export const runtime = "nodejs";

const notion = new Client({ auth: process.env.NOTION_TOKEN });

type JsonObject = Record<string, unknown>;
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

type DatabaseMeta = {
    dataSourceId: string;
    propertyNames: Set<string>;
};

type BackfillResultItem = {
    pageId: string;
    isbn: string | null;
    action: "updated" | "would_update" | "skipped" | "error";
    reason?: string;
    updatedFields?: string[];
};

type BookMetadata = {
    googleId: string | null;
    title: string | null;
    subtitle: string | null;
    authors: string[];
    publisher: string | null;
    publishedDate: string | null;
    pageCount: number | null;
    categories: string[];
    coverUrl: string | null;
    coverSource: "openlibrary" | "google_books" | null;
    description: string | null;
    sourceUrl: string | null;
};

function normalizeIsbn(raw: unknown) {
    if (!raw) return null;
    const s = String(raw).toUpperCase();
    const m13 = s.match(/97[89]\d{10}/);
    if (m13) return m13[0];
    const m10 = s.match(/\b\d{9}[\dX]\b/);
    if (m10) return m10[0];
    const cleaned = s.replace(/[^0-9X]/g, "");
    return cleaned.length >= 10 ? cleaned : null;
}

function upgradeGoogleThumb(url: string) {
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
        return { coverUrl: googleCoverUrl, coverSource: "google_books" as const };
    }

    if (await urlLooksLikeImage(openLibraryLarge)) {
        return { coverUrl: openLibraryLarge, coverSource: "openlibrary" as const };
    }
    if (await urlLooksLikeImage(openLibraryMedium)) {
        return { coverUrl: openLibraryMedium, coverSource: "openlibrary" as const };
    }

    return { coverUrl: null, coverSource: null };
}

function cleanCategories(cats: string[]) {
    const uniq = Array.from(new Set(cats.map((c) => String(c).trim()).filter(Boolean)));
    return uniq.slice(0, 8).map((name) => ({ name: name.slice(0, 50) }));
}

function notionDate(publishedDate: string | null) {
    if (!publishedDate) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(publishedDate)) return publishedDate;
    if (/^\d{4}-\d{2}$/.test(publishedDate)) return `${publishedDate}-01`;
    if (/^\d{4}$/.test(publishedDate)) return `${publishedDate}-01-01`;
    return null;
}

function extractRichText(prop: unknown) {
    const p = asRecord(prop);
    const parts = asArray(p?.rich_text);
    return parts
        .map((r) => getString(asRecord(r)?.plain_text) ?? "")
        .join("")
        .trim();
}

function extractTitle(prop: unknown) {
    const p = asRecord(prop);
    const parts = asArray(p?.title);
    return parts
        .map((r) => getString(asRecord(r)?.plain_text) ?? "")
        .join("")
        .trim();
}

function isUnknownTitle(value: string) {
    return /^unknown\s+title/i.test(value.trim());
}

function hasMeaningfulValue(prop: unknown) {
    const p = asRecord(prop);
    if (!p) return false;
    if (p.type === "rich_text") return extractRichText(p).length > 0;
    if (p.type === "title") {
        const t = asArray(p.title)
            .map((r) => getString(asRecord(r)?.plain_text) ?? "")
            .join("")
            .trim();
        return t.length > 0;
    }
    if (p.type === "number") return p.number !== null && p.number !== undefined;
    if (p.type === "url") return typeof p.url === "string" && p.url.length > 0;
    if (p.type === "multi_select") return asArray(p.multi_select).length > 0;
    if (p.type === "date") return Boolean(asRecord(p.date)?.start);
    return false;
}

async function getDatabaseMeta(databaseId: string): Promise<DatabaseMeta> {
    const database = await notion.databases.retrieve({ database_id: databaseId });
    const databaseObj = asRecord(database);
    const dataSources = asArray(databaseObj?.data_sources);
    const firstDataSourceId = getString(asRecord(dataSources[0])?.id);

    if (!firstDataSourceId) {
        throw new Error("No Notion data source found for NOTION_DATABASE_ID");
    }

    const dataSource = await notion.dataSources.retrieve({ data_source_id: firstDataSourceId });
    const dataSourceObj = asRecord(dataSource);
    const propsObj = asRecord(dataSourceObj?.properties) ?? asRecord(databaseObj?.properties) ?? {};
    return { dataSourceId: firstDataSourceId, propertyNames: new Set(Object.keys(propsObj)) };
}

async function fetchGoogleBooksByIsbn(isbn: string): Promise<BookMetadata | null> {
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
        publishedDate: v.publishedDate ?? null,
        pageCount: typeof v.pageCount === "number" ? v.pageCount : null,
        categories: Array.isArray(v.categories) ? v.categories : [],
        coverUrl,
        coverSource,
        description: v.description ?? null,
        sourceUrl: v.infoLink ?? v.previewLink ?? null,
    };
}

async function fetchOpenLibraryByIsbn(isbn: string): Promise<Partial<BookMetadata> | null> {
    const url = `https://openlibrary.org/isbn/${encodeURIComponent(isbn)}.json`;
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) return null;

    const data = await res.json();
    const title = typeof data?.title === "string" ? data.title : null;
    const subtitle = typeof data?.subtitle === "string" ? data.subtitle : null;
    const publishedDate = typeof data?.publish_date === "string" ? data.publish_date : null;
    const pageCount = typeof data?.number_of_pages === "number" ? data.number_of_pages : null;

    let authors: string[] = [];
    if (Array.isArray(data?.authors) && data.authors.length) {
        const names = await Promise.all(
            data.authors.map(async (authorRef: unknown) => {
                const key = getString(asRecord(authorRef)?.key);
                if (!key || typeof key !== "string") return null;
                try {
                    const authorRes = await fetch(`https://openlibrary.org${key}.json`, { cache: "no-store" });
                    if (!authorRes.ok) return null;
                    const authorData = await authorRes.json();
                    return getString(asRecord(authorData)?.name);
                } catch {
                    return null;
                }
            })
        );
        authors = names.filter((n): n is string => Boolean(n));
    }

    const { coverUrl, coverSource } = await resolveBestCoverUrl(isbn, null);

    return {
        title,
        subtitle,
        authors,
        publishedDate,
        pageCount,
        coverUrl,
        coverSource,
        sourceUrl: `https://openlibrary.org/isbn/${encodeURIComponent(isbn)}`,
    };
}

async function fetchBookMetadataByIsbn(isbn: string): Promise<BookMetadata | null> {
    const google = await fetchGoogleBooksByIsbn(isbn);
    const openLibrary = await fetchOpenLibraryByIsbn(isbn);

    if (!google && !openLibrary) return null;

    return {
        googleId: google?.googleId ?? null,
        title: google?.title ?? openLibrary?.title ?? null,
        subtitle: google?.subtitle ?? openLibrary?.subtitle ?? null,
        authors: google?.authors?.length ? google.authors : (openLibrary?.authors ?? []),
        publisher: google?.publisher ?? null,
        publishedDate: google?.publishedDate ?? (openLibrary?.publishedDate ?? null),
        pageCount: google?.pageCount ?? (openLibrary?.pageCount ?? null),
        categories: google?.categories ?? [],
        coverUrl: google?.coverUrl ?? (openLibrary?.coverUrl ?? null),
        coverSource: google?.coverSource ?? (openLibrary?.coverSource ?? null),
        description: google?.description ?? null,
        sourceUrl: google?.sourceUrl ?? (openLibrary?.sourceUrl ?? null),
    };
}

function extractIsbnFromPage(properties: unknown) {
    const props = asRecord(properties);
    const isbnProp = props?.ISBN;
    if (!isbnProp) return null;
    const isbnObj = asRecord(isbnProp);
    if (isbnObj?.type === "rich_text") return normalizeIsbn(extractRichText(isbnObj));
    return null;
}

export async function POST(req: Request) {
    const startedAt = Date.now();

    try {
        const body = await req.json().catch(() => ({}));
        const dryRun = Boolean(body?.dryRun);
        const maxPages = Math.max(1, Math.min(200, Number(body?.maxPages ?? 50)));
        const onlyMissing = body?.onlyMissing !== false;

        const db = process.env.NOTION_DATABASE_ID;
        if (!db) {
            return NextResponse.json(
                {
                    ok: false,
                    code: "SERVER_MISCONFIGURED",
                    message: "NOTION_DATABASE_ID not set.",
                },
                { status: 500 }
            );
        }

        const { dataSourceId, propertyNames } = await getDatabaseMeta(db);

        const results: BackfillResultItem[] = [];
        const stats = {
            scanned: 0,
            updated: 0,
            wouldUpdate: 0,
            skipped: 0,
            errors: 0,
        };

        let cursor: string | undefined;

        while (stats.scanned < maxPages) {
            const pageSize = Math.min(50, maxPages - stats.scanned);
            const resp = await notion.dataSources.query({
                data_source_id: dataSourceId,
                page_size: pageSize,
                start_cursor: cursor,
            });

            for (const row of resp.results) {
                const rowObj = asRecord(row);
                if (stats.scanned >= maxPages) break;
                if (rowObj?.object !== "page") continue;

                stats.scanned += 1;
                const pageId = String(rowObj?.id ?? "");
                const props = asRecord(rowObj?.properties) ?? {};
                const isbn = extractIsbnFromPage(props);

                if (!isbn) {
                    stats.skipped += 1;
                    results.push({ pageId, isbn: null, action: "skipped", reason: "Missing ISBN property" });
                    continue;
                }

                try {
                    const book = await fetchBookMetadataByIsbn(isbn);
                    if (!book) {
                        stats.skipped += 1;
                        results.push({ pageId, isbn, action: "skipped", reason: "No metadata found in Google Books or Open Library" });
                        continue;
                    }

                    const updates: Record<string, unknown> = {};
                    const updatedFields: string[] = [];

                    const currentTitle = extractTitle(props.Name);
                    const mergedTitle = book.title
                        ? (book.subtitle ? `${book.title}: ${book.subtitle}` : book.title)
                        : null;

                    if (
                        propertyNames.has("Name") &&
                        mergedTitle &&
                        (!onlyMissing || !currentTitle || isUnknownTitle(currentTitle))
                    ) {
                        updates.Name = { title: [{ text: { content: mergedTitle.slice(0, 2000) } }] };
                        updatedFields.push("Name");
                    }

                    if (propertyNames.has("Authors") && (!onlyMissing || !hasMeaningfulValue(props.Authors)) && book.authors.length) {
                        updates.Authors = { rich_text: [{ text: { content: book.authors.join(", ") } }] };
                        updatedFields.push("Authors");
                    }

                    if (propertyNames.has("Publisher") && (!onlyMissing || !hasMeaningfulValue(props.Publisher)) && book.publisher) {
                        updates.Publisher = { rich_text: [{ text: { content: book.publisher } }] };
                        updatedFields.push("Publisher");
                    }

                    if (propertyNames.has("Page Count") && (!onlyMissing || !hasMeaningfulValue(props["Page Count"])) && book.pageCount != null) {
                        updates["Page Count"] = { number: book.pageCount };
                        updatedFields.push("Page Count");
                    }

                    if (propertyNames.has("Categories") && (!onlyMissing || !hasMeaningfulValue(props.Categories)) && book.categories.length) {
                        updates.Categories = { multi_select: cleanCategories(book.categories) };
                        updatedFields.push("Categories");
                    }

                    if (propertyNames.has("Cover URL") && (!onlyMissing || !hasMeaningfulValue(props["Cover URL"])) && book.coverUrl) {
                        updates["Cover URL"] = { url: book.coverUrl };
                        updatedFields.push("Cover URL");
                    }

                    if (propertyNames.has("Google Books ID") && (!onlyMissing || !hasMeaningfulValue(props["Google Books ID"])) && book.googleId) {
                        updates["Google Books ID"] = { rich_text: [{ text: { content: book.googleId } }] };
                        updatedFields.push("Google Books ID");
                    }

                    if (propertyNames.has("Source URL") && (!onlyMissing || !hasMeaningfulValue(props["Source URL"])) && book.sourceUrl) {
                        updates["Source URL"] = { url: book.sourceUrl };
                        updatedFields.push("Source URL");
                    }

                    if (propertyNames.has("Description") && (!onlyMissing || !hasMeaningfulValue(props.Description)) && book.description) {
                        updates.Description = { rich_text: [{ text: { content: book.description.slice(0, 2000) } }] };
                        updatedFields.push("Description");
                    }

                    const published = notionDate(book.publishedDate ?? null);
                    if (propertyNames.has("Published") && (!onlyMissing || !hasMeaningfulValue(props.Published)) && published) {
                        updates.Published = { date: { start: published } };
                        updatedFields.push("Published");
                    }

                    if (updatedFields.length === 0) {
                        stats.skipped += 1;
                        results.push({ pageId, isbn, action: "skipped", reason: "No missing fields to backfill" });
                        continue;
                    }

                    if (dryRun) {
                        stats.wouldUpdate += 1;
                        results.push({ pageId, isbn, action: "would_update", updatedFields });
                        continue;
                    }

                    await notion.pages.update({
                        page_id: pageId,
                        properties: updates as NotionUpdateArgs["properties"],
                    });

                    stats.updated += 1;
                    results.push({ pageId, isbn, action: "updated", updatedFields });
                } catch (error: unknown) {
                    stats.errors += 1;
                    results.push({
                        pageId,
                        isbn,
                        action: "error",
                        reason: error instanceof Error ? error.message : "Unknown error",
                    });
                }
            }

            if (!resp.has_more || !resp.next_cursor) break;
            cursor = resp.next_cursor;
        }

        return NextResponse.json({
            ok: true,
            code: "BACKFILL_COMPLETE",
            message: dryRun ? "Backfill dry run complete." : "Backfill complete.",
            config: { dryRun, maxPages, onlyMissing },
            stats,
            durationMs: Date.now() - startedAt,
            results,
        });
    } catch (err: unknown) {
        return NextResponse.json(
            {
                ok: false,
                code: "BACKFILL_FAILED",
                message: "Backfill failed.",
                error: err instanceof Error ? err.message : "Server error",
                durationMs: Date.now() - startedAt,
            },
            { status: 500 }
        );
    }
}
