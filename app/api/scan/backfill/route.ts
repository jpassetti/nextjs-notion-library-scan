import { NextResponse } from "next/server";
import { Client } from "@notionhq/client";

export const runtime = "nodejs";

const notion = new Client({ auth: process.env.NOTION_TOKEN });

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

    if (await urlLooksLikeImage(openLibraryLarge)) {
        return { coverUrl: openLibraryLarge, coverSource: "openlibrary" as const };
    }
    if (await urlLooksLikeImage(openLibraryMedium)) {
        return { coverUrl: openLibraryMedium, coverSource: "openlibrary" as const };
    }

    if (googleCoverUrl && !isKnownUnavailableCoverUrl(googleCoverUrl) && await urlLooksLikeImage(googleCoverUrl)) {
        return { coverUrl: googleCoverUrl, coverSource: "google_books" as const };
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

function extractRichText(prop: any) {
    if (!prop || !Array.isArray(prop.rich_text)) return "";
    return prop.rich_text.map((r: any) => r?.plain_text ?? "").join("").trim();
}

function hasMeaningfulValue(prop: any) {
    if (!prop || typeof prop !== "object") return false;
    if (prop.type === "rich_text") return extractRichText(prop).length > 0;
    if (prop.type === "title") {
        const t = Array.isArray(prop.title) ? prop.title.map((r: any) => r?.plain_text ?? "").join("").trim() : "";
        return t.length > 0;
    }
    if (prop.type === "number") return prop.number !== null && prop.number !== undefined;
    if (prop.type === "url") return typeof prop.url === "string" && prop.url.length > 0;
    if (prop.type === "multi_select") return Array.isArray(prop.multi_select) && prop.multi_select.length > 0;
    if (prop.type === "date") return Boolean(prop.date?.start);
    return false;
}

async function getDatabaseMeta(databaseId: string): Promise<DatabaseMeta> {
    const database = await notion.databases.retrieve({ database_id: databaseId });
    const dataSources = (database as any)?.data_sources;
    const firstDataSourceId = Array.isArray(dataSources) ? dataSources[0]?.id : null;

    if (!firstDataSourceId) {
        throw new Error("No Notion data source found for NOTION_DATABASE_ID");
    }

    const dataSource = await notion.dataSources.retrieve({ data_source_id: firstDataSourceId });
    const propsObj = (dataSource as any)?.properties ?? (database as any)?.properties ?? {};
    return { dataSourceId: firstDataSourceId, propertyNames: new Set(Object.keys(propsObj)) };
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
        publishedDate: v.publishedDate ?? null,
        pageCount: typeof v.pageCount === "number" ? v.pageCount : null,
        categories: Array.isArray(v.categories) ? v.categories : [],
        coverUrl,
        coverSource,
        description: v.description ?? null,
        sourceUrl: v.infoLink ?? v.previewLink ?? null,
    };
}

function extractIsbnFromPage(properties: any) {
    const isbnProp = properties?.ISBN;
    if (!isbnProp) return null;
    if (isbnProp.type === "rich_text") return normalizeIsbn(extractRichText(isbnProp));
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

            for (const row of resp.results as any[]) {
                if (stats.scanned >= maxPages) break;
                if (row?.object !== "page") continue;

                stats.scanned += 1;
                const pageId = row.id as string;
                const props = row.properties ?? {};
                const isbn = extractIsbnFromPage(props);

                if (!isbn) {
                    stats.skipped += 1;
                    results.push({ pageId, isbn: null, action: "skipped", reason: "Missing ISBN property" });
                    continue;
                }

                try {
                    const book = await fetchGoogleBooksByIsbn(isbn);
                    if (!book) {
                        stats.skipped += 1;
                        results.push({ pageId, isbn, action: "skipped", reason: "No Google Books metadata found" });
                        continue;
                    }

                    const updates: Record<string, unknown> = {};
                    const updatedFields: string[] = [];

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
                        properties: updates as any,
                    });

                    stats.updated += 1;
                    results.push({ pageId, isbn, action: "updated", updatedFields });
                } catch (error: any) {
                    stats.errors += 1;
                    results.push({ pageId, isbn, action: "error", reason: String(error?.message ?? "Unknown error") });
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
    } catch (err: any) {
        return NextResponse.json(
            {
                ok: false,
                code: "BACKFILL_FAILED",
                message: "Backfill failed.",
                error: String(err?.message ?? "Server error"),
                durationMs: Date.now() - startedAt,
            },
            { status: 500 }
        );
    }
}
