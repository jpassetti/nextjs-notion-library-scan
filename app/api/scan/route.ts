import { NextResponse } from "next/server";
import { Client } from "@notionhq/client";

export const runtime = "nodejs"; // important for Notion SDK

const notion = new Client({ auth: process.env.NOTION_TOKEN });

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
    const coverUrl = rawCoverUrl ? upgradeGoogleThumb(rawCoverUrl) : null;
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
        if (!body?.isbn && searchParams.get("isbn")) {
            body.isbn = searchParams.get("isbn");
        }

        const isbn = extractIsbn(body?.isbn);
        if (!isbn) {
            return NextResponse.json({ ok: false, error: "Missing/invalid isbn" }, { status: 400 });
        }

        const db = process.env.NOTION_DATABASE_ID;
        if (!db) {
            return NextResponse.json({ ok: false, error: "NOTION_DATABASE_ID not set" }, { status: 500 });
        }

        const { dataSourceId, propertyNames } = await getDatabaseMeta(db);

        // Debug: return database property names without creating/updating anything
        if (searchParams.get("debug") === "1") {
            return NextResponse.json({
                ok: true,
                databaseId: db,
                propertyNames: Array.from(propertyNames).sort(),
            });
        }

        const existing = await findExistingByIsbn(isbn);

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

        return NextResponse.json({
            ok: true,
            isbn,
            title,
            authors: book?.authors?.join(", ") ?? "",
            action: existing ? "updated" : "created",
            message,
            notionPageId: pageId,
            notionUrl: url,
            notionDeepLinkUrl: deepLinkUrl,
            updated: Boolean(existing),
        });
    } catch (err: any) {
        return NextResponse.json(
            { ok: false, error: err?.message ?? "Server error" },
            { status: 500 }
        );
    }
}