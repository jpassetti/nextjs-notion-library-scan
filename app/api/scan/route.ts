import { NextResponse } from "next/server";
import { Client } from "@notionhq/client";

export const runtime = "nodejs"; // important for Notion SDK

const notion = new Client({ auth: process.env.NOTION_TOKEN });

function normalizeIsbn(raw: unknown) {
    if (!raw) return null;
    const cleaned = String(raw).toUpperCase().replace(/[^0-9X]/g, "");
    return cleaned.length >= 10 ? cleaned : null;
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
    return {
        googleId: item.id ?? null,
        title: v.title ?? null,
        subtitle: v.subtitle ?? null,
        authors: Array.isArray(v.authors) ? v.authors : [],
        publisher: v.publisher ?? null,
        publishedDate: v.publishedDate ?? null, // YYYY or YYYY-MM or YYYY-MM-DD
        pageCount: typeof v.pageCount === "number" ? v.pageCount : null,
        categories: Array.isArray(v.categories) ? v.categories : [],
        coverUrl: v.imageLinks?.thumbnail ?? v.imageLinks?.smallThumbnail ?? null,
        description: v.description ?? null,
    };
}

function notionDate(publishedDate: string | null) {
    if (!publishedDate) return null;
    if (/^\d{4}-\d{2}-\d{2}$/.test(publishedDate)) return publishedDate;
    if (/^\d{4}-\d{2}$/.test(publishedDate)) return `${publishedDate}-01`;
    if (/^\d{4}$/.test(publishedDate)) return `${publishedDate}-01-01`;
    return null;
}

// OPTIONAL: prevent duplicates by ISBN
async function getDataSourceIdFromDatabase(databaseId: string) {
    const database = await notion.databases.retrieve({ database_id: databaseId });
    const dataSources = (database as any)?.data_sources;
    const firstDataSourceId = Array.isArray(dataSources) ? dataSources[0]?.id : null;

    if (!firstDataSourceId) {
        throw new Error("No Notion data source found for NOTION_DATABASE_ID");
    }

    return firstDataSourceId;
}

async function findExistingByIsbn(isbn: string) {
    const db = process.env.NOTION_DATABASE_ID!;
    const dataSourceId = await getDataSourceIdFromDatabase(db);
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
        const body = await req.json().catch(() => ({}));
        const isbn = normalizeIsbn(body?.isbn);
        if (!isbn) {
            return NextResponse.json({ ok: false, error: "Missing/invalid isbn" }, { status: 400 });
        }

        const db = process.env.NOTION_DATABASE_ID;
        if (!db) {
            return NextResponse.json({ ok: false, error: "NOTION_DATABASE_ID not set" }, { status: 500 });
        }

        const existing = await findExistingByIsbn(isbn);

        const book = await fetchGoogleBooksByIsbn(isbn);

        const title = book?.title
            ? (book.subtitle ? `${book.title}: ${book.subtitle}` : book.title)
            : `Unknown title (${isbn})`;

        // NOTE: Property names must match your Notion database exactly.
        const properties: any = {
            Name: { title: [{ text: { content: title } }] },
            ISBN: { rich_text: [{ text: { content: isbn } }] },
        };

        if (book?.authors?.length) properties.Authors = { rich_text: [{ text: { content: book.authors.join(", ") } }] };
        if (book?.publisher) properties.Publisher = { rich_text: [{ text: { content: book.publisher } }] };
        if (book?.pageCount != null) properties["Page Count"] = { number: book.pageCount };
        if (book?.categories?.length) {
            properties.Categories = {
                multi_select: book.categories.slice(0, 10).map((c: string) => ({ name: c })),
            };
        }
        if (book?.coverUrl) properties["Cover URL"] = { url: book.coverUrl };
        if (book?.googleId) properties["Google Books ID"] = { rich_text: [{ text: { content: book.googleId } }] };

        const published = notionDate(book?.publishedDate ?? null);
        if (published) properties.Published = { date: { start: published } };

        // If exists, update; otherwise create
        let pageId: string;

        if (existing) {
            const updated = await notion.pages.update({
                page_id: (existing as any).id,
                properties,
            });
            pageId = (updated as any).id;
        } else {
            const created = await notion.pages.create({
                parent: { database_id: db },
                cover: book?.coverUrl
                    ? {
                        type: "external",
                        external: { url: book.coverUrl },
                    }
                    : undefined,
                properties,
                // optional: put description into the page content
                children: book?.description
                    ? [
                        {
                            object: "block",
                            type: "paragraph",
                            paragraph: {
                                rich_text: [{ type: "text", text: { content: book.description.slice(0, 1800) } }],
                            },
                        },
                    ]
                    : [],
            });
            pageId = (created as any).id;
        }

        return NextResponse.json({
            ok: true,
            isbn,
            title,
            notionPageId: pageId,
            updated: Boolean(existing),
        });
    } catch (err: any) {
        return NextResponse.json(
            { ok: false, error: err?.message ?? "Server error" },
            { status: 500 }
        );
    }
}