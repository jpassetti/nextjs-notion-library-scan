import { NextResponse } from "next/server";
import { Client } from "@notionhq/client";

export const runtime = "nodejs";

const notion = new Client({ auth: process.env.NOTION_TOKEN });

type JsonObject = Record<string, unknown>;
type NotionQueryArgs = Parameters<typeof notion.dataSources.query>[0];
type NotionCreateArgs = Parameters<typeof notion.pages.create>[0];

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

function parseBool(input: unknown) {
    if (typeof input === "boolean") return input;
    const s = String(input ?? "").trim().toLowerCase();
    return s === "1" || s === "true" || s === "yes" || s === "on";
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

type DataSourceMeta = {
    databaseId: string;
    dataSourceId: string;
    propertyNames: Set<string>;
    propertySchemas: Record<string, unknown>;
};

function normalizeTitle(raw: string) {
    return raw
        .toLowerCase()
        .replace(/\([^)]*\)/g, " ")
        .replace(/[^a-z0-9]+/g, " ")
        .replace(/\s+/g, " ")
        .trim();
}

function extractTitleFromNotionProp(prop: unknown) {
    const p = asRecord(prop);
    const parts = asArray(p?.title);
    return parts
        .map((part) => getString(asRecord(part)?.plain_text) ?? "")
        .join("")
        .trim();
}

function extractRichText(prop: unknown) {
    const p = asRecord(prop);
    const parts = asArray(p?.rich_text);
    return parts
        .map((part) => getString(asRecord(part)?.plain_text) ?? "")
        .join("")
        .trim();
}

function extractWorkKeyFromSourceUrl(url: string) {
    const m = url.match(/openlibrary\.org\/works\/(OL\d+W)/i);
    return m ? m[1].toUpperCase() : null;
}

async function getDataSourceMeta(databaseId: string): Promise<DataSourceMeta> {
    const database = await notion.databases.retrieve({ database_id: databaseId });
    const databaseObj = asRecord(database);
    const dataSources = asArray(databaseObj?.data_sources);
    const firstDataSourceId = getString(asRecord(dataSources[0])?.id);

    if (!firstDataSourceId) {
        throw new Error(`No Notion data source found for database: ${databaseId}`);
    }

    const dataSource = await notion.dataSources.retrieve({ data_source_id: firstDataSourceId });
    const dataSourceObj = asRecord(dataSource);
    const propertySchemas = asRecord(dataSourceObj?.properties) ?? asRecord(databaseObj?.properties) ?? {};

    return {
        databaseId,
        dataSourceId: firstDataSourceId,
        propertyNames: new Set(Object.keys(propertySchemas)),
        propertySchemas,
    };
}

function resolveAuthorNameProperty(meta: DataSourceMeta) {
    const configured = process.env.NOTION_AUTHOR_NAME_PROPERTY ?? "Name";
    if (meta.propertyNames.has(configured)) return configured;

    for (const [name, schema] of Object.entries(meta.propertySchemas)) {
        if (asRecord(schema)?.type === "title") return name;
    }

    for (const [name, schema] of Object.entries(meta.propertySchemas)) {
        if (asRecord(schema)?.type === "rich_text") return name;
    }

    return null;
}

function findBookAuthorRelationProperty(bookMeta: DataSourceMeta, authorsMeta: DataSourceMeta) {
    const configured = process.env.NOTION_BOOK_AUTHOR_RELATION_PROPERTY;
    if (configured && bookMeta.propertyNames.has(configured)) return configured;

    for (const [name, schema] of Object.entries(bookMeta.propertySchemas)) {
        const s = asRecord(schema);
        if (s?.type !== "relation") continue;
        const relation = asRecord(s?.relation);
        if (!relation) continue;

        if (relation.data_source_id === authorsMeta.dataSourceId || relation.database_id === authorsMeta.databaseId) {
            return name;
        }
    }

    return null;
}

async function findAuthorPage(authorName: string, authorsMeta: DataSourceMeta) {
    const nameProperty = resolveAuthorNameProperty(authorsMeta);
    if (!nameProperty) return null;

    const schemaType = asRecord(authorsMeta.propertySchemas[nameProperty])?.type;
    if (schemaType !== "title" && schemaType !== "rich_text") return null;

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

    const resp = await notion.dataSources.query({
        data_source_id: authorsMeta.dataSourceId,
        filter: filter as NotionQueryArgs["filter"],
        page_size: 1,
    });

    const found = asRecord(resp.results?.[0]);
    return found ?? null;
}

async function fetchNotionTitlesForAuthor(params: {
    authorName: string;
    maxPages: number;
}) {
    const { authorName, maxPages } = params;
    const booksDb = process.env.NOTION_DATABASE_ID;
    if (!booksDb) {
        throw new Error("NOTION_DATABASE_ID not set.");
    }

    const bookMeta = await getDataSourceMeta(booksDb);
    const entries: Array<{ title: string; workKey: string | null; sourceUrl: string | null }> = [];
    let queryMode: "relation" | "rich_text" = "rich_text";
    let relationPropertyUsed: string | null = null;
    let matchedAuthorPageId: string | null = null;

    const authorsDb = process.env.NOTION_AUTHORS_DATABASE_ID;
    if (authorsDb) {
        const authorsMeta = await getDataSourceMeta(authorsDb);
        const relationProperty = findBookAuthorRelationProperty(bookMeta, authorsMeta);
        if (relationProperty) {
            const authorPage = await findAuthorPage(authorName, authorsMeta);
            if (authorPage?.id) {
                queryMode = "relation";
                relationPropertyUsed = relationProperty;
                matchedAuthorPageId = String(authorPage.id);
            }
        }
    }

    if (queryMode === "rich_text" && !bookMeta.propertyNames.has("Authors")) {
        throw new Error("No author relation and no Authors text property found on book database.");
    }

    let cursor: string | undefined;
    let scanned = 0;

    while (scanned < maxPages) {
        const pageSize = Math.min(50, maxPages - scanned);
        const filter =
            queryMode === "relation"
                ? {
                    property: relationPropertyUsed!,
                    relation: { contains: matchedAuthorPageId! },
                }
                : {
                    property: "Authors",
                    rich_text: { contains: authorName },
                };

        const resp = await notion.dataSources.query({
            data_source_id: bookMeta.dataSourceId,
            filter: filter as NotionQueryArgs["filter"],
            page_size: pageSize,
            start_cursor: cursor,
        });

        for (const row of resp.results) {
            const rowObj = asRecord(row);
            scanned += 1;
            if (scanned > maxPages) break;
            const properties = asRecord(rowObj?.properties) ?? {};
            const title = extractTitleFromNotionProp(properties.Name);
            const explicitWorkKey = extractRichText(properties["OpenLibrary Work Key"])
                || extractRichText(properties["OL Work Key"]);
            const sourceUrl = getString(asRecord(properties["Source URL"])?.url);
            const derivedWorkKey = sourceUrl ? extractWorkKeyFromSourceUrl(String(sourceUrl)) : null;
            if (title) {
                entries.push({
                    title,
                    workKey: (explicitWorkKey || derivedWorkKey || null)?.toUpperCase() ?? null,
                    sourceUrl: sourceUrl ? String(sourceUrl) : null,
                });
            }
        }

        if (!resp.has_more || !resp.next_cursor) break;
        cursor = resp.next_cursor;
    }

    return {
        entries,
        queryMode,
        relationPropertyUsed,
        matchedAuthorPageId,
    };
}

async function fetchOpenLibraryAuthorKey(authorName: string) {
    const url = `https://openlibrary.org/search/authors.json?q=${encodeURIComponent(authorName)}`;
    const res = await fetchWithRetry(url, { cache: "no-store" }, { retries: 2 });
    if (!res.ok) return null;

    const data = await res.json();
    const docs = Array.isArray(data?.docs) ? data.docs : [];
    if (!docs.length) return null;

    const lowerTarget = authorName.toLowerCase();
    const exact = docs.find((d: unknown) => String(asRecord(d)?.name ?? "").toLowerCase() === lowerTarget);
    const chosen = exact ?? docs[0];

    if (!chosen?.key || !chosen?.name) return null;

    return {
        key: String(chosen.key),
        name: String(chosen.name),
    };
}

async function fetchOpenLibraryWorks(authorKey: string, maxWorks: number) {
    const works: Array<{ title: string; workKey: string | null }> = [];
    let offset = 0;

    while (works.length < maxWorks) {
        const limit = Math.min(100, maxWorks - works.length);
        const url = `https://openlibrary.org/authors/${encodeURIComponent(authorKey)}/works.json?limit=${limit}&offset=${offset}`;
        const res = await fetchWithRetry(url, { cache: "no-store" }, { retries: 2 });
        if (!res.ok) break;

        const data = await res.json();
        const entries = Array.isArray(data?.entries) ? data.entries : [];
        if (!entries.length) break;

        for (const entry of entries) {
            const title = String(entry?.title ?? "").trim();
            const keyRaw = String(entry?.key ?? "").trim();
            const workKey = keyRaw.startsWith("/works/") ? keyRaw.replace("/works/", "").toUpperCase() : null;
            if (title) works.push({ title, workKey });
        }

        if (entries.length < limit) break;
        offset += entries.length;
    }

    return works;
}

async function syncMissingWorksToNotion(params: {
    missingWorks: Array<{ title: string; workKey: string | null }>;
    authorName: string;
}) {
    const dataSourceId = process.env.NOTION_MISSING_BOOKS_DATA_SOURCE_ID;
    if (!dataSourceId) {
        return { synced: 0, skipped: params.missingWorks.length, reason: "Missing data source not configured." };
    }

    const titleProperty = process.env.NOTION_MISSING_TITLE_PROPERTY ?? "Name";
    const authorProperty = process.env.NOTION_MISSING_AUTHOR_PROPERTY ?? "Author";
    const workKeyProperty = process.env.NOTION_MISSING_WORK_KEY_PROPERTY ?? "OpenLibrary Work Key";

    let synced = 0;
    let skipped = 0;

    for (const work of params.missingWorks) {
        const findFilter = work.workKey
            ? {
                property: workKeyProperty,
                rich_text: { equals: work.workKey },
            }
            : {
                property: titleProperty,
                title: { equals: work.title },
            };

        const found = await notion.dataSources.query({
            data_source_id: dataSourceId,
            filter: findFilter as NotionQueryArgs["filter"],
            page_size: 1,
        });

        if (found.results?.length) {
            skipped += 1;
            continue;
        }

        await notion.pages.create({
            parent: { data_source_id: dataSourceId },
            properties: {
                [titleProperty]: { title: [{ text: { content: work.title.slice(0, 2000) } }] },
                [authorProperty]: { rich_text: [{ text: { content: params.authorName.slice(0, 2000) } }] },
                [workKeyProperty]: {
                    rich_text: [{ text: { content: String(work.workKey ?? "").slice(0, 2000) } }],
                },
            } as NotionCreateArgs["properties"],
        });

        synced += 1;
    }

    return { synced, skipped, reason: null as string | null };
}

export async function GET(req: Request) {
    const startedAt = Date.now();

    try {
        if (!isApiAuthorized(req)) {
            return NextResponse.json(
                {
                    ok: false,
                    code: "UNAUTHORIZED",
                    message: "Unauthorized request.",
                },
                { status: 401 }
            );
        }

        const { searchParams } = new URL(req.url);
        const authorName = String(searchParams.get("author") ?? "").trim();
        const maxPages = Math.max(1, Math.min(500, Number(searchParams.get("maxPages") ?? 250)));
        const maxWorks = Math.max(10, Math.min(2000, Number(searchParams.get("maxWorks") ?? 600)));
        const maxMissing = Math.max(10, Math.min(500, Number(searchParams.get("maxMissing") ?? 100)));
        const syncToNotion = parseBool(searchParams.get("sync") ?? "0");

        if (!authorName) {
            return NextResponse.json(
                {
                    ok: false,
                    code: "MISSING_AUTHOR",
                    message: "Provide ?author=Author Name",
                },
                { status: 400 }
            );
        }

        const { entries: notionEntries, queryMode, relationPropertyUsed, matchedAuthorPageId } = await fetchNotionTitlesForAuthor({
            authorName,
            maxPages,
        });

        const authorLookup = await fetchOpenLibraryAuthorKey(authorName);
        if (!authorLookup) {
            return NextResponse.json(
                {
                    ok: false,
                    code: "AUTHOR_NOT_FOUND",
                    message: `Open Library author lookup failed for ${authorName}.`,
                },
                { status: 404 }
            );
        }

        const expectedWorks = await fetchOpenLibraryWorks(authorLookup.key, maxWorks);

        const notionTitleSet = new Set(notionEntries.map((entry) => normalizeTitle(entry.title)).filter(Boolean));
        const notionWorkKeySet = new Set(
            notionEntries
                .map((entry) => entry.workKey)
                .filter((key): key is string => Boolean(key))
                .map((key) => key.toUpperCase())
        );

        const dedupExpectedByKey = new Map<string, { title: string; workKey: string | null }>();
        for (const work of expectedWorks) {
            const identity = (work.workKey || `TITLE:${normalizeTitle(work.title)}`).toUpperCase();
            if (!dedupExpectedByKey.has(identity)) {
                dedupExpectedByKey.set(identity, {
                    title: work.title,
                    workKey: work.workKey ? work.workKey.toUpperCase() : null,
                });
            }
        }
        const dedupExpected = Array.from(dedupExpectedByKey.values());

        const missingWorks = dedupExpected
            .filter((work) => {
                if (work.workKey) {
                    return !notionWorkKeySet.has(work.workKey);
                }
                const key = normalizeTitle(work.title);
                return key.length > 0 && !notionTitleSet.has(key);
            })
            .slice(0, maxMissing);

        const syncResult = syncToNotion
            ? await syncMissingWorksToNotion({ missingWorks, authorName: authorLookup.name })
            : { synced: 0, skipped: 0, reason: "Sync disabled." };

        if (parseBool(process.env.AUDIT_LOG_TO_CONSOLE ?? "1")) {
            console.info("missing_audit", {
                ts: new Date().toISOString(),
                route: "/api/authors/missing",
                author: authorLookup.name,
                missingCount: missingWorks.length,
                syncToNotion,
                syncResult,
                durationMs: Date.now() - startedAt,
            });
        }

        return NextResponse.json({
            ok: true,
            code: "AUTHOR_MISSING_REPORT",
            message: `Computed missing works for ${authorLookup.name}.`,
            author: {
                requested: authorName,
                matched: authorLookup.name,
                openLibraryKey: authorLookup.key,
            },
            notion: {
                bookCount: notionEntries.length,
                queryMode,
                relationProperty: relationPropertyUsed,
                authorPageId: matchedAuthorPageId,
                canonicalWorkKeyCount: notionWorkKeySet.size,
                sampleTitles: notionEntries.map((entry) => entry.title).slice(0, 25),
            },
            openLibrary: {
                expectedCount: dedupExpected.length,
            },
            missing: {
                count: missingWorks.length,
                titles: missingWorks.map((w) => w.title),
                works: missingWorks,
            },
            sync: {
                enabled: syncToNotion,
                ...syncResult,
            },
            durationMs: Date.now() - startedAt,
        });
    } catch (err: unknown) {
        if (parseBool(process.env.AUDIT_LOG_TO_CONSOLE ?? "1")) {
            console.info("missing_audit_error", {
                ts: new Date().toISOString(),
                route: "/api/authors/missing",
                error: err instanceof Error ? err.message : "Server error",
                durationMs: Date.now() - startedAt,
            });
        }
        return NextResponse.json(
            {
                ok: false,
                code: "AUTHOR_MISSING_REPORT_FAILED",
                message: "Failed to compute missing works.",
                error: err instanceof Error ? err.message : "Server error",
                durationMs: Date.now() - startedAt,
            },
            { status: 500 }
        );
    }
}
