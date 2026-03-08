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

function parseBool(input: unknown) {
    if (typeof input === "boolean") return input;
    if (typeof input === "number") return input !== 0;
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

function normalizeName(name: string) {
    return name
        .toLowerCase()
        .replace(/\s+/g, " ")
        .trim();
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

function resolveAuthorOpenLibraryKeyProperty(meta: DataSourceMeta) {
    const configured = process.env.NOTION_AUTHOR_OPENLIBRARY_KEY_PROPERTY ?? "OpenLibrary Author Key";
    if (meta.propertyNames.has(configured)) return configured;
    return null;
}

function extractTextValue(prop: unknown, schemaType: string | null) {
    const propObj = asRecord(prop);
    if (!propObj || !schemaType) return "";

    if (schemaType === "title") {
        return asArray(propObj.title)
            .map((part) => getString(asRecord(part)?.plain_text) ?? "")
            .join("")
            .trim();
    }
    if (schemaType === "rich_text") {
        return asArray(propObj.rich_text)
            .map((part) => getString(asRecord(part)?.plain_text) ?? "")
            .join("")
            .trim();
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

function extractOpenLibraryAuthorKey(raw: unknown) {
    const key = String(raw ?? "").trim();
    if (!key) return null;
    return key.startsWith("/authors/") ? key.replace("/authors/", "") : key;
}

const AUTHOR_OPENLIBRARY_KEY_CACHE = new Map<string, string | null>();

async function fetchOpenLibraryAuthorKeyByName(authorName: string) {
    const cacheKey = normalizeName(authorName);
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

        const target = normalizeName(authorName);
        const exact = docs.find((doc) => normalizeName(String(asRecord(doc)?.name ?? "")) === target);
        const chosen = asRecord(exact ?? docs[0]);
        const key = extractOpenLibraryAuthorKey(chosen?.key);

        AUTHOR_OPENLIBRARY_KEY_CACHE.set(cacheKey, key);
        return key;
    } catch {
        AUTHOR_OPENLIBRARY_KEY_CACHE.set(cacheKey, null);
        return null;
    }
}

type BackfillAuthorResult = {
    pageId: string;
    authorName: string | null;
    action: "updated" | "would_update" | "skipped" | "error";
    reason?: string;
    openLibraryAuthorKey?: string | null;
};

export async function POST(req: Request) {
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

        const body = await req.json().catch(() => ({}));
        const dryRun = parseBool(body?.dryRun);
        const onlyMissing = body?.onlyMissing !== false;
        const maxPages = Math.max(1, Math.min(500, Number(body?.maxPages ?? 200)));

        const authorsDb = process.env.NOTION_AUTHORS_DATABASE_ID;
        if (!authorsDb) {
            return NextResponse.json(
                {
                    ok: false,
                    code: "SERVER_MISCONFIGURED",
                    message: "NOTION_AUTHORS_DATABASE_ID not set.",
                },
                { status: 500 }
            );
        }

        const authorsMeta = await getDataSourceMeta(authorsDb);
        const nameProperty = resolveAuthorNameProperty(authorsMeta);
        if (!nameProperty) {
            return NextResponse.json(
                {
                    ok: false,
                    code: "SERVER_MISCONFIGURED",
                    message: "No author name property found (title/rich_text).",
                },
                { status: 500 }
            );
        }

        const keyProperty = resolveAuthorOpenLibraryKeyProperty(authorsMeta);
        if (!keyProperty) {
            return NextResponse.json(
                {
                    ok: false,
                    code: "SERVER_MISCONFIGURED",
                    message: "OpenLibrary author key property not found on authors database.",
                },
                { status: 500 }
            );
        }

        const nameSchemaType = getString(asRecord(authorsMeta.propertySchemas[nameProperty])?.type);
        const keySchemaType = getString(asRecord(authorsMeta.propertySchemas[keyProperty])?.type);

        const results: BackfillAuthorResult[] = [];
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
                data_source_id: authorsMeta.dataSourceId,
                page_size: pageSize,
                start_cursor: cursor,
            });

            for (const row of resp.results) {
                const rowObj = asRecord(row);
                if (stats.scanned >= maxPages) break;
                if (rowObj?.object !== "page") continue;

                stats.scanned += 1;
                const pageId = String(rowObj?.id ?? "");
                const properties = asRecord(rowObj?.properties) ?? {};
                const authorName = extractTextValue(properties[nameProperty], nameSchemaType);

                if (!authorName) {
                    stats.skipped += 1;
                    results.push({ pageId, authorName: null, action: "skipped", reason: "Missing author name" });
                    continue;
                }

                const existingKey = extractTextValue(properties[keyProperty], keySchemaType);
                if (onlyMissing && existingKey) {
                    stats.skipped += 1;
                    results.push({ pageId, authorName, action: "skipped", reason: "OpenLibrary Author Key already present" });
                    continue;
                }

                try {
                    const openLibraryAuthorKey = await fetchOpenLibraryAuthorKeyByName(authorName);
                    if (!openLibraryAuthorKey) {
                        stats.skipped += 1;
                        results.push({
                            pageId,
                            authorName,
                            action: "skipped",
                            reason: "No Open Library author match found",
                            openLibraryAuthorKey: null,
                        });
                        continue;
                    }

                    const keyValue = buildOpenLibraryAuthorKeyPropertyValue(keySchemaType, openLibraryAuthorKey);
                    if (!keyValue) {
                        stats.skipped += 1;
                        results.push({
                            pageId,
                            authorName,
                            action: "skipped",
                            reason: "OpenLibrary Author Key property type unsupported",
                            openLibraryAuthorKey,
                        });
                        continue;
                    }

                    if (dryRun) {
                        stats.wouldUpdate += 1;
                        results.push({
                            pageId,
                            authorName,
                            action: "would_update",
                            openLibraryAuthorKey,
                        });
                        continue;
                    }

                    await notion.pages.update({
                        page_id: pageId,
                        properties: {
                            [keyProperty]: keyValue,
                        } as NotionUpdateArgs["properties"],
                    });

                    stats.updated += 1;
                    results.push({
                        pageId,
                        authorName,
                        action: "updated",
                        openLibraryAuthorKey,
                    });
                } catch (error: unknown) {
                    stats.errors += 1;
                    results.push({
                        pageId,
                        authorName,
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
            code: "AUTHORS_BACKFILL_COMPLETE",
            message: dryRun
                ? "Authors OpenLibrary key backfill dry run complete."
                : "Authors OpenLibrary key backfill complete.",
            config: {
                dryRun,
                onlyMissing,
                maxPages,
                keyProperty,
            },
            stats,
            durationMs: Date.now() - startedAt,
            results,
        });
    } catch (err: unknown) {
        return NextResponse.json(
            {
                ok: false,
                code: "AUTHORS_BACKFILL_FAILED",
                message: "Authors backfill failed.",
                error: err instanceof Error ? err.message : "Server error",
                durationMs: Date.now() - startedAt,
            },
            { status: 500 }
        );
    }
}
