"use client";

import { FormEvent, useMemo, useState } from "react";

type RouteLink = {
  label: string;
  path: string;
  method: "GET" | "POST";
  note: string;
};

type ApiPayload = {
  ok?: boolean;
  code?: string;
  message?: string;
  suggestion?: string | null;
  requestId?: string;
  durationMs?: number;
  [key: string]: unknown;
};

const ROUTE_LINKS: RouteLink[] = [
  {
    label: "Scan ISBN",
    method: "POST",
    path: "/api/scan",
    note: "Primary import endpoint for Apple Shortcuts and manual form submission.",
  },
  {
    label: "Scan Debug",
    method: "GET",
    path: "/api/scan?debug=1",
    note: "Returns detected Notion property names to validate database schema mapping.",
  },
  {
    label: "Backfill Metadata",
    method: "POST",
    path: "/api/scan/backfill",
    note: "Backfills previously scanned books by ISBN with missing metadata fields.",
  },
  {
    label: "Verbose Scan Example",
    method: "POST",
    path: "/api/scan?verbose=1",
    note: "Returns enriched response payload including data/details for debugging.",
  },
];

function getSummary(response: ApiPayload | null) {
  if (!response) return "Submit an ISBN to preview the API response.";
  const message = typeof response.message === "string" ? response.message : "Request completed.";
  const code = typeof response.code === "string" ? response.code : "NO_CODE";
  return `${code}: ${message}`;
}

export default function Home() {
  const [isbn, setIsbn] = useState("");
  const [onDuplicate, setOnDuplicate] = useState<"update" | "skip">("update");
  const [verbose, setVerbose] = useState(true);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isRunningDebug, setIsRunningDebug] = useState(false);
  const [isRunningBackfill, setIsRunningBackfill] = useState(false);
  const [isRunningVerboseScan, setIsRunningVerboseScan] = useState(false);
  const [backfillDryRun, setBackfillDryRun] = useState(true);
  const [backfillOnlyMissing, setBackfillOnlyMissing] = useState(true);
  const [backfillMaxPages, setBackfillMaxPages] = useState(50);
  const [status, setStatus] = useState<"idle" | "success" | "error">("idle");
  const [response, setResponse] = useState<ApiPayload | null>(null);

  const summary = useMemo(() => getSummary(response), [response]);

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setIsSubmitting(true);
    setStatus("idle");

    try {
      const query = verbose ? "?verbose=1" : "";
      const res = await fetch(`/api/scan${query}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ isbn, onDuplicate }),
      });

      const payload = (await res.json()) as ApiPayload;
      setResponse(payload);
      setStatus(payload.ok ? "success" : "error");
    } catch {
      setResponse({
        ok: false,
        code: "NETWORK_ERROR",
        message: "Could not reach /api/scan.",
        suggestion: "Confirm the dev server is running and retry.",
      });
      setStatus("error");
    } finally {
      setIsSubmitting(false);
    }
  }

  async function runDebugCheck() {
    setIsRunningDebug(true);
    setStatus("idle");

    try {
      const res = await fetch("/api/scan?debug=1");
      const payload = (await res.json()) as ApiPayload;
      setResponse(payload);
      setStatus(payload.ok ? "success" : "error");
    } catch {
      setResponse({
        ok: false,
        code: "NETWORK_ERROR",
        message: "Could not run debug check.",
        suggestion: "Confirm /api/scan is reachable and try again.",
      });
      setStatus("error");
    } finally {
      setIsRunningDebug(false);
    }
  }

  async function runVerboseScan() {
    if (!isbn.trim()) {
      setResponse({
        ok: false,
        code: "MISSING_ISBN",
        message: "Enter an ISBN before running a verbose scan.",
      });
      setStatus("error");
      return;
    }

    setIsRunningVerboseScan(true);
    setStatus("idle");

    try {
      const res = await fetch("/api/scan?verbose=1", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ isbn, onDuplicate }),
      });

      const payload = (await res.json()) as ApiPayload;
      setResponse(payload);
      setStatus(payload.ok ? "success" : "error");
    } catch {
      setResponse({
        ok: false,
        code: "NETWORK_ERROR",
        message: "Could not run verbose scan.",
      });
      setStatus("error");
    } finally {
      setIsRunningVerboseScan(false);
    }
  }

  async function runBackfill() {
    setIsRunningBackfill(true);
    setStatus("idle");

    try {
      const res = await fetch("/api/scan/backfill", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dryRun: backfillDryRun,
          maxPages: backfillMaxPages,
          onlyMissing: backfillOnlyMissing,
        }),
      });

      const payload = (await res.json()) as ApiPayload;
      setResponse(payload);
      setStatus(payload.ok ? "success" : "error");
    } catch {
      setResponse({
        ok: false,
        code: "NETWORK_ERROR",
        message: "Could not run metadata backfill.",
      });
      setStatus("error");
    } finally {
      setIsRunningBackfill(false);
    }
  }

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_10%_20%,#f4f6ff_0%,#ece8ff_35%,#fff4ec_75%,#ffffff_100%)] text-slate-900">
      <main className="mx-auto flex w-full max-w-6xl flex-col gap-8 px-6 py-10 md:px-10 md:py-14">
        <section className="relative overflow-hidden rounded-3xl border border-white/60 bg-white/75 p-7 shadow-[0_20px_60px_rgba(37,28,78,0.12)] backdrop-blur md:p-10">
          <div className="pointer-events-none absolute -right-12 -top-12 h-44 w-44 rounded-full bg-gradient-to-br from-cyan-300/40 to-blue-400/20 blur-2xl" />
          <div className="pointer-events-none absolute -bottom-16 left-6 h-52 w-52 rounded-full bg-gradient-to-br from-orange-200/50 to-rose-200/30 blur-2xl" />
          <p className="relative text-xs font-semibold uppercase tracking-[0.22em] text-indigo-600">Notion Library Import Console</p>
          <h1 className="relative mt-3 max-w-3xl text-3xl font-bold tracking-tight text-slate-950 md:text-5xl">
            Scan, import, and backfill your book catalog from one control panel.
          </h1>
          <p className="relative mt-5 max-w-3xl text-sm leading-7 text-slate-700 md:text-base">
            This homepage is an operator dashboard for your ISBN workflow. Use the route shortcuts below, run a manual import,
            and inspect a structured API response before wiring or updating Apple Shortcuts automations.
          </p>
        </section>

        <section className="grid gap-6 lg:grid-cols-[1.1fr_1fr]">
          <article className="rounded-3xl border border-slate-200/80 bg-white/85 p-6 shadow-[0_12px_40px_rgba(26,23,43,0.08)] backdrop-blur">
            <h2 className="text-lg font-semibold text-slate-900">Important Route Links</h2>
            <p className="mt-2 text-sm text-slate-600">Use these endpoints for scanning, diagnostics, and metadata backfills.</p>
            <div className="mt-5 grid gap-3">
              {ROUTE_LINKS.map((route) => (
                <div
                  key={route.path}
                  className="group rounded-2xl border border-slate-200 bg-white p-4 transition hover:-translate-y-0.5 hover:border-indigo-300 hover:shadow-[0_10px_25px_rgba(76,70,180,0.12)]"
                >
                  <div className="flex items-center justify-between gap-4">
                    <span className="text-sm font-semibold text-slate-900">{route.label}</span>
                    <span className="rounded-full bg-slate-100 px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-slate-600">
                      {route.method}
                    </span>
                  </div>
                  <p className="mt-2 text-xs font-mono text-indigo-700 group-hover:text-indigo-900">{route.path}</p>
                  <p className="mt-2 text-sm leading-6 text-slate-600">{route.note}</p>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {route.path === "/api/scan?debug=1" ? (
                      <button
                        type="button"
                        onClick={runDebugCheck}
                        disabled={isRunningDebug}
                        className="rounded-lg bg-indigo-600 px-3 py-2 text-xs font-semibold text-white transition hover:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-65"
                      >
                        {isRunningDebug ? "Running..." : "Run Debug Check"}
                      </button>
                    ) : null}

                    {route.path === "/api/scan/backfill" ? (
                      <button
                        type="button"
                        onClick={runBackfill}
                        disabled={isRunningBackfill}
                        className="rounded-lg bg-violet-600 px-3 py-2 text-xs font-semibold text-white transition hover:bg-violet-700 disabled:cursor-not-allowed disabled:opacity-65"
                      >
                        {isRunningBackfill ? "Running..." : "Run Backfill"}
                      </button>
                    ) : null}

                    {route.path === "/api/scan?verbose=1" ? (
                      <button
                        type="button"
                        onClick={runVerboseScan}
                        disabled={isRunningVerboseScan}
                        className="rounded-lg bg-sky-600 px-3 py-2 text-xs font-semibold text-white transition hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-65"
                      >
                        {isRunningVerboseScan ? "Running..." : "Run Verbose Scan"}
                      </button>
                    ) : null}

                    <a
                      href={route.path}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="rounded-lg border border-slate-300 px-3 py-2 text-xs font-semibold text-slate-700 transition hover:bg-slate-100"
                    >
                      Open Route
                    </a>
                  </div>
                </div>
              ))}
            </div>

            <div className="mt-5 rounded-2xl border border-violet-200 bg-violet-50/80 p-4">
              <h3 className="text-sm font-semibold text-violet-900">Backfill Controls</h3>
              <p className="mt-1 text-xs leading-5 text-violet-800">Configure the backfill run, then click <span className="font-semibold">Run Backfill</span> above.</p>
              <div className="mt-3 grid gap-3 sm:grid-cols-3">
                <label className="text-xs font-medium text-violet-900" htmlFor="backfill-max-pages">
                  Max pages
                  <input
                    id="backfill-max-pages"
                    type="number"
                    min={1}
                    max={200}
                    value={backfillMaxPages}
                    onChange={(e) => setBackfillMaxPages(Number(e.target.value || 1))}
                    className="mt-1 w-full rounded-lg border border-violet-300 bg-white px-3 py-2 text-sm"
                  />
                </label>

                <label className="flex items-center gap-2 rounded-lg border border-violet-200 bg-white px-3 py-2 text-xs font-medium text-violet-900">
                  <input
                    type="checkbox"
                    checked={backfillDryRun}
                    onChange={(e) => setBackfillDryRun(e.target.checked)}
                    className="h-4 w-4"
                  />
                  Dry run only
                </label>

                <label className="flex items-center gap-2 rounded-lg border border-violet-200 bg-white px-3 py-2 text-xs font-medium text-violet-900">
                  <input
                    type="checkbox"
                    checked={backfillOnlyMissing}
                    onChange={(e) => setBackfillOnlyMissing(e.target.checked)}
                    className="h-4 w-4"
                  />
                  Fill missing only
                </label>
              </div>
            </div>
          </article>

          <article className="rounded-3xl border border-slate-200/80 bg-white/85 p-6 shadow-[0_12px_40px_rgba(26,23,43,0.08)] backdrop-blur">
            <h2 className="text-lg font-semibold text-slate-900">Manual ISBN Import</h2>
            <p className="mt-2 text-sm text-slate-600">Submit an ISBN manually to trigger the same import endpoint used by your shortcut.</p>

            <form className="mt-5 space-y-4" onSubmit={handleSubmit}>
              <label className="block text-sm font-medium text-slate-700" htmlFor="isbn">
                ISBN
              </label>
              <input
                id="isbn"
                type="text"
                value={isbn}
                onChange={(e) => setIsbn(e.target.value)}
                placeholder="9780143127741"
                required
                className="w-full rounded-xl border border-slate-300 bg-white px-4 py-3 text-sm shadow-inner shadow-slate-100 outline-none ring-indigo-200 transition focus:border-indigo-500 focus:ring"
              />

              <div className="grid gap-4 sm:grid-cols-2">
                <label className="text-sm font-medium text-slate-700" htmlFor="duplicate">
                  Duplicate behavior
                  <select
                    id="duplicate"
                    value={onDuplicate}
                    onChange={(e) => setOnDuplicate(e.target.value as "update" | "skip")}
                    className="mt-1 block w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
                  >
                    <option value="update">Update existing</option>
                    <option value="skip">Skip existing</option>
                  </select>
                </label>

                <label className="flex items-end gap-2 text-sm font-medium text-slate-700">
                  <input
                    type="checkbox"
                    checked={verbose}
                    onChange={(e) => setVerbose(e.target.checked)}
                    className="h-4 w-4 rounded border-slate-300 text-indigo-600"
                  />
                  Include verbose response
                </label>
              </div>

              <button
                type="submit"
                disabled={isSubmitting}
                className="inline-flex w-full items-center justify-center rounded-xl bg-gradient-to-r from-indigo-600 to-blue-600 px-4 py-3 text-sm font-semibold text-white shadow-[0_10px_24px_rgba(65,86,220,0.28)] transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-65"
              >
                {isSubmitting ? "Importing..." : "Import Book"}
              </button>
            </form>

            <div
              className={`mt-5 rounded-xl border px-4 py-3 text-sm ${
                status === "success"
                  ? "border-emerald-200 bg-emerald-50 text-emerald-900"
                  : status === "error"
                    ? "border-rose-200 bg-rose-50 text-rose-900"
                    : "border-slate-200 bg-slate-50 text-slate-700"
              }`}
            >
              <p className="font-medium">{summary}</p>
              {response?.requestId ? <p className="mt-1 text-xs opacity-85">Request ID: {String(response.requestId)}</p> : null}
            </div>
          </article>
        </section>

        <section className="rounded-3xl border border-slate-200/80 bg-white/85 p-6 shadow-[0_12px_40px_rgba(26,23,43,0.08)] backdrop-blur">
          <div className="flex items-center justify-between gap-4">
            <h2 className="text-lg font-semibold text-slate-900">Last API Response</h2>
            <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-600">
              JSON Preview
            </span>
          </div>
          <pre className="mt-4 overflow-x-auto rounded-2xl bg-slate-950 p-4 text-xs leading-6 text-slate-100">
            {JSON.stringify(response ?? { message: "No request yet." }, null, 2)}
          </pre>
        </section>
      </main>
    </div>
  );
}
