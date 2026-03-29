"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";

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

type DashboardTab = "ingest" | "manual" | "maintenance" | "diagnostics" | "routes";

type TabItem = {
  id: DashboardTab;
  label: string;
  subtitle: string;
};

const TABS: TabItem[] = [
  { id: "ingest", label: "Ingest", subtitle: "ISBN scan + duplicate checks" },
  { id: "manual", label: "Manual Entry", subtitle: "Rare books without ISBN" },
  { id: "maintenance", label: "Maintenance", subtitle: "Backfill + gap reports" },
  { id: "diagnostics", label: "Diagnostics", subtitle: "Debug + response health" },
  { id: "routes", label: "API Explorer", subtitle: "Reference endpoints" },
];

function parseTabParam(value: string | null): DashboardTab {
  if (value === "ingest" || value === "manual" || value === "maintenance" || value === "diagnostics" || value === "routes") {
    return value;
  }
  return "ingest";
}

const ROUTE_LINKS: RouteLink[] = [
  {
    label: "Scan ISBN",
    method: "POST",
    path: "/api/scan",
    note: "Primary import endpoint for Apple Shortcuts and manual form submission.",
  },
  {
    label: "Check ISBN Exists",
    method: "POST",
    path: "/api/scan?mode=check&verbose=1",
    note: "Check-only scan for Apple Shortcuts: verifies if ISBN already exists without writing.",
  },
  {
    label: "Validate ISBN",
    method: "POST",
    path: "/api/scan?mode=validate&verbose=1",
    note: "Verifies ISBN checksum + metadata match and reports if it already exists in your library.",
  },
  {
    label: "Compact Shortcut Scan",
    method: "POST",
    path: "/api/scan?compact=1",
    note: "Fast response payload optimized for Apple Shortcuts variable extraction.",
  },
  {
    label: "Dry Run Scan",
    method: "POST",
    path: "/api/scan?dryRun=1&verbose=1",
    note: "Preview the write payload without mutating Notion records.",
  },
  {
    label: "Async Enrichment Scan",
    method: "POST",
    path: "/api/scan?enrichAsync=1&verbose=1",
    note: "Returns quickly, then performs best-effort metadata enrichment in background.",
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
    label: "Missing Books By Author",
    method: "GET",
    path: "/api/authors/missing?author=Ursula%20K.%20Le%20Guin",
    note: "Compares your Notion titles with Open Library works for an author.",
  },
  {
    label: "Sync Missing To Notion",
    method: "GET",
    path: "/api/authors/missing?author=Ursula%20K.%20Le%20Guin&sync=1",
    note: "Optionally writes missing works into a dedicated Missing Books Notion data source.",
  },
  {
    label: "Verbose Scan Example",
    method: "POST",
    path: "/api/scan?verbose=1",
    note: "Returns enriched response payload including data/details for debugging.",
  },
];

function getSummary(response: ApiPayload | null) {
  if (!response) return "Run any module to preview API output.";
  const message = typeof response.message === "string" ? response.message : "Request completed.";
  const code = typeof response.code === "string" ? response.code : "NO_CODE";
  return `${code}: ${message}`;
}

function getStatusTone(status: "idle" | "success" | "error") {
  if (status === "success") return "border-emerald-200 bg-emerald-50 text-emerald-900";
  if (status === "error") return "border-rose-200 bg-rose-50 text-rose-900";
  return "border-slate-200 bg-slate-50 text-slate-700";
}

export default function Home() {
  const [activeTab, setActiveTab] = useState<DashboardTab>("ingest");
  const [isbn, setIsbn] = useState("");
  const [onDuplicate, setOnDuplicate] = useState<"update" | "skip">("update");
  const [verbose, setVerbose] = useState(true);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isRunningDebug, setIsRunningDebug] = useState(false);
  const [isRunningCheckOnly, setIsRunningCheckOnly] = useState(false);
  const [isRunningValidateOnly, setIsRunningValidateOnly] = useState(false);
  const [isRunningBackfill, setIsRunningBackfill] = useState(false);
  const [isRunningVerboseScan, setIsRunningVerboseScan] = useState(false);
  const [isRunningMissingByAuthor, setIsRunningMissingByAuthor] = useState(false);
  const [backfillDryRun, setBackfillDryRun] = useState(true);
  const [backfillOnlyMissing, setBackfillOnlyMissing] = useState(true);
  const [backfillMaxPages, setBackfillMaxPages] = useState(50);
  const [missingAuthor, setMissingAuthor] = useState("");
  const [missingMax, setMissingMax] = useState(75);
  const [manualTitle, setManualTitle] = useState("");
  const [manualAuthors, setManualAuthors] = useState("");
  const [manualEdition, setManualEdition] = useState("");
  const [manualPublisher, setManualPublisher] = useState("");
  const [manualPublishedDate, setManualPublishedDate] = useState("");
  const [manualPageCount, setManualPageCount] = useState("");
  const [manualCategories, setManualCategories] = useState("");
  const [manualSourceUrl, setManualSourceUrl] = useState("");
  const [manualDescription, setManualDescription] = useState("");
  const [isSubmittingManual, setIsSubmittingManual] = useState(false);
  const [isCheckingManualTitle, setIsCheckingManualTitle] = useState(false);
  const [manualTitleExists, setManualTitleExists] = useState<boolean | null>(null);
  const [manualTitleCheckMessage, setManualTitleCheckMessage] = useState("");
  const [lastCheckedManualTitle, setLastCheckedManualTitle] = useState("");
  const [status, setStatus] = useState<"idle" | "success" | "error">("idle");
  const [response, setResponse] = useState<ApiPayload | null>(null);

  const summary = useMemo(() => getSummary(response), [response]);
  const statusTone = getStatusTone(status);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const search = new URLSearchParams(window.location.search);
    setActiveTab(parseTabParam(search.get("tab")));
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const url = new URL(window.location.href);
    url.searchParams.set("tab", activeTab);
    window.history.replaceState({}, "", `${url.pathname}?${url.searchParams.toString()}`);
  }, [activeTab]);

  async function runIsbnImport() {
    if (!isbn.trim()) {
      setResponse({
        ok: false,
        code: "MISSING_ISBN",
        message: "Enter an ISBN before importing.",
      });
      setStatus("error");
      return;
    }

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

  async function runCustomScan(path: string) {
    if (!isbn.trim()) {
      setResponse({
        ok: false,
        code: "MISSING_ISBN",
        message: "Enter an ISBN before running this scan route.",
      });
      setStatus("error");
      return;
    }

    setStatus("idle");
    try {
      const res = await fetch(path, {
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
        message: "Could not run the selected scan route.",
      });
      setStatus("error");
    }
  }

  async function runManualTitlePrecheck() {
    const title = manualTitle.trim();
    if (!title) {
      setManualTitleExists(null);
      setManualTitleCheckMessage("");
      setLastCheckedManualTitle("");
      return;
    }

    setIsCheckingManualTitle(true);
    try {
      const res = await fetch("/api/scan?dryRun=1&verbose=1", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          mode: "manual",
          manual: true,
          title,
          onDuplicate,
        }),
      });

      const payload = (await res.json()) as ApiPayload;
      const data = (payload.data ?? null) as { duplicated?: boolean } | null;
      const duplicate = payload.code === "BOOK_DRY_RUN_UPDATE" || data?.duplicated === true;

      setManualTitleExists(duplicate);
      setLastCheckedManualTitle(title);
      setManualTitleCheckMessage(
        duplicate
          ? "Possible duplicate title detected. This submission may update an existing entry."
          : "No existing title match detected."
      );
    } catch {
      setManualTitleExists(null);
      setManualTitleCheckMessage("Unable to run title pre-check right now.");
      setLastCheckedManualTitle(title);
    } finally {
      setIsCheckingManualTitle(false);
    }
  }

  function isRouteBusy(path: string) {
    if (path === "/api/scan") return isSubmitting;
    if (path === "/api/scan?mode=check&verbose=1") return isRunningCheckOnly;
    if (path === "/api/scan?mode=validate&verbose=1") return isRunningValidateOnly;
    if (path === "/api/scan?verbose=1") return isRunningVerboseScan;
    if (path === "/api/scan?debug=1") return isRunningDebug;
    if (path === "/api/scan/backfill") return isRunningBackfill;
    if (path.startsWith("/api/authors/missing")) return isRunningMissingByAuthor;
    return false;
  }

  async function runRouteAction(route: RouteLink) {
    if (route.path === "/api/scan") {
      await runIsbnImport();
      return;
    }
    if (route.path === "/api/scan?mode=check&verbose=1") {
      await runCheckOnlyScan();
      return;
    }
    if (route.path === "/api/scan?mode=validate&verbose=1") {
      await runValidateOnlyScan();
      return;
    }
    if (route.path === "/api/scan?verbose=1") {
      await runVerboseScan();
      return;
    }
    if (route.path === "/api/scan?debug=1") {
      await runDebugCheck();
      return;
    }
    if (route.path === "/api/scan/backfill") {
      await runBackfill();
      return;
    }
    if (route.path === "/api/authors/missing?author=Ursula%20K.%20Le%20Guin") {
      await runMissingByAuthor();
      return;
    }

    if (route.method === "POST") {
      await runCustomScan(route.path);
      return;
    }

    setStatus("idle");
    try {
      const res = await fetch(route.path);
      const payload = (await res.json()) as ApiPayload;
      setResponse(payload);
      setStatus(payload.ok ? "success" : "error");
    } catch {
      setResponse({
        ok: false,
        code: "NETWORK_ERROR",
        message: "Could not run the selected route.",
      });
      setStatus("error");
    }
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await runIsbnImport();
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

  async function runCheckOnlyScan() {
    if (!isbn.trim()) {
      setResponse({
        ok: false,
        code: "MISSING_ISBN",
        message: "Enter an ISBN before running check-only scan.",
      });
      setStatus("error");
      return;
    }

    setIsRunningCheckOnly(true);
    setStatus("idle");

    try {
      const res = await fetch("/api/scan?mode=check&verbose=1", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ isbn }),
      });

      const payload = (await res.json()) as ApiPayload;
      setResponse(payload);
      setStatus(payload.ok ? "success" : "error");
    } catch {
      setResponse({
        ok: false,
        code: "NETWORK_ERROR",
        message: "Could not run check-only scan.",
      });
      setStatus("error");
    } finally {
      setIsRunningCheckOnly(false);
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

  async function runValidateOnlyScan() {
    if (!isbn.trim()) {
      setResponse({
        ok: false,
        code: "MISSING_ISBN",
        message: "Enter an ISBN before validating.",
      });
      setStatus("error");
      return;
    }

    setIsRunningValidateOnly(true);
    setStatus("idle");

    try {
      const res = await fetch("/api/scan?mode=validate&verbose=1", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ isbn }),
      });

      const payload = (await res.json()) as ApiPayload;
      setResponse(payload);
      setStatus(payload.ok ? "success" : "error");
    } catch {
      setResponse({
        ok: false,
        code: "NETWORK_ERROR",
        message: "Could not run ISBN validation.",
      });
      setStatus("error");
    } finally {
      setIsRunningValidateOnly(false);
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

  async function runMissingByAuthor() {
    const author = missingAuthor.trim();
    if (!author) {
      setResponse({
        ok: false,
        code: "MISSING_AUTHOR",
        message: "Enter an author name before running missing-books report.",
      });
      setStatus("error");
      return;
    }

    setIsRunningMissingByAuthor(true);
    setStatus("idle");

    try {
      const query = new URLSearchParams({
        author,
        maxMissing: String(Math.max(10, missingMax)),
      });
      const res = await fetch(`/api/authors/missing?${query.toString()}`);
      const payload = (await res.json()) as ApiPayload;
      setResponse(payload);
      setStatus(payload.ok ? "success" : "error");
    } catch {
      setResponse({
        ok: false,
        code: "NETWORK_ERROR",
        message: "Could not run missing-books-by-author report.",
      });
      setStatus("error");
    } finally {
      setIsRunningMissingByAuthor(false);
    }
  }

  async function handleManualSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();

    if (manualTitle.trim() && lastCheckedManualTitle !== manualTitle.trim()) {
      await runManualTitlePrecheck();
    }

    setIsSubmittingManual(true);
    setStatus("idle");

    try {
      const body = {
        mode: "manual",
        manual: true,
        onDuplicate,
        title: manualTitle,
        authors: manualAuthors,
        edition: manualEdition,
        publisher: manualPublisher,
        publishedDate: manualPublishedDate,
        pageCount: manualPageCount,
        categories: manualCategories,
        sourceUrl: manualSourceUrl,
        description: manualDescription,
      };

      const query = verbose ? "?verbose=1" : "";
      const res = await fetch(`/api/scan${query}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      const payload = (await res.json()) as ApiPayload;
      setResponse(payload);
      setStatus(payload.ok ? "success" : "error");
    } catch {
      setResponse({
        ok: false,
        code: "NETWORK_ERROR",
        message: "Could not reach /api/scan for manual entry.",
        suggestion: "Confirm the dev server is running and retry.",
      });
      setStatus("error");
    } finally {
      setIsSubmittingManual(false);
    }
  }

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_2%_5%,#e8f3ff_0,#f3f8ff_25%,#fff8ec_58%,#fff 100%)] text-slate-900">
      <main className="mx-auto flex w-full max-w-7xl flex-col gap-6 px-5 py-8 md:px-9 md:py-12">
        <section className="relative overflow-hidden rounded-3xl border border-white/70 bg-white/80 p-7 shadow-[0_26px_65px_rgba(25,35,70,0.14)] backdrop-blur-md md:p-10">
          <div className="pointer-events-none absolute -right-16 -top-16 h-52 w-52 rounded-full bg-linear-to-br from-cyan-300/45 to-sky-500/25 blur-3xl" />
          <div className="pointer-events-none absolute -bottom-16 left-6 h-52 w-52 rounded-full bg-linear-to-br from-amber-300/45 to-rose-300/25 blur-3xl" />
          <p className="relative text-xs font-semibold uppercase tracking-[0.2em] text-blue-700">Notion Library Operations Dashboard</p>
          <h1 className="relative mt-3 text-3xl font-bold tracking-tight text-slate-950 md:text-5xl">
            Professional control center for ingest, manual curation, and metadata maintenance.
          </h1>
          <p className="relative mt-4 max-w-4xl text-sm leading-7 text-slate-700 md:text-base">
            Organize the workflow by module: scan ISBNs, enter rare books manually, run backfills, and inspect API behavior.
            Every action writes into the same core endpoints so your automations and dashboard stay aligned.
          </p>
        </section>

        <section className="grid gap-4 rounded-2xl border border-slate-200/80 bg-white/85 p-4 shadow-[0_10px_34px_rgba(25,28,46,0.08)] md:grid-cols-4">
          <article className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Last Status</p>
            <p className="mt-1 text-sm font-semibold text-slate-900">{status.toUpperCase()}</p>
          </article>
          <article className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Last Code</p>
            <p className="mt-1 truncate text-sm font-semibold text-slate-900">{typeof response?.code === "string" ? response.code : "NO_CODE"}</p>
          </article>
          <article className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Duration</p>
            <p className="mt-1 text-sm font-semibold text-slate-900">{typeof response?.durationMs === "number" ? `${response.durationMs} ms` : "-"}</p>
          </article>
          <article className="rounded-xl border border-slate-200 bg-slate-50 p-3">
            <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Request ID</p>
            <p className="mt-1 truncate text-sm font-semibold text-slate-900">{typeof response?.requestId === "string" ? response.requestId : "-"}</p>
          </article>
        </section>

        <section className="grid gap-6 xl:grid-cols-[1.25fr_1fr]">
          <article className="rounded-3xl border border-slate-200/80 bg-white/90 p-5 shadow-[0_14px_45px_rgba(29,31,55,0.1)] backdrop-blur md:p-6">
            <nav className="grid gap-2 sm:grid-cols-2 lg:grid-cols-5">
              {TABS.map((tab) => {
                const active = activeTab === tab.id;
                return (
                  <button
                    key={tab.id}
                    type="button"
                    onClick={() => setActiveTab(tab.id)}
                    className={`rounded-xl border px-3 py-3 text-left transition ${
                      active
                        ? "border-blue-300 bg-blue-50 shadow-[0_8px_18px_rgba(37,88,210,0.16)]"
                        : "border-slate-200 bg-white hover:border-blue-200 hover:bg-blue-50/45"
                    }`}
                  >
                    <p className={`text-sm font-semibold ${active ? "text-blue-900" : "text-slate-900"}`}>{tab.label}</p>
                    <p className={`mt-0.5 text-[11px] ${active ? "text-blue-700" : "text-slate-500"}`}>{tab.subtitle}</p>
                  </button>
                );
              })}
            </nav>

            {activeTab === "ingest" ? (
              <section className="mt-6 space-y-6">
                <div>
                  <h2 className="text-xl font-semibold text-slate-900">ISBN Ingest Module</h2>
                  <p className="mt-1 text-sm text-slate-600">Run normal scans, check duplicates, and trigger verbose payload inspection.</p>
                </div>

                <form className="space-y-4" onSubmit={handleSubmit}>
                  <label className="block text-sm font-medium text-slate-700" htmlFor="isbn">
                    ISBN
                    <input
                      id="isbn"
                      type="text"
                      value={isbn}
                      onChange={(e) => setIsbn(e.target.value)}
                      placeholder="9780143127741"
                      required
                      className="mt-1 w-full rounded-xl border border-slate-300 bg-white px-4 py-3 text-sm outline-none ring-blue-200 transition focus:border-blue-500 focus:ring"
                    />
                  </label>

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

                    <label className="flex items-end gap-2 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm font-medium text-slate-700">
                      <input
                        type="checkbox"
                        checked={verbose}
                        onChange={(e) => setVerbose(e.target.checked)}
                        className="h-4 w-4"
                      />
                      Include verbose response
                    </label>
                  </div>

                  <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                    <button
                      type="submit"
                      disabled={isSubmitting}
                      className="inline-flex items-center justify-center rounded-xl bg-linear-to-r from-blue-600 to-indigo-600 px-4 py-3 text-sm font-semibold text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-65"
                    >
                      {isSubmitting ? "Importing..." : "Import ISBN"}
                    </button>

                    <button
                      type="button"
                      onClick={runCheckOnlyScan}
                      disabled={isRunningCheckOnly}
                      className="inline-flex items-center justify-center rounded-xl bg-linear-to-r from-emerald-600 to-teal-600 px-4 py-3 text-sm font-semibold text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-65"
                    >
                      {isRunningCheckOnly ? "Checking..." : "Check ISBN"}
                    </button>

                    <button
                      type="button"
                      onClick={runValidateOnlyScan}
                      disabled={isRunningValidateOnly}
                      className="inline-flex items-center justify-center rounded-xl bg-linear-to-r from-amber-500 to-orange-600 px-4 py-3 text-sm font-semibold text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-65"
                    >
                      {isRunningValidateOnly ? "Validating..." : "Validate ISBN"}
                    </button>

                    <button
                      type="button"
                      onClick={runVerboseScan}
                      disabled={isRunningVerboseScan}
                      className="inline-flex items-center justify-center rounded-xl bg-linear-to-r from-sky-600 to-cyan-600 px-4 py-3 text-sm font-semibold text-white transition hover:brightness-110 disabled:cursor-not-allowed disabled:opacity-65"
                    >
                      {isRunningVerboseScan ? "Running..." : "Verbose Scan"}
                    </button>
                  </div>
                </form>
              </section>
            ) : null}

            {activeTab === "manual" ? (
              <section className="mt-6 space-y-5">
                <div>
                  <h2 className="text-xl font-semibold text-slate-900">Manual Curation Module</h2>
                  <p className="mt-1 text-sm text-slate-600">Create entries for rare or older books without ISBNs. Edition, categories, and authors sync through API logic.</p>
                </div>

                <form className="space-y-3" onSubmit={handleManualSubmit}>
                  <label className="block text-sm font-medium text-slate-700" htmlFor="manual-title">
                    Title
                    <input
                      id="manual-title"
                      type="text"
                      value={manualTitle}
                        onChange={(e) => {
                          const next = e.target.value;
                          setManualTitle(next);
                          if (!next.trim()) {
                            setManualTitleExists(null);
                            setManualTitleCheckMessage("");
                            setLastCheckedManualTitle("");
                          }
                        }}
                        onBlur={runManualTitlePrecheck}
                      placeholder="The Odyssey"
                      required
                      className="mt-1 w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
                    />
                  </label>

                    <div
                      className={`rounded-lg border px-3 py-2 text-xs ${
                        manualTitleExists === true
                          ? "border-amber-300 bg-amber-50 text-amber-900"
                          : manualTitleExists === false
                            ? "border-emerald-300 bg-emerald-50 text-emerald-900"
                            : "border-slate-200 bg-slate-50 text-slate-600"
                      }`}
                    >
                      <p className="font-medium">
                        {isCheckingManualTitle
                          ? "Checking title for duplicates..."
                          : manualTitleCheckMessage || "Title pre-check runs automatically when you leave the title field."}
                      </p>
                    </div>

                  <div className="grid gap-3 sm:grid-cols-2">
                    <label className="text-sm font-medium text-slate-700" htmlFor="manual-authors">
                      Authors (comma-separated)
                      <input
                        id="manual-authors"
                        type="text"
                        value={manualAuthors}
                        onChange={(e) => setManualAuthors(e.target.value)}
                        placeholder="Homer"
                        className="mt-1 w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
                      />
                    </label>

                    <label className="text-sm font-medium text-slate-700" htmlFor="manual-edition">
                      Edition
                      <input
                        id="manual-edition"
                        type="text"
                        value={manualEdition}
                        onChange={(e) => setManualEdition(e.target.value)}
                        placeholder="First Edition"
                        className="mt-1 w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
                      />
                    </label>
                  </div>

                  <div className="grid gap-3 sm:grid-cols-2">
                    <label className="text-sm font-medium text-slate-700" htmlFor="manual-publisher">
                      Publisher
                      <input
                        id="manual-publisher"
                        type="text"
                        value={manualPublisher}
                        onChange={(e) => setManualPublisher(e.target.value)}
                        placeholder="Penguin Classics"
                        className="mt-1 w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
                      />
                    </label>

                    <label className="text-sm font-medium text-slate-700" htmlFor="manual-published-date">
                      Published Date
                      <input
                        id="manual-published-date"
                        type="text"
                        value={manualPublishedDate}
                        onChange={(e) => setManualPublishedDate(e.target.value)}
                        placeholder="1923-01-01 or 1923"
                        className="mt-1 w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
                      />
                    </label>
                  </div>

                  <div className="grid gap-3 sm:grid-cols-2">
                    <label className="text-sm font-medium text-slate-700" htmlFor="manual-page-count">
                      Page Count
                      <input
                        id="manual-page-count"
                        type="number"
                        min={1}
                        value={manualPageCount}
                        onChange={(e) => setManualPageCount(e.target.value)}
                        placeholder="412"
                        className="mt-1 w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
                      />
                    </label>

                    <label className="text-sm font-medium text-slate-700" htmlFor="manual-categories">
                      Categories (comma-separated)
                      <input
                        id="manual-categories"
                        type="text"
                        value={manualCategories}
                        onChange={(e) => setManualCategories(e.target.value)}
                        placeholder="Classics, Epic Poetry"
                        className="mt-1 w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
                      />
                    </label>
                  </div>

                  <label className="block text-sm font-medium text-slate-700" htmlFor="manual-source-url">
                    Source URL
                    <input
                      id="manual-source-url"
                      type="url"
                      value={manualSourceUrl}
                      onChange={(e) => setManualSourceUrl(e.target.value)}
                      placeholder="https://example.com/catalog-entry"
                      className="mt-1 w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
                    />
                  </label>

                  <label className="block text-sm font-medium text-slate-700" htmlFor="manual-description">
                    Notes / Description
                    <textarea
                      id="manual-description"
                      value={manualDescription}
                      onChange={(e) => setManualDescription(e.target.value)}
                      rows={4}
                      placeholder="Rare binding; inherited collection copy."
                      className="mt-1 w-full rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm"
                    />
                  </label>

                  <button
                    type="submit"
                    disabled={isSubmittingManual}
                    className="inline-flex w-full items-center justify-center rounded-xl bg-teal-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-teal-700 disabled:cursor-not-allowed disabled:opacity-65"
                  >
                    {isSubmittingManual ? "Saving..." : "Save Manual Entry"}
                  </button>
                </form>
              </section>
            ) : null}

            {activeTab === "maintenance" ? (
              <section className="mt-6 space-y-6">
                <div>
                  <h2 className="text-xl font-semibold text-slate-900">Maintenance Module</h2>
                  <p className="mt-1 text-sm text-slate-600">Bulk maintenance tools for metadata quality and author gap analysis.</p>
                </div>

                <div className="rounded-2xl border border-violet-200 bg-violet-50/70 p-4">
                  <h3 className="text-sm font-semibold text-violet-900">Metadata Backfill</h3>
                  <div className="mt-3 grid gap-3 md:grid-cols-3">
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
                  <button
                    type="button"
                    onClick={runBackfill}
                    disabled={isRunningBackfill}
                    className="mt-4 inline-flex w-full items-center justify-center rounded-lg bg-violet-600 px-3 py-2 text-sm font-semibold text-white transition hover:bg-violet-700 disabled:cursor-not-allowed disabled:opacity-65"
                  >
                    {isRunningBackfill ? "Running..." : "Run Backfill"}
                  </button>
                </div>

                <div className="rounded-2xl border border-amber-200 bg-amber-50/70 p-4">
                  <h3 className="text-sm font-semibold text-amber-900">Missing Books By Author</h3>
                  <div className="mt-3 grid gap-3 sm:grid-cols-[1fr_auto]">
                    <label className="text-xs font-medium text-amber-900" htmlFor="missing-author">
                      Author name
                      <input
                        id="missing-author"
                        type="text"
                        value={missingAuthor}
                        onChange={(e) => setMissingAuthor(e.target.value)}
                        placeholder="Ursula K. Le Guin"
                        className="mt-1 w-full rounded-lg border border-amber-300 bg-white px-3 py-2 text-sm"
                      />
                    </label>

                    <label className="text-xs font-medium text-amber-900" htmlFor="missing-max">
                      Max missing
                      <input
                        id="missing-max"
                        type="number"
                        min={10}
                        max={500}
                        value={missingMax}
                        onChange={(e) => setMissingMax(Number(e.target.value || 10))}
                        className="mt-1 w-full rounded-lg border border-amber-300 bg-white px-3 py-2 text-sm"
                      />
                    </label>
                  </div>
                  <button
                    type="button"
                    onClick={runMissingByAuthor}
                    disabled={isRunningMissingByAuthor}
                    className="mt-3 inline-flex w-full items-center justify-center rounded-lg bg-amber-600 px-3 py-2 text-sm font-semibold text-white transition hover:bg-amber-700 disabled:cursor-not-allowed disabled:opacity-65"
                  >
                    {isRunningMissingByAuthor ? "Running..." : "Run Missing Report"}
                  </button>
                </div>
              </section>
            ) : null}

            {activeTab === "diagnostics" ? (
              <section className="mt-6 space-y-5">
                <div>
                  <h2 className="text-xl font-semibold text-slate-900">Diagnostics Module</h2>
                  <p className="mt-1 text-sm text-slate-600">Inspect schema mapping, request metadata, and workflow health.</p>
                </div>

                <div className={`rounded-xl border px-4 py-3 text-sm ${statusTone}`}>
                  <p className="font-semibold">{summary}</p>
                  {typeof response?.requestId === "string" ? <p className="mt-1 text-xs opacity-85">Request ID: {response.requestId}</p> : null}
                </div>

                <div className="grid gap-3 sm:grid-cols-2">
                  <button
                    type="button"
                    onClick={runDebugCheck}
                    disabled={isRunningDebug}
                    className="inline-flex items-center justify-center rounded-xl bg-indigo-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-indigo-700 disabled:cursor-not-allowed disabled:opacity-65"
                  >
                    {isRunningDebug ? "Running..." : "Run Schema Debug"}
                  </button>

                  <button
                    type="button"
                    onClick={runVerboseScan}
                    disabled={isRunningVerboseScan}
                    className="inline-flex items-center justify-center rounded-xl bg-sky-600 px-4 py-3 text-sm font-semibold text-white transition hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-65"
                  >
                    {isRunningVerboseScan ? "Running..." : "Run Verbose Scan"}
                  </button>
                </div>
              </section>
            ) : null}

            {activeTab === "routes" ? (
              <section className="mt-6 space-y-5">
                <div>
                  <h2 className="text-xl font-semibold text-slate-900">API Explorer</h2>
                  <p className="mt-1 text-sm text-slate-600">Reference and test the routes that power the dashboard and shortcuts.</p>
                </div>

                <div className="grid gap-3">
                  {ROUTE_LINKS.map((route) => (
                    <article
                      key={route.path}
                      className="rounded-xl border border-slate-200 bg-white p-4 transition hover:-translate-y-0.5 hover:border-blue-300 hover:shadow-[0_10px_26px_rgba(53,90,170,0.14)]"
                    >
                      <div className="flex items-center justify-between gap-4">
                        <p className="text-sm font-semibold text-slate-900">{route.label}</p>
                        <span className="rounded-full bg-slate-100 px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-slate-600">
                          {route.method}
                        </span>
                      </div>
                      <p className="mt-2 break-all text-xs font-mono text-blue-700">{route.path}</p>
                      <p className="mt-2 text-sm text-slate-600">{route.note}</p>
                      <div className="mt-3 flex flex-wrap gap-2">
                        <button
                          type="button"
                          onClick={() => runRouteAction(route)}
                          disabled={isRouteBusy(route.path)}
                          className="rounded-lg bg-blue-600 px-3 py-2 text-xs font-semibold text-white transition hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-65"
                        >
                          {isRouteBusy(route.path) ? "Running..." : "Run"}
                        </button>
                        <a
                          href={route.path}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="rounded-lg border border-slate-300 px-3 py-2 text-xs font-semibold text-slate-700 transition hover:bg-slate-100"
                        >
                          Open Route
                        </a>
                      </div>
                    </article>
                  ))}
                </div>
              </section>
            ) : null}
          </article>

          <aside className="rounded-3xl border border-slate-200/80 bg-white/90 p-5 shadow-[0_14px_45px_rgba(29,31,55,0.1)] backdrop-blur md:p-6">
            <div className="flex items-center justify-between gap-4">
              <h2 className="text-lg font-semibold text-slate-900">Response Console</h2>
              <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-600">Live JSON</span>
            </div>

            <div className={`mt-4 rounded-xl border px-4 py-3 text-sm ${statusTone}`}>
              <p className="font-medium">{summary}</p>
              {typeof response?.requestId === "string" ? <p className="mt-1 text-xs opacity-85">Request ID: {response.requestId}</p> : null}
            </div>

            <pre className="mt-4 max-h-170 overflow-auto rounded-2xl bg-slate-950 p-4 text-xs leading-6 text-slate-100">
              {JSON.stringify(response ?? { message: "No request yet." }, null, 2)}
            </pre>
          </aside>
        </section>
      </main>
    </div>
  );
}
