/**
 * Simplified @motherduck/react-sql-query shim for local Vite preview.
 * Same API as the production dive runtime — useSQLQuery, useConnection,
 * useConnectionStatus, and MotherDuckSDKProvider.
 */
import { MDConnection } from "@motherduck/wasm-client";
import type { DuckDBRow } from "@motherduck/wasm-client";
import {
  createContext, useContext, useEffect, useMemo, useRef, useState,
  useCallback, useSyncExternalStore,
} from "react";
import type { ReactNode } from "react";

// ── Types ──────────────────────────────────────────────────────────

type QueryStatus = "idle" | "loading" | "success" | "error";

type ExportFormat = "csv" | "json" | "parquet" | "xlsx";
type ExportCompression = "none" | "gzip" | "zstd";

export type ExportQueryOptions = {
  format: ExportFormat;
  title?: string;
  filename?: string;
  sheetName?: string;
  copy?: { preserveOrder?: boolean };
  csv?: {
    compression?: ExportCompression;
    dateformat?: string;
    delim?: string;
    escape?: string;
    forceQuote?: readonly string[] | "*";
    header?: boolean;
    nullstr?: string;
    prefix?: string;
    quote?: string;
    suffix?: string;
    timestampformat?: string;
  };
  json?: {
    array?: boolean;
    compression?: ExportCompression;
    dateformat?: string;
    timestampformat?: string;
  };
  parquet?: {
    compression?:
      | "uncompressed"
      | "snappy"
      | "gzip"
      | "zstd"
      | "brotli"
      | "lz4"
      | "lz4_raw";
    compressionLevel?: number;
    fieldIds?: "auto" | Record<string, number | Record<string, number>>;
    kvMetadata?: Record<string, string>;
    parquetVersion?: "v1" | "v2" | "V1" | "V2";
    rowGroupSize?: number;
    rowGroupSizeBytes?: number | string;
    stringDictionaryPageSizeLimit?: number | string;
  };
  xlsx?: {
    header?: boolean;
    sheet?: string;
    sheetRowLimit?: number;
  };
};

type ExportQueryRequest = { sql: string } & ExportQueryOptions;

type ConnectionState =
  | { status: "idle" }
  | { status: "connecting" }
  | { status: "connected"; connection: MDConnection }
  | { status: "error"; error: Error };

export type UseSQLQueryResult<TData = readonly DuckDBRow[]> = {
  data: TData | undefined;
  isLoading: boolean;
  isSuccess: boolean;
  isError: boolean;
  isPlaceholderData: boolean;
  error: Error | null;
  refetch: () => void;
  exportAs: (options: ExportQueryOptions) => Promise<void>;
  status: QueryStatus;
};

type UseSQLQueryOptions<TData = readonly DuckDBRow[]> = {
  enabled?: boolean;
  select?: (data: readonly DuckDBRow[]) => TData;
  initialData?: TData;
  placeholderData?: TData | ((prev: TData | undefined) => TData | undefined);
};

async function simulateLocalExport(sql: string, options: ExportQueryOptions) {
  const filename = options.filename || options.title || "export";
  console.info("Dive export requested in local preview", { sql, options });
  window.alert?.(
    `Export requested: ${filename}.${options.format}. Saved MotherDuck dives generate and download the real file.`,
  );
}

// ── QueryObserver (external store for useSyncExternalStore) ────────

interface QueryObserverState {
  status: QueryStatus;
  data: readonly DuckDBRow[] | undefined;
  error: Error | undefined;
  hasHadData: boolean;
  lastData: readonly DuckDBRow[] | undefined;
}

class QueryObserver {
  private state: QueryObserverState = {
    status: "idle", data: undefined, error: undefined,
    hasHadData: false, lastData: undefined,
  };
  private listeners = new Set<() => void>();
  private abortController: AbortController | null = null;

  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => { this.listeners.delete(listener); };
  };
  getSnapshot = () => this.state;
  getStatus() { return this.state.status; }

  private setState(updates: Partial<QueryObserverState>) {
    this.state = { ...this.state, ...updates };
    this.listeners.forEach((l) => l());
  }

  async execute(connection: MDConnection, sql: string) {
    this.abortController?.abort();
    this.abortController = new AbortController();
    const { signal } = this.abortController;
    this.setState({ status: "loading", data: undefined, error: undefined });
    try {
      const result = await connection.safeEvaluateQuery(sql);
      if (signal.aborted) return;
      if (result.status === "error") {
        this.setState({ status: "error", error: result.err, data: undefined });
        return;
      }
      const data = result.result.data.toRows();
      this.setState({
        status: "success", data, error: undefined,
        hasHadData: true, lastData: data,
      });
    } catch (err) {
      if (signal.aborted) return;
      this.setState({
        status: "error",
        error: err instanceof Error ? err : new Error(String(err)),
        data: undefined,
      });
    }
  }

  reset() {
    this.abortController?.abort();
    this.abortController = null;
    this.setState({ status: "idle", data: undefined, error: undefined });
  }
  cancel() {
    this.abortController?.abort();
    this.abortController = null;
  }
}

// ── Provider ───────────────────────────────────────────────────────

type ContextValue = { state: ConnectionState };
const SDKContext = createContext<ContextValue | null>(null);

function useSDKContext() {
  const ctx = useContext(SDKContext);
  if (!ctx) throw new Error("Must be used within MotherDuckSDKProvider");
  return ctx;
}

export function MotherDuckSDKProvider(
  { token, children }: { token: string; children: ReactNode },
) {
  const [state, setState] = useState<ConnectionState>({ status: "idle" });

  useEffect(() => {
    if (!token) { setState({ status: "idle" }); return; }
    let cancelled = false;
    let conn: MDConnection | null = null;
    (async () => {
      setState({ status: "connecting" });
      try {
        conn = MDConnection.create({ mdToken: token, useDuckDBWasmCOI: false });
        await conn.isInitialized();
        if (!cancelled) setState({ status: "connected", connection: conn });
      } catch (err) {
        if (!cancelled) setState({
          status: "error",
          error: err instanceof Error ? err : new Error(String(err)),
        });
      }
    })();
    return () => { cancelled = true; conn?.close(); };
  }, [token]);

  const value = useMemo(() => ({ state }), [state]);
  return <SDKContext.Provider value={value}>{children}</SDKContext.Provider>;
}

// ── useSQLQuery ────────────────────────────────────────────────────

export function useSQLQuery<TData = readonly DuckDBRow[]>(
  sql: string,
  options?: UseSQLQueryOptions<TData>,
): UseSQLQueryResult<TData> {
  const { state: connState } = useSDKContext();
  const observerRef = useRef<QueryObserver | null>(null);
  if (!observerRef.current) observerRef.current = new QueryObserver();
  const observer = observerRef.current;

  const snap = useSyncExternalStore(
    observer.subscribe, observer.getSnapshot, observer.getSnapshot,
  );
  const enabled = options?.enabled !== false;

  useEffect(() => {
    if (!enabled || connState.status !== "connected") {
      if (observer.getStatus() !== "idle") observer.reset();
      return;
    }
    observer.execute(connState.connection, sql);
    return () => observer.cancel();
  }, [sql, enabled, connState, observer]);

  const refetch = useCallback(() => {
    if (connState.status === "connected" && enabled) {
      observer.execute(connState.connection, sql);
    }
  }, [observer, connState, enabled, sql]);

  const exportAs = useCallback(async (exportOptions: ExportQueryOptions) => {
    if (connState.status !== "connected") {
      throw new Error("Cannot export before the MotherDuck connection is ready.");
    }
    await simulateLocalExport(sql, exportOptions);
  }, [connState.status, sql]);

  const isLoading = snap.status === "loading" || connState.status === "connecting";
  const rawData = snap.data ?? snap.lastData;

  const transformed = useMemo(() => {
    if (rawData === undefined) return undefined;
    return options?.select
      ? options.select(rawData)
      : (rawData as unknown as TData);
  }, [rawData, options?.select]);

  const { data, isPlaceholderData } = useMemo(() => {
    if (transformed !== undefined)
      return { data: transformed, isPlaceholderData: false };
    if (!snap.hasHadData && options?.initialData !== undefined)
      return { data: options.initialData, isPlaceholderData: false };
    if (isLoading && options?.placeholderData !== undefined) {
      const ph = typeof options.placeholderData === "function"
        ? (options.placeholderData as (p: TData | undefined) => TData | undefined)(transformed)
        : options.placeholderData;
      if (ph !== undefined) return { data: ph, isPlaceholderData: true };
    }
    return { data: undefined, isPlaceholderData: false };
  }, [transformed, snap.hasHadData, options?.initialData, options?.placeholderData, isLoading]);

  return {
    data, isLoading,
    isSuccess: snap.status === "success",
    isError: snap.status === "error",
    isPlaceholderData,
    error: snap.error ?? null,
    refetch, exportAs, status: snap.status,
  };
}

// ── useConnection / useConnectionStatus ────────────────────────────

export function useConnection(): MDConnection | null {
  const { state } = useSDKContext();
  return state.status === "connected" ? state.connection : null;
}

export function useConnectionStatus() {
  const { state } = useSDKContext();
  return {
    isConnected: state.status === "connected",
    isConnecting: state.status === "connecting",
    error: state.status === "error" ? state.error : null,
  };
}

export function useExport() {
  const { state } = useSDKContext();
  const exportQuery = useCallback(async ({ sql, ...options }: ExportQueryRequest) => {
    if (state.status !== "connected") {
      throw new Error("Cannot export before the MotherDuck connection is ready.");
    }
    await simulateLocalExport(sql, options);
  }, [state.status]);

  return { exportQuery };
}

// ── useDiveState ─────────────────────────────────────────────────
// Local-preview shim: persists a shared JSON bag to the URL hash fragment
// (#state=<base64url(json)>), same as the production dive runtime for
// small bags. Every callsite with the same key reads/writes the same entry.

function base64UrlEncode(str: string): string {
  return btoa(unescape(encodeURIComponent(str)))
    .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function base64UrlDecode(str: string): string {
  const padded = str.replace(/-/g, "+").replace(/_/g, "/");
  const withPad = padded + "===".slice((padded.length + 3) % 4);
  return decodeURIComponent(escape(atob(withPad)));
}

function readBag(): Record<string, unknown> {
  const match = window.location.hash.match(/#state=(.+)/);
  if (!match) return {};
  try {
    return JSON.parse(base64UrlDecode(match[1]));
  } catch {
    return {};
  }
}

function writeBag(bag: Record<string, unknown>) {
  window.history.replaceState(null, "", `#state=${base64UrlEncode(JSON.stringify(bag))}`);
}

const diveStateListeners = new Set<() => void>();
function notifyDiveStateListeners() {
  diveStateListeners.forEach((l) => l());
}

export function useDiveState<T>(
  key: string,
  initialValue: T,
): [T, (value: T | undefined | ((prev: T) => T | undefined)) => void] {
  const [, forceRender] = useState(0);

  useEffect(() => {
    const listener = () => forceRender((x) => x + 1);
    diveStateListeners.add(listener);
    return () => { diveStateListeners.delete(listener); };
  }, []);

  const bag = readBag();
  const value = (key in bag ? bag[key] : initialValue) as T;

  const setValue = useCallback(
    (update: T | undefined | ((prev: T) => T | undefined)) => {
      const currentBag = readBag();
      const currentValue = (key in currentBag ? currentBag[key] : initialValue) as T;
      const nextValue = typeof update === "function"
        ? (update as (prev: T) => T | undefined)(currentValue)
        : update;

      const nextBag = { ...currentBag };
      if (nextValue === undefined) {
        delete nextBag[key];
      } else {
        nextBag[key] = nextValue;
      }
      writeBag(nextBag);
      notifyDiveStateListeners();
    },
    [key, initialValue],
  );

  return [value, setValue];
}
