// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2026 Andreas Krause / storagebit
//
// Thin, typed client for the Prometheus HTTP API (v1). Uses the Node 18+
// global `fetch`; no third-party HTTP dependency. The central Prometheus that
// scrapes every node's `keinexport` sidecar (`:9909/metrics`) is the only data
// source this MCP server talks to.

/** A single label set + value pair returned by an instant query. */
export interface InstantSample {
  metric: Record<string, string>;
  /** [unix_seconds, value-as-string] as Prometheus returns it. */
  value: [number, string];
}

/** A single label set + value matrix returned by a range query. */
export interface RangeSample {
  metric: Record<string, string>;
  /** Array of [unix_seconds, value-as-string] points. */
  values: Array<[number, string]>;
}

export interface InstantQueryResult {
  resultType: string;
  result: InstantSample[];
}

export interface RangeQueryResult {
  resultType: string;
  result: RangeSample[];
}

interface PromEnvelope<T> {
  status: "success" | "error";
  data?: T;
  errorType?: string;
  error?: string;
  warnings?: string[];
}

/** Raised when Prometheus is unreachable or returns a non-success status. */
export class PrometheusError extends Error {
  constructor(
    message: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = "PrometheusError";
  }
}

/**
 * Minimal Prometheus HTTP API client. One instance is shared across all MCP
 * tool handlers.
 */
export class PrometheusClient {
  private readonly baseUrl: string;
  private readonly timeoutMs: number;

  constructor(baseUrl: string, timeoutMs = 15_000) {
    // Normalize: drop trailing slash so we can append `/api/v1/...`.
    this.baseUrl = baseUrl.replace(/\/+$/, "");
    this.timeoutMs = timeoutMs;
  }

  get url(): string {
    return this.baseUrl;
  }

  private async getJson<T>(path: string, params: Record<string, string>): Promise<T> {
    const qs = new URLSearchParams(params).toString();
    const target = `${this.baseUrl}${path}?${qs}`;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), this.timeoutMs);
    let resp: Response;
    try {
      resp = await fetch(target, { signal: ctrl.signal });
    } catch (err) {
      throw new PrometheusError(
        `failed to reach Prometheus at ${this.baseUrl} (${path}): ${
          err instanceof Error ? err.message : String(err)
        }`,
        err,
      );
    } finally {
      clearTimeout(timer);
    }

    let body: PromEnvelope<T>;
    try {
      body = (await resp.json()) as PromEnvelope<T>;
    } catch (err) {
      throw new PrometheusError(
        `Prometheus returned non-JSON (HTTP ${resp.status}) for ${path}`,
        err,
      );
    }

    if (!resp.ok || body.status !== "success") {
      throw new PrometheusError(
        `Prometheus query failed (HTTP ${resp.status}${
          body.errorType ? `, ${body.errorType}` : ""
        }): ${body.error ?? resp.statusText}`,
      );
    }
    if (body.data === undefined) {
      throw new PrometheusError(`Prometheus success envelope had no data for ${path}`);
    }
    return body.data;
  }

  /** Run an instant PromQL query. `time` is an optional RFC3339 / unix string. */
  async instant(query: string, time?: string): Promise<InstantQueryResult> {
    const params: Record<string, string> = { query };
    if (time) params.time = time;
    return this.getJson<InstantQueryResult>("/api/v1/query", params);
  }

  /** Run a range PromQL query between `start` and `end` at `step` resolution. */
  async range(
    query: string,
    start: string,
    end: string,
    step: string,
  ): Promise<RangeQueryResult> {
    return this.getJson<RangeQueryResult>("/api/v1/query_range", {
      query,
      start,
      end,
      step,
    });
  }

  /** List the values of a label across all series (e.g. `__name__`). */
  async labelValues(label: string, match?: string[]): Promise<string[]> {
    const params: Record<string, string> = {};
    if (match && match.length) {
      // URLSearchParams can't repeat a key directly, so build manually below.
      const qs = new URLSearchParams();
      for (const m of match) qs.append("match[]", m);
      const target = `${this.baseUrl}/api/v1/label/${encodeURIComponent(
        label,
      )}/values?${qs.toString()}`;
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), this.timeoutMs);
      try {
        const resp = await fetch(target, { signal: ctrl.signal });
        const body = (await resp.json()) as PromEnvelope<string[]>;
        if (!resp.ok || body.status !== "success") {
          throw new PrometheusError(
            `label values query failed: ${body.error ?? resp.statusText}`,
          );
        }
        return body.data ?? [];
      } catch (err) {
        if (err instanceof PrometheusError) throw err;
        throw new PrometheusError(
          `failed to list label values for ${label}: ${
            err instanceof Error ? err.message : String(err)
          }`,
          err,
        );
      } finally {
        clearTimeout(timer);
      }
    }
    return this.getJson<string[]>(
      `/api/v1/label/${encodeURIComponent(label)}/values`,
      params,
    );
  }

  /**
   * Convenience: run an instant query and return a single scalar value, or
   * `null` when the query produced no series. Useful for health rollups.
   */
  async scalar(query: string): Promise<number | null> {
    const res = await this.instant(query);
    if (!res.result.length) return null;
    const v = res.result[0].value?.[1];
    if (v === undefined) return null;
    const n = Number(v);
    return Number.isNaN(n) ? null : n;
  }
}
