/**
 * src/etl-helpers.ts
 *
 * Shared helpers for the ETL pipeline.
 * Self-contained — no imports from the main app server.
 */

import type { FacilityDbRow } from "./etl-types";
import type { EtlConfig } from "./etl-config";

const CHHS_BASE = "https://data.chhs.ca.gov/api/3/action/datastore_search";
const CCLD_BASE = "https://www.ccld.dss.ca.gov/transparencyapi/api";

// ── CHHS pagination ───────────────────────────────────────────────────────────

export async function fetchAllPages(
  resourceId: string,
  pageSize = 5000,
  filters?: Record<string, string>,
  q?: string,
): Promise<any[]> {
  const rows: any[] = [];
  let offset = 0;
  let page = 1;

  while (true) {
    process.stdout.write(`\r    page ${page}  (offset ${offset}, total so far: ${rows.length})…`);

    const params = new URLSearchParams({
      resource_id: resourceId,
      limit: String(pageSize),
      offset: String(offset),
    });
    if (filters) params.set("filters", JSON.stringify(filters));
    if (q) params.set("q", q);

    const res = await fetch(`${CHHS_BASE}?${params}`, {
      headers: { Accept: "application/json" },
    });
    if (!res.ok) {
      throw new Error(`CHHS API returned HTTP ${res.status} for resource ${resourceId}`);
    }

    const json = await res.json();
    const records: any[] = json.result?.records ?? [];
    rows.push(...records);

    if (records.length < pageSize) break;
    offset += pageSize;
    page++;
  }

  process.stdout.write("\r" + " ".repeat(60) + "\r");
  return rows;
}

// ── Facility number helpers ───────────────────────────────────────────────────

export function normalizeFacilityNumber(raw: string): string {
  return String(raw ?? "").trim().replace(/[\s-]/g, "");
}

export function validateFacilityNumber(num: string): boolean {
  return /^\d{6,12}$/.test(num);
}

export function dedupeByNumber<T>(
  rows: T[],
  getNum: (row: T) => string,
  sourceName: string,
): Map<string, T> {
  const map = new Map<string, T>();
  let skippedInvalid = 0;
  let skippedDupe = 0;

  for (const row of rows) {
    const num = normalizeFacilityNumber(getNum(row));
    if (!validateFacilityNumber(num)) { skippedInvalid++; continue; }
    if (map.has(num)) { skippedDupe++; continue; }
    map.set(num, row);
  }

  if (skippedInvalid > 0)
    console.warn(`[etl] ${sourceName}: ${skippedInvalid} rows skipped (invalid facility number)`);
  if (skippedDupe > 0)
    console.warn(`[etl] ${sourceName}: ${skippedDupe} duplicate facility numbers dropped (first-wins)`);

  return map;
}

// ── Merge helpers ─────────────────────────────────────────────────────────────

export function normalizeFacilityType(
  rawCclType: string | undefined,
  geoTypeCode: string | undefined,
  typeToName: Record<string, string>,
): string {
  const fromCcl = (rawCclType ?? "").trim();
  if (fromCcl) return fromCcl;
  const fromGeo = typeToName[String(geoTypeCode ?? "")] ?? "";
  return fromGeo || "Adult Residential Facility";
}

export function mergeFacilityRow(
  num: string,
  ccl: Record<string, any> | undefined,
  geo: Record<string, any> | undefined,
  fm: {
    fromGeo: Record<string, string>;
    fromCcl: Record<string, string>;
  },
  typeToName: Record<string, string>,
  geoStatus: Record<string, string>,
  skipMissingGeo: boolean,
  includeCclOnly: boolean,
  formatPhoneFn: (raw: string) => string,
  typeToGroupFn: (type: string) => string,
): Omit<FacilityDbRow, "updated_at"> | null {
  const lat = geo ? parseFloat(geo[fm.fromGeo.lat] ?? "") : NaN;
  const lng = geo ? parseFloat(geo[fm.fromGeo.lng] ?? "") : NaN;
  const hasGeo = Number.isFinite(lat) && lat !== 0 &&
                 Number.isFinite(lng) && lng !== 0;

  if (!geo && !includeCclOnly) return null;
  if (geo && skipMissingGeo && !hasGeo) return null;

  const facilityType = normalizeFacilityType(
    ccl?.[fm.fromCcl.facilityType],
    geo?.[fm.fromGeo.typeCode],
    typeToName,
  );
  const facilityGroup = typeToGroupFn(facilityType);

  const status = (
    ccl?.[fm.fromCcl.status] ??
    geoStatus[String(geo?.[fm.fromGeo.status] ?? "")] ??
    "LICENSED"
  ).toUpperCase();

  const capacity =
    parseInt(geo?.[fm.fromGeo.capacity] ?? "", 10) ||
    parseInt(ccl?.[fm.fromCcl.capacity] ?? "0", 10) ||
    0;

  return {
    number:              num,
    name:                (ccl?.[fm.fromCcl.name] ?? geo?.[fm.fromGeo.name] ?? "").trim(),
    facility_type:       facilityType,
    facility_group:      facilityGroup,
    status,
    address:             (geo?.[fm.fromGeo.address]  ?? "").trim(),
    city:                (geo?.[fm.fromGeo.city]     ?? "").trim().toUpperCase(),
    county:              (ccl?.[fm.fromCcl.county]   ?? geo?.COUNTY ?? "").trim(),
    zip:                 (geo?.[fm.fromGeo.zip]       ?? "").trim(),
    phone:               formatPhoneFn(geo?.[fm.fromGeo.phone] ?? ""),
    licensee:            (ccl?.[fm.fromCcl.licensee]      ?? "").trim(),
    administrator:       (ccl?.[fm.fromCcl.administrator] ?? "").trim(),
    capacity,
    first_license_date:  ccl?.[fm.fromCcl.firstLicenseDate] ?? "",
    closed_date:         ccl?.[fm.fromCcl.closedDate]       ?? "",
    last_inspection_date: "",
    total_visits:         0,
    total_type_b:         0,
    citations:            0,
    lat:                  hasGeo ? lat : null,
    lng:                  hasGeo ? lng : null,
    geocode_quality:      hasGeo ? "api" : "",
  };
}

// ── CCLD Transparency API — enrichment helpers ────────────────────────────────

export interface FacilityEnrichmentData {
  last_inspection_date?: string;
  administrator?:        string;
  licensee?:             string;
  total_type_b?:         number;
  citations?:            number;
}

function normalizeDate(raw: string): string {
  const mdy = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (mdy) {
    return `${mdy[3]}-${mdy[1].padStart(2, "0")}-${mdy[2].padStart(2, "0")}`;
  }
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) return raw.slice(0, 10);
  return raw;
}

function parseMostRecentVisitDate(data: unknown): string | null {
  const records: any[] = Array.isArray(data)
    ? data
    : ((data as any)?.records ?? (data as any)?.value ?? []);

  let latestMs = 0;
  let latestNorm = "";

  for (const r of records) {
    const raw: string =
      r.visitDate ?? r.VisitDate ?? r.visit_date ?? r.VisitDt ?? r.visitdt ?? "";
    if (!raw) continue;
    const ms = new Date(raw).getTime();
    if (!isNaN(ms) && ms > latestMs) {
      latestMs = ms;
      latestNorm = normalizeDate(raw);
    }
  }

  return latestNorm || null;
}

export function parseEvaluationReport(html: string): FacilityEnrichmentData {
  const text = html
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/\s{2,}/g, " ");

  const result: FacilityEnrichmentData = {};

  const dateM = text.match(
    /VISIT\s+DATE\s*[:\s]+(\d{1,2}\/\d{1,2}\/\d{4}|\d{4}-\d{2}-\d{2})/i,
  );
  if (dateM) result.last_inspection_date = normalizeDate(dateM[1]);

  const adminM = text.match(
    /ADMINISTRATOR(?:\s*\/\s*DIRECTOR)?\s*[:\s]+([A-Za-z][A-Za-z\s,.'"\-]{1,60}?)(?=\s{2,}|PHONE|FAX|LICENSE|CAPACITY|LICENSEE|VISIT|COUNTY|CITY|ADDRESS|$)/i,
  );
  if (adminM) result.administrator = adminM[1].trim();

  const licenseeM = text.match(
    /LICENSEE(?:\s*\/\s*ENTITY)?\s*[:\s]+([A-Za-z0-9][^\n]{2,80}?)(?=\s{2,}|ADMINISTRATOR|FACILITY|ADDRESS|PHONE|VISIT|$)/im,
  );
  if (licenseeM) result.licensee = licenseeM[1].trim();

  const typeBM =
    text.match(/TOTAL\s+TYPE\s*[-–]?\s*B\s+DEFICIENCIES?\s*[:\s]+(\d+)/i) ??
    text.match(/TYPE\s*[-–]?\s*B\s+DEFICIENCIES?\s+TOTAL\s*[:\s]+(\d+)/i) ??
    text.match(/TYPE\s*[-–]?\s*B\s+TOTAL\s*[:\s]+(\d+)/i);
  if (typeBM) result.total_type_b = parseInt(typeBM[1], 10);

  const citM =
    text.match(/TOTAL\s+CITATIONS?\s*[:\s]+(\d+)/i) ??
    text.match(/CITATIONS?\s+ISSUED\s*[:\s]+(\d+)/i) ??
    text.match(/\bCITATIONS?\s*[:\s]+(\d+)/i);
  if (citM) result.citations = parseInt(citM[1], 10);

  return result;
}

export async function fetchFacilityEnrichment(
  facNum: string,
  throttle: () => Promise<void>,
): Promise<FacilityEnrichmentData> {
  const result: FacilityEnrichmentData = {};

  try {
    await throttle();
    const res = await fetch(
      `${CCLD_BASE}/FacilityInspections?facNum=${encodeURIComponent(facNum)}`,
      { headers: { Accept: "application/json" } },
    );
    if (res.ok) {
      const ct   = res.headers.get("content-type") ?? "";
      const body = await res.text();
      const looksJson =
        ct.includes("json") ||
        body.trimStart().startsWith("[") ||
        body.trimStart().startsWith("{");
      if (looksJson) {
        try {
          const date = parseMostRecentVisitDate(JSON.parse(body));
          if (date) result.last_inspection_date = date;
        } catch {}
      }
    }
  } catch {}

  for (const inx of [1, 4]) {
    try {
      await throttle();
      const res = await fetch(
        `${CCLD_BASE}/FacilityReports?facNum=${encodeURIComponent(facNum)}&inx=${inx}`,
      );
      if (!res.ok) continue;

      const html = await res.text();
      if (html.trim().length < 200) continue;

      const parsed = parseEvaluationReport(html);

      if (!result.last_inspection_date && parsed.last_inspection_date)
        result.last_inspection_date = parsed.last_inspection_date;

      if (parsed.administrator) result.administrator = parsed.administrator;
      if (parsed.licensee)      result.licensee      = parsed.licensee;
      if (parsed.total_type_b !== undefined) result.total_type_b = parsed.total_type_b;
      if (parsed.citations     !== undefined) result.citations    = parsed.citations;

      break;
    } catch {}
  }

  return result;
}

export function rateLimiter(requestsPerSecond: number): () => Promise<void> {
  const minIntervalMs = 1000 / Math.max(requestsPerSecond, 0.1);
  let lastCallMs = 0;
  return async (): Promise<void> => {
    const now = Date.now();
    const wait = minIntervalMs - (now - lastCallMs);
    if (wait > 0) await new Promise<void>((r) => setTimeout(r, wait));
    lastCallMs = Date.now();
  };
}

function fmtEta(seconds: number): string {
  if (seconds < 60) return `${Math.ceil(seconds)}s`;
  const m = Math.floor(seconds / 60);
  const s = Math.ceil(seconds % 60);
  return `${m}m ${s}s`;
}

export async function enrichFacilities(
  facilities: Array<{
    number:               string;
    administrator?:       string;
    licensee?:            string;
    last_inspection_date?: string;
  }>,
  config: EtlConfig["enrichment"],
): Promise<Map<string, Partial<FacilityDbRow>>> {
  const results  = new Map<string, Partial<FacilityDbRow>>();
  const throttle = rateLimiter(config.requestsPerSecond);

  const candidates =
    config.enrichLimit > 0
      ? facilities.slice(0, config.enrichLimit)
      : facilities;

  const total   = candidates.length;
  let done      = 0;
  let enriched  = 0;
  const startMs = Date.now();

  for (const fac of candidates) {
    try {
      const data = await fetchFacilityEnrichment(fac.number, throttle);
      const patch: Partial<FacilityDbRow> = {};

      if (config.fields.lastInspectionDate && data.last_inspection_date) {
        const alreadyHas = config.skipIfPopulated && !!fac.last_inspection_date;
        if (!alreadyHas) patch.last_inspection_date = data.last_inspection_date;
      }
      if (config.fields.administrator && data.administrator) {
        const alreadyHas = config.skipIfPopulated && !!fac.administrator;
        if (!alreadyHas) patch.administrator = data.administrator;
      }
      if (config.fields.licensee && data.licensee) {
        const alreadyHas = config.skipIfPopulated && !!fac.licensee;
        if (!alreadyHas) patch.licensee = data.licensee;
      }
      if (data.total_type_b !== undefined) patch.total_type_b = data.total_type_b;
      if (data.citations    !== undefined) patch.citations    = data.citations;

      if (Object.keys(patch).length > 0) {
        results.set(fac.number, patch);
        enriched++;
      }
    } catch (err) {
      process.stdout.write("\n");
      console.warn(`[etl] Warning: enrichment failed for ${fac.number}: ${err}`);
    }

    done++;
    if (done % 10 === 0 || done === total) {
      const elapsedS = (Date.now() - startMs) / 1000;
      const rate     = done / Math.max(elapsedS, 0.001);
      const etaS     = rate > 0 ? (total - done) / rate : 0;
      process.stdout.write(
        `\r    ${done.toLocaleString()} / ${total.toLocaleString()}` +
        `   enriched: ${enriched.toLocaleString()}` +
        `   eta: ~${fmtEta(etaS)}` +
        " ".repeat(8),
      );
    }
  }

  process.stdout.write("\n");
  return results;
}
