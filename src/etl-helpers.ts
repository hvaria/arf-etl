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

function stripHtml(html: string): string {
  return html
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/\s{2,}/g, " ");
}

/**
 * Attempt to extract plain text from a PDF buffer using pdf-parse.
 * Returns empty string if pdf-parse is unavailable or parsing fails.
 */
async function parsePdfBuffer(buffer: ArrayBuffer): Promise<string> {
  try {
    // Dynamic import so the rest of the pipeline works even if pdf-parse is absent
    const pdfParse = await import("pdf-parse");
    const fn = (pdfParse as any).default ?? pdfParse;
    const data = await fn(Buffer.from(buffer));
    return (data as any).text ?? "";
  } catch {
    return "";
  }
}

/**
 * Parse a CCLD Facility Evaluation Report (HTML or pre-extracted PDF text).
 *
 * Key field locations in the stripped text:
 *   DATE: MM/DD/YYYY           — visit date (appears in form header AND deficiency pages)
 *   ADMINISTRATOR: LAST, FIRST — or ADMINISTRATOR/ DIRECTOR: ...
 *   LICENSEE: ...              — may not appear in evaluation reports (comes from CHHS data)
 *   Type B ... Section Cited   — each Type-B deficiency block
 *   Type A ... Section Cited   — each Type-A deficiency block (civil penalty trigger)
 */
export function parseEvaluationReport(html: string): FacilityEnrichmentData {
  const text = stripHtml(html);
  const result: FacilityEnrichmentData = {};

  // ── Visit date ─────────────────────────────────────────────────────────────
  // "DATE: MM/DD/YYYY" matches "Report Date: 04/14/2022" and "DATE: 04/14/2022"
  // Does NOT match "Date Signed: ..." (colon is after "Signed", not after "Date")
  const dateM =
    text.match(/\bDATE\s*:\s*(\d{1,2}\/\d{1,2}\/\d{4})/i) ??
    text.match(/(?:VISIT\s+DATE|INSPECTION\s+DATE|DATE\s+OF\s+VISIT)\s*[:\s]+(\d{1,2}\/\d{1,2}\/\d{4})/i) ??
    text.match(/\b(\d{1,2}\/\d{1,2}\/\d{4})\b/);
  if (dateM) result.last_inspection_date = normalizeDate(dateM[1]);

  // ── Administrator ──────────────────────────────────────────────────────────
  // Patterns seen in the wild:
  //   ADMINISTRATOR: LASTNAME, FIRSTNAME
  //   ADMINISTRATOR/ DIRECTOR: LASTNAME, FIRSTNAME
  //   ADMINISTRATOR: ADMINISTRATOR/ DIRECTOR: LASTNAME, FIRSTNAME  (duplicate artifact)
  const ADMIN_STOP =
    "PHONE|FAX|LICENSE|CAPACITY|LICENSEE|COUNTY|CITY|ADDRESS|" +
    "FACILITY\\s+TYPE|TYPE\\s*:|TELEPHONE|DATE\\s*:";
  const adminM = text.match(
    new RegExp(
      "ADMINISTRATOR(?:\\s*\\/\\s*DIRECTOR)?\\s*[:\\s]+" +
        "([A-Za-z][A-Za-z\\s,.'\"\\-]{1,80}?)" +
        `(?=\\s+(?:${ADMIN_STOP})|$)`,
      "i",
    ),
  );
  if (adminM) {
    // Strip duplicated "ADMINISTRATOR[/ DIRECTOR]:" prefix that appears in some reports
    const admin = adminM[1]
      .trim()
      .replace(/^ADMINISTRATOR(?:\s*\/\s*DIRECTOR)?\s*[:\s]+/i, "")
      .trim();
    if (admin.length >= 2) result.administrator = admin;
  }

  // ── Licensee ───────────────────────────────────────────────────────────────
  // Not present in most evaluation reports (sourced from CHHS CCL dataset instead),
  // but kept as a best-effort fallback for report types that do include it.
  // Requires a colon to avoid matching "the licensee did not comply..." in narratives.
  const licenseeM = text.match(
    /\bLICENSEE(?:\s*\/\s*ENTITY)?\s*:\s*([A-Za-z0-9][^\n]{2,80}?)(?=\s{2,}|ADMINISTRATOR|FACILITY|ADDRESS|PHONE|VISIT|$)/im,
  );
  if (licenseeM) result.licensee = licenseeM[1].trim();

  // ── Type-B deficiency count ────────────────────────────────────────────────
  // Each deficiency block has the form "Type B [optional date] Section Cited [code]".
  // The legend/explanatory text ("Type B deficiencies are violations...") does NOT
  // contain "Section Cited", so this pattern is safe to count.
  const typeBMatches = text.match(/\bType\s*B\b[^.]{0,80}?Section\s+Cited/gi);
  if (typeBMatches) result.total_type_b = typeBMatches.length;

  // ── Citation count (Type-A deficiencies) ──────────────────────────────────
  // Type-A deficiencies (immediate risk) trigger civil penalties / citations.
  const typeAMatches = text.match(/\bType\s*A\b[^.]{0,80}?Section\s+Cited/gi);
  if (typeAMatches) result.citations = typeAMatches.length;

  return result;
}

/**
 * Fetch enrichment data for one facility from the CCLD Transparency API.
 *
 * Strategy:
 *  1. Probe FacilityReports?facNum=X&inx=1,2,3,… until HTTP 400 (no more reports)
 *     or a safety cap of MAX_REPORTS.
 *  2. Extract visit date and type from each report header.
 *  3. Filter for evaluation / annual / required-visit reports.
 *  4. Sort by date descending — pick the most recent evaluation report.
 *  5. Parse that report for administrator, licensee, total_type_b, citations.
 *
 * NOTE: The FacilityInspections endpoint returns 404 for all known facility numbers
 * and has been removed. FacilityReports is the sole reliable data source.
 */
export async function fetchFacilityEnrichment(
  facNum: string,
  throttle: () => Promise<void>,
): Promise<FacilityEnrichmentData> {
  const MAX_REPORTS = 20;

  interface ReportMeta {
    html:         string;   // original HTML (or PDF-extracted text)
    date:         string;   // normalized YYYY-MM-DD extracted from header
    isEvaluation: boolean;  // true = required/annual/evaluation visit type
  }

  const reports: ReportMeta[] = [];

  for (let inx = 1; inx <= MAX_REPORTS; inx++) {
    await throttle();

    let res: Response;
    try {
      res = await fetch(
        `${CCLD_BASE}/FacilityReports?facNum=${encodeURIComponent(facNum)}&inx=${inx}`,
      );
    } catch {
      break;
    }

    // 400 = no report at this index (past the end of the list)
    // 404 = unknown facility
    if (res.status === 400 || res.status === 404) break;
    if (!res.ok) continue;

    const ct = res.headers.get("content-type") ?? "";
    let reportHtml: string;

    if (ct.includes("pdf")) {
      // Some older reports are served as PDFs; extract text and treat as plain text
      const pdfText = await parsePdfBuffer(await res.arrayBuffer());
      if (!pdfText.trim()) continue;
      reportHtml = pdfText;   // parseEvaluationReport handles pre-stripped text safely
    } else {
      reportHtml = await res.text();
      if (reportHtml.trim().length < 200) continue;
    }

    // Quick header scan (stripped text) for date + visit type
    const preview = stripHtml(reportHtml);

    // "DATE: MM/DD/YYYY" — matches "Report Date:" and the form "DATE:" field.
    // Capped to the first 1 500 chars where the form header always appears.
    const dateM = preview.slice(0, 1500).match(/\bDATE\s*:\s*(\d{1,2}\/\d{1,2}\/\d{4})/i);
    const date  = dateM ? normalizeDate(dateM[1]) : "";

    // Evaluation visits include "Required - N Year", "Annual", and "Comprehensive Inspection"
    const isEvaluation = /required\s*[-–]?\s*\d+\s*year|annual\b|comprehensive\s+inspection/i
      .test(preview.slice(0, 1500));

    reports.push({ html: reportHtml, date, isEvaluation });
  }

  if (reports.length === 0) return {};

  // Prefer evaluation/annual reports; fall back to all reports if none found
  const pool = reports.some(r => r.isEvaluation)
    ? reports.filter(r => r.isEvaluation)
    : reports;

  // Sort by date descending — most recent first
  pool.sort((a, b) => b.date.localeCompare(a.date));
  const best = pool[0];

  const parsed = parseEvaluationReport(best.html);

  // Use the header-extracted date as fallback if the report parser couldn't find one
  if (!parsed.last_inspection_date && best.date) {
    parsed.last_inspection_date = best.date;
  }

  return parsed;
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
