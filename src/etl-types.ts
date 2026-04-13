/**
 * src/etl-types.ts
 *
 * Types and pure utility functions for the ETL pipeline.
 * Self-contained — no imports from the main app server.
 */

// ── FacilityDbRow ─────────────────────────────────────────────────────────────

/** Shape of a row in the `facilities` SQLite table. */
export interface FacilityDbRow {
  number:               string;
  name:                 string;
  facility_type:        string;
  facility_group:       string;
  status:               string;
  address:              string;
  city:                 string;
  county:               string;
  zip:                  string;
  phone:                string;
  licensee:             string;
  administrator:        string;
  capacity:             number;
  first_license_date:   string;
  closed_date:          string;
  last_inspection_date: string;
  total_visits:         number;
  total_type_b:         number;
  citations:            number;
  lat:                  number | null;
  lng:                  number | null;
  geocode_quality:      string;
  updated_at:           number;
}

// ── CCLD lookup tables ────────────────────────────────────────────────────────

/** GeoJSON STATUS numeric code → human-readable status text */
export const GEO_STATUS: Record<string, string> = {
  "1": "PENDING",
  "2": "PENDING",
  "3": "LICENSED",
  "4": "ON PROBATION",
  "5": "CLOSED",
  "6": "REVOKED",
};

/** CCLD GeoJSON TYPE numeric code → human-readable facility type name */
export const TYPE_TO_NAME: Record<string, string> = {
  "140": "Foster Family Agency",
  "180": "Group Home",
  "192": "Enhanced Behavioral Supports Home",
  "193": "Community Treatment Facility",
  "194": "Short-Term Residential Therapeutic Program",
  "250": "Family Child Care Home - Small",
  "255": "Family Child Care Home - Large",
  "310": "Child Care Center",
  "385": "Residential Care Facility for the Elderly",
  "400": "Adult Day Program",
  "410": "Social Rehabilitation Facility",
  "425": "Congregate Living Health Facility",
  "500": "Residential Care Facility for the Chronically Ill",
  "735": "Adult Residential Facility",
  "740": "Adult Residential Facility for Persons with Special Health Care Needs",
  "800": "Home Care Organization",
};

// ── Pure utility functions ────────────────────────────────────────────────────

/** Map a facility type name to its top-level facility group. */
export function typeToGroup(facilityType: string): string {
  const t = facilityType.toLowerCase();
  if (
    t.includes("child care center") ||
    t.includes("family child care")
  ) return "Child Care";
  if (
    t.includes("group home") ||
    t.includes("short-term residential") ||
    t.includes("strtp") ||
    t.includes("community treatment") ||
    t.includes("foster family")
  ) return "Children's Residential";
  if (t.includes("home care organization")) return "Home Care";
  return "Adult & Senior Care";
}

/** Format a raw phone number string to (XXX) XXX-XXXX. */
export function formatPhone(raw: string | null | undefined): string {
  const d = (raw ?? "").replace(/\D/g, "");
  if (d.length === 10) return `(${d.slice(0, 3)}) ${d.slice(3, 6)}-${d.slice(6)}`;
  if (d.length === 11 && d[0] === "1")
    return `(${d.slice(1, 4)}) ${d.slice(4, 7)}-${d.slice(7)}`;
  return raw ?? "";
}
