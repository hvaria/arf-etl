/**
 * src/extract-ccld-data.ts
 *
 * Fetches ALL California CCLD facility types from CHHS open-data,
 * merges the two datasets, and writes:
 *   data/ccld_all_facilities.json
 *
 * Usage:
 *   npx tsx src/extract-ccld-data.ts
 *
 * Then seed the DB:
 *   npx tsx src/seed-facilities-db.ts
 */

import * as fs from "fs";
import * as path from "path";
import { typeToGroup, formatPhone, GEO_STATUS, TYPE_TO_NAME } from "./etl-types";

const CHHS_BASE = "https://data.chhs.ca.gov/api/3/action/datastore_search";
const GEO_RESOURCE = "f9c77b0d-9711-4f34-8c7f-90f542fbc24a";
const CCL_RESOURCE = "9f5d1d00-6b24-4f44-a158-9cbe4b43f117";
const PAGE_SIZE = 5000;

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function fetchAllPages(resourceId: string, filters?: Record<string, string>): Promise<any[]> {
  const rows: any[] = [];
  let offset = 0;
  let page = 1;

  while (true) {
    const params = new URLSearchParams({
      resource_id: resourceId,
      limit: String(PAGE_SIZE),
      offset: String(offset),
    });
    if (filters) params.set("filters", JSON.stringify(filters));

    console.log(`  Page ${page} (offset ${offset})…`);

    const res = await fetch(`${CHHS_BASE}?${params}`, { headers: { Accept: "application/json" } });
    if (!res.ok) throw new Error(`CHHS API ${res.status} for resource ${resourceId}`);

    const json = await res.json();
    const records: any[] = json.result?.records ?? [];
    rows.push(...records);

    if (records.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
    page++;

    await sleep(1000);
  }

  return rows;
}

async function main() {
  console.log("=== CCLD California Facility Extractor ===\n");

  console.log("Fetching GeoJSON dataset (coordinates + type codes)…");
  const geoRows = await fetchAllPages(GEO_RESOURCE);
  console.log(`  → ${geoRows.length} GEO rows\n`);
  await sleep(1000);

  console.log("Fetching CCL dataset (names, licensees, statuses)…");
  const cclRows = await fetchAllPages(CCL_RESOURCE);
  console.log(`  → ${cclRows.length} CCL rows\n`);

  const cclByNumber = new Map<string, any>();
  for (const row of cclRows) {
    if (row.facility_number) cclByNumber.set(String(row.facility_number).trim(), row);
  }

  console.log("Merging datasets…");
  const facilities: any[] = [];
  let skippedNoCoords = 0;

  for (const geo of geoRows) {
    const num = String(geo.FAC_NBR ?? "").trim();
    if (!num) continue;

    const lat = parseFloat(geo.FAC_LATITUDE ?? "0");
    const lng = parseFloat(geo.FAC_LONGITUDE ?? "0");
    if (!lat || !lng || Math.abs(lat) < 0.01 || Math.abs(lng) < 0.01) {
      skippedNoCoords++;
      continue;
    }

    const ccl = cclByNumber.get(num);
    const rawType = (ccl?.facility_type ?? TYPE_TO_NAME[String(geo.TYPE)] ?? "").trim();
    const facilityType = rawType || "Adult Residential Facility";
    const facilityGroup = typeToGroup(facilityType);
    const county = (ccl?.county ?? geo.COUNTY ?? "").trim();

    facilities.push({
      number: num,
      name: (ccl?.facility_name ?? geo.NAME ?? "").trim(),
      facilityType,
      facilityGroup,
      status: (ccl?.facility_status ?? GEO_STATUS[String(geo.STATUS)] ?? "LICENSED").toUpperCase(),
      address: (geo.RES_STREET_ADDR ?? "").trim(),
      city: (geo.RES_CITY ?? "").trim().toUpperCase(),
      county,
      zip: (geo.RES_ZIP_CODE ?? "").trim(),
      phone: formatPhone(geo.FAC_PHONE_NBR),
      licensee: (ccl?.licensee ?? "").trim(),
      administrator: (ccl?.facility_administrator ?? "").trim(),
      capacity: parseInt(geo.CAPACITY ?? ccl?.facility_capacity ?? "0", 10) || 0,
      firstLicenseDate: ccl?.license_first_date ?? "",
      closedDate: ccl?.closed_date ?? "",
      lastInspectionDate: "",
      totalVisits: 0,
      totalTypeB: 0,
      citations: 0,
      lat,
      lng,
      geocodeQuality: "api",
    });
  }

  console.log(`  → ${facilities.length} facilities (skipped ${skippedNoCoords} without coords)`);

  const byGroup: Record<string, number> = {};
  const byType: Record<string, number> = {};
  for (const f of facilities) {
    byGroup[f.facilityGroup] = (byGroup[f.facilityGroup] ?? 0) + 1;
    byType[f.facilityType]   = (byType[f.facilityType]   ?? 0) + 1;
  }

  console.log("\n=== Facility Groups ===");
  for (const [group, count] of Object.entries(byGroup).sort()) {
    console.log(`  ${group}: ${count}`);
  }
  console.log("\n=== Facility Types (top 20) ===");
  for (const [type, count] of Object.entries(byType)
    .sort((a, b) => (b[1] as number) - (a[1] as number))
    .slice(0, 20)) {
    console.log(`  ${type}: ${count}`);
  }

  const outDir = path.resolve(process.cwd(), "data");
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const outPath = path.join(outDir, "ccld_all_facilities.json");
  fs.writeFileSync(outPath, JSON.stringify(facilities, null, 2));
  console.log(`\n✓ Saved ${facilities.length} facilities to ${outPath}`);
  console.log(`\nNext step: npx tsx src/seed-facilities-db.ts`);
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
