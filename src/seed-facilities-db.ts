/**
 * src/seed-facilities-db.ts
 *
 * Reads data/ccld_all_facilities.json (produced by extract-ccld-data.ts)
 * and bulk-upserts all records into the SQLite `facilities` table.
 *
 * Usage:
 *   npx tsx src/seed-facilities-db.ts
 *
 * Safe to re-run — uses INSERT OR REPLACE so it won't duplicate rows.
 */

import * as fs from "fs";
import * as path from "path";
import { bulkUpsertFacilities } from "./db-writer";

async function main() {
  const dataPath = path.resolve(process.cwd(), "data", "ccld_all_facilities.json");

  if (!fs.existsSync(dataPath)) {
    console.error(`❌ File not found: ${dataPath}`);
    console.error("   Run: npx tsx src/extract-ccld-data.ts first");
    process.exit(1);
  }

  console.log(`Reading ${dataPath}…`);
  const raw = fs.readFileSync(dataPath, "utf-8");
  const facilities: any[] = JSON.parse(raw);
  console.log(`  → ${facilities.length} facilities to seed`);

  const rows = facilities.map((f) => ({
    number: f.number,
    name: f.name,
    facility_type: f.facilityType ?? "",
    facility_group: f.facilityGroup ?? "",
    status: f.status,
    address: f.address ?? "",
    city: f.city ?? "",
    county: f.county ?? "",
    zip: f.zip ?? "",
    phone: f.phone ?? "",
    licensee: f.licensee ?? "",
    administrator: f.administrator ?? "",
    capacity: f.capacity ?? 0,
    first_license_date: f.firstLicenseDate ?? "",
    closed_date: f.closedDate ?? "",
    last_inspection_date: f.lastInspectionDate ?? "",
    total_visits: f.totalVisits ?? 0,
    total_type_b: f.totalTypeB ?? 0,
    citations: f.citations ?? 0,
    lat: f.lat ?? null,
    lng: f.lng ?? null,
    geocode_quality: f.geocodeQuality ?? "",
  }));

  const CHUNK = 1000;
  let inserted = 0;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    bulkUpsertFacilities(chunk);
    inserted += chunk.length;
    process.stdout.write(`\r  Seeded ${inserted} / ${rows.length}…`);
  }

  console.log(`\n✓ Seeded ${inserted} facilities into SQLite`);
}

main().catch((err) => {
  console.error("\nFatal:", err);
  process.exit(1);
});
