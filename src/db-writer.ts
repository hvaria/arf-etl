/**
 * src/db-writer.ts
 *
 * Self-contained SQLite connection and write utilities.
 * No imports from the main app server — this is the ETL repo's own DB layer.
 *
 * DATABASE_PATH env var controls which file to open.
 * Falls back to DATA_DIR/data.db (Fly.io volume convention) then ./data.db.
 */

import Database from "better-sqlite3";
import path from "path";
import type { FacilityDbRow } from "./etl-types";

// ── Connection ────────────────────────────────────────────────────────────────

const DB_PATH = process.env.DATABASE_PATH
  ?? (process.env.DATA_DIR ? path.join(process.env.DATA_DIR, "data.db") : "data.db");

export const sqlite = new Database(DB_PATH);
sqlite.pragma("journal_mode = WAL");
sqlite.pragma("synchronous = NORMAL");
sqlite.pragma("foreign_keys = ON");

// ── Schema bootstrap ──────────────────────────────────────────────────────────

sqlite.exec(`
  CREATE TABLE IF NOT EXISTS facilities (
    number TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    facility_type TEXT NOT NULL DEFAULT '',
    facility_group TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    address TEXT NOT NULL DEFAULT '',
    city TEXT NOT NULL DEFAULT '',
    county TEXT NOT NULL DEFAULT '',
    zip TEXT NOT NULL DEFAULT '',
    phone TEXT NOT NULL DEFAULT '',
    licensee TEXT NOT NULL DEFAULT '',
    administrator TEXT NOT NULL DEFAULT '',
    capacity INTEGER DEFAULT 0,
    first_license_date TEXT DEFAULT '',
    closed_date TEXT DEFAULT '',
    last_inspection_date TEXT DEFAULT '',
    total_visits INTEGER DEFAULT 0,
    total_type_b INTEGER DEFAULT 0,
    citations INTEGER DEFAULT 0,
    lat REAL,
    lng REAL,
    geocode_quality TEXT DEFAULT '',
    updated_at INTEGER NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_facilities_county ON facilities(county);
  CREATE INDEX IF NOT EXISTS idx_facilities_type ON facilities(facility_type);
  CREATE INDEX IF NOT EXISTS idx_facilities_group ON facilities(facility_group);
  CREATE INDEX IF NOT EXISTS idx_facilities_status ON facilities(status);
  CREATE INDEX IF NOT EXISTS idx_facilities_latlng ON facilities(lat, lng);

  CREATE TABLE IF NOT EXISTS enrichment_runs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at       INTEGER NOT NULL,
    finished_at      INTEGER,
    trigger          TEXT NOT NULL DEFAULT 'scheduled',
    total_processed  INTEGER NOT NULL DEFAULT 0,
    total_enriched   INTEGER NOT NULL DEFAULT 0,
    total_no_data    INTEGER NOT NULL DEFAULT 0,
    total_failed     INTEGER NOT NULL DEFAULT 0
  );
`);

function addColumnIfMissing(table: string, column: string, definition: string) {
  const cols = (sqlite.pragma(`table_info(${table})`) as any[]).map((c) => c.name);
  if (!cols.includes(column)) {
    sqlite.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
  }
}
addColumnIfMissing("facilities", "enriched_at", "INTEGER");

// ── bulkUpsertFacilities ──────────────────────────────────────────────────────

export function bulkUpsertFacilities(rows: Omit<FacilityDbRow, "updated_at">[]): void {
  const stmt = sqlite.prepare(`
    INSERT INTO facilities (
      number, name, facility_type, facility_group, status,
      address, city, county, zip, phone,
      licensee, administrator, capacity,
      first_license_date, closed_date, last_inspection_date,
      total_visits, total_type_b, citations,
      lat, lng, geocode_quality, updated_at
    ) VALUES (
      ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?,
      ?, ?, ?,
      ?, ?, ?,
      ?, ?, ?,
      ?, ?, ?, ?
    ) ON CONFLICT(number) DO UPDATE SET
      name=excluded.name, facility_type=excluded.facility_type,
      facility_group=excluded.facility_group, status=excluded.status,
      address=excluded.address, city=excluded.city, county=excluded.county,
      zip=excluded.zip, phone=excluded.phone, licensee=excluded.licensee,
      administrator=excluded.administrator, capacity=excluded.capacity,
      first_license_date=excluded.first_license_date, closed_date=excluded.closed_date,
      last_inspection_date=CASE WHEN last_inspection_date != '' THEN last_inspection_date ELSE excluded.last_inspection_date END,
      total_visits=excluded.total_visits, total_type_b=excluded.total_type_b,
      citations=excluded.citations, lat=excluded.lat, lng=excluded.lng,
      geocode_quality=excluded.geocode_quality, updated_at=excluded.updated_at
  `);

  const now = Date.now();
  const insertMany = sqlite.transaction((items: Omit<FacilityDbRow, "updated_at">[]) => {
    for (const row of items) {
      stmt.run(
        row.number, row.name, row.facility_type, row.facility_group, row.status,
        row.address, row.city, row.county, row.zip, row.phone,
        row.licensee, row.administrator, row.capacity ?? 0,
        row.first_license_date, row.closed_date, row.last_inspection_date,
        row.total_visits ?? 0, row.total_type_b ?? 0, row.citations ?? 0,
        row.lat, row.lng, row.geocode_quality,
        now,
      );
    }
  });

  insertMany(rows);
}

// ── logEnrichmentRun ──────────────────────────────────────────────────────────

export function logEnrichmentRun(data: {
  startedAt:      number;
  finishedAt:     number;
  trigger:        string;
  totalProcessed: number;
  totalEnriched:  number;
  totalNoData:    number;
  totalFailed:    number;
}): void {
  sqlite
    .prepare(
      `INSERT INTO enrichment_runs
         (started_at, finished_at, trigger, total_processed, total_enriched, total_no_data, total_failed)
       VALUES (?, ?, ?, ?, ?, ?, ?)`
    )
    .run(
      data.startedAt, data.finishedAt, data.trigger,
      data.totalProcessed, data.totalEnriched, data.totalNoData, data.totalFailed,
    );
}
