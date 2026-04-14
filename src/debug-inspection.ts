/**
 * src/debug-inspection.ts
 *
 * Diagnostic script — shows the full CCLD report structure for a facility
 * and exercises fetchFacilityEnrichment so you can verify the output.
 *
 * Run with:
 *   npx tsx src/debug-inspection.ts [facilityNumber]
 *
 * Default test facilities (valid Adult Day Program numbers from CHHS open-data):
 *   286803618  – A BRIGHT FUTURE ACADEMY  (5 reports, inx 6 returns 400)
 *   397005472  – A DAY AWAY               (5 reports, inx 6 returns 400)
 *
 * NOTE: FacilityInspections?facNum=X always returns 404 and has been removed
 * from the ETL pipeline. FacilityReports?facNum=X&inx=N is the only working
 * data source.
 */

import { fetchFacilityEnrichment, parseEvaluationReport, rateLimiter } from "./etl-helpers";

const CCLD_BASE = "https://www.ccld.dss.ca.gov/transparencyapi/api";
const FAC_NUMS  = process.argv.slice(2).length
  ? process.argv.slice(2)
  : ["286803618", "397005472"];

function stripHtml(html: string): string {
  return html
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/\s{2,}/g, " ")
    .trim();
}

async function probeReports(facNum: string) {
  console.log(`\n${"═".repeat(60)}`);
  console.log(`  Facility: ${facNum}`);
  console.log(`${"═".repeat(60)}`);

  for (let inx = 1; inx <= 10; inx++) {
    const res = await fetch(
      `${CCLD_BASE}/FacilityReports?facNum=${encodeURIComponent(facNum)}&inx=${inx}`,
    );
    if (res.status === 400 || res.status === 404) {
      console.log(`  inx=${inx}: HTTP ${res.status} — end of report list`);
      break;
    }
    if (!res.ok) {
      console.log(`  inx=${inx}: HTTP ${res.status}`);
      continue;
    }

    const ct   = res.headers.get("content-type") ?? "";
    const html = await res.text();
    const text = stripHtml(html);

    // Extract quick metadata
    const dateM = text.slice(0, 1500).match(/\bDATE\s*:\s*(\d{1,2}\/\d{1,2}\/\d{4})/i);
    const typeM = text.slice(0, 1500).match(/TYPE\s+OF\s+VISIT\s*[:\s]+([^\n]{3,80}?)(?=\s+UNANNOUNCED|\s+TIME\s+|\s+MET\s+WITH|$)/i);
    const adminM = text.slice(0, 1500).match(/ADMINISTRATOR(?:\s*\/\s*DIRECTOR)?\s*[:\s]+([^\n]{2,80}?)(?=\s+FACILITY|\s+TYPE\s*:|\s+ADDRESS|\s+TELEPHONE|$)/i);

    console.log(
      `  inx=${inx}: HTTP 200  ct="${ct}"  size=${html.length}B` +
      `  date=${dateM?.[1] ?? "?"}`  +
      `  type="${typeM?.[1]?.trim().slice(0, 40) ?? "?"}"` +
      `  admin="${adminM?.[1]?.trim().slice(0, 40) ?? "?"}"`
    );

    // Show deficiency counts using the same patterns as parseEvaluationReport
    const typeA = (text.match(/\bType\s*A\b[^.]{0,80}?Section\s+Cited/gi) ?? []).length;
    const typeB = (text.match(/\bType\s*B\b[^.]{0,80}?Section\s+Cited/gi) ?? []).length;
    if (typeA || typeB) {
      console.log(`         → Type A (citations): ${typeA}  Type B deficiencies: ${typeB}`);
    }
  }
}

async function main() {
  // ── 1. Probe each report index to show what's available ───────────────────
  for (const num of FAC_NUMS) {
    await probeReports(num);
  }

  // ── 2. Run fetchFacilityEnrichment and print the result ───────────────────
  console.log("\n\n" + "═".repeat(60));
  console.log("  fetchFacilityEnrichment results");
  console.log("═".repeat(60));

  const throttle = rateLimiter(3);
  for (const num of FAC_NUMS) {
    process.stdout.write(`\n  ${num} … `);
    const data = await fetchFacilityEnrichment(num, throttle);
    console.log(JSON.stringify(data, null, 2));
  }
}

main().catch(console.error);
