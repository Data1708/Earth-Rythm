#!/usr/bin/env node
// ---------------------------------------------------------------
// scripts/update-wind.mjs
//
// Fetches the latest NOAA GFS wind forecast (10m surface wind, U and V
// components), processes it through wgrib2 for parsing, downsamples to a
// 1° lat/lon grid, and writes the result as compact JSON.
//
// Output: public/data/wind.json (~250KB)
//
// Pipeline:
//   1. Determine the most recent GFS run (00/06/12/18 UTC, with lag)
//   2. Fetch a small GRIB2 subset from NOMADS' filter endpoint
//      — only U & V wind at 10m height for the f000 (analysis) timestep
//   3. Convert GRIB2 → CSV using wgrib2
//   4. Parse the CSV, downsample to 1° resolution
//   5. Write JSON
//
// Requirements:
//   - Node 20+
//   - wgrib2 in PATH (installed in the Action via apt-get)
//
// Run locally (for testing):
//   node scripts/update-wind.mjs
// ---------------------------------------------------------------

import { writeFile, mkdir, rm, readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { tmpdir } from 'node:os';

const execFileP = promisify(execFile);
const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_PATH = resolve(__dirname, '..', 'public', 'data', 'wind.json');

const TARGET_RES_DEG = 1.0;
const GRID_LONS = 360; // -180..179 inclusive at 1°
const GRID_LATS = 181; // -90..90 inclusive at 1°

// ---------------------------------------------------------------
// Determine the most recent reliably-available GFS run
//
// GFS runs publish at 00, 06, 12, 18 UTC. They're typically available
// ~4-5 hours after the cycle time. We pick the most recent run that
// should be published given the current UTC time, with a safety margin.
// ---------------------------------------------------------------
function pickGfsRun() {
  const now = new Date();
  // Subtract 5 hours of safety margin
  const cutoff = new Date(now.getTime() - 5 * 60 * 60 * 1000);
  const cutoffUtcHour = cutoff.getUTCHours();
  // Floor to most recent 6-hourly cycle
  const cycle = Math.floor(cutoffUtcHour / 6) * 6;
  const runDate = new Date(Date.UTC(
    cutoff.getUTCFullYear(),
    cutoff.getUTCMonth(),
    cutoff.getUTCDate(),
    cycle, 0, 0
  ));
  const yyyymmdd = runDate.toISOString().slice(0, 10).replace(/-/g, '');
  const hh = String(cycle).padStart(2, '0');
  return { yyyymmdd, hh, runDate };
}

// ---------------------------------------------------------------
// Build the NOMADS filter URL for U/V wind at 10m, analysis (f000)
// ---------------------------------------------------------------
function buildSubsetUrl(yyyymmdd, hh) {
  const base = 'https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl';
  const params = new URLSearchParams({
    'dir': `/gfs.${yyyymmdd}/${hh}/atmos`,
    'file': `gfs.t${hh}z.pgrb2.0p25.f000`,
    'var_UGRD': 'on',
    'var_VGRD': 'on',
    'lev_10_m_above_ground': 'on',
    'subregion': '',
    'leftlon': '0',
    'rightlon': '360',
    'toplat': '90',
    'bottomlat': '-90'
  });
  return `${base}?${params.toString()}`;
}

// ---------------------------------------------------------------
// Try fetching the GRIB2 subset, with retries on older runs if the
// most recent one isn't ready yet.
// ---------------------------------------------------------------
async function fetchGribWithFallback() {
  let candidate = pickGfsRun();
  const attempts = [candidate];
  // Try one cycle back, then two cycles back, as fallback
  for (let i = 1; i <= 2; i++) {
    const earlier = new Date(candidate.runDate.getTime() - i * 6 * 60 * 60 * 1000);
    const yyyymmdd = earlier.toISOString().slice(0, 10).replace(/-/g, '');
    const hh = String(earlier.getUTCHours()).padStart(2, '0');
    attempts.push({ yyyymmdd, hh, runDate: earlier });
  }

  for (const a of attempts) {
    const url = buildSubsetUrl(a.yyyymmdd, a.hh);
    console.log(`  Trying GFS run ${a.yyyymmdd} ${a.hh}Z`);
    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': 'a-living-earth-app/1.0 (wind update script)' }
      });
      if (!res.ok) {
        console.warn(`    ✗ HTTP ${res.status}`);
        continue;
      }
      const buf = Buffer.from(await res.arrayBuffer());
      // Basic sanity check — GRIB2 files start with "GRIB" magic bytes
      if (buf.length < 100 || buf.slice(0, 4).toString() !== 'GRIB') {
        console.warn(`    ✗ Response did not look like GRIB2 (${buf.length} bytes)`);
        continue;
      }
      console.log(`    ✓ Got ${buf.length} bytes`);
      return { buffer: buf, run: a };
    } catch (err) {
      console.warn(`    ✗ Fetch error: ${err.message}`);
    }
  }
  throw new Error('Could not fetch any of the recent GFS runs');
}

// ---------------------------------------------------------------
// Convert GRIB2 buffer → CSV via wgrib2
// CSV format: "time","level","var","longitude","latitude","value"
// ---------------------------------------------------------------
async function gribToCsv(gribBuf) {
  const tmpGrib = resolve(tmpdir(), `gfs-${Date.now()}.grb2`);
  const tmpCsv  = resolve(tmpdir(), `gfs-${Date.now()}.csv`);
  await writeFile(tmpGrib, gribBuf);
  try {
    // -csv writes headerless rows: time,level,var,lon,lat,value
    await execFileP('wgrib2', [tmpGrib, '-csv', tmpCsv]);
    const csv = await readFile(tmpCsv, 'utf8');
    return csv;
  } finally {
    await rm(tmpGrib, { force: true });
    await rm(tmpCsv,  { force: true });
  }
}

// ---------------------------------------------------------------
// Parse the wgrib2 CSV and build U / V grids at 1° resolution
//
// wgrib2 -csv emits rows like:
//   "2026-05-05 12:00:00","2026-05-05 12:00:00","UGRD","10 m above ground",
//     0.000000,90.000000,3.45
//
// Columns:    refTime,validTime,var,level,lon,lat,value
//
// Longitude in NOMADS subsets is 0..360. We map to -180..180 here.
// ---------------------------------------------------------------
function parseCsvToGrid(csv) {
  const u = new Float32Array(GRID_LONS * GRID_LATS);
  const v = new Float32Array(GRID_LONS * GRID_LATS);
  // Counters for averaging (we collect 0.25° samples → 1° cells)
  const uCount = new Int16Array(GRID_LONS * GRID_LATS);
  const vCount = new Int16Array(GRID_LONS * GRID_LATS);

  const lines = csv.split('\n');
  for (const line of lines) {
    if (!line) continue;
    // Simple CSV split: wgrib2 quotes string fields, numeric fields are bare
    // Fields: "ref","valid","var","level",lon,lat,value
    const parts = line.split(',');
    if (parts.length < 7) continue;
    const variable = parts[2].replace(/"/g, '');
    const lonRaw = parseFloat(parts[4]);
    const lat = parseFloat(parts[5]);
    const value = parseFloat(parts[6]);
    if (Number.isNaN(lonRaw) || Number.isNaN(lat) || Number.isNaN(value)) continue;
    if (variable !== 'UGRD' && variable !== 'VGRD') continue;

    // Map 0..360 → -180..180
    const lon = lonRaw > 180 ? lonRaw - 360 : lonRaw;

    // Quantize to 1° grid index
    const i = Math.round(lon + 180) % GRID_LONS;
    const j = Math.round(90 - lat);
    if (i < 0 || i >= GRID_LONS || j < 0 || j >= GRID_LATS) continue;
    const idx = j * GRID_LONS + i;

    if (variable === 'UGRD') {
      u[idx] += value;
      uCount[idx]++;
    } else {
      v[idx] += value;
      vCount[idx]++;
    }
  }

  // Average accumulated samples per cell
  for (let k = 0; k < u.length; k++) {
    if (uCount[k] > 0) u[k] /= uCount[k];
    if (vCount[k] > 0) v[k] /= vCount[k];
  }

  return { u, v };
}

// ---------------------------------------------------------------
// Encode grid as compact JSON
// We round to 0.1 m/s precision (plenty for visualization) and store as
// flat arrays. Final size is ~250KB.
// ---------------------------------------------------------------
function encodeJson(grid, run) {
  const round1 = x => Math.round(x * 10) / 10;
  const u = Array.from(grid.u, round1);
  const v = Array.from(grid.v, round1);
  return {
    schemaVersion: 1,
    source: 'NOAA GFS',
    level: '10m above ground',
    run: {
      date: run.yyyymmdd,
      cycle: run.hh + 'Z',
      isoDate: run.runDate.toISOString()
    },
    grid: {
      lons: GRID_LONS,
      lats: GRID_LATS,
      resolutionDeg: TARGET_RES_DEG,
      lonStart: -180,
      latStart: 90,
      latStep: -1
    },
    generatedAt: new Date().toISOString(),
    u,
    v
  };
}

// ---------------------------------------------------------------
// Main
// ---------------------------------------------------------------
async function main() {
  console.log(`\n[update-wind] ${new Date().toISOString()}\n`);

  console.log('Fetching GFS GRIB2 subset...');
  const { buffer, run } = await fetchGribWithFallback();

  console.log('\nConverting GRIB2 → CSV via wgrib2...');
  const csv = await gribToCsv(buffer);
  const csvLines = csv.split('\n').filter(Boolean).length;
  console.log(`  ✓ ${csvLines} CSV rows`);

  console.log('\nBuilding 1° wind grid...');
  const grid = parseCsvToGrid(csv);

  // Sanity check: count non-zero cells
  let nonZero = 0;
  for (let k = 0; k < grid.u.length; k++) {
    if (grid.u[k] !== 0 || grid.v[k] !== 0) nonZero++;
  }
  const total = GRID_LONS * GRID_LATS;
  console.log(`  ✓ ${nonZero}/${total} cells populated (${(nonZero/total*100).toFixed(1)}%)`);
  if (nonZero < total * 0.95) {
    throw new Error('Too many empty cells — likely parsing failed');
  }

  // Quick statistics
  let maxSpeed = 0, sumSpeed = 0;
  for (let k = 0; k < grid.u.length; k++) {
    const sp = Math.sqrt(grid.u[k]**2 + grid.v[k]**2);
    if (sp > maxSpeed) maxSpeed = sp;
    sumSpeed += sp;
  }
  console.log(`  ✓ Max wind speed: ${maxSpeed.toFixed(1)} m/s`);
  console.log(`  ✓ Mean wind speed: ${(sumSpeed/total).toFixed(1)} m/s`);

  console.log('\nEncoding JSON...');
  const json = encodeJson(grid, run);
  const out = JSON.stringify(json);
  console.log(`  ✓ ${(out.length/1024).toFixed(1)} KB`);

  await mkdir(dirname(OUTPUT_PATH), { recursive: true });
  await writeFile(OUTPUT_PATH, out);
  console.log(`\n✓ Wrote ${OUTPUT_PATH}\n`);
}

main().catch(err => {
  console.error('\nUpdate failed:', err.message);
  process.exit(1);
});
