#!/usr/bin/env node
// ---------------------------------------------------------------
// scripts/update-wind.mjs
//
// Fetches global wind data from Open-Meteo and writes a downsampled JSON
// wind grid for the frontend. Open-Meteo aggregates several weather
// models (GFS, ECMWF IFS, ICON, etc.) and serves the best forecast
// available — better than raw GFS since it picks the highest-quality
// model for each region.
//
// Why Open-Meteo instead of NOAA GFS directly?
//   - No GRIB2 parsing (no wgrib2/eccodes/system tools)
//   - No NOMADS URL fragility
//   - Free tier, no API key, generous rate limits
//   - Aggregates multiple models for global best-quality
//
// Output: public/data/wind.json (~250KB)
//
// Run locally:
//   node scripts/update-wind.mjs
// ---------------------------------------------------------------

import { writeFile, mkdir } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_PATH = resolve(__dirname, '..', 'public', 'data', 'wind.json');

// ---------------------------------------------------------------
// Grid configuration
//
// 2° resolution: 180 lons × 91 lats = 16,380 cells. Plenty of detail
// for visualization at globe scale (jet streams span ~10° of latitude).
// Fetched in batches of 100 lat/lon pairs per request.
// ---------------------------------------------------------------

const RES_DEG = 2.0;
const GRID_LONS = Math.round(360 / RES_DEG);  // 180
const GRID_LATS = Math.round(180 / RES_DEG) + 1; // 91 (covers -90 to +90 inclusive)
const BATCH_SIZE = 100;
const API_BASE = 'https://api.open-meteo.com/v1/forecast';

// Build the list of all (lat, lon) sample points
function buildSamplePoints() {
  const points = [];
  for (let j = 0; j < GRID_LATS; j++) {
    const lat = 90 - j * RES_DEG;
    for (let i = 0; i < GRID_LONS; i++) {
      const lon = -180 + i * RES_DEG;
      points.push({ lat, lon, i, j });
    }
  }
  return points;
}

// ---------------------------------------------------------------
// Fetch one batch of points from Open-Meteo
// Returns one row per location with U/V wind components in m/s.
// ---------------------------------------------------------------
async function fetchBatch(batch) {
  const lats = batch.map(p => p.lat.toFixed(2)).join(',');
  const lons = batch.map(p => p.lon.toFixed(2)).join(',');
  const params = new URLSearchParams({
    latitude: lats,
    longitude: lons,
    // Wind speed and direction at 10m, current observation only
    current: 'wind_speed_10m,wind_direction_10m',
    wind_speed_unit: 'ms',
    timezone: 'UTC'
  });
  const url = `${API_BASE}?${params.toString()}`;
  const res = await fetch(url, {
    headers: { 'User-Agent': 'a-living-earth-app/1.0 (wind update)' }
  });
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`HTTP ${res.status}: ${body.slice(0, 200)}`);
  }
  const data = await res.json();
  // Open-Meteo returns either a single object (one location) or an array
  // (multiple locations). Normalize to array.
  return Array.isArray(data) ? data : [data];
}

// Convert speed + direction → U/V components.
// Direction in meteorological convention: 0° = wind FROM north (blowing south),
// 90° = wind FROM east (blowing west), 180° = FROM south (blowing north),
// 270° = FROM west (blowing east). To get the vector OF motion:
//   U (east-west, +east) = -speed * sin(direction)
//   V (north-south, +north) = -speed * cos(direction)
function speedDirToUV(speed, direction) {
  const rad = direction * Math.PI / 180;
  const u = -speed * Math.sin(rad);
  const v = -speed * Math.cos(rad);
  return [u, v];
}

// ---------------------------------------------------------------
// Main
// ---------------------------------------------------------------
async function main() {
  console.log(`\n[update-wind] ${new Date().toISOString()}\n`);
  console.log(`Grid: ${GRID_LONS}×${GRID_LATS} at ${RES_DEG}°`);

  const points = buildSamplePoints();
  console.log(`Total sample points: ${points.length}`);

  const u = new Float32Array(GRID_LONS * GRID_LATS);
  const v = new Float32Array(GRID_LONS * GRID_LATS);

  let fetched = 0;
  let failed = 0;
  let runDate = null;

  // Process in batches with brief pauses between to be polite to the API
  for (let start = 0; start < points.length; start += BATCH_SIZE) {
    const batch = points.slice(start, start + BATCH_SIZE);
    let attempt = 0;
    let success = false;
    while (attempt < 3 && !success) {
      attempt++;
      try {
        const results = await fetchBatch(batch);
        if (results.length !== batch.length) {
          throw new Error(`Expected ${batch.length} results, got ${results.length}`);
        }
        for (let k = 0; k < batch.length; k++) {
          const pt = batch[k];
          const r = results[k];
          if (!r.current ||
              typeof r.current.wind_speed_10m !== 'number' ||
              typeof r.current.wind_direction_10m !== 'number') {
            // Some grid points (poles, etc) sometimes return no data
            continue;
          }
          const [uVal, vVal] = speedDirToUV(
            r.current.wind_speed_10m,
            r.current.wind_direction_10m
          );
          const idx = pt.j * GRID_LONS + pt.i;
          u[idx] = uVal;
          v[idx] = vVal;
          if (!runDate && r.current.time) runDate = r.current.time;
          fetched++;
        }
        success = true;
      } catch (err) {
        console.warn(`  Batch ${start} attempt ${attempt} failed: ${err.message}`);
        if (attempt < 3) {
          // Brief backoff before retry
          await new Promise(r => setTimeout(r, 1500 * attempt));
        }
      }
    }
    if (!success) failed += batch.length;

    // Brief pause between batches — be a polite API consumer
    await new Promise(r => setTimeout(r, 100));

    // Progress every 10 batches
    if ((start / BATCH_SIZE) % 10 === 0) {
      const pct = Math.round((start + batch.length) / points.length * 100);
      process.stdout.write(`  ${pct}% (${fetched} fetched, ${failed} failed)\r`);
    }
  }
  process.stdout.write('\n');

  const total = GRID_LONS * GRID_LATS;
  console.log(`\nResults:`);
  console.log(`  Fetched:  ${fetched}/${total} (${(fetched/total*100).toFixed(1)}%)`);
  console.log(`  Failed:   ${failed}`);

  if (fetched < total * 0.85) {
    throw new Error(`Too few cells populated (${fetched}/${total}); aborting`);
  }

  // Quick statistics
  let maxSpeed = 0, sumSpeed = 0;
  for (let k = 0; k < u.length; k++) {
    const sp = Math.sqrt(u[k]**2 + v[k]**2);
    if (sp > maxSpeed) maxSpeed = sp;
    sumSpeed += sp;
  }
  console.log(`  Max wind: ${maxSpeed.toFixed(1)} m/s`);
  console.log(`  Mean wind: ${(sumSpeed/total).toFixed(1)} m/s`);

  // Encode JSON, rounded to 0.1 m/s precision (small file size)
  const round1 = x => Math.round(x * 10) / 10;
  const json = {
    schemaVersion: 1,
    source: 'Open-Meteo',
    level: '10m above ground',
    run: {
      date: runDate ? runDate.slice(0, 10).replace(/-/g, '') : null,
      cycle: runDate ? runDate.slice(11, 13) + 'Z' : null,
      isoDate: runDate || null
    },
    grid: {
      lons: GRID_LONS,
      lats: GRID_LATS,
      resolutionDeg: RES_DEG,
      lonStart: -180,
      latStart: 90,
      latStep: -RES_DEG
    },
    generatedAt: new Date().toISOString(),
    u: Array.from(u, round1),
    v: Array.from(v, round1)
  };
  const out = JSON.stringify(json);
  console.log(`  JSON size: ${(out.length/1024).toFixed(1)} KB`);

  await mkdir(dirname(OUTPUT_PATH), { recursive: true });
  await writeFile(OUTPUT_PATH, out);
  console.log(`\n✓ Wrote ${OUTPUT_PATH}\n`);
}

main().catch(err => {
  console.error('\nUpdate failed:', err.message);
  process.exit(1);
});
